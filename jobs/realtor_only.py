"""Realtor-only: find the Redfin property id of listings only Realtor has, so the details step reads their Redfin page.

python -m jobs.realtor_only [--state XX] [--limit N] [--workers N] [--rate N] [--records-only]

The mirror of jobs.redfin_only, in two passes:
  1. records  no requests: the same address (property_key) in our own Redfin records of any listing type, e.g. a
              rental only Realtor lists whose sale Redfin had                              found_by 'record'
  2. address  Redfin's address search; the property is kept only when its address has the same property_key
              (zip, house number, street; the unit is not compared, rentals are looked up by building) found_by 'address'
              Redfin refuses this search from some addresses (CloudFront 403): after REFUSALS_TO_STOP refusals the
              pass stops for this run; REDFIN_LOOKUP_PROXY sends it through a proxy instead.
  realtor_only results: listing_redfin_ids; one row per address searched: redfin_address_lookups.
Pace: REDFIN_ONLY_WORKERS at once, at most REDFIN_ONLY_RATE a second (jobs/settings.py), shared with redfin_only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from jobs import settings  # noqa: E402
from jobs.pace import Pace  # noqa: E402
from jobs.redfin_only import building, full_address  # noqa: E402
from matching.address import property_key  # noqa: E402
from sources.http import browser_headers  # noqa: E402

URL = "https://www.redfin.com/stingray/do/location-autocomplete"
HEADERS = {"accept": "*/*", "referer": "https://www.redfin.com/", "sec-fetch-site": "same-origin"}
ATTEMPTS = 3
REFUSALS_TO_STOP = 20

# pass 1: Realtor-only listings whose address Redfin gave us under any listing type
MATCH_RECORDS = """
INSERT INTO listing_redfin_ids (listing_id, redfin_property_id, found_by)
SELECT DISTINCT ON (l.id) l.id, s.source_id, 'record'
FROM listings l
JOIN source_records s ON s.source = 'redfin' AND s.property_key = l.property_key
WHERE l.active AND l.sources = '{realtor}' AND (%(state)s::text IS NULL OR l.state = %(state)s)
ORDER BY l.id, s.last_seen DESC
ON CONFLICT (listing_id) DO NOTHING
"""
# pass 2: the rest, with a house number, not searched yet (or not found long enough ago)
SELECT = """
SELECT l.id, l.address, l.city, l.state, l.zip
FROM listings l
LEFT JOIN listing_redfin_ids r ON r.listing_id = l.id
LEFT JOIN redfin_address_lookups a ON a.address = concat_ws(', ', l.address, l.city, l.state || ' ' || l.zip)
WHERE l.active AND l.sources = '{realtor}' AND r.listing_id IS NULL
  AND l.address ~ '^\\d' AND (%(state)s::text IS NULL OR l.state = %(state)s)
  AND (a.address IS NULL OR (a.redfin_property_id IS NULL
       AND a.looked_up_at < now() - make_interval(days => %(retry_days)s)))
ORDER BY l.listing_type DESC, l.id
LIMIT %(limit)s
"""
HOME_URL = re.compile(r"/home/(\d+)")


def parse(text: str) -> list[dict]:
    """Redfin's answer is '{}&&{json}': the exact match first, then every address row of the sections."""
    data = json.loads(text[text.index("{", 2):] if text.startswith("{}&&") else text)
    p = data.get("payload") or {}
    rows = [p["exactMatch"]] if p.get("exactMatch") else []
    rows += [r for s in p.get("sections") or [] for r in s.get("rows") or []]
    return [r for r in rows if str(r.get("type")) == "1" and HOME_URL.search(r.get("url") or "")]


class RealtorOnlyJob:
    def __init__(self, conn, pace: Pace, log=print):
        self.conn, self.pace, self.log = conn, pace, log
        self.session = requests.Session()
        proxy = os.environ.get("REDFIN_LOOKUP_PROXY")
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
        self.stats = {"found": 0, "not_found": 0, "errors": 0}
        self.stopped = False

    def lookup(self, search: str, key: str | None) -> tuple[str | None, str | None, str | None]:
        err = None
        for attempt in range(ATTEMPTS):
            if self.stopped:
                return None, None, "stopped"
            self.pace.wait()
            try:
                r = self.session.get(URL, params={"location": search, "v": 2}, timeout=30,
                                     headers={**browser_headers(), **HEADERS})
                if r.status_code == 200:
                    for row in parse(r.text):
                        zip_code = (re.search(r"-(\d{5})(?:/|$)", row["url"].split("/home/")[0]) or [None, None])[1]
                        theirs = property_key(row.get("name"), zip_code)
                        if key and theirs and theirs.rsplit("|", 1)[0] == key.rsplit("|", 1)[0]:
                            return HOME_URL.search(row["url"])[1], f"{row.get('name')}, {row.get('subName')}", None
                    return None, None, "not found"
                err = f"HTTP {r.status_code}"
                if r.status_code in (403, 429):
                    self.pace.refused(r.status_code)
                    if self.pace.refusals >= REFUSALS_TO_STOP and not self.stopped:
                        self.stopped = True
                        self.log(f"[realtor_only] Redfin refused the address search {self.pace.refusals} times; "
                                 "stopping the address pass for this run (set REDFIN_LOOKUP_PROXY to go through a proxy)")
                    continue
            except (requests.RequestException, ValueError) as e:
                err = type(e).__name__
            time.sleep(2 * (attempt + 1))
        return None, None, err

    def one(self, row) -> None:
        lid, address, city, state, zip_code = row
        street = building(address)
        search = full_address(street, city or "", state or "", zip_code or "")
        pid, theirs, err = self.lookup(search, property_key(street, zip_code))
        if err and err != "not found":
            self.stats["errors"] += 1
            return  # a failed request is not recorded: tried again next run
        self.conn.execute(
            """INSERT INTO redfin_address_lookups (address, redfin_property_id, redfin_address, error)
               VALUES (%s, %s, %s, %s) ON CONFLICT (address) DO UPDATE SET redfin_property_id =
               EXCLUDED.redfin_property_id, redfin_address = EXCLUDED.redfin_address,
               error = EXCLUDED.error, looked_up_at = now()""", (search, pid, theirs, err))
        if pid:
            self.stats["found"] += 1
            self.conn.execute("""INSERT INTO listing_redfin_ids (listing_id, redfin_property_id, found_by)
                                 VALUES (%s, %s, 'address') ON CONFLICT (listing_id) DO NOTHING""", (lid, pid))
        else:
            self.stats["not_found"] += 1

    def run(self, rows: list, workers: int) -> dict:
        t0 = time.time()
        with ThreadPoolExecutor(workers, thread_name_prefix="realtor_only") as pool:
            for i, _ in enumerate(pool.map(self.one, rows), 1):
                if i % 500 == 0 or i == len(rows):
                    self.log(f"[realtor_only] {i}/{len(rows)} listings, {time.time() - t0:.0f}s: {json.dumps(self.stats)}")
        return {"listings": len(rows), "seconds": round(time.time() - t0, 1), **self.stats,
                "refusals": self.pace.refusals, "stopped": self.stopped}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--state")
    ap.add_argument("--limit", type=int, default=100_000)
    ap.add_argument("--workers", type=int, default=settings.REDFIN_ONLY_WORKERS)
    ap.add_argument("--rate", type=float, default=settings.REDFIN_ONLY_RATE)
    ap.add_argument("--records-only", action="store_true", help="only pass 1 (our own records, no requests)")
    a = ap.parse_args(argv)
    from db.migrate import connect
    state = a.state and a.state.upper()
    with connect() as conn:
        conn.autocommit = True
        t0 = time.time()
        n = conn.execute(MATCH_RECORDS, {"state": state}).rowcount
        print(f"[realtor_only] records: {n} listings matched to our own Redfin records ({time.time() - t0:.0f}s)",
              flush=True)
        if a.records_only:
            return 0
        rows = conn.execute(SELECT, {"state": state, "limit": a.limit,
                                     "retry_days": settings.REDFIN_ONLY_RETRY_DAYS}).fetchall()
        print(f"[realtor_only] address: {len(rows)} listings to look up (state={a.state or 'all'}, "
              f"workers={a.workers})", flush=True)
        res = RealtorOnlyJob(conn, Pace(a.rate, 120, "realtor_only"), log=lambda m: print(m, flush=True)).run(
            rows, a.workers)
    print(json.dumps({"records": n, **res}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
