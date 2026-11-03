"""Redfin-only: find the Realtor property id of listings only Redfin has, so the details step reads their page too.

python -m jobs.redfin_only [--state XX] [--limit N] [--workers N] [--rate N]

Realtor has a page for nearly every address, also homes it has no listing of. Each address is looked up in
Realtor's address search, and the property found is kept only when it is the same address (same property_key:
zip, house number, street and unit). Rentals are looked up by their building's address once for all units.
Addresses without a house number (lots, builder plans) are left out.
  realtor_address_lookups   one row per address looked up (found or 'not found'); a found address is never looked
                            up again, a 'not found' one after REDFIN_ONLY_RETRY_DAYS
  listing_realtor_ids       the listing's Realtor property id (found_by 'address'); jobs.fetch_details reads these
Pace: REDFIN_ONLY_WORKERS at once, at most REDFIN_ONLY_RATE a second (jobs/settings.py).
"""
from __future__ import annotations

import argparse
import json
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
from matching.address import property_key  # noqa: E402
from sources.http import browser_headers  # noqa: E402

URL = "https://parser-external.geo.moveaws.com/suggest"
HEADERS = {"accept": "application/json", "origin": "https://www.realtor.com", "referer": "https://www.realtor.com/"}
ATTEMPTS = 4

# Redfin-only active listings with a house number, whose address is not looked up yet (or not found long enough ago)
SELECT = """
SELECT l.id, l.address, l.unit, l.city, l.state, l.zip
FROM listings l
LEFT JOIN listing_realtor_ids r ON r.listing_id = l.id
LEFT JOIN realtor_address_lookups a ON a.address = concat_ws(', ', l.address, l.city, l.state || ' ' || l.zip)
WHERE l.active AND l.sources = '{redfin}' AND r.listing_id IS NULL
  AND l.address ~ '^\\d' AND l.address !~* '\\splan$'
  AND (%(state)s::text IS NULL OR l.state = %(state)s)
  AND (a.address IS NULL OR (a.realtor_property_id IS NULL
       AND a.looked_up_at < now() - make_interval(days => %(retry_days)s)))
ORDER BY l.listing_type DESC, l.id
LIMIT %(limit)s
"""


def full_address(address: str, city: str, state: str, zip_code: str) -> str:
    return f"{address}, {city}, {state} {zip_code}"


def building(address: str) -> str:
    """'12 Main St Unit 4' -> '12 Main St': rentals are looked up by their building once."""
    return re.split(r"\s+(?:#|apt\b|unit\b|ste\b|suite\b)", address, maxsplit=1, flags=re.I)[0]


class RedfinOnlyJob:
    def __init__(self, conn, pace: Pace, log=print):
        self.conn, self.pace, self.log = conn, pace, log
        self.session = requests.Session()
        self.stats = {"found": 0, "not_found": 0, "errors": 0, "reused": 0}
        self.found: dict[str, tuple] = {}  # address -> (id, realtor address); units of one building share it

    def lookup(self, address: str, key: str | None) -> tuple[str | None, str | None, str | None]:
        params = {"input": address, "client_id": "rdc-home", "limit": 5, "area_types": "address"}
        err = None
        for attempt in range(ATTEMPTS):
            self.pace.wait()
            try:
                r = self.session.get(URL, params=params, headers={**browser_headers(), **HEADERS}, timeout=30)
                if r.status_code == 200:
                    for c in r.json().get("autocomplete") or []:
                        if c.get("area_type") != "address" or not c.get("mpr_id"):
                            continue
                        theirs = property_key(c.get("line"), c.get("postal_code"))
                        if key and theirs and theirs.rsplit("|", 1)[0] == key.rsplit("|", 1)[0]:
                            return str(c["mpr_id"]), full_address(c.get("line", ""), c.get("city", ""),
                                                                  c.get("state_code", ""), c.get("postal_code", "")), None
                    return None, None, "not found"
                err = f"HTTP {r.status_code}"
                if r.status_code in (403, 429):
                    self.pace.refused(r.status_code)
                    continue
            except (requests.RequestException, ValueError) as e:
                err = type(e).__name__
            time.sleep(2 * (attempt + 1))
        return None, None, err

    def one(self, row) -> None:
        lid, address, unit, city, state, zip_code = row
        street = building(address)
        search = full_address(street, city or "", state or "", zip_code or "")
        if search in self.found:
            self.stats["reused"] += 1
            pid, theirs, err = *self.found[search], None
        else:
            pid, theirs, err = self.lookup(search, property_key(street, zip_code))
            if pid:
                self.found[search] = (pid, theirs)
            if err and err != "not found":
                self.stats["errors"] += 1
                return  # a failed request is not recorded: tried again next run
            self.conn.execute(
                """INSERT INTO realtor_address_lookups (address, realtor_property_id, realtor_address, error)
                   VALUES (%s, %s, %s, %s) ON CONFLICT (address) DO UPDATE SET realtor_property_id =
                   EXCLUDED.realtor_property_id, realtor_address = EXCLUDED.realtor_address,
                   error = EXCLUDED.error, looked_up_at = now()""", (search, pid, theirs, err))
        if pid:
            self.stats["found"] += 1
            self.conn.execute("""INSERT INTO listing_realtor_ids (listing_id, realtor_property_id) VALUES (%s, %s)
                                 ON CONFLICT (listing_id) DO NOTHING""", (lid, pid))
        else:
            self.stats["not_found"] += 1

    def run(self, rows: list, workers: int) -> dict:
        t0 = time.time()
        with ThreadPoolExecutor(workers, thread_name_prefix="redfin_only") as pool:
            for i, _ in enumerate(pool.map(self.one, rows), 1):
                if i % 500 == 0 or i == len(rows):
                    self.log(f"[redfin_only] {i}/{len(rows)} listings, {time.time() - t0:.0f}s: {json.dumps(self.stats)}")
        return {"listings": len(rows), "seconds": round(time.time() - t0, 1), **self.stats,
                "refusals": self.pace.refusals}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--state")
    ap.add_argument("--limit", type=int, default=100_000)
    ap.add_argument("--workers", type=int, default=settings.REDFIN_ONLY_WORKERS)
    ap.add_argument("--rate", type=float, default=settings.REDFIN_ONLY_RATE)
    a = ap.parse_args(argv)
    from db.migrate import connect
    with connect() as conn:
        conn.autocommit = True
        rows = conn.execute(SELECT, {"state": a.state and a.state.upper(), "limit": a.limit,
                                     "retry_days": settings.REDFIN_ONLY_RETRY_DAYS}).fetchall()
        print(f"[redfin_only] {len(rows)} listings to look up (state={a.state or 'all'}, workers={a.workers})",
              flush=True)
        res = RedfinOnlyJob(conn, Pace(a.rate, 120, "redfin_only"), log=lambda m: print(m, flush=True)).run(
            rows, a.workers)
    print(json.dumps(res), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
