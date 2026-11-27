"""Off-market homes: page every home Realtor's search knows that is not listed now, per county, in idle time.

python -m jobs.offmarket --fips 48171,48259 [--inflight 2] [--stop-file PATH] [--redo] [--dry-run]
python -m jobs.offmarket --state TX            (every county of the state not done yet)

What the search gives (probed 2026-11-24): status `sold` = every home sold in about the last 10 years (sold_date
from ~2016), status `off_market` = homes delisted without a sale (small). Nothing older or never-sold is
searchable; Redfin has no off-market search at all. Each status is split by sold date (sold) until every part is
under the 10k offset cap, then paged in full, so nothing is capped.

Rows go to offmarket_homes (hdd tablespace), one per property_key (location + unit, same key as listings), status
off_market. Not in `listings`, so the website sync does not see them. Per-county cost goes to offmarket_progress.
Requests use SourceClient (global DNS/403/429 backoff, block circuit breaker). --stop-file ends it between pages.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from jobs.collect import load_counties  # noqa: E402
from matching.merge import key_for  # noqa: E402
from sources.http import BlockedError, SourceClient  # noqa: E402
from sources.realtor import ENDPOINT, HEADERS, MAX_RESULTS, PAGE_SIZE, RealtorSource, build_body, parse_search  # noqa: E402

STATUSES = ("sold", "off_market")
FIRST_DAY = date(2000, 1, 1)   # older than anything the search returns; bands with 0 homes cost one request
LAST_DAY = date(2099, 12, 31)  # fixed, so the same county splits into the same bands on a later day (resume)


class Stop(Exception):
    pass


PHOTO_PREFIX = "http://ap.rdcpix.com/"
EXTRA = "allp:photos(limit:200){href} estimate{estimate} "   # alias: plain photos{href} is already in fields()
HIST_BATCH = 50   # home() aliases per request (75+ fails with MAX_ROOT_FIELDS_LIMIT)
HOME_FIELDS = ("property_id property_history{date event_name price listing{list_price status}} "
               "tax_history{year tax assessment{total building land} market{total}}")


def body(location, status, offset, limit=PAGE_SIZE, dates=None):
    b = build_body(location, "sold", offset, limit=limit, dates=dates)
    if limit > 1:   # paging (not counting): every photo and the estimate in the same request
        b["query"] = b["query"].replace("results{", "results{" + EXTRA, 1)
    q = b["variables"]["query"]
    q["status"] = [status]
    q.pop("sold_date", None)
    if dates:
        q["sold_date"] = {"min": dates[0], "max": dates[1]}
    return b


class County:
    def __init__(self, county, client: SourceClient, inflight: int, stop_file: str | None, done_bands=()):
        self.c, self.client, self.inflight, self.stop_file = county, client, inflight, stop_file
        self.done_bands = set(done_bands)
        self.src = RealtorSource(client)
        self.bands = 0

    def check_stop(self):
        if self.stop_file and os.path.exists(self.stop_file):
            raise Stop()

    def total(self, status, dates=None) -> int:
        return parse_search(self.src._fetch(body(self.c.realtor_location, status, 0, 1, dates)))[1]

    def split(self, status, lo: date, hi: date) -> list:
        """Date bands [lo, hi] (inclusive) with <= MAX_RESULTS homes each (empty bands dropped)."""
        self.check_stop()
        n = self.total(status, (lo.isoformat(), hi.isoformat()))
        if n == 0:
            return []
        if n <= MAX_RESULTS or hi <= lo:
            return [((lo.isoformat(), hi.isoformat()), n)]
        mid = lo + (hi - lo) // 2
        return self.split(status, lo, mid) + self.split(status, mid + timedelta(days=1), hi)

    def pages(self, status, dates, n):
        offsets = list(range(0, min(n, MAX_RESULTS), PAGE_SIZE))
        with ThreadPoolExecutor(self.inflight) as ex:
            for i in range(0, len(offsets), self.inflight):
                self.check_stop()
                chunk = offsets[i:i + self.inflight]
                for data in ex.map(lambda o: self.src._fetch(body(self.c.realtor_location, status, o, dates=dates)), chunk):
                    yield parse_search(data)[0]

    def run(self):
        """Yield (status, raw) for every home, and (None, band_key) after each band is fully paged.
        Bands in self.done_bands (saved by an earlier, stopped run) are skipped."""
        self.api_total, self.complete = 0, True
        for status in STATUSES:
            whole = self.total(status)
            self.api_total += whole
            if whole <= MAX_RESULTS:
                bands = [(None, whole)] if whole else []
            else:
                bands = self.split(status, FIRST_DAY, LAST_DAY)
                if sum(n for _, n in bands) < whole:   # homes without a sold date are not in any band
                    bands.insert(0, (None, min(whole, MAX_RESULTS)))
                    self.complete = False
            self.bands += len(bands)
            for dates, n in bands:
                key = f"{status}:{dates[0]}:{dates[1]}" if dates else f"{status}:all"
                if key in self.done_bands:
                    continue
                for results in self.pages(status, dates, n):
                    for r in results:
                        yield status, r
                yield None, key


UPSERT = """
INSERT INTO offmarket_homes (property_key, source_status, realtor_id, county_fips, state, address, city, zip, lat, lng,
  property_type, beds, baths, sqft, lot_sqft, year_built, last_sold_date, last_sold_price, list_price, photo_url,
  photos, estimate)
VALUES (%(property_key)s, %(source_status)s, %(realtor_id)s, %(county_fips)s, %(state)s, %(address)s, %(city)s, %(zip)s,
  %(lat)s, %(lng)s, %(property_type)s, %(beds)s, %(baths)s, %(sqft)s, %(lot_sqft)s, %(year_built)s, %(sold_date)s,
  %(sold_price)s, %(list_price)s, %(photo_url)s, %(photos)s, %(estimate)s)
ON CONFLICT (property_key) DO UPDATE SET last_seen = now(),
  source_status = EXCLUDED.source_status, realtor_id = EXCLUDED.realtor_id,
  last_sold_date = greatest(offmarket_homes.last_sold_date, EXCLUDED.last_sold_date),
  last_sold_price = CASE WHEN EXCLUDED.last_sold_date >= offmarket_homes.last_sold_date OR offmarket_homes.last_sold_date IS NULL
                         THEN coalesce(EXCLUDED.last_sold_price, offmarket_homes.last_sold_price) ELSE offmarket_homes.last_sold_price END,
  beds = coalesce(EXCLUDED.beds, offmarket_homes.beds), baths = coalesce(EXCLUDED.baths, offmarket_homes.baths),
  sqft = coalesce(EXCLUDED.sqft, offmarket_homes.sqft), year_built = coalesce(EXCLUDED.year_built, offmarket_homes.year_built),
  photo_url = coalesce(EXCLUDED.photo_url, offmarket_homes.photo_url),
  photos = coalesce(EXCLUDED.photos, offmarket_homes.photos), estimate = coalesce(EXCLUDED.estimate, offmarket_homes.estimate),
  details_at = CASE WHEN EXCLUDED.realtor_id IS DISTINCT FROM offmarket_homes.realtor_id THEN NULL ELSE offmarket_homes.details_at END
"""


def row(src: RealtorSource, county, status, raw) -> dict | None:
    rec = src.normalize(raw, "sold")
    k = key_for(rec)
    if not k:
        return None

    def i(v):
        return None if v is None else int(round(float(v)))
    return {"property_key": k, "source_status": status, "realtor_id": rec["source_id"], "county_fips": county.fips,
            "state": county.state, "address": rec["address"], "city": rec["city"], "zip": rec["zip"],
            "lat": rec["lat"], "lng": rec["lng"], "property_type": rec["property_type"], "beds": rec["beds"],
            "baths": rec["baths"], "sqft": i(rec["sqft"]) if rec["sqft"] and 0 < float(rec["sqft"]) < 2e9 else None, "lot_sqft": i(rec["lot_sqft"]),
            "year_built": i(rec["year_built"]) if rec["year_built"] and 1600 < float(rec["year_built"]) < 2100 else None,
            "sold_date": rec["sold_date"], "sold_price": i(rec["price"]), "list_price": i(rec["list_price"]),
            "photo_url": rec["photo_url"], "photos": photos(raw), "estimate": i((raw.get("estimate") or {}).get("estimate"))}


def photos(raw) -> list | None:
    out = [p["href"].removeprefix(PHOTO_PREFIX) for p in (raw.get("allp") or raw.get("photos") or []) if p and p.get("href")]
    return out or None


def _n(v):
    return None if v is None else int(round(float(v)))


def compact_history(h: dict) -> tuple[list, list]:
    ph = [[e.get("date"), e.get("event_name"), _n(e.get("price")), _n((e.get("listing") or {}).get("list_price")),
           (e.get("listing") or {}).get("status")] for e in (h.get("property_history") or []) if e]
    th = []
    for t in h.get("tax_history") or []:
        if not t:
            continue
        a, m = t.get("assessment") or {}, t.get("market") or {}
        th.append([t.get("year"), _n(t.get("tax")), _n(a.get("total")), _n(a.get("building")), _n(a.get("land")),
                   _n(m.get("total"))])
    for rows in (ph, th):   # drop trailing nulls per entry: smaller jsonb
        for r in rows:
            while r and r[-1] is None:
                r.pop()
    return ph, th


def history_county(conn, fips, inflight, stop_file, client=None) -> dict:
    """Second pass: price + tax history for homes of a county without details_at, HIST_BATCH homes per request.
    Resumes by details_at. Returns stats; stopped=True when the stop file appeared."""
    import json
    client = client or SourceClient("realtor", min_interval=float(os.environ.get("OFFMARKET_MIN_INTERVAL", 0.5)))
    t0, done, filled, stopped = time.time(), 0, 0, False
    req0 = client.stats["requests"]

    def fetch(ids):
        q = "query B{" + " ".join(f'h{i}:home(property_id:"{pid}"){{{HOME_FIELDS}}}' for i, pid in enumerate(ids)) + "}"
        for attempt in range(5):
            data = client.request_json("POST", ENDPOINT, headers=HEADERS,
                                       json={"operationName": "B", "query": q, "variables": {}})
            d = (data or {}).get("data")
            if d is not None:
                return ids, d
            client.note_error()
            if attempt == 4:
                raise RuntimeError(f"realtor history: {str((data or {}).get('errors'))[:200]}")
            client.sleep(client.backoff(attempt))

    with ThreadPoolExecutor(inflight) as ex:
        while True:
            if stop_file and os.path.exists(stop_file):
                stopped = True
                break
            ids = [r[0] for r in conn.execute(
                "SELECT DISTINCT realtor_id FROM offmarket_homes WHERE county_fips=%s AND details_at IS NULL "
                "AND realtor_id IS NOT NULL LIMIT %s", (fips, HIST_BATCH * inflight * 4))]
            if not ids:
                break
            chunks = [ids[i:i + HIST_BATCH] for i in range(0, len(ids), HIST_BATCH)]
            rows = []
            for i in range(0, len(chunks), inflight):
                if stop_file and os.path.exists(stop_file):
                    stopped = True
                    break
                for got, d in ex.map(fetch, chunks[i:i + inflight]):
                    for k, pid in enumerate(got):
                        h = d.get(f"h{k}") or {}
                        ph, th = compact_history(h)
                        filled += bool(ph or th)
                        rows.append((json.dumps(ph) if ph else None, json.dumps(th) if th else None, pid))
            if rows:
                with conn.cursor() as cur:
                    cur.executemany("UPDATE offmarket_homes SET price_history=%s::jsonb, tax_history=%s::jsonb, "
                                    "details_at=now() WHERE realtor_id=%s AND county_fips=%s",
                                    [r + (fips,) for r in rows])
                conn.commit()
                done += len(rows)
            if stopped:
                break
    if not stopped:
        conn.execute("UPDATE offmarket_progress SET history_at=now() WHERE county_fips=%s", (fips,))
        conn.commit()
    return {"fips": fips, "history_homes": done, "with_history": filled,
            "requests": client.stats["requests"] - req0, "seconds": round(time.time() - t0, 1), "stopped": stopped}


def do_county(conn, county, inflight, stop_file, dry_run=False) -> dict:
    client = SourceClient("realtor", min_interval=float(os.environ.get("OFFMARKET_MIN_INTERVAL", 0.5)))
    done_bands = []
    if not dry_run:   # resume: bands an earlier stopped run already saved
        row_ = conn.execute("SELECT done_bands FROM offmarket_progress WHERE county_fips=%s AND finished_at IS NULL",
                            (county.fips,)).fetchone()
        done_bands = list(row_[0]) if row_ else []
    job = County(county, client, inflight, stop_file, done_bands)
    t0, fetched, batch, keys = time.time(), 0, {}, set()
    if not dry_run:
        conn.execute("INSERT INTO offmarket_progress (county_fips, state, started_at) VALUES (%s,%s,now()) "
                     "ON CONFLICT (county_fips) DO UPDATE SET started_at=now(), finished_at=NULL, complete=NULL",
                     (county.fips, county.state))
        conn.commit()

    def flush():
        if batch and not dry_run:
            try:
                with conn.cursor() as cur:
                    cur.executemany(UPSERT, list(batch.values()))
                conn.commit()
            except Exception as e:   # one bad home must not stop the job: save the rest one by one
                conn.rollback()
                print(f"[offmarket] batch failed ({type(e).__name__}: {str(e)[:120]}); saving one by one", flush=True)
                for rr in batch.values():
                    try:
                        conn.execute(UPSERT, rr)
                        conn.commit()
                    except Exception as e1:
                        conn.rollback()
                        print(f"[offmarket] skipped bad home {rr.get('property_key')}: {str(e1)[:120]}", flush=True)
        batch.clear()
    stopped = False
    try:
        for status, raw in job.run():
            if status is None:   # a band is complete: save it and remember it
                flush()
                if not dry_run:
                    conn.execute("UPDATE offmarket_progress SET done_bands = array_append(done_bands, %s) "
                                 "WHERE county_fips=%s", (raw, county.fips))
                    conn.commit()
                continue
            fetched += 1
            r = row(job.src, county, status, raw)
            if r:
                keys.add(r["property_key"])
                old = batch.get(r["property_key"])   # same home twice in one batch: keep the newer sale
                if not old or (r["sold_date"] or "") >= (old["sold_date"] or ""):
                    batch[r["property_key"]] = r
            if len(batch) >= 500:
                flush()
        flush()
    except Stop:
        flush()
        stopped = True
    s = client.stats
    out = {"fips": county.fips, "name": county.name, "api_total": job.api_total, "fetched": fetched,
           "homes": len(keys), "requests": s["requests"], "blocked": s["blocked"], "errors": s["errors"],
           "seconds": round(time.time() - t0, 1), "bands": job.bands, "complete": job.complete and not stopped,
           "stopped": stopped}
    if not dry_run:
        conn.execute("""UPDATE offmarket_progress SET finished_at = CASE WHEN %(stopped)s THEN NULL ELSE now() END,
                        complete=%(complete)s, api_total=%(api_total)s, fetched=%(fetched)s, homes=%(homes)s,
                        requests=%(requests)s, blocked=%(blocked)s, errors=%(errors)s, seconds=%(seconds)s, bands=%(bands)s,
                        note = CASE WHEN %(stopped)s THEN 'stopped (pipeline busy)' END,
                        done_bands = CASE WHEN %(stopped)s THEN done_bands ELSE '{}' END WHERE county_fips=%(fips)s""", out)
        conn.commit()
    return out


def county_error(conn, fips, e) -> None:
    """Log a county that failed (not blocked) and record it, so the run moves on and the health check sees it."""
    import traceback
    print(f"[offmarket] ERROR county {fips}: {type(e).__name__}: {str(e)[:200]}", flush=True)
    traceback.print_exc()
    try:
        conn.rollback()
        conn.execute("UPDATE offmarket_progress SET note = %s WHERE county_fips = %s",
                     (f"error {time.strftime('%Y-%m-%d %H:%M')}: {type(e).__name__}: {str(e)[:150]}", fips))
        conn.commit()
    except Exception:
        conn.rollback()


def nvme_free_gb() -> float:
    st = os.statvfs("/")
    return st.f_bavail * st.f_frsize / 1e9


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fips", default=os.environ.get("OFFMARKET_FIPS", ""))
    ap.add_argument("--state", default=os.environ.get("OFFMARKET_STATES", ""))
    ap.add_argument("--inflight", type=int, default=int(os.environ.get("OFFMARKET_INFLIGHT", 2)))
    ap.add_argument("--stop-file")
    ap.add_argument("--redo", action="store_true", help="also counties already done")
    ap.add_argument("--min-free-gb", type=float, default=float(os.environ.get("OFFMARKET_MIN_FREE_GB", 80)))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--history-only", action="store_true", help="only the price/tax history pass")
    ap.add_argument("--no-history", action="store_true", help="only the search pass")
    ap.add_argument("--search-first", action="store_true", default=os.environ.get("OFFMARKET_SEARCH_FIRST") == "1",
                    help="search every county first (most homes per hour), price/tax history afterwards")
    ap.add_argument("--biggest-first", action="store_true", default=os.environ.get("OFFMARKET_BIGGEST_FIRST") == "1",
                    help="order counties by size (most active listings in the last 14 days) instead of state order")
    a = ap.parse_args(argv)
    fips = {f.strip() for f in a.fips.split(",") if f.strip()}
    states = [s.strip() for s in a.state.split(",") if s.strip()] or sorted({f"{_state_of(f)}" for f in fips})
    counties = load_counties(states, fips or None)
    counties = [c for c in counties if c.realtor_location]
    if not counties:
        print("[offmarket] nothing to do (set --fips or --state)")
        return 0
    from db.migrate import connect
    with connect() as conn:
        if a.biggest_first:
            counties = by_size(conn, counties)
        done = set() if a.redo else {r[0] for r in conn.execute(
            "SELECT county_fips FROM offmarket_progress WHERE finished_at IS NOT NULL")}
        for c in counties:
            if c.fips in done:
                continue
            if nvme_free_gb() < a.min_free_gb:
                print(f"[offmarket] NVMe free {nvme_free_gb():.0f} GB < {a.min_free_gb:.0f} GB: stop", flush=True)
                return 3
            if a.stop_file and os.path.exists(a.stop_file):
                return 0
            try:
                if not a.history_only:
                    out = do_county(conn, c, a.inflight, a.stop_file, a.dry_run)
                    print(f"[offmarket] {out}", flush=True)
                    if out["stopped"]:
                        return 0
                if not a.dry_run and not a.no_history and not a.search_first:
                    out = history_county(conn, c.fips, a.inflight, a.stop_file)
                    print(f"[offmarket] history {out}", flush=True)
            except BlockedError:
                raise   # blocked: stop everything, the next idle run tries again
            except Exception as e:   # anything else: note it, skip this county, keep going
                county_error(conn, c.fips, e)
                continue
            if not a.dry_run and not a.no_history and not a.search_first and out["stopped"]:   # history stopped (pipeline busy)
                return 0
        if a.dry_run or a.no_history:
            return 0
        # counties searched before history existed (or with homes still missing it): history only
        wanted = {c.fips for c in counties}
        for (f,) in conn.execute("SELECT county_fips FROM offmarket_progress WHERE finished_at IS NOT NULL "
                                 "AND history_at IS NULL ORDER BY finished_at").fetchall():
            if f not in wanted:
                continue
            if a.stop_file and os.path.exists(a.stop_file):
                return 0
            try:
                out = history_county(conn, f, a.inflight, a.stop_file)
            except BlockedError:
                raise
            except Exception as e:
                county_error(conn, f, e)
                continue
            print(f"[offmarket] history {out}", flush=True)
            if out["stopped"]:
                return 0
    return 0


def by_size(conn, counties: list) -> list:
    """Biggest counties first: most active listings in any run of the last 14 days (unknown = last)."""
    try:
        conn.execute("SET statement_timeout = '60s'")
        size = dict(conn.execute("SELECT county_fips, max(active) FROM run_county_stats "
                                 "WHERE loaded_at > now() - interval '14 days' GROUP BY 1").fetchall())
        conn.execute("SET statement_timeout = 0")
        conn.commit()
    except Exception as e:   # sizes are only an ordering hint
        conn.rollback()
        print(f"[offmarket] county sizes unavailable ({str(e)[:120]}); state order", flush=True)
        return counties
    return sorted(counties, key=lambda c: -(size.get(c.fips) or 0))


def _state_of(fips: str) -> str:
    for p in (ROOT / "states").glob("*.json"):
        if f'"fips": "{fips}"' in p.read_text():
            return p.stem.upper()
    raise SystemExit(f"unknown fips {fips}")


if __name__ == "__main__":
    sys.exit(main())
