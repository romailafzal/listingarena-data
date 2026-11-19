"""Missing check: re-fetch homes one by one before calling them gone (docs/all-us-homes-design-2026-11-19.md s.5).

python -m jobs.missing_check [--days 7] [--limit N] [--inflight 8] [--deadline SECONDS] [--stop-file PATH]
                             [--removed] [--ensure-index] [--dry-run]

A listing absent from a complete county fetch only gets listings.missing_since (db/load.py). This job takes listings
missing for --days (default MISSING_DAYS=7) and not checked in that time, reads the home's own Realtor page (the
listing's Realtor id, or the one jobs.redfin_only found for Redfin-only homes) and acts on what the page says:

  page gone ("no home for")        -> active=false, listing_state='removed'
  sold                             -> active=false, listing_state='sold'       (the sold search adds the sold row)
  off market / other / other type  -> active=false, listing_state='off_market'
  still listed (same channel)      -> stays active, last_checked_at=now(); pending/contingent noted in listing_state

The page is also stored in listing_details (full info for the home). Every decision writes listing_status_events.
--removed: the backlog instead: homes removed in the past after one miss (active=false, listing_state NULL) get their
page read and labelled sold / off_market / removed (they stay hidden from browse lists either way).

Meant for idle time: --deadline / --stop-file end it between batches (phase.sh runs it in the gap between cycles).
Homes without any Realtor id wait for jobs.redfin_only to find one (never removed unchecked).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from psycopg.types.json import Jsonb  # noqa: E402
from jobs.fetch_details import NOT_FOUND, bronze_path, record_error, store_detail, write_bronze  # noqa: E402
from sources.details import FETCHERS, DetailError, detail_client  # noqa: E402
from sources.http import BlockedError  # noqa: E402

LISTED = {"sale": {"for_sale", "ready_to_build", "pending", "contingent", "coming_soon", "active"},
          "rent": {"for_rent", "active"},
          "sold": set()}
KEEP_STATE = {"pending", "contingent", "coming_soon"}

# realtor id: the listing's own, else the one redfin_only found by address
SELECT = """
SELECT l.id, l.listing_type, l.property_key, l.county_fips, l.state, l.active, l.listing_state, l.price,
       coalesce(l.source_ids->>'realtor', r.realtor_property_id) AS rid
FROM listings l LEFT JOIN listing_realtor_ids r ON r.listing_id = l.id
WHERE {where}
  AND (l.last_checked_at IS NULL OR l.last_checked_at < now() - make_interval(days => %(days)s))
  AND coalesce(l.source_ids->>'realtor', r.realtor_property_id) IS NOT NULL
LIMIT %(limit)s
"""
WHERE_MISSING = "l.active AND l.missing_since IS NOT NULL AND l.missing_since < now() - make_interval(days => %(days)s)"
WHERE_REMOVED = "NOT l.active AND l.listing_state IS NULL AND l.removed_at IS NOT NULL"
INDEX = ("CREATE INDEX CONCURRENTLY IF NOT EXISTS listings_missing_q ON listings (missing_since) "
         "WHERE missing_since IS NOT NULL AND active")


def decide(listing_type: str, page_status: str | None, not_found: bool) -> tuple[str, bool]:
    """-> (listing_state, still_active)."""
    if not_found:
        return "removed", False
    s = (page_status or "").strip().lower().replace(" ", "_")
    if s in LISTED.get(listing_type, set()):
        return (s if s in KEEP_STATE else ("for_rent" if listing_type == "rent" else "for_sale")), True
    if s == "sold":
        return "sold", False
    return "off_market", False


def apply(conn, home: dict, state: str, active: bool, run_id: str, reason: str) -> None:
    with conn.transaction():
        if active:
            ls = state if state in KEEP_STATE else None
            conn.execute("""UPDATE listings SET last_checked_at=now(), listing_state=%s,
                              updated_at=CASE WHEN listing_state IS DISTINCT FROM %s THEN now() ELSE updated_at END
                            WHERE id=%s""", (ls, ls, home["id"]))
        else:
            conn.execute("""UPDATE listings SET active=false, listing_state=%s, last_checked_at=now(),
                              removed_at=coalesce(removed_at, now()), updated_at=now(), last_run_id=%s WHERE id=%s""",
                         (state, run_id, home["id"]))
            if home["active"]:
                conn.execute("""INSERT INTO listing_events (run_id, listing_id, property_key, listing_type, county_fips,
                                event, changes) VALUES (NULL,%s,%s,%s,%s,'removed',%s)""",
                             (home["id"], home["property_key"], home["listing_type"], home["county_fips"],
                              Jsonb({"state": state, "by": reason})))
        if (home.get("listing_state") or ("for_sale" if home["active"] else None)) != state:
            conn.execute("""INSERT INTO listing_status_events (listing_id, property_key, listing_type, old_state,
                            new_state, price, source, reason, run_id) VALUES (%s,%s,%s,%s,%s,%s,'realtor',%s,%s)""",
                         (home["id"], home["property_key"], home["listing_type"], home.get("listing_state"), state,
                          home.get("price"), reason, run_id))


def check_one(fetcher, conn_factory, out: Path, run_id: str, home: dict, reason: str, stats: dict) -> None:
    h = {**home, "source": "realtor", "source_id": home["rid"], "listing_id": home["id"]}
    not_found, detail = False, None
    try:
        raw, detail = fetcher.fetch(home["rid"])
    except BlockedError as e:
        stats["blocked"] += 1
        with conn_factory() as c:
            record_error(c, run_id, h, "blocked", str(e))
        return
    except DetailError as e:
        if not any(m in str(e) for m in NOT_FOUND):
            stats["errors"] += 1
            with conn_factory() as c:
                record_error(c, run_id, h, "detail", str(e))
            return
        not_found = True
    except Exception as e:  # noqa: BLE001 - one bad home never stops the job
        stats["errors"] += 1
        with conn_factory() as c:
            record_error(c, run_id, h, "detail", f"{type(e).__name__}: {e}")
        return
    state, active = decide(home["listing_type"], (detail or {}).get("status"), not_found)
    with conn_factory() as c:
        c.autocommit = True
        if detail is not None:
            p = bronze_path(out, run_id, h)
            write_bronze(p, raw)
            store_detail(c, h, detail, run_id, str(p.relative_to(out)))
        apply(c, home, state, active, run_id, reason)
    stats[state] = stats.get(state, 0) + 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--days", type=int, default=int(os.environ.get("MISSING_DAYS", 7)))
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--inflight", type=int, default=int(os.environ.get("MISSING_INFLIGHT", 8)))
    ap.add_argument("--deadline", type=int, help="stop starting new batches after this many seconds")
    ap.add_argument("--stop-file", help="stop starting new batches once this file exists")
    ap.add_argument("--removed", action="store_true", help="label past one-miss removals instead")
    ap.add_argument("--ensure-index", action="store_true", help="create the queue index (CONCURRENTLY) and exit")
    ap.add_argument("--run-id", default="miss_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M"))
    ap.add_argument("--out", default=str(ROOT / "exports"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    from db.migrate import connect
    t0 = time.time()
    stop = lambda: (a.deadline and time.time() - t0 > a.deadline) or (a.stop_file and os.path.exists(a.stop_file))  # noqa: E731
    with connect() as conn:
        conn.autocommit = True
        if a.ensure_index:
            conn.execute(INDEX)
            print("[missing] queue index ready", flush=True)
            return 0
        reason = "backfill" if a.removed else "missing_check"
        fetcher = FETCHERS["realtor"](detail_client("realtor", inflight=a.inflight))
        stats = {"checked": 0, "errors": 0, "blocked": 0}
        done = 0
        while done < a.limit and not stop():
            n = min(a.batch, a.limit - done)
            rows = conn.execute(SELECT.format(where=WHERE_REMOVED if a.removed else WHERE_MISSING),
                                {"days": a.days, "limit": n}).fetchall()
            cols = ["id", "listing_type", "property_key", "county_fips", "state", "active", "listing_state", "price", "rid"]
            homes = [dict(zip(cols, r)) for r in rows]
            if not homes:
                break
            if a.dry_run:
                for h in homes[:20]:
                    print(json.dumps(h, default=str))
                return 0
            with ThreadPoolExecutor(max_workers=a.inflight) as ex:
                list(ex.map(lambda h: check_one(fetcher, connect, Path(a.out), a.run_id, h, reason, stats), homes))
            # homes that errored keep last_checked_at NULL: mark them so the next batch moves on (retried in --days)
            conn.execute("UPDATE listings SET last_checked_at=now() WHERE id = ANY(%s) AND (last_checked_at IS NULL "
                         "OR last_checked_at < now() - interval '1 hour')", ([h["id"] for h in homes],))
            done += len(homes)
            stats["checked"] = done
            print(f"[missing] {done} checked, {time.time() - t0:.0f}s: {json.dumps(stats)}", flush=True)
            if stats["blocked"] >= 20:
                print("[missing] blocked by the site; stopping", flush=True)
                break
    print(json.dumps({"run_id": a.run_id, **stats, "seconds": round(time.time() - t0)}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
