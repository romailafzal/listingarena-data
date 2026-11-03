"""Floor plans: each Redfin rental building's units, fetched as often as they change.

python -m jobs.floor_plans [--state XX] [--limit N] [--workers N] [--rate N]

Which buildings: active rentals Redfin has with a rental_id. Due: never fetched, or as often as its units have been
changing (changed in the last 7 days: daily; in the last 30: every 3 days; otherwise weekly). A building's turn
inside its interval is spread by its id, so the work is even across days. A fetch that fails stays due.
"Changed" means a unit's rent, deposit, beds, baths or size, or a unit added or removed (units_key); Redfin's
renumbered ids and names, dates and lease terms do not count.
Pace: FLOOR_PLANS_WORKERS at once, at most FLOOR_PLANS_RATE a second (jobs/settings.py).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from psycopg.types.json import Jsonb  # noqa: E402

from jobs import settings  # noqa: E402
from jobs.pace import Pace  # noqa: E402
from sources.http import BlockedError, SourceClient  # noqa: E402

URL = "https://www.redfin.com/stingray/api/v1/rentals/{rental_id}/floorPlans"
ACTIVE_DAYS, STABLE_DAYS, SETTLING_EVERY, STABLE_EVERY = 7, 30, 3, 7

SELECT = """
SELECT DISTINCT ON (l.extra->'redfin'->>'rental_id') l.extra->'redfin'->>'rental_id', l.id, l.state,
       f.fetched_on, f.changed_on, f.units_key
FROM listings l
LEFT JOIN floor_plans f ON f.rental_id = l.extra->'redfin'->>'rental_id'
WHERE l.active AND l.listing_type = 'rent' AND l.extra->'redfin'->>'rental_id' IS NOT NULL
  AND (%(state)s::text IS NULL OR l.state = %(state)s)
ORDER BY l.extra->'redfin'->>'rental_id', l.id
"""


def units_key(data: dict) -> str:
    buckets = (data or {}).get("unitTypesByBedroom") or []
    types = next((b.get("availableUnitTypes") or [] for b in buckets if b.get("bedroomTitle") == "All"), [])
    units = [[u.get("rentPrice") or u.get("rentPriceMin"), u.get("deposit") or t.get("deposit"), u.get("bedrooms"),
              u.get("fullBaths"), u.get("halfBaths"), u.get("sqft") or u.get("sqftMin")]
             for t in types for u in t.get("units") or [t]]
    return json.dumps(sorted(units, key=json.dumps))


def interval(changed_on: date, today: date) -> int:
    since = (today - changed_on).days
    return 1 if since < ACTIVE_DAYS else SETTLING_EVERY if since < STABLE_DAYS else STABLE_EVERY


def due(rental_id: str, fetched_on: date | None, changed_on: date | None, today: date) -> bool:
    if fetched_on is None:
        return True
    every = interval(changed_on or fetched_on, today)
    age = (today - fetched_on).days
    its_turn = (today.toordinal() + zlib.crc32(rental_id.encode())) % every == 0
    return age >= every and (its_turn or age >= 2 * every)


class FloorPlanJob:
    def __init__(self, conn, client: SourceClient, pace: Pace, log=print):
        self.conn, self.client, self.pace, self.log = conn, client, pace, log
        self.stats = {"fetched": 0, "changed": 0, "errors": 0}
        self.today = date.today()

    def one(self, row) -> None:
        rental_id, listing_id, state, _, changed_on, old_key = row
        self.pace.wait()
        try:
            data = self.client.request_json("GET", URL.format(rental_id=rental_id))
            if not isinstance(data, dict) or "totalUnits" not in data:
                raise ValueError("not a floor plans answer")
        except BlockedError as e:
            self.pace.refused(403)
            self._error(rental_id, listing_id, state, str(e))
            return
        except Exception as e:  # noqa: BLE001 - one building must never stop the run
            self._error(rental_id, listing_id, state, f"{type(e).__name__}: {str(e)[:200]}")
            return
        key = units_key(data)
        changed = key != old_key
        self.conn.execute(
            """INSERT INTO floor_plans (rental_id, listing_id, state, data, units_key, fetched_on, changed_on, error)
               VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
               ON CONFLICT (rental_id) DO UPDATE SET listing_id = EXCLUDED.listing_id, data = EXCLUDED.data,
                 units_key = EXCLUDED.units_key, fetched_on = EXCLUDED.fetched_on, changed_on = EXCLUDED.changed_on,
                 error = NULL""",
            (rental_id, listing_id, state, Jsonb(data), key, self.today, self.today if changed else changed_on))
        self.stats["fetched"] += 1
        self.stats["changed"] += int(changed)

    def _error(self, rental_id, listing_id, state, msg) -> None:
        self.stats["errors"] += 1
        self.conn.execute("""INSERT INTO floor_plans (rental_id, listing_id, state, error) VALUES (%s, %s, %s, %s)
                             ON CONFLICT (rental_id) DO UPDATE SET error = EXCLUDED.error""",
                          (rental_id, listing_id, state, msg))

    def run(self, rows: list, workers: int) -> dict:
        t0 = time.time()
        with ThreadPoolExecutor(workers, thread_name_prefix="floor_plans") as pool:
            for i, _ in enumerate(pool.map(self.one, rows), 1):
                if i % 200 == 0 or i == len(rows):
                    self.log(f"[floor_plans] {i}/{len(rows)} buildings, {time.time() - t0:.0f}s: {json.dumps(self.stats)}")
        return {"buildings": len(rows), "seconds": round(time.time() - t0, 1), **self.stats}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--state")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--workers", type=int, default=settings.FLOOR_PLANS_WORKERS)
    ap.add_argument("--rate", type=float, default=settings.FLOOR_PLANS_RATE)
    a = ap.parse_args(argv)
    from db.migrate import connect
    today = date.today()
    with connect() as conn:
        conn.autocommit = True
        rows = [r for r in conn.execute(SELECT, {"state": a.state and a.state.upper()})
                if due(r[0], r[3], r[4], today)][: a.limit]
        print(f"[floor_plans] {len(rows)} buildings due (state={a.state or 'all'}, workers={a.workers})", flush=True)
        client = SourceClient("redfin_floor_plans", min_interval=0, retries=3)
        res = FloorPlanJob(conn, client, Pace(a.rate, 120, "floor_plans"), log=lambda m: print(m, flush=True)).run(
            rows, a.workers)
    print(json.dumps(res), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
