"""Phase 2: fetch per-home detail for listings that are new / changed / relisted since a collection run.

python -m jobs.fetch_details --since-run RUN_ID [--state XX] [--limit N] [--source redfin|realtor]
    [--include-missing] [--inflight 3] [--run-id ID] [--out exports] [--dry-run]

Selection: listing_events (new, changed, relisted) of RUN_ID and every later run, on still-active listings, expanded
to each backing source record (listings.source_ids). --include-missing adds active listings of those sources that
have no listing_details row yet. A home is skipped when listing_details.fetched_at is newer than its latest event:
that is the checkpoint, so a restart (same or new --run-id) only fetches what is still missing.

Output:
  exports/bronze/<source>_detail/<st>/<fips>/<run_id>/<source_id>.json.gz   raw response(s)
  listing_details (+ listing_detail_history / _taxes / _schools / _comps)  normalized, see docs/schema.md
  errors (kind 'detail' or 'blocked')                                      failed homes (retried next time)

Concurrency is deliberately low (per-home APIs blocked at 10x20): --inflight per source (default 3, env
<SOURCE>_DETAIL_MAX_INFLIGHT), adaptive limiter, and a circuit breaker that stops a source after
DETAIL_MAX_BLOCKS (default 3) consecutive BlockedErrors.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from psycopg.types.json import Jsonb  # noqa: E402

from sources.details import DETAIL_COLUMNS, DETAIL_JSON, FETCHERS, DetailError, detail_client  # noqa: E402
from jobs import settings  # noqa: E402
from sources.http import BlockedError  # noqa: E402

SOURCES = ("realtor", "redfin")

SELECT_EVENTS = """
SELECT l.id, l.listing_type, l.property_key, l.county_fips, l.state, s.source, s.sid,
       sr.data->'extra'->>'listing_id' AS source_listing_id, e.created_at
FROM listing_events e
JOIN runs r ON r.run_id = e.run_id AND r.started_at >= (SELECT started_at FROM runs WHERE run_id = %(since)s)
JOIN listings l ON l.id = e.listing_id AND l.active
CROSS JOIN LATERAL jsonb_each_text(l.source_ids) AS s(source, sid)
LEFT JOIN source_records sr ON sr.source = s.source AND sr.listing_type = l.listing_type AND sr.source_id = s.sid
LEFT JOIN listing_details d ON d.source = s.source AND d.source_id = s.sid
WHERE e.event IN ('new', 'changed', 'relisted')
  AND s.source = ANY(%(sources)s)
  AND (%(fips)s::text IS NULL OR e.county_fips LIKE %(fips)s)   -- uses listing_events_run (run_id, county_fips)
  AND (d.fetched_at IS NULL OR d.fetched_at < e.created_at)
"""
SELECT_MISSING = """
SELECT l.id, l.listing_type, l.property_key, l.county_fips, l.state, s.source, s.sid,
       sr.data->'extra'->>'listing_id', l.updated_at
FROM listings l
CROSS JOIN LATERAL jsonb_each_text(l.source_ids) AS s(source, sid)
LEFT JOIN source_records sr ON sr.source = s.source AND sr.listing_type = l.listing_type AND sr.source_id = s.sid
WHERE l.active AND s.source = ANY(%(sources)s) AND (%(fips)s::text IS NULL OR l.county_fips LIKE %(fips)s)
  AND NOT EXISTS (SELECT 1 FROM listing_details d WHERE d.source = s.source AND d.source_id = s.sid)
"""
# listings only Redfin has, whose Realtor property id the redfin_only step found: their Realtor page too
SELECT_REDFIN_ONLY = """
SELECT l.id, l.listing_type, l.property_key, l.county_fips, l.state, 'realtor', r.realtor_property_id, NULL, r.found_at
FROM listing_realtor_ids r
JOIN listings l ON l.id = r.listing_id AND l.active
WHERE (%(fips)s::text IS NULL OR l.county_fips LIKE %(fips)s)
  AND NOT EXISTS (SELECT 1 FROM listing_details d WHERE d.source = 'realtor' AND d.source_id = r.realtor_property_id
                  AND d.fetched_at >= r.found_at)
"""
KEYS = ("listing_id", "listing_type", "property_key", "county_fips", "state", "source", "source_id",
        "source_listing_id", "event_at")


def state_fips_prefix(state: str) -> str:
    """2-digit state FIPS from states/<st>.json (county fips prefix), so the filter can use the event index."""
    f = ROOT / "states" / f"{state.lower()}.json"
    if not f.exists():
        raise SystemExit(f"unknown state {state!r} (no {f.name})")
    return json.loads(f.read_text())["counties"][0]["fips"][:2]


def select_homes(conn, since: str, sources: list[str], state: str | None, limit: int | None,
                 include_missing: bool) -> list[dict]:
    if conn.execute("SELECT 1 FROM runs WHERE run_id = %s", (since,)).fetchone() is None:
        raise SystemExit(f"unknown run id {since!r}")
    p = {"since": since, "sources": list(sources), "fips": state_fips_prefix(state) + "%" if state else None}
    # no ORDER/DISTINCT in SQL so Postgres can stream and stop early under LIMIT; dedupe here (latest event wins)
    lim = f" LIMIT {int(limit) * 4}" if limit else ""
    best: dict = {}
    for r in conn.execute(SELECT_EVENTS + lim, p):
        d = dict(zip(KEYS, r))
        k = (d["source"], d["source_id"])
        if k not in best or (d["event_at"] and d["event_at"] > best[k]["event_at"]):
            best[k] = d
    rows = list(best.values())[:limit] if limit else list(best.values())
    if include_missing and (not limit or len(rows) < limit):
        seen = {(r["source"], r["source_id"]) for r in rows}
        more = f" LIMIT {int(limit) - len(rows) + len(seen)}" if limit else ""
        for r in conn.execute(SELECT_MISSING + more, p):
            d = dict(zip(KEYS, r))
            if (d["source"], d["source_id"]) not in seen and (not limit or len(rows) < limit):
                seen.add((d["source"], d["source_id"]))
                rows.append(d)
    if "realtor" in sources and (not limit or len(rows) < limit):
        seen = {(r["source"], r["source_id"]) for r in rows}
        for r in conn.execute(SELECT_REDFIN_ONLY, p):
            d = dict(zip(KEYS, r))
            if (d["source"], d["source_id"]) not in seen and (not limit or len(rows) < limit):
                seen.add((d["source"], d["source_id"]))
                rows.append(d)
    return rows


def detail_hash(detail: dict) -> str:
    return hashlib.sha1(json.dumps(detail, sort_keys=True, default=str).encode()).hexdigest()


def bronze_path(out: Path, run_id: str, home: dict) -> Path:
    st = (home.get("state") or "xx").lower()
    return out / "bronze" / f"{home['source']}_detail" / st / (home.get("county_fips") or "unknown") / run_id / \
        f"{home['source_id']}.json.gz"


def write_bronze(path: Path, raw) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(raw, f)
    tmp.replace(path)


def store_detail(conn, home: dict, detail: dict, run_id: str, bronze: str | None) -> bool:
    """Upsert one home's detail; rewrite child rows only when the hash changed. Returns True if changed."""
    h = detail_hash(detail)
    src, sid = home["source"], home["source_id"]
    cols = ["source", "source_id", "listing_type", "listing_id", "property_key", "county_fips", "state",
            *DETAIL_COLUMNS, *DETAIL_JSON, "payload", "row_hash", "bronze_path", "run_id"]
    vals = [src, sid, home.get("listing_type"), home.get("listing_id"), home.get("property_key"),
            home.get("county_fips"), home.get("state"),
            *[detail.get(c) for c in DETAIL_COLUMNS],
            *[Jsonb(detail[c]) if detail.get(c) is not None else None for c in DETAIL_JSON],
            Jsonb(detail), h, bronze, run_id]
    upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols[2:])
    with conn.transaction():
        prev = conn.execute("SELECT row_hash FROM listing_details WHERE source=%s AND source_id=%s", (src, sid)).fetchone()
        conn.execute(f"""INSERT INTO listing_details ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})
                         ON CONFLICT (source, source_id) DO UPDATE SET {upd}, fetched_at=now(),
                           changed_at=CASE WHEN listing_details.row_hash = EXCLUDED.row_hash
                                           THEN listing_details.changed_at ELSE now() END""", vals)
        changed = prev is None or prev[0] != h
        if not changed:
            return False
        for t in ("listing_detail_history", "listing_detail_taxes", "listing_detail_schools", "listing_detail_comps"):
            conn.execute(f"DELETE FROM {t} WHERE source=%s AND source_id=%s", (src, sid))
        with conn.cursor() as cur:
            cur.executemany("""INSERT INTO listing_detail_history (source, source_id, seq, event_date, event, price,
                               price_sqft, source_name, mls_listing_id, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [(src, sid, i, e.get("date"), e.get("event"), e.get("price"), e.get("price_sqft"),
                              e.get("source_name"), e.get("mls_listing_id"), e.get("status"))
                             for i, e in enumerate(detail.get("price_history") or [])])
            taxes = {t["year"]: t for t in detail.get("tax_history") or [] if t.get("year") is not None}
            cur.executemany("""INSERT INTO listing_detail_taxes (source, source_id, year, tax, assessed_total,
                               assessed_building, assessed_land, market_total) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [(src, sid, y, t.get("tax"), t.get("assessed_total"), t.get("assessed_building"),
                              t.get("assessed_land"), t.get("market_total")) for y, t in taxes.items()])
            cur.executemany("""INSERT INTO listing_detail_schools (source, source_id, seq, name, rating, distance_mi,
                               levels, grades, funding, district) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [(src, sid, i, s.get("name"), s.get("rating"), s.get("distance_mi"), Jsonb(s.get("levels")),
                              Jsonb(s.get("grades")), s.get("funding"), s.get("district"))
                             for i, s in enumerate(detail.get("schools") or [])])
            comps = {(c.get("kind") or "similar", c["comp_source_id"]): c for c in detail.get("comps") or []
                     if c.get("comp_source_id")}
            cur.executemany("""INSERT INTO listing_detail_comps (source, source_id, comp_source_id, kind, rank,
                               comp_listing_id, status, price, sold_date, beds, baths, sqft, address, zip, lat, lng,
                               distance_mi, url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [(src, sid, cid, kind, c.get("rank"), c.get("comp_listing_id"), c.get("status"),
                              c.get("price"), c.get("sold_date"), c.get("beds"), c.get("baths"), c.get("sqft"),
                              c.get("address"), c.get("zip"), c.get("lat"), c.get("lng"), c.get("distance_mi"),
                              c.get("url")) for (kind, cid), c in comps.items()])
    return True


def record_error(conn, run_id: str, home: dict, kind: str, msg: str) -> None:
    try:
        with conn.transaction():
            conn.execute("""INSERT INTO errors (run_id, source, state, county_fips, listing_type, kind, message)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                         (run_id, home["source"], home.get("state"), home.get("county_fips"), home.get("listing_type"),
                          kind, f"{home['source_id']}: {msg}"[:2000]))
    except Exception as e:  # noqa: BLE001 - never let error logging kill the run
        print(f"[details] could not record error: {e}", flush=True)


class DetailRunner:
    def __init__(self, conn, fetchers: dict, out: Path, run_id: str, inflight: int = 3,
                 max_blocks: int = 3, log=print):
        self.conn, self.fetchers, self.out, self.run_id = conn, fetchers, out, run_id
        self.inflight, self.max_blocks, self.log = inflight, max_blocks, log
        self.db_lock = threading.Lock()
        self.stats = {s: {"fetched": 0, "changed": 0, "errors": 0, "blocked": 0, "skipped": 0} for s in fetchers}
        self.consecutive_blocks = {s: 0 for s in fetchers}
        self.stopped: set[str] = set()
        self.slock = threading.Lock()

    def _inc(self, src: str, key: str, n: int = 1) -> None:
        with self.slock:
            self.stats[src][key] += n

    def one(self, home: dict) -> None:
        src = home["source"]
        if src in self.stopped:
            self._inc(src, "skipped", 1)
            return
        try:
            raw, detail = self.fetchers[src].fetch(home["source_id"], home.get("source_listing_id"))
        except BlockedError as e:
            self._inc(src, "blocked", 1)
            with self.slock:
                self.consecutive_blocks[src] += 1
            with self.db_lock:
                record_error(self.conn, self.run_id, home, "blocked", str(e))
            if self.consecutive_blocks[src] >= self.max_blocks and src not in self.stopped:
                self.stopped.add(src)
                self.log(f"[details] {src}: {self.max_blocks} consecutive blocks; stopping this source for this run")
            return
        except (DetailError, Exception) as e:  # noqa: BLE001 - one bad home must never stop the run
            self._inc(src, "errors", 1)
            self.consecutive_blocks[src] = 0
            self.log(f"[details] {src} {home['source_id']}: {type(e).__name__}: {str(e)[:200]}")
            with self.db_lock:
                record_error(self.conn, self.run_id, home, "detail", f"{type(e).__name__}: {e}")
            return
        self.consecutive_blocks[src] = 0
        path = bronze_path(self.out, self.run_id, home)
        write_bronze(path, raw)
        with self.db_lock:
            changed = store_detail(self.conn, home, detail, self.run_id, str(path.relative_to(self.out)))
        self._inc(src, "fetched", 1)
        self._inc(src, "changed", int(changed))

    def run(self, homes: list[dict]) -> dict:
        by_src: dict[str, list] = {}
        for h in homes:
            if h["source"] in self.fetchers:
                by_src.setdefault(h["source"], []).append(h)
        t0 = time.time()
        pools = {s: ThreadPoolExecutor(max_workers=self.inflight, thread_name_prefix=f"{s}_detail") for s in by_src}
        futs = [pools[s].submit(self.one, h) for s, hs in by_src.items() for h in hs]
        for i, f in enumerate(as_completed(futs), 1):
            f.result()
            if i % 50 == 0 or i == len(futs):
                self.log(f"[details] {i}/{len(futs)} homes, {time.time() - t0:.0f}s: {json.dumps(self.stats)}")
        for p in pools.values():
            p.shutdown()
        return {"run_id": self.run_id, "homes": len(futs), "seconds": round(time.time() - t0, 1), "sources": self.stats,
                "stopped": sorted(self.stopped),
                "http": {s: f.client.stats for s, f in self.fetchers.items() if hasattr(f, "client")}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--since-run", required=True, help="collection run id; events of it and later runs are used")
    ap.add_argument("--state")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--source", choices=SOURCES, help="only this source (default: both)")
    ap.add_argument("--include-missing", action="store_true", help="also active listings with no detail yet")
    ap.add_argument("--inflight", type=int, default=int(os.environ.get("DETAIL_INFLIGHT") or settings.DETAILS_WORKERS),
                    help="max simultaneous requests per source (default DETAILS_WORKERS, jobs/settings.py)")
    ap.add_argument("--run-id", default="det_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M"))
    ap.add_argument("--out", default=str(ROOT / "exports"))
    ap.add_argument("--dry-run", action="store_true", help="only print what would be fetched")
    a = ap.parse_args(argv)
    from db.migrate import connect
    sources = [a.source] if a.source else list(SOURCES)
    with connect() as conn:
        conn.autocommit = True
        homes = select_homes(conn, a.since_run, sources, a.state, a.limit, a.include_missing)
        print(f"[details] {len(homes)} homes to fetch (since {a.since_run}, state={a.state or 'all'}, "
              f"sources={','.join(sources)})", flush=True)
        if a.dry_run:
            for h in homes[:50]:
                print(json.dumps(h, default=str))
            return 0
        fetchers = {s: FETCHERS[s](detail_client(s, inflight=a.inflight)) for s in sources}
        res = DetailRunner(conn, fetchers, Path(a.out), a.run_id, inflight=a.inflight,
                           max_blocks=int(os.environ.get("DETAIL_MAX_BLOCKS", 3)),
                           log=lambda m: print(m, flush=True)).run(homes)
    print(json.dumps(res, default=str), flush=True)
    return 0 if not any(s["errors"] or s["blocked"] for s in res["sources"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
