"""Load one collection run (silver parts + checkpoints) into Postgres with change tracking.

Per (county, listing_type) unit, in one transaction:
  1. per source whose county fetch finished: upsert source_records; deactivate records of that
     source/county/type not seen this run ONLY if the fetch was complete (all pages). Failed or
     partial fetches never remove anything.
  2. re-merge all active source_records of the unit into canonical listings, diff against `listings`,
     upsert and write listing_events (new / changed / removed / relisted).
  3. write run_county_stats (per source + 'merged') and errors.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from matching.address import normalize_address
from matching.changes import detect
from matching.merge import key_for, merge, row_hash
from normalization.schema import SILVER_FIELDS

LISTING_COLS = ["external_id", "address", "unit", "city", "state", "zip", "county_fips", "property_type", "beds",
                "baths", "sqft", "lot_sqft", "year_built", "price", "list_price", "sold_date", "listed_date",
                "available_from", "deposit", "lat", "lng", "description", "photo_url", "status", "sources",
                "source_ids", "urls", "extra", "row_hash"]
JSON_COLS = {"source_ids", "urls", "extra"}
SRC_HASH_FIELDS = [f for f in SILVER_FIELDS if f not in ("fetched_at", "extra")]


@dataclass
class Unit:
    state: str
    fips: str
    listing_type: str
    fetched: dict = field(default_factory=dict)    # source -> checkpoint result
    failed: dict = field(default_factory=dict)     # source -> error marker
    rows: dict = field(default_factory=dict)       # source -> [silver records]


def discover(exports: Path, run_id: str, states: set[str] | None = None, fips: set[str] | None = None) -> list[Unit]:
    units: dict[tuple, Unit] = {}
    cpdir = exports / "checkpoints" / run_id
    for p in sorted(cpdir.glob("*/*/*.json")):
        src, st = p.parts[-3], p.parts[-2].upper()
        is_err = p.name.endswith(".error.json")
        stem = p.name.removesuffix(".error.json").removesuffix(".json")
        cfips, t = stem.split("_")
        if (states and st not in states) or (fips and cfips not in fips):
            continue
        u = units.setdefault((cfips, t), Unit(st, cfips, t))
        info = json.loads(p.read_text())
        if is_err:
            u.failed[src] = info
        else:
            u.fetched[src] = info
            part = exports / "silver" / src / run_id / "parts" / f"{st.lower()}_{cfips}_{t}.jsonl"
            u.rows[src] = [json.loads(l) for l in part.read_text().splitlines() if l.strip()] if part.exists() else []
    for u in units.values():  # a later success supersedes an error marker
        for s in list(u.failed):
            if s in u.fetched:
                del u.failed[s]
    return sorted(units.values(), key=lambda u: (u.state, u.fips, u.listing_type))


def start_run(conn, run_id: str, trigger: str = "cli", states=None) -> None:
    conn.execute("""INSERT INTO runs (run_id, trigger, states) VALUES (%s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET status = 'running', finished_at = NULL""",
                 (run_id, trigger, sorted(states) if states else None))
    conn.commit()


def finish_run(conn, run_id: str, exports: Path) -> str:
    sp = exports / "checkpoints" / run_id / "http_stats.json"
    http = json.loads(sp.read_text()) if sp.exists() else {}
    n_err = conn.execute("SELECT count(*) FROM run_county_stats WHERE run_id=%s AND errors>0", (run_id,)).fetchone()[0]
    n_ok = conn.execute("SELECT count(*) FROM run_county_stats WHERE run_id=%s AND errors=0 AND source<>'merged'", (run_id,)).fetchone()[0]
    status = "success" if not n_err else ("partial" if n_ok else "failed")
    conn.execute("UPDATE runs SET finished_at=now(), status=%s, stats = stats || %s WHERE run_id=%s",
                 (status, Jsonb({"http": http}), run_id))
    conn.commit()
    return status


def _src_hash(r: dict) -> str:
    return row_hash(r, SRC_HASH_FIELDS)


def load_unit(conn: psycopg.Connection, run_id: str, u: Unit, batch: int = 500) -> dict:
    t0 = time.time()
    out = {"sources": {}, "merged": {}}
    with conn.transaction():
        # ---- 1. source records
        for src, rows in u.rows.items():
            cp = u.fetched[src]
            existing = {r[0]: (r[1], r[2]) for r in conn.execute(
                "SELECT source_id, row_hash, active FROM source_records WHERE source=%s AND listing_type=%s AND county_fips=%s",
                (src, u.listing_type, u.fips))}
            st = {"fetched": len(rows), "new": 0, "changed": 0, "removed": 0, "relisted": 0}
            params = []
            seen = set()
            for r in rows:
                sid = str(r["source_id"])
                if sid in seen:
                    continue
                seen.add(sid)
                h = _src_hash(r)
                r["property_key"] = key_for(r)
                old = existing.get(sid)
                if old is None:
                    st["new"] += 1
                elif not old[1]:
                    st["relisted"] += 1
                elif old[0] != h:
                    st["changed"] += 1
                params.append((src, u.listing_type, sid, r["property_key"], u.fips, u.state, Jsonb(r), h, run_id, run_id))
            for i in range(0, len(params), batch):
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO source_records (source, listing_type, source_id, property_key, county_fips, state, data,
                                                    row_hash, first_run_id, last_run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (source, listing_type, source_id) DO UPDATE SET
                          property_key=EXCLUDED.property_key, county_fips=EXCLUDED.county_fips, data=EXCLUDED.data,
                          row_hash=EXCLUDED.row_hash, active=true, removed_at=NULL, last_seen=now(),
                          last_run_id=EXCLUDED.last_run_id""", params[i:i + batch])
            if cp.get("complete"):
                gone = [sid for sid, (_, act) in existing.items() if act and sid not in seen]
                if gone:
                    conn.execute("""UPDATE source_records SET active=false, removed_at=now(), last_run_id=%s
                                    WHERE source=%s AND listing_type=%s AND source_id = ANY(%s)""",
                                 (run_id, src, u.listing_type, gone))
                st["removed"] = len(gone)
            st.update(pages=cp.get("pages"), duration_s=cp.get("seconds"), complete=bool(cp.get("complete")))
            out["sources"][src] = st
        for src, err in u.failed.items():
            out["sources"][src] = {"errors": 1, "blocks": int(bool(err.get("blocked"))), "complete": False}
            conn.execute("""INSERT INTO errors (run_id, source, state, county_fips, listing_type, kind, message, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s, coalesce(%s::timestamptz, now()))""",
                         (run_id, src, u.state, u.fips, u.listing_type, "blocked" if err.get("blocked") else "fetch",
                          err.get("error", "?"), err.get("at")))

        # ---- 2. canonical listings
        active = [r[0] for r in conn.execute(
            "SELECT data FROM source_records WHERE county_fips=%s AND listing_type=%s AND active", (u.fips, u.listing_type))]
        merged, nokey = merge(active)
        incoming = {m["property_key"]: m for m in merged}
        current = {}
        for row in conn.execute(f"""SELECT id, property_key, active, {', '.join(c for c in LISTING_COLS)}
                                    FROM listings WHERE county_fips=%s AND listing_type=%s""", (u.fips, u.listing_type)):
            d = dict(zip(["id", "property_key", "active"] + LISTING_COLS, row))
            current[d["property_key"]] = d
        events = detect(current, incoming)
        # properties
        props = []
        for m in merged:
            street, unit = normalize_address(m.get("address"))
            props.append((m["property_key"], m.get("address"), unit or None, m.get("city"), m.get("state"),
                          m.get("zip"), m.get("county_fips"), m.get("lat"), m.get("lng")))
        with conn.cursor() as cur:
            for i in range(0, len(props), batch):
                cur.executemany("""INSERT INTO properties (property_key, address, unit, city, state, zip, county_fips, lat, lng)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                   ON CONFLICT (property_key) DO UPDATE SET last_seen=now(),
                                     lat=coalesce(EXCLUDED.lat, properties.lat), lng=coalesce(EXCLUDED.lng, properties.lng)""",
                                props[i:i + batch])
        touched = {e["property_key"] for e in events if e["event"] != "removed"}
        touched |= {k for k, m in incoming.items() if k in current and current[k]["row_hash"] != m["row_hash"]}
        upserts = [incoming[k] for k in touched]
        cols = ["property_key", "listing_type"] + LISTING_COLS
        sql = (f"INSERT INTO listings ({', '.join(cols)}, first_run_id, last_run_id) VALUES ({', '.join(['%s'] * (len(cols) + 2))}) "
               f"ON CONFLICT (property_key, listing_type) DO UPDATE SET "
               + ", ".join(f"{c}=EXCLUDED.{c}" for c in LISTING_COLS)
               + ", active=true, removed_at=NULL, updated_at=now(), last_seen=now(), last_run_id=EXCLUDED.last_run_id")
        with conn.cursor() as cur:
            for i in range(0, len(upserts), batch):
                cur.executemany(sql, [[m["property_key"], u.listing_type]
                                      + [Jsonb(m[c]) if c in JSON_COLS else m.get(c) for c in LISTING_COLS]
                                      + [run_id, run_id] for m in upserts[i:i + batch]])
        conn.execute("UPDATE listings SET last_seen=now(), last_run_id=%s WHERE county_fips=%s AND listing_type=%s AND active "
                     "AND property_key = ANY(%s)", (run_id, u.fips, u.listing_type, list(incoming)))
        removed = [e["property_key"] for e in events if e["event"] == "removed"]
        if removed:
            conn.execute("""UPDATE listings SET active=false, removed_at=now(), updated_at=now(), last_run_id=%s
                            WHERE listing_type=%s AND property_key = ANY(%s)""", (run_id, u.listing_type, removed))
        ids = dict(conn.execute("SELECT property_key, id FROM listings WHERE county_fips=%s AND listing_type=%s",
                                (u.fips, u.listing_type)).fetchall())
        with conn.cursor() as cur:
            for i in range(0, len(events), batch):
                cur.executemany("""INSERT INTO listing_events (run_id, listing_id, property_key, listing_type, county_fips, event, changes)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                                [(run_id, ids.get(e["property_key"]), e["property_key"], u.listing_type, u.fips, e["event"],
                                  Jsonb(e["changes"])) for e in events[i:i + batch]])
        cnt = defaultdict(int)
        for e in events:
            cnt[e["event"]] += 1
        out["merged"] = {"fetched": len(merged), "new": cnt["new"], "changed": cnt["changed"], "removed": cnt["removed"],
                         "relisted": cnt["relisted"], "active": len(merged), "nokey": len(nokey),
                         "multi_source": sum(1 for m in merged if len(m["sources"]) > 1),
                         "complete": all(s.get("complete") for s in out["sources"].values()) and bool(out["sources"]),
                         "errors": len(u.failed), "duration_s": round(time.time() - t0, 2)}

        # ---- 3. stats
        rows = [(src, s) for src, s in out["sources"].items()] + [("merged", out["merged"])]
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO run_county_stats (run_id, state, county_fips, source, listing_type, fetched, new, changed, removed,
                                              relisted, active, errors, blocks, pages, duration_s, complete)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id, county_fips, source, listing_type) DO UPDATE SET fetched=EXCLUDED.fetched, new=EXCLUDED.new,
                  changed=EXCLUDED.changed, removed=EXCLUDED.removed, relisted=EXCLUDED.relisted, active=EXCLUDED.active,
                  errors=EXCLUDED.errors, blocks=EXCLUDED.blocks, pages=EXCLUDED.pages, duration_s=EXCLUDED.duration_s,
                  complete=EXCLUDED.complete, loaded_at=now()""",
                [(run_id, u.state, u.fips, src, u.listing_type, s.get("fetched", 0), s.get("new", 0), s.get("changed", 0),
                  s.get("removed", 0), s.get("relisted", 0), s.get("active"), s.get("errors", 0), s.get("blocks", 0),
                  s.get("pages"), s.get("duration_s"), bool(s.get("complete"))) for src, s in rows])
    return out


def load_run(conn, exports: Path, run_id: str, states=None, fips=None, trigger="cli", log=print) -> dict:
    start_run(conn, run_id, trigger, states)
    totals = defaultdict(int)
    for u in discover(exports, run_id, states, fips):
        try:
            r = load_unit(conn, run_id, u)
        except Exception as e:  # noqa: BLE001 - record and continue with the next county
            conn.rollback()
            conn.execute("INSERT INTO errors (run_id, state, county_fips, listing_type, kind, message) VALUES (%s,%s,%s,%s,'load',%s)",
                         (run_id, u.state, u.fips, u.listing_type, f"{type(e).__name__}: {e}"[:2000]))
            conn.commit()
            log(f"LOAD FAILED {u.fips} {u.listing_type}: {e}")
            totals["load_errors"] += 1
            continue
        m = r["merged"]
        log(f"{u.state} {u.fips} {u.listing_type:5} " + " ".join(f"{s}={v.get('fetched', 0)}" for s, v in r["sources"].items())
            + f" -> listings={m['active']} multi={m['multi_source']} new={m['new']} changed={m['changed']} "
              f"removed={m['removed']} relisted={m['relisted']}")
        for k in ("active", "multi_source", "new", "changed", "removed", "relisted", "nokey"):
            totals[k] += m[k]
        totals["units"] += 1
    totals["status"] = finish_run(conn, run_id, exports)
    return dict(totals)
