"""Push merged listings to listingarena-app POST /api/listings (upsert on listing_type + external_id).

Contract (listingarena-app/app/api/listings/route.js, lib/db.js COLS): JSON array, header x-admin-token,
only COLS are kept. We send canonical external_id "la:<hash>", lat/lng, listed_date, sold_date ...
Removed listings are sent once with status "removed" (the app has no delete endpoint).
Only rows whose row_hash differs from published_hash are sent; chunks < 1000 rows.

uv run la-publish [--dry-run out.json] [--limit N] [--states TX]
env: LISTINGARENA_API_URL, LISTINGARENA_ADMIN_TOKEN, PUBLISH_CHUNK (default 500)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db.migrate import connect, database_url  # noqa: E402
from normalization.schema import APP_COLS  # noqa: E402

SELECT_COLS = [c for c in APP_COLS if c != "listing_type"]


def to_app(row: dict) -> dict:
    out = {}
    for c in APP_COLS:
        v = row.get(c)
        if isinstance(v, Decimal):
            v = int(v) if v == v.to_integral_value() else float(v)
        elif isinstance(v, date):
            v = v.isoformat()
        out[c] = v
    if not row["active"]:
        out["status"] = "removed"
    if out.get("address") and row.get("unit") and row["unit"].lower() not in out["address"].lower():
        out["address"] = f"{out['address']} #{row['unit']}"
    return out


def pending(conn, states=None, limit=None):
    q = f"""SELECT id, listing_type, active, unit, row_hash, {', '.join(SELECT_COLS)} FROM listings
            WHERE published_hash IS DISTINCT FROM (CASE WHEN active THEN row_hash ELSE 'removed:' || row_hash END)
              AND (active OR published_hash IS NOT NULL)
              {"AND state = ANY(%(states)s)" if states else ""}
            ORDER BY id {"LIMIT %(limit)s" if limit else ""}"""
    cur = conn.execute(q, {"states": states, "limit": limit})
    names = [d.name for d in cur.description]
    return [dict(zip(names, r)) for r in cur]


def publish(conn, url: str | None, token: str | None, chunk: int = 500, states=None, limit=None, dry_run: Path | None = None,
            session=None, log=print) -> dict:
    chunk = max(1, min(chunk, 999))
    rows = pending(conn, states, limit)
    sent, errors, dump = 0, 0, []
    http = session or requests.Session()
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        body = [to_app(r) for r in part]
        if dry_run:
            dump.extend(body)
        else:
            try:
                resp = http.post(url, json=body, headers={"x-admin-token": token or ""}, timeout=120)
                resp.raise_for_status()
            except Exception as e:  # noqa: BLE001
                errors += 1
                conn.execute("INSERT INTO errors (kind, message) VALUES ('publish', %s)", (f"{type(e).__name__}: {e}"[:2000],))
                conn.commit()
                log(f"chunk {i // chunk}: {e}")
                continue
        with conn.transaction():
            conn.execute("""UPDATE listings SET published_hash = CASE WHEN active THEN row_hash ELSE 'removed:' || row_hash END
                            WHERE id = ANY(%s)""", ([r["id"] for r in part],))
        sent += len(part)
    if dry_run:
        dry_run.write_text(json.dumps(dump, indent=1))
    return {"pending": len(rows), "sent": sent, "failed_chunks": errors}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", type=Path, help="write the payload to this file instead of POSTing (still marks rows!)")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--states", default="")
    a = ap.parse_args(argv)
    url, token = os.environ.get("LISTINGARENA_API_URL"), os.environ.get("LISTINGARENA_ADMIN_TOKEN")
    if not a.dry_run and not (url and token):
        raise SystemExit("set LISTINGARENA_API_URL and LISTINGARENA_ADMIN_TOKEN (or use --dry-run)")
    database_url()
    with connect() as conn:
        res = publish(conn, url, token, int(os.environ.get("PUBLISH_CHUNK", 500)),
                      [s for s in a.states.upper().split(",") if s] or None, a.limit, a.dry_run)
    print(json.dumps(res))
    return 0 if not res["failed_chunks"] else 1


if __name__ == "__main__":
    sys.exit(main())
