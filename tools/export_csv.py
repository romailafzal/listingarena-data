"""Export merged listings to one wide CSV for review.

Main columns, then which sources had the listing, their ids/urls, then every
`extra` field flattened as <source>_<field> (lists/objects as JSON).

usage: uv run python -m tools.export_csv [--state TX] [--type sale] [--all] [--out PATH]
"""
import argparse
import csv
import json
from pathlib import Path

from db.migrate import connect

MAIN = ["external_id", "listing_type", "status", "active", "address", "unit", "city", "state", "zip",
        "county_fips", "county_name", "property_type", "price", "list_price", "beds", "baths", "sqft",
        "lot_sqft", "year_built", "listed_date", "sold_date", "available_from", "deposit", "lat", "lng",
        "photo_url", "description", "first_seen", "last_seen", "removed_at"]


def cell(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"))
    return "" if v is None else v


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state")
    ap.add_argument("--type", dest="listing_type")
    ap.add_argument("--all", action="store_true", help="include removed listings")
    ap.add_argument("--out", default="exports/csv/listings_review.csv")
    a = ap.parse_args(argv)

    where, params = [], []
    if not a.all:
        where.append("l.active")
    if a.state:
        where.append("l.state = %s"); params.append(a.state.upper())
    if a.listing_type:
        where.append("l.listing_type = %s"); params.append(a.listing_type)
    sql = ("SELECT l.*, c.name AS county_name FROM listings l LEFT JOIN counties c ON c.fips = l.county_fips"
           + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY l.state, l.county_fips, l.listing_type, l.id")

    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    sources = sorted({s for r in rows for s in (r["sources"] or [])})
    extra_cols = sorted({f"{s}_{k}" for r in rows for s, v in (r["extra"] or {}).items() for k in v})
    header = (MAIN + ["sources", "source_count"] + [f"{s}_id" for s in sources]
              + [f"{s}_url" for s in sources] + extra_cols)

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            flat = {f"{s}_{k}": v for s, d in (r["extra"] or {}).items() for k, v in d.items()}
            ids, urls = r["source_ids"] or {}, r["urls"] or {}
            w.writerow([cell(r.get(c)) for c in MAIN]
                       + ["|".join(r["sources"] or []), len(r["sources"] or [])]
                       + [cell(ids.get(s)) for s in sources] + [cell(urls.get(s)) for s in sources]
                       + [cell(flat.get(c)) for c in extra_cols])
    print(f"{len(rows)} listings, {len(header)} columns -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
