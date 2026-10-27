"""Silver schema: the app's COLS (listingarena-app/lib/db.js) plus source metadata and extras."""
from __future__ import annotations

from datetime import datetime, timezone

APP_COLS = ["listing_type", "external_id", "address", "city", "state", "zip", "property_type", "beds", "baths",
            "sqft", "lot_sqft", "year_built", "price", "list_price", "sold_date", "listed_date", "available_from",
            "deposit", "lat", "lng", "description", "photo_url", "status"]
META_COLS = ["source", "source_id", "county_fips", "url", "fetched_at", "extra"]
SILVER_FIELDS = APP_COLS + META_COLS


def blank_record() -> dict:
    return {k: None for k in SILVER_FIELDS}


def ms_to_date(ms) -> str | None:
    if ms in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def iso_date(s) -> str | None:
    return s[:10] if isinstance(s, str) and len(s) >= 10 else None


def num(v):
    if isinstance(v, dict):
        v = v.get("value")
    if v in (None, ""):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f.is_integer() else f
