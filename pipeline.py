"""Core primitives for the Austin County listing ingestion pilot."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    endpoint: str
    api_key_env: str | None = None
    records_path: tuple[str, ...] = ()
    next_cursor_path: tuple[str, ...] = ()
    cursor_param: str = "cursor"
    updated_since_param: str = "updated_since"


@dataclass(frozen=True)
class County:
    name: str = "Austin"
    state: str = "TX"
    fips: str = "48015"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def read_path(value: Any, path: Iterable[str]) -> Any:
    for part in path:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def stable_id(provider: str, record: dict[str, Any]) -> str:
    source_id = record.get("id") or record.get("listing_id") or record.get("property_id")
    if source_id is not None:
        return f"{provider}:{source_id}"
    address = "|".join(str(record.get(key, "")).strip().lower() for key in ("address", "city", "state", "zip"))
    return f"{provider}:hash:{hashlib.sha256(address.encode()).hexdigest()[:24]}"


def fetch_json(config: ProviderConfig, county: County, cursor: str | None = None, updated_since: str | None = None) -> dict[str, Any]:
    params = {"county": county.name, "state": county.state, "fips": county.fips}
    if cursor:
        params[config.cursor_param] = cursor
    if updated_since:
        params[config.updated_since_param] = updated_since
    url = f"{config.endpoint}?{urllib.parse.urlencode(params)}"
    headers = {"Accept": "application/json", "User-Agent": "ListingArenaData/0.1"}
    if config.api_key_env and os.getenv(config.api_key_env):
        headers["Authorization"] = f"Bearer {os.environ[config.api_key_env]}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read())
    if not isinstance(payload, dict):
        raise ValueError(f"{config.name} returned a JSON {type(payload).__name__}, expected an object")
    return payload


def normalize_record(provider: str, record: dict[str, Any], county: County, fetched_at: str) -> dict[str, Any]:
    listing_type = str(record.get("listing_type") or record.get("type") or "sale").lower()
    status = str(record.get("status") or "active").lower()
    if listing_type == "sold" or status in {"sold", "closed"}:
        listing_type = "sold"
    elif listing_type in {"for_sale", "sale"}:
        listing_type = "sale"
    elif listing_type in {"for_rent", "rent", "rental"}:
        listing_type = "rent"
    return {
        "external_id": stable_id(provider, record),
        "listing_type": listing_type,
        "status": record.get("status") or ("sold" if listing_type == "sold" else "active"),
        "address": record.get("address") or record.get("street_address"),
        "city": record.get("city"),
        "state": record.get("state") or county.state,
        "zip": record.get("zip") or record.get("postal_code"),
        "county": county.name,
        "county_fips": county.fips,
        "price": record.get("price") or record.get("list_price") or record.get("sold_price"),
        "beds": record.get("beds") or record.get("bedrooms"),
        "baths": record.get("baths") or record.get("bathrooms"),
        "sqft": record.get("sqft") or record.get("living_area"),
        "lot_sqft": record.get("lot_sqft") or record.get("lot_size"),
        "latitude": record.get("latitude") or record.get("lat"),
        "longitude": record.get("longitude") or record.get("lng") or record.get("lon"),
        "listed_at": record.get("listed_at") or record.get("list_date"),
        "sold_at": record.get("sold_at") or record.get("sale_date"),
        "history": record.get("history") or record.get("property_history") or [],
        "owner_info": record.get("owner_info") or record.get("owner") or {},
        "tax_info": record.get("tax_info") or record.get("tax") or {},
        "property_details": record.get("property_details") or record.get("details") or {},
        "description": record.get("description"),
        "photo_url": record.get("photo_url") or record.get("thumbnail"),
        "source": provider,
        "source_record": record,
        "fetched_at": fetched_at,
    }


class StateStore:
    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                provider TEXT PRIMARY KEY,
                cursor TEXT,
                updated_since TEXT,
                last_run TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS listings (
                external_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                payload TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );
            """
        )

    def state(self, provider: str) -> tuple[str | None, str | None]:
        row = self.connection.execute(
            "SELECT cursor, updated_since FROM sync_state WHERE provider = ?", (provider,)
        ).fetchone()
        return (row[0], row[1]) if row else (None, None)

    def save(self, provider: str, records: Iterable[dict[str, Any]], cursor: str | None, fetched_at: str) -> None:
        for record in records:
            self.connection.execute(
                "INSERT INTO listings(external_id, provider, payload, fetched_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(external_id) DO UPDATE SET payload=excluded.payload, fetched_at=excluded.fetched_at",
                (record["external_id"], provider, json.dumps(record, sort_keys=True), fetched_at),
            )
        self.connection.execute(
            "INSERT INTO sync_state(provider, cursor, updated_since, last_run) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(provider) DO UPDATE SET cursor=excluded.cursor, updated_since=excluded.updated_since, last_run=excluded.last_run",
            (provider, cursor, fetched_at, fetched_at),
        )
        self.connection.commit()

    def records(self, provider: str | None = None) -> list[dict[str, Any]]:
        if provider:
            rows = self.connection.execute(
                "SELECT payload FROM listings WHERE provider = ? ORDER BY external_id", (provider,)
            ).fetchall()
        else:
            rows = self.connection.execute("SELECT payload FROM listings ORDER BY external_id").fetchall()
        return [json.loads(row[0]) for row in rows]

    def close(self) -> None:
        self.connection.close()


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record, sort_keys=True) + "\n")


CSV_FIELDS = (
    "external_id", "listing_type", "status", "address", "city", "state", "zip", "county",
    "county_fips", "price", "beds", "baths", "sqft", "lot_sqft", "latitude", "longitude",
    "listed_at", "sold_at", "history", "owner_info", "tax_info", "property_details",
    "description", "photo_url", "source", "fetched_at",
)


def csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)
    return value


def write_csv(path: Path, records: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({field: csv_value(record.get(field)) for field in CSV_FIELDS})