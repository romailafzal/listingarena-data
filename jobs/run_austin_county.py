"""Run the Austin County, TX listing sync."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import (
    County, ProviderConfig, StateStore, fetch_json, normalize_record, read_path, utc_now,
    write_csv, write_jsonl,
)


PROVIDERS = ("realtor", "redfin", "homes")


def provider_config(name: str) -> ProviderConfig | None:
    endpoint = os.getenv(f"{name.upper()}_API_URL")
    if not endpoint:
        return None
    records_path = tuple(filter(None, os.getenv(f"{name.upper()}_RECORDS_PATH", "data").split(".")))
    cursor_path = tuple(filter(None, os.getenv(f"{name.upper()}_NEXT_CURSOR_PATH", "next_cursor").split(".")))
    return ProviderConfig(
        name=name,
        endpoint=endpoint,
        api_key_env=os.getenv(f"{name.upper()}_API_KEY_ENV", f"{name.upper()}_API_KEY"),
        records_path=records_path,
        next_cursor_path=cursor_path,
        cursor_param=os.getenv(f"{name.upper()}_CURSOR_PARAM", "cursor"),
        updated_since_param=os.getenv(f"{name.upper()}_UPDATED_SINCE_PARAM", "updated_since"),
    )


def publish(records: list[dict], endpoint: str, token: str) -> None:
    body = json.dumps(records).encode("utf-8")
    request = Request(endpoint, data=body, method="POST", headers={
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-admin-token": token,
    })
    with urlopen(request, timeout=60) as response:
        if response.status >= 300:
            raise RuntimeError(f"ListingArena publish failed with HTTP {response.status}")


def run(full_sync: bool, publish_enabled: bool) -> dict[str, int]:
    county = County()
    fetched_at = utc_now()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bronze_dir = ROOT / "exports" / "bronze" / county.state.lower() / county.fips / run_id
    silver_dir = ROOT / "exports" / "silver" / county.state.lower() / county.fips / run_id
    gold_dir = ROOT / "exports" / "gold" / county.state.lower() / county.fips
    csv_dir = ROOT / "exports" / "csv" / county.state.lower() / county.fips
    store = StateStore(ROOT / "exports" / "state.sqlite3")
    counts = {"providers": 0, "fetched": 0, "normalized": 0, "rejected": 0, "gold": 0}
    try:
        for name in PROVIDERS:
            config = provider_config(name)
            if config is None:
                continue
            counts["providers"] += 1
            previous_cursor, previous_updated = store.state(name)
            cursor = None if full_sync else previous_cursor
            updated_since = None if full_sync else previous_updated
            provider_records: list[dict] = []
            while True:
                payload = fetch_json(config, county, cursor=cursor, updated_since=updated_since)
                raw_path = bronze_dir / name / f"page-{len(provider_records)}.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
                raw_records = read_path(payload, config.records_path)
                if raw_records is None and isinstance(payload.get("results"), list):
                    raw_records = payload["results"]
                if not isinstance(raw_records, list):
                    raise ValueError(f"{name} response did not contain a list at {'.'.join(config.records_path)}")
                normalized = [normalize_record(name, item, county, fetched_at) for item in raw_records if isinstance(item, dict)]
                provider_records.extend(normalized)
                next_cursor = read_path(payload, config.next_cursor_path)
                if not next_cursor or next_cursor == cursor:
                    cursor = next_cursor
                    break
                cursor = str(next_cursor)
            write_jsonl(silver_dir / f"{name}.jsonl", provider_records)
            store.save(name, provider_records, cursor, fetched_at)
            counts["fetched"] += len(provider_records)
            counts["normalized"] += len(provider_records)

        gold_records = store.records()
        write_jsonl(gold_dir / "listings.jsonl", gold_records)
        for provider in PROVIDERS:
            provider_records = store.records(provider)
            for category in ("sale", "rent", "sold"):
                write_csv(
                    csv_dir / provider / f"{category}.csv",
                    (record for record in provider_records if record.get("listing_type") == category),
                )
        for category in ("sale", "rent", "sold"):
            write_csv(
                csv_dir / "merged" / f"{category}.csv",
                (record for record in gold_records if record.get("listing_type") == category),
            )
        write_csv(csv_dir / "merged" / "all_sources.csv", gold_records)
        counts["gold"] = len(gold_records)
        if publish_enabled:
            endpoint = os.getenv("LISTINGARENA_API_URL")
            token = os.getenv("LISTINGARENA_ADMIN_TOKEN")
            if not endpoint or not token:
                raise RuntimeError("Publishing requires LISTINGARENA_API_URL and LISTINGARENA_ADMIN_TOKEN")
            publish(gold_records, endpoint, token)
        return counts
    finally:
        store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true", help="Ignore sync state and backfill provider history")
    parser.add_argument("--publish", action="store_true", help="Publish the gold snapshot to ListingArena")
    args = parser.parse_args()
    print(json.dumps(run(full_sync=args.full, publish_enabled=args.publish), sort_keys=True))