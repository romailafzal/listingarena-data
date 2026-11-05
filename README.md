# ListingArena Data

Central data-ingestion repository for ListingArena. This repository will collect, normalize, validate, and publish property data for the ListingArena API and customer dashboard.

## Repository roles

- `listingarena-app`: public site, listings API, explorer, and admin import UI.
- `listingarena-api-dashboard`: customer workspace, API documentation, plans, and account experience.
- `listingarena-data`: provider connectors, scheduled jobs, normalization, quality checks, and exports.

The data repository should publish normalized records to the production database or to the protected import endpoint in `listingarena-app`:

```text
POST https://listingarena.com/api/listings
Header: x-admin-token: $ADMIN_TOKEN
Content-Type: application/json
```

Confirm the production hostname before creating scheduled jobs. Keep the URL in environment variables, never in connector code.

## Planned provider connectors

Create one connector per provider under `sources/`. Candidate sources to evaluate include the providers mentioned in the project brief: Regrid, RentCast, Inspect, Realtor, and Redfin. Before implementation, record the exact provider, approved API/product, license, rate limits, supported geography, and allowed uses in `docs/providers.md`.

Redfin and Realtor.com are collected from their public website search endpoints by owner decision (terms-of-service risk accepted; see `docs/providers.md`). Never bypass logins, CAPTCHAs or other access controls. Other providers still need an official API, licensed feed, or written permission.

## Pipeline (collect -> normalize -> merge -> load -> publish)

Sibling repos: `../listingarena-orchestrator` (Dagster, :3070) and `../listingarena-monitor` (map dashboard, :8090).
This repo is an installable package (`listingarena-data`, Python 3.12, uv).

```bash
~/.local/bin/uv sync                                   # .venv (Python 3.12) + uv.lock
cp .env.example .env                                   # DATABASE_URL=postgresql://listingarena:listingarena@localhost:5434/listingarena
docker compose up -d postgres                          # postgres:16 on 127.0.0.1:5434, volume pgdata
uv run la-migrate                                      # apply db/migrations/*.sql
uv run la-collect --states TX --counties 48015 --max-pages 8 --run-id my_run     # = python -m jobs.collect
uv run la-load --run-id my_run                         # Postgres + change tracking
uv run la-publish --dry-run /tmp/payload.json          # or with LISTINGARENA_API_URL + LISTINGARENA_ADMIN_TOKEN
uv run python -m jobs.fetch_details --since-run my_run --state TX --limit 50 --source realtor   # Phase 2 per-home detail
uv run pytest -q tests                                 # DB tests use a scratch DB listingarena_test, skip without Postgres
```

No Docker? `uv run python -m tools.dev_postgres` starts a throwaway Postgres (pgserver wheel) and prints a URL.

- **Collect** (`jobs/collect.py`, `sources/`): bronze `exports/bronze/<src>/<st>/<fips>/<run_id>/<type>_pNNNN.json.gz`,
  silver parts `exports/silver/<src>/<run_id>/parts/<st>_<fips>_<type>.jsonl`, checkpoints
  `exports/checkpoints/<run_id>/<src>/<st>/<fips>_<type>.json` (`complete` = all pages fetched and >=95% of the
  reported total) or `.error.json` (failed/blocked; rerun with the same run id retries). `http_stats.json` per run.
  Silver records carry `extra` (MLS id, HOA, DOM, agents, AVM ... see `normalization/extras.py`).
  `python -m tools.renormalize --run-id X` rebuilds silver from bronze.
- **Match/merge** (`matching/`): `address.py` builds `property_key = zip5|<number> <street core>|<unit>`
  (unit variants `#304`/`Unit 304`/`Apt 304`, directionals and suffixes dropped, FM/Hwy/TX/SH/IH route spellings
  unified; geo fallback `geo|lat4|lng4|unit` when there is no street number). `merge.py` makes one canonical
  listing per (property_key, listing_type) with per-field source precedence (Realtor default; Redfin for
  lat/lng/sqft/status/sold_date and sold price), `sources`, `source_ids`, `urls`, `extra` per source, `row_hash`.
- **Load** (`db/load.py`, `db/migrations/001_init.sql`): tables `properties`, `listings` (current state),
  `listing_events` (new/changed/removed/relisted with before/after), `source_records`, `runs`, `run_county_stats`,
  `errors`. Per county x type in one transaction: upsert source records; deactivate missing ones only when that
  source's county fetch was complete (failed/partial fetches never remove); re-merge; diff; events; stats.
- **Publish** (`publish/app.py`): POSTs changed listings (`row_hash != published_hash`) to listingarena-app
  `/api/listings` in chunks of `PUBLISH_CHUNK` (max 999) using the app's COLS; canonical `external_id = la:<hash>`;
  removed listings are sent once with `status = "removed"`.

### Incremental collection

`python -m jobs.collect ... --incremental` (or `LA_INCREMENTAL=1`) fetches, per county/type, only listings
updated since the watermark in `fetch_watermarks` (migration 007; seeded from the last complete fetch).
Sources opt in with `supports_since` + `search_since()`; today only Realtor (sorted by `last_update_date`).
Incremental checkpoints carry `incremental`, `since`, `caught_up` and are never `complete`, so the loader
removes nothing. The loader advances a watermark to the fetch's `started_at` in the load transaction, only
when the fetch was complete or caught up, and skips the county re-merge when an incremental fetch found nothing.

## Suggested layout

```text
sources/
  regrid/
  rentcast/
  inspect/
  realtor/
  redfin/
normalization/
quality/
jobs/
exports/
docs/
```

## Environment variables

Copy `.env.example` to `.env.local` and fill in only the providers that have been approved and enabled:

- `LISTINGARENA_API_URL`: production import endpoint, such as `https://listingarena.com/api/listings`
- `LISTINGARENA_ADMIN_TOKEN`: same secret configured as `ADMIN_TOKEN` in the ListingArena app
- Provider API keys: add names only after confirming each provider's official API and credentials

Never commit `.env.local`, provider keys, database passwords, or raw licensed data.

### ProxyRotator (optional)

`sources/proxy.py` adds the ProxyRotator rotating gateway (new exit IP per request, Basic auth). Nothing changes
unless `PROXYROTATOR_GATEWAY` is set. Per source family `<SOURCE>_USE_PROXYROTATOR=hybrid|proxy|off` (Redfin and
Realtor default `hybrid`; an explicit `<SOURCE>_PROXY` URL wins):

- **hybrid**: our direct IP (existing adaptive cap) and the gateway run in parallel; a request takes a proxy slot when
  one is free, otherwise goes direct. A direct 403/429 backs direct off and retries immediately through the proxy;
  while direct is backed off the source runs proxy-only.
- **proxy**: always through the gateway. **off**: direct only.

Proxy concurrency is a machine-wide cap: `PROXY_MAX_THREADS` (default 10, never above 10) flock'd slot files in
`data/proxy_slots/`, shared by all processes; a crashed process's slots free automatically. On 403/429/connection
errors the connection is dropped (fresh IP) and endpoints rotate/retire. Bandwidth is counted up+down in
`data/proxy_usage.json` (`PROXY_BUDGET_GB`, default 150; resets on `PROXY_RESET_DATE`, default 2026-11-09, monthly);
at the budget hybrid falls back to direct only, proxy mode skips (or goes direct with
`PROXYROTATOR_DIRECT_FALLBACK=1`). Credentials are masked in all logs and exceptions.

## Normalized output

The first importer accepts the fields documented in `listingarena-app/README.md`, including `listing_type`, `external_id`, address fields, property facts, pricing, dates, coordinates, description, photo URL, and status. Every connector must preserve the provider name and source record ID in its internal metadata, and must produce a stable `external_id`.

## Data workflow

1. Authenticate with an approved provider API.
2. Fetch incrementally using the provider's supported cursor or timestamp.
3. Normalize into the ListingArena schema.
4. Run validation and duplicate checks.
5. Store an audit record with source, fetched time, and source ID.
6. Publish in batches to the protected ListingArena import endpoint.
7. Record counts, rejected rows, rate-limit events, and the source request window.

## Austin County pilot

The first job targets Austin County, Texas (`county_fips=48015`). It calls configured, approved JSON APIs for Realtor, Redfin, and Homes.com. The provider URL and response paths are environment variables because these products require different contracts and access approvals. Do not copy URLs from a website's browser network panel into this repository.

The pipeline uses three layers:

- **Bronze:** immutable provider response pages under `exports/bronze/`.
- **Silver:** one normalized JSONL file per provider under `exports/silver/`.
- **Gold:** the deduplicated current snapshot under `exports/gold/`, keyed by `provider:source_id`.
- **CSV exports:** provider folders under `exports/csv/tx/48015/`, each with `sale.csv`, `rent.csv`, and `sold.csv`; `merged/` contains the three category files across all providers and `all_sources.csv` contains the complete combined export.

SQLite state in `exports/state.sqlite3` stores cursors and the last successful update time. The first run uses `--full`; subsequent runs use the saved state for incremental requests.

## Local setup

The job uses only the Python standard library:

```bash
cp .env.example .env.local
set -a; source .env.local; set +a
python3 jobs/run_austin_county.py --full
python3 jobs/run_austin_county.py
```

Add `--publish` only after the normalized contract and provider redistribution rights are approved. A scheduler can run the second command daily, for example at 02:00 UTC:

```cron
0 2 * * * cd /path/to/listingarena-data && . .env.local && python3 jobs/run_austin_county.py --publish >> exports/daily.log 2>&1
```

The normalized record retains `history`, `owner_info`, `tax_info`, and `property_details` when the licensed provider returns them. Provider-specific fields remain under `source_record` so no source information is discarded during normalization.

## Related repositories

- Public app: `../listingarena-app`
- Customer dashboard: `../listingarena-api-dashboard`
- Workspace map: `../README.md`

## Steps

The names used in logs, Dagster and the monitor (`jobs/steps.py`; every worker count in `jobs/settings.py`):

| Step | What it does | Run with | Concurrency (env, default) |
|---|---|---|---|
| **Collect** | Searches every county on Redfin and Realtor at the same time, merges by address, records new / changed / removed / relisted | `jobs.collect` | states one after another, `PIPELINE_GAP_HOURS` 3 between runs |
| **Clean** | Stable ids, `row_hash`, drops site-specific columns | `normalization` | inside collect |
| **Load** | Upserts into PostgreSQL and appends `listing_events` | `db.load` | one transaction per batch |
| **Details** | Each listing's own page: price events, sales, taxes, schools | `jobs.fetch_details` | `DETAILS_WORKERS` 32 |
| **Redfin-only** | Realtor property ids of listings only Redfin has, so Details reads their page | `jobs.redfin_only` | `REDFIN_ONLY_WORKERS` 8, `REDFIN_ONLY_RATE` 5/s |
| **Photos** | First `PHOTOS_PER_LISTING` (3) photos of active rentals (+ sale in `SALE_PHOTOS_STATES`) to `PHOTOS_DIR` | `jobs.photos` | `PHOTOS_WORKERS` 8, `PHOTOS_RATE` 15/s |
| **Floor plans** | Redfin rental buildings' units, daily / every 3 days / weekly by how often they change | `jobs.floor_plans` | `FLOOR_PLANS_WORKERS` 8, `FLOOR_PLANS_RATE` 5/s |

Each extras job runs every `<JOB>_EVERY_MINUTES` (30) apart from the listings runs; a 403/429 pauses all of that job's workers.
