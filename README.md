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

Do not scrape websites or bypass access controls. Use an official API, licensed feed, or written permission. Realtor and Redfin access may be restricted or product-specific, so they must be verified before a connector is built.

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
