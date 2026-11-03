# Postgres schema (`listingarena` database)

Created by `uv run la-migrate` from `db/migrations/*.sql` (applied once each, tracked in `schema_migrations`);
`la-migrate` also upserts the `counties` reference rows from `states/*.json`. Local server: `docker compose up -d postgres`
→ `postgresql://listingarena:listingarena@localhost:5434/listingarena`.

Data flow: collector → silver JSONL → **`source_records`** (one row per source listing) → merge by `property_key`
→ **`listings`** (one canonical row per property × listing type) → diff → **`listing_events`**; every load writes
**`run_county_stats`** and failures go to **`errors`**; **`runs`** groups one collection run.

## ER diagram

```mermaid
erDiagram
    runs ||--o{ run_county_stats : "run_id"
    runs ||--o{ listing_events : "run_id"
    runs ||..o{ errors : "run_id (soft)"
    counties ||..o{ run_county_stats : "county_fips (soft)"
    counties ||..o{ listings : "county_fips (soft)"
    counties ||..o{ source_records : "county_fips (soft)"
    properties ||--o{ listings : "property_key"
    properties ||..o{ source_records : "property_key (soft)"
    listings ||--o{ listing_events : "listing_id"

    runs { text run_id PK  timestamptz started_at  timestamptz finished_at  text status  text trigger  text_arr states  jsonb stats }
    counties { text fips PK  text state  text state_name  text name  text kind  float lat  float lng }
    properties { text property_key PK  text address  text unit  text city  text state  text zip  text county_fips  float lat  float lng }
    source_records { text source PK  text listing_type PK  text source_id PK  text property_key  text county_fips  jsonb data  text row_hash  bool active }
    listings { bigint id PK  text property_key FK  text listing_type  text external_id  text_arr sources  jsonb source_ids  jsonb extra  text row_hash  bool active }
    listing_events { bigint id PK  text run_id FK  bigint listing_id FK  text event  jsonb changes }
    run_county_stats { text run_id PK  text county_fips PK  text source PK  text listing_type PK  int fetched  int new  int changed  int removed  int errors }
    errors { bigint id PK  text run_id  text source  text county_fips  text kind  text message }
```

"soft" = joined by value, no foreign-key constraint (so stats/errors can be written for counties or runs before the
referenced row exists, and source rows without a usable address still load).

## Tables

### `runs` — one collection run (e.g. `20261028T06` from the schedule, or a manual id)
| column | type | meaning |
|---|---|---|
| `run_id` | text **PK** | collection run id; same id as `exports/checkpoints/<run_id>` |
| `started_at` / `finished_at` | timestamptz | load start / end (re-loading the same run resets `finished_at`) |
| `status` | text | `running`, `success` (no failed fetch), `partial` (some failed), `failed` (all failed) |
| `trigger` | text | `cli`, `dagster` |
| `states` | text[] | states loaded under this run id (last loader call) |
| `stats` | jsonb | `{"http": {source: {requests, retries, blocked, errors, latency_s}}}` |

### `counties` — reference (3,143 rows, 50 states + DC)
| column | type | meaning |
|---|---|---|
| `fips` | text **PK** | 5-digit Census county FIPS |
| `state`, `state_name` | text | `TX`, `Texas` |
| `name` | text | `Austin County`, `Orleans Parish` … |
| `kind` | text | county / parish / borough / census area / city … |
| `lat`, `lng` | float8 | centroid |
Index: `counties_state (state)`.

### `properties` — a physical home/unit, keyed by normalized address
| column | type | meaning |
|---|---|---|
| `property_key` | text **PK** | `zip5|<number> <street core>|<unit>` (directionals/suffixes dropped, routes unified, unit normalized); or `geo|lat4|lng4|unit` without a street number. See `matching/address.py` |
| `address`, `unit`, `city`, `state`, `zip`, `county_fips` | text | display address from the winning source |
| `lat`, `lng` | float8 | coordinates (kept if a later source has none) |
| `first_seen`, `last_seen` | timestamptz | |
Index: `properties_county (county_fips)`.

### `source_records` — every listing as each source reports it (latest version)
| column | type | meaning |
|---|---|---|
| `source` | text **PK** | `redfin`, `realtor` (later `homes`) |
| `listing_type` | text **PK** | `sale`, `rent`, `sold` |
| `source_id` | text **PK** | the source's id (Redfin propertyId, Realtor property_id) |
| `property_key` | text | match key (NULL when the address is unusable) |
| `county_fips`, `state` | text | county the record was collected under |
| `data` | jsonb | full normalized silver record: app columns + `url`, `fetched_at`, `extra` (MLS id/status, HOA, DOM, agents, offices, AVM estimate, tax id, rental details …) |
| `row_hash` | text | sha1 of the normalized fields (excl. `fetched_at`, `extra`) — detects source-level changes |
| `active` | bool | false once the source stops returning it in a **complete** county fetch |
| `first_seen`, `last_seen`, `removed_at` | timestamptz | |
| `first_run_id`, `last_run_id` | text | |
Indexes: PK, `source_records_county (county_fips, listing_type, source)`, `source_records_pkey (property_key, listing_type)`.

### `listings` — canonical current state (one per property × type); this is what is published to the app
| column | type | meaning |
|---|---|---|
| `id` | bigserial **PK** | |
| `property_key` | text **FK → properties** | |
| `listing_type` | text | `sale` / `rent` / `sold` (CHECK) |
| `external_id` | text | `la:<sha1(property_key)[:16]>`, stable id sent to listingarena-app |
| `address` … `status` | text/numeric/date/float8 | the app's COLS, each filled by per-field source precedence (Realtor first; Redfin first for lat/lng, sqft, status, sold_date, sold price) |
| `unit`, `county_fips` | text | |
| `sources` | text[] | sources currently backing the listing, e.g. `{realtor,redfin}` |
| `source_ids` | jsonb | `{"redfin": "44848294", "realtor": "8041856248"}` |
| `urls` | jsonb | per-source listing URLs |
| `extra` | jsonb | per-source extra fields `{"redfin": {...}, "realtor": {...}}` |
| `row_hash` | text | hash of tracked fields (price, list_price, status, beds, baths, sqft, lot_sqft, year_built, sold/listed dates, available_from, deposit, property_type) |
| `active` | bool | false after a `removed` event |
| `first_seen`, `last_seen`, `updated_at`, `removed_at` | timestamptz | |
| `first_run_id`, `last_run_id` | text | |
| `published_hash` | text | `row_hash` (or `removed:<hash>`) last sent to the app; publisher sends rows where it differs |
Unique: `(property_key, listing_type)`, `(listing_type, external_id)`. Index: `listings_county (county_fips, listing_type) WHERE active`.

### `listing_events` — change log
| column | type | meaning |
|---|---|---|
| `id` | bigserial **PK** | |
| `run_id` | text **FK → runs** | run that detected it |
| `listing_id` | bigint **FK → listings** | |
| `property_key`, `listing_type`, `county_fips` | text | denormalized for filtering |
| `event` | text | `new`, `changed`, `removed`, `relisted` (CHECK) |
| `changes` | jsonb | for changed/relisted: `{"price": {"before": 464990, "after": 519990}, ...}` |
| `created_at` | timestamptz | |
Indexes: `listing_events_run (run_id, county_fips)`, `listing_events_listing (listing_id)`.

### `run_county_stats` — one row per run × county × source × type (`source = 'merged'` = canonical level)
| column | type | meaning |
|---|---|---|
| `run_id`, `county_fips`, `source`, `listing_type` | text **PK** (`run_id` FK → runs) | |
| `state` | text | |
| `fetched` | int | records returned (merged: canonical listings) |
| `new`, `changed`, `removed`, `relisted` | int | per source: source_records transitions; merged: listing events |
| `active` | int | merged only: active listings after the load |
| `errors`, `blocks` | int | failed fetch (1) and whether it was a 403/429 block |
| `pages`, `duration_s` | int, float8 | fetch pages / seconds (merged: load seconds) |
| `complete` | bool | all pages fetched and ≥95% of the reported total; removals only happen when true |
| `loaded_at` | timestamptz | |
Index: `run_county_stats_county (county_fips, loaded_at DESC)`.

### `errors`
| column | type | meaning |
|---|---|---|
| `id` | bigserial **PK** | |
| `run_id`, `source`, `state`, `county_fips`, `listing_type` | text | context (nullable) |
| `kind` | text | `blocked`, `fetch`, `load`, `publish` |
| `message` | text | exception text (≤2,000 chars) |
| `created_at` | timestamptz | |
Index: `errors_created (created_at DESC)`.

### `schema_migrations`
`name` text PK (migration file), `applied_at`.

## Removal rule
A source record is deactivated only when that source's fetch for that county × type in this run was `complete`.
A failed, blocked or page-capped fetch leaves the source's records active, so its listings are kept. A canonical
listing is `removed` when no active source record backs it any more, and `relisted` if one comes back.

## Example queries

```sql
-- Active listings per state and type, with share matched across sources
SELECT state, listing_type, count(*) AS active,
       round(100.0 * count(*) FILTER (WHERE cardinality(sources) > 1) / count(*), 1) AS pct_multi_source
FROM listings WHERE active GROUP BY 1, 2 ORDER BY 1, 2;

-- Freshness per county (last load, any errors in its latest fetches), with names
SELECT c.state, c.name, s.county_fips, max(s.loaded_at) AS last_loaded, sum(s.errors) AS errors
FROM run_county_stats s JOIN counties c ON c.fips = s.county_fips
WHERE s.source <> 'merged' GROUP BY 1, 2, 3 ORDER BY last_loaded;

-- Price changes in the last run
SELECT l.address, l.city, e.changes->'price' AS price
FROM listing_events e JOIN listings l ON l.id = e.listing_id
WHERE e.run_id = (SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1)
  AND e.event = 'changed' AND e.changes ? 'price';

-- Full history of one listing
SELECT e.created_at, e.run_id, e.event, e.changes FROM listing_events e
JOIN listings l ON l.id = e.listing_id WHERE l.external_id = 'la:ac7eb3511325021c' ORDER BY e.id;

-- Both sources' raw view of one property
SELECT source, source_id, data->>'price' AS price, data->>'status' AS status, data->'extra'->>'mls_id' AS mls_id
FROM source_records WHERE property_key = '77474|768 main|' AND listing_type = 'sale';

-- Source health over the last 7 days
SELECT source, count(*) AS fetches, sum(errors) AS failed, sum(blocks) AS blocked, round(avg(duration_s)::numeric, 1) AS avg_s
FROM run_county_stats WHERE source <> 'merged' AND loaded_at > now() - interval '7 days' GROUP BY 1;

-- Listings waiting to be published to the app
SELECT count(*) FROM listings
WHERE published_hash IS DISTINCT FROM (CASE WHEN active THEN row_hash ELSE 'removed:' || row_hash END)
  AND (active OR published_hash IS NOT NULL);

-- Realtor HOA fee and MLS id from the per-source extras
SELECT address, extra->'realtor'->>'hoa_fee' AS hoa, extra->'realtor'->>'mls_id' AS mls
FROM listings WHERE active AND extra ? 'realtor' LIMIT 20;
```
