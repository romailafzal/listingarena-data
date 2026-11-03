-- ListingArena pipeline schema
CREATE TABLE IF NOT EXISTS runs (
  run_id       text PRIMARY KEY,
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  status       text NOT NULL DEFAULT 'running',      -- running | success | partial | failed
  trigger      text,                                  -- cli | dagster | schedule
  states       text[],
  stats        jsonb NOT NULL DEFAULT '{}'            -- {"http": {source: {requests, blocked, latency_s...}}, ...}
);

CREATE TABLE IF NOT EXISTS properties (
  property_key text PRIMARY KEY,
  address      text, unit text, city text, state text, zip text, county_fips text,
  lat double precision, lng double precision,
  first_seen   timestamptz NOT NULL DEFAULT now(),
  last_seen    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS properties_county ON properties (county_fips);

CREATE TABLE IF NOT EXISTS source_records (
  source        text NOT NULL,
  listing_type  text NOT NULL,
  source_id     text NOT NULL,
  property_key  text,
  county_fips   text,
  state         text,
  data          jsonb NOT NULL,                       -- full normalized silver record incl. extra
  row_hash      text NOT NULL,
  active        boolean NOT NULL DEFAULT true,
  first_seen    timestamptz NOT NULL DEFAULT now(),
  last_seen     timestamptz NOT NULL DEFAULT now(),
  first_run_id  text, last_run_id text, removed_at timestamptz,
  PRIMARY KEY (source, listing_type, source_id)
);
CREATE INDEX IF NOT EXISTS source_records_county ON source_records (county_fips, listing_type, source);
CREATE INDEX IF NOT EXISTS source_records_pkey ON source_records (property_key, listing_type);

CREATE TABLE IF NOT EXISTS listings (
  id            bigserial PRIMARY KEY,
  property_key  text NOT NULL REFERENCES properties(property_key),
  listing_type  text NOT NULL CHECK (listing_type IN ('sale','rent','sold')),
  external_id   text NOT NULL,
  address text, unit text, city text, state text, zip text, county_fips text, property_type text,
  beds numeric, baths numeric, sqft numeric, lot_sqft numeric, year_built int,
  price numeric, list_price numeric, sold_date date, listed_date date, available_from date, deposit numeric,
  lat double precision, lng double precision, description text, photo_url text, status text,
  sources       text[] NOT NULL,
  source_ids    jsonb NOT NULL DEFAULT '{}',
  urls          jsonb NOT NULL DEFAULT '{}',
  extra         jsonb NOT NULL DEFAULT '{}',          -- {source: {mls_id, hoa, dom, agents...}}
  row_hash      text NOT NULL,
  active        boolean NOT NULL DEFAULT true,
  first_seen    timestamptz NOT NULL DEFAULT now(),
  last_seen     timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  removed_at    timestamptz,
  first_run_id text, last_run_id text,
  published_hash text,                                -- row_hash last pushed to listingarena-app
  UNIQUE (property_key, listing_type),
  UNIQUE (listing_type, external_id)
);
CREATE INDEX IF NOT EXISTS listings_county ON listings (county_fips, listing_type) WHERE active;

CREATE TABLE IF NOT EXISTS listing_events (
  id           bigserial PRIMARY KEY,
  run_id       text REFERENCES runs(run_id),
  listing_id   bigint REFERENCES listings(id),
  property_key text NOT NULL,
  listing_type text NOT NULL,
  county_fips  text,
  event        text NOT NULL CHECK (event IN ('new','changed','removed','relisted')),
  changes      jsonb NOT NULL DEFAULT '{}',            -- {field: {before, after}}
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS listing_events_run ON listing_events (run_id, county_fips);
CREATE INDEX IF NOT EXISTS listing_events_listing ON listing_events (listing_id);

-- one row per run x county x source x type; source = 'merged' holds canonical-listing level counts
CREATE TABLE IF NOT EXISTS run_county_stats (
  run_id       text NOT NULL REFERENCES runs(run_id),
  state        text NOT NULL,
  county_fips  text NOT NULL,
  source       text NOT NULL,
  listing_type text NOT NULL,
  fetched int NOT NULL DEFAULT 0, new int NOT NULL DEFAULT 0, changed int NOT NULL DEFAULT 0,
  removed int NOT NULL DEFAULT 0, relisted int NOT NULL DEFAULT 0, active int,
  errors int NOT NULL DEFAULT 0, blocks int NOT NULL DEFAULT 0,
  pages int, duration_s double precision, complete boolean NOT NULL DEFAULT false,
  loaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, county_fips, source, listing_type)
);
CREATE INDEX IF NOT EXISTS run_county_stats_county ON run_county_stats (county_fips, loaded_at DESC);

CREATE TABLE IF NOT EXISTS errors (
  id           bigserial PRIMARY KEY,
  run_id       text,
  source       text, state text, county_fips text, listing_type text,
  kind         text NOT NULL,                          -- blocked | fetch | load | publish
  message      text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS errors_created ON errors (created_at DESC);
