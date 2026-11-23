-- Off-market homes (not listed now): one row per home (property_key = location + unit), filled by jobs.offmarket
-- in idle time. Kept out of `listings` so the website sync does not pick them up yet. Bulk data: hdd tablespace.
CREATE TABLE IF NOT EXISTS offmarket_homes (
  property_key text PRIMARY KEY,
  status text NOT NULL DEFAULT 'off_market',   -- always off_market here; source_status says what the search said
  source_status text,                          -- sold | off_market
  realtor_id text, county_fips text, state text,
  address text, city text, zip text, lat double precision, lng double precision,
  property_type text, beds real, baths real, sqft integer, lot_sqft bigint, year_built smallint,
  last_sold_date date, last_sold_price bigint, list_price bigint, photo_url text,
  first_seen timestamptz NOT NULL DEFAULT now(), last_seen timestamptz NOT NULL DEFAULT now()
) TABLESPACE hdd;
CREATE INDEX IF NOT EXISTS offmarket_homes_county ON offmarket_homes (county_fips) TABLESPACE hdd;
-- one row per county pass: what it cost (requests, time, blocks) and whether paging reached every home
CREATE TABLE IF NOT EXISTS offmarket_progress (
  county_fips text PRIMARY KEY, state text, started_at timestamptz, finished_at timestamptz, complete boolean,
  api_total integer, fetched integer, homes integer, requests integer, blocked integer, errors integer,
  seconds real, bands integer, note text
);
