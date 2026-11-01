-- Phase 2: per-home detail (jobs.fetch_details). One row per source home (latest fetch); 1-to-many in child tables.
CREATE TABLE IF NOT EXISTS listing_details (
  source            text NOT NULL,                     -- redfin | realtor
  source_id         text NOT NULL,                     -- Redfin propertyId / Realtor property_id (= source_records.source_id)
  listing_type      text,                              -- type of the listing that triggered the fetch
  listing_id        bigint,                            -- listings.id (soft link, no FK)
  property_key      text,
  county_fips       text,
  state             text,
  source_listing_id text,                              -- Redfin listingId / Realtor listing_id
  url               text,
  status            text,
  list_price        numeric,
  list_date         date,
  last_sold_price   numeric,
  last_sold_date    date,
  description       text,
  property_type     text,
  beds numeric, baths numeric, baths_full numeric, baths_half numeric,
  sqft numeric, lot_sqft numeric, year_built int, stories numeric, garage numeric,
  parking text, heating text, cooling text, construction text, style text,
  hoa_fee           numeric,                           -- monthly
  tax_annual        numeric,
  tax_year          int,
  assessed_value    numeric,
  property_tax_rate float8,
  lat float8, lng float8,
  apn               text,
  neighborhood      text,
  school_district   text,
  mls_name text, mls_id text, mls_listing_id text,
  list_office text, list_office_phone text, list_office_email text, list_broker text,
  list_agent_name text, list_agent_phone text, list_agent_email text, list_agent_license text,
  buyer_agent_name text, buyer_office text,
  photo_count       int,
  photos            jsonb,                             -- ordered [{href, tags}]
  virtual_tours     jsonb,                             -- [{type, href}] (virtual tours, videos, matterport)
  features          jsonb,                             -- {category: ["Key: value", ...]} as the source groups them
  interior_features jsonb, exterior_features jsonb, appliances jsonb,
  agents            jsonb,                             -- all advertiser / agent / buyer blocks as returned
  mortgage          jsonb,                             -- estimate + monthly payment breakdown + rates
  estimates         jsonb,                             -- AVM values by provider
  open_houses       jsonb,
  flags             jsonb,
  extra             jsonb,
  payload           jsonb NOT NULL,                    -- full normalized detail (incl. child lists)
  row_hash          text NOT NULL,                     -- sha1 of payload; unchanged hash => child rows not rewritten
  bronze_path       text,
  run_id            text,                              -- detail run id that last fetched it
  first_fetched_at  timestamptz NOT NULL DEFAULT now(),
  fetched_at        timestamptz NOT NULL DEFAULT now(),
  changed_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, source_id)
);
CREATE INDEX IF NOT EXISTS listing_details_listing ON listing_details (listing_id);
CREATE INDEX IF NOT EXISTS listing_details_county ON listing_details (county_fips);

CREATE TABLE IF NOT EXISTS listing_detail_history (    -- property history events (list / price change / sale / rent ...)
  source text NOT NULL, source_id text NOT NULL, seq int NOT NULL,
  event_date date, event text, price numeric, price_sqft numeric, source_name text, mls_listing_id text, status text,
  PRIMARY KEY (source, source_id, seq)
);
CREATE TABLE IF NOT EXISTS listing_detail_taxes (
  source text NOT NULL, source_id text NOT NULL, year int NOT NULL,
  tax numeric, assessed_total numeric, assessed_building numeric, assessed_land numeric, market_total numeric,
  PRIMARY KEY (source, source_id, year)
);
CREATE TABLE IF NOT EXISTS listing_detail_schools (
  source text NOT NULL, source_id text NOT NULL, seq int NOT NULL,
  name text, rating numeric, distance_mi numeric, levels jsonb, grades jsonb, funding text, district text,
  PRIMARY KEY (source, source_id, seq)
);
CREATE TABLE IF NOT EXISTS listing_detail_comps (      -- similar / nearby homes returned with the subject
  source text NOT NULL, source_id text NOT NULL,       -- subject
  comp_source_id text NOT NULL,                        -- same source's id; join source_records(source, source_id)
  kind text NOT NULL DEFAULT 'similar',
  rank int, comp_listing_id text, status text, price numeric, sold_date date,
  beds numeric, baths numeric, sqft numeric, address text, zip text, lat float8, lng float8, distance_mi numeric, url text,
  PRIMARY KEY (source, source_id, kind, comp_source_id)
);
CREATE INDEX IF NOT EXISTS listing_detail_comps_comp ON listing_detail_comps (source, comp_source_id);
