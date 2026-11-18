-- detail pages the site says do not exist ("Home not found"); jobs.fetch_details skips them for
-- DETAIL_NOT_FOUND_RETRY_DAYS (default 30) instead of asking again every cycle
CREATE TABLE IF NOT EXISTS detail_not_found (
  source text NOT NULL, source_id text NOT NULL, checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, source_id)
);
-- the website details sync maps a Realtor details record to Redfin-only listings through this id
CREATE INDEX IF NOT EXISTS listing_realtor_ids_pid ON listing_realtor_ids (realtor_property_id);
