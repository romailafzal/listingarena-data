-- Incremental refresh: per (source, county, listing_type) time up to which the source's changes are loaded.
-- An incremental fetch sorts by the source's update time and stops paging at (fetched_through - overlap).
-- Advanced by the loader in the same transaction that loads the fetch, from the checkpoint's started_at.
CREATE TABLE IF NOT EXISTS fetch_watermarks (
  source          text NOT NULL,
  county_fips     text NOT NULL,
  listing_type    text NOT NULL,
  fetched_through timestamptz NOT NULL,
  run_id          text,
  full_sweep      boolean NOT NULL DEFAULT false,      -- set by a complete (non-incremental) fetch
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, county_fips, listing_type)
);

-- Seed from the latest complete, error-free fetch of each unit; the run's start time is a conservative bound.
INSERT INTO fetch_watermarks (source, county_fips, listing_type, fetched_through, run_id, full_sweep)
SELECT DISTINCT ON (s.source, s.county_fips, s.listing_type) s.source, s.county_fips, s.listing_type, r.started_at, s.run_id, true
FROM run_county_stats s JOIN runs r USING (run_id)
WHERE s.source <> 'merged' AND s.complete AND s.errors = 0
ORDER BY s.source, s.county_fips, s.listing_type, r.started_at DESC
ON CONFLICT DO NOTHING;
