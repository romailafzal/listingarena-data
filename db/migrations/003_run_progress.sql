-- Live per-county/source/type progress written by jobs.collect while a run is in flight.
CREATE TABLE IF NOT EXISTS run_progress (
  run_id        text NOT NULL,
  source        text NOT NULL,
  state         text NOT NULL,
  county_fips   text NOT NULL,
  listing_type  text NOT NULL,
  status        text NOT NULL,             -- queued | running | done | failed | blocked
  pages         int  NOT NULL DEFAULT 0,
  records       int  NOT NULL DEFAULT 0,
  started_at    timestamptz,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, source, county_fips, listing_type)
);
CREATE INDEX IF NOT EXISTS run_progress_status ON run_progress (run_id, status);
CREATE INDEX IF NOT EXISTS run_progress_updated ON run_progress (updated_at DESC);
