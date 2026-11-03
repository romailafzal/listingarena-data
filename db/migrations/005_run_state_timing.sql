-- Wall-clock timing per state partition of a multi-state run (written by orchestrator us_run.sh).
CREATE TABLE IF NOT EXISTS run_state_timing (
  run_id      text NOT NULL,
  state       text NOT NULL,
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  exit_code   int,
  records     bigint,          -- source records fetched (sum of run_county_stats.fetched, both sources)
  PRIMARY KEY (run_id, state)
);
