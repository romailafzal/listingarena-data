-- Missing check (docs/all-us-homes-design-2026-11-19.md section 5): a home absent from a complete county fetch is no
-- longer removed at once. It gets missing_since; jobs.missing_check fetches its own page after MISSING_DAYS (7) and
-- only then marks it sold / off_market / removed. All new columns are nullable: no table rewrite.
SET LOCAL lock_timeout = '30s';
ALTER TABLE listings ADD COLUMN IF NOT EXISTS missing_since timestamptz;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_checked_at timestamptz;
-- set by the missing check: for_sale | pending | contingent | coming_soon | for_rent | sold | off_market | removed
-- (NULL = derive from status; the website maps raw status itself)
ALTER TABLE listings ADD COLUMN IF NOT EXISTS listing_state text;

CREATE TABLE IF NOT EXISTS listing_status_events (
  id bigserial PRIMARY KEY,
  listing_id bigint, property_key text, listing_type text,
  old_state text, new_state text, price numeric, source text,
  reason text,            -- search | missing_check | detail | backfill
  run_id text, at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS listing_status_events_listing ON listing_status_events (listing_id);

-- new event kinds: 'missing' (absent from a complete fetch, not removed yet) and 'state' (state set by a check)
ALTER TABLE listing_events DROP CONSTRAINT IF EXISTS listing_events_event_check;
ALTER TABLE listing_events ADD CONSTRAINT listing_events_event_check
  CHECK (event IN ('new','changed','removed','relisted','missing','state')) NOT VALID;
-- partial index for the missing-check queue: created CONCURRENTLY by hand (see jobs/missing_check.py), not here,
-- so this migration never blocks loads while it scans listings

-- listing_details.payload is no longer written (owner approved dropping it; tools/drop_detail_payload.py empties it,
-- a later migration drops it)
ALTER TABLE listing_details ALTER COLUMN payload DROP NOT NULL;
