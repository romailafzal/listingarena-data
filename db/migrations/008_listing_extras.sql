-- Listing extras, the jobs that run apart from the listings runs (jobs/steps.py): Photos, Floor plans, Redfin-only.

-- Photos (jobs.photos): each listing's first PHOTOS_PER_LISTING photos, downloaded once and stored by link hash.
CREATE TABLE IF NOT EXISTS listing_photos (
  listing_id     bigint NOT NULL,                    -- listings.id
  position       int    NOT NULL,                    -- 0 = main photo
  source_url     text   NOT NULL,
  storage_key    text,                               -- <state>/<sha1 of source_url>.jpg under PHOTOS_DIR / the bucket
  bytes          int,
  downloaded_at  timestamptz,
  download_error text,                               -- '404' counts as done; anything else is retried next time
  PRIMARY KEY (listing_id, position)
);
CREATE INDEX IF NOT EXISTS listing_photos_todo ON listing_photos (listing_id) WHERE downloaded_at IS NULL;

-- Floor plans (jobs.floor_plans): one row per Redfin rental building, refreshed as often as its units change.
CREATE TABLE IF NOT EXISTS floor_plans (
  rental_id   text PRIMARY KEY,                      -- Redfin rentalExtension.rentalId
  listing_id  bigint,
  state       text,
  data        jsonb,                                 -- Redfin's floorPlans answer
  units_key   text,                                  -- rent / deposit / beds / size of every unit, to spot real changes
  fetched_on  date,
  changed_on  date,                                  -- last day the units really changed
  error       text
);

-- Redfin-only (jobs.redfin_only): Realtor property ids of listings only Redfin has, so Details reads their page too.
CREATE TABLE IF NOT EXISTS realtor_address_lookups (
  address             text PRIMARY KEY,              -- the address searched
  realtor_property_id text,
  realtor_address     text,
  error               text,                          -- 'not found' is retried after REDFIN_ONLY_RETRY_DAYS
  looked_up_at        timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS listing_realtor_ids (
  listing_id          bigint PRIMARY KEY,
  realtor_property_id text NOT NULL,
  found_by            text NOT NULL DEFAULT 'address',
  found_at            timestamptz NOT NULL DEFAULT now()
);
