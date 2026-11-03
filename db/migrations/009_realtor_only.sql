-- Realtor-only (jobs.realtor_only): Redfin property ids of listings only Realtor has, so Details reads their Redfin
-- page too. The mirror of 008's Redfin-only tables.
CREATE TABLE IF NOT EXISTS redfin_address_lookups (
  address            text PRIMARY KEY,               -- the address searched
  redfin_property_id text,
  redfin_address     text,
  error              text,                           -- 'not found' is retried after REDFIN_ONLY_RETRY_DAYS
  looked_up_at       timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS listing_redfin_ids (
  listing_id         bigint PRIMARY KEY,
  redfin_property_id text NOT NULL,
  found_by           text NOT NULL,                  -- 'record': same address in our own Redfin records; 'address': search
  found_at           timestamptz NOT NULL DEFAULT now()
);
