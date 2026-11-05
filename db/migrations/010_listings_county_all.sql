-- The loader reads every listing of a (county, type), active or not (removed ones can be relisted), but the only
-- county index was partial (WHERE active), so each unit did a full scan of `listings`.
-- On a large live DB build it first without blocking writers:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS listings_county_type_all ON listings (county_fips, listing_type);
CREATE INDEX IF NOT EXISTS listings_county_type_all ON listings (county_fips, listing_type);
