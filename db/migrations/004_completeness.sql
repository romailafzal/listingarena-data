-- Completeness evidence per source/county/type (see docs/rate_limits.md "Completeness rule").
ALTER TABLE run_county_stats ADD COLUMN IF NOT EXISTS reported_total int;    -- source's own count for the search
ALTER TABLE run_county_stats ADD COLUMN IF NOT EXISTS unique_listings int;   -- distinct listings fetched (source listing key)
ALTER TABLE run_county_stats ADD COLUMN IF NOT EXISTS unique_records int;    -- distinct properties kept after dedup
