-- listing_details.payload held a second copy of each normalized detail page (~50 GB, on the hdd tablespace). The
-- typed columns, JSON columns and bronze files keep everything; jobs.fetch_details stopped writing it (migration 012
-- made it nullable). Dropping is instant; the space comes back when the table is rewritten
-- (VACUUM FULL listing_details in a window with no details job). Owner approved deleting it (2026-11-19).
SET LOCAL lock_timeout = '5s';
ALTER TABLE listing_details DROP COLUMN IF EXISTS payload;
