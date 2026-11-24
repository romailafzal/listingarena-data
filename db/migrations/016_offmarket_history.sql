-- Off-market homes: every photo + estimate from the search page itself, and price/tax history from batched
-- home() calls (details_at = history pass done). Nullable ADD COLUMN only: no table rewrite on the live table.
ALTER TABLE offmarket_homes ADD COLUMN IF NOT EXISTS photos text[];        -- hrefs, "http://ap.rdcpix.com/" prefix stripped
ALTER TABLE offmarket_homes ADD COLUMN IF NOT EXISTS estimate bigint;
ALTER TABLE offmarket_homes ADD COLUMN IF NOT EXISTS price_history jsonb;  -- [[date, event, price, list_price, status], ...]
ALTER TABLE offmarket_homes ADD COLUMN IF NOT EXISTS tax_history jsonb;    -- [[year, tax, assessed_total, building, land, market_total], ...]
ALTER TABLE offmarket_homes ADD COLUMN IF NOT EXISTS details_at timestamptz;
ALTER TABLE offmarket_progress ADD COLUMN IF NOT EXISTS history_at timestamptz;  -- history pass of the county finished
