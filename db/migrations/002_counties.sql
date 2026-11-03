-- County reference (seeded from states/*.json by la-migrate): names for dashboards and pgAdmin
CREATE TABLE IF NOT EXISTS counties (
  fips        text PRIMARY KEY,          -- 5-digit Census FIPS
  state       text NOT NULL,             -- USPS code
  state_name  text,
  name        text NOT NULL,             -- "Austin County"
  kind        text,                      -- county | parish | borough | census area | city ...
  lat double precision, lng double precision
);
CREATE INDEX IF NOT EXISTS counties_state ON counties (state);
