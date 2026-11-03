# Provider Evaluation

## Decision (2026-10-28): Redfin and Realtor.com public website search endpoints

The owner has decided to collect listing search results from **Redfin** and **Realtor.com** through the same
public JSON endpoints their websites use, and **accepts the terms-of-service risk** of doing so (neither site
grants permission for automated access; either may block us, change the endpoints, or object). This supersedes the
earlier rule that website browser endpoints are not an acceptable substitute for an official API, for these two
providers only. Mitigations we keep: low request rate per source, backoff on 403/429, no logins or access-control
bypass, no CAPTCHA solving, raw data kept internal (bronze) until redistribution is reviewed.

| Provider | Endpoint used | Credentials | Coverage | Pace / limits seen | Restrictions | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Redfin | `GET /stingray/api/gis` (sale: `sf=1,2,3,5,6,7&status=9`; sold: `mpt=99&sold_within_days=N`, no `sf`), `GET /stingray/api/v1/search/rentals` (rent). Region = county `region_id` + `region_type=5`; polygon (`poly=`) fallback for rentals. JSON prefixed with `{}&&`. | None | 3,118 of 3,143 counties have a region id | 350 homes/page; default 1 req / ~1.5 s, 2 counties in parallel. Reference pipeline reports blocks above ~8 parallel unpaced. | Website ToS; risk accepted | **Enabled** (`sources/redfin.py`) |
| Realtor.com | `POST https://www.realtor.com/frontdoor/graphql`, `home_search` (operationName `ConsumerSearchQuery`, headers `rdc-client-name`/`rdc-client-version`). Introspection disabled; widest accepted field set discovered by `tools/realtor_fields.py`. | None | All counties via `"<County>, <ST>"` location | 200 results/page, max 10,000 per search (offset+limit) -> split by price band; default 1 req / ~2 s | Website ToS; risk accepted. Sold prices are null in non-disclosure states (e.g. TX) | **Enabled** (`sources/realtor.py`) |
| Homes.com | - | - | - | - | - | Next phase |
| Regrid | TBD | No | TBD | TBD | TBD | Evaluate |
| RentCast | TBD | No | TBD | TBD | TBD | Evaluate |
| Inspect | TBD | No | TBD | TBD | TBD | Confirm exact provider |

## Operating rules for the website sources

- Keep per-source pacing (`REDFIN_MIN_INTERVAL`, `REALTOR_MIN_INTERVAL`) and worker caps conservative; raise only gradually.
- On repeated 403/429 the client backs off for minutes and then fails the county (checkpoint not written, so a later run resumes it).
- Optional proxies: `REDFIN_PROXY`, `REALTOR_PROXY`, or `HTTPS_PROXY`.
- Do not redistribute raw responses; publishing normalized data still needs a separate review.

## Checklist for other providers

- Confirm the exact company and API product name, and whether automated access is allowed.
- Record authentication, endpoint, pagination, incremental sync, fields, geography, freshness, rate limits, pricing, attribution and retention.
- Store credentials only in the deployment secret manager or `.env.local`.

## Phase 2: per-home detail endpoints (`sources/details.py`, `jobs/fetch_details.py`)

| Source | Endpoint | Key | Returns | Status (2026-11-02 smoke) |
| --- | --- | --- | --- | --- |
| Realtor.com | `POST /frontdoor/graphql`, `home(property_id: ID!)` (operationName `GetHome`, same `rdc-client-*` headers as search) | `source_records.source_id` (= `property_id`) | description + structured `details{category text}` (rooms, interior/exterior, appliances, heating/cooling, construction, style, parking, utilities, HOA, school info), beds/baths full/half, sqft, lot, year built, stories, garage; `property_history` (date, event, price, $/sqft, MLS name, MLS listing id); `tax_history` (tax, assessed + market building/land/total); `tax_record` (APN, parcel id); `mortgage` (tax rate, insurance rate, estimate: loan, down payment, rate/term, monthly breakdown); `estimates.current_values` (AVMs by provider); `schools` (rating, distance, levels, district); `advertisers` (agent name/email/phone/license, office name/email/phone, broker), `source` (MLS id/name/listing number, agents incl. buyer side), `buyers`; all `photos` (with room tags), `virtual_tours`, `videos`, `matterport`; coordinates, neighborhoods, county; `related_homes(query:{type:similar_homes})` (comps: property_id, listing_id, status, price, beds/baths/sqft, address, coordinates) | **Working**: 6 homes fetched sequentially, 0 errors, ~1 s each |
| Redfin | `GET /stingray/api/home/details/aboveTheFold` and `/belowTheFold` `?propertyId=&listingId=&accessLevel=1` (`listingId` from `source_records.data.extra.listing_id`, optional) | Redfin `propertyId` | above: photos, remarks, address section (beds/baths/sqft/lot/year/price/status/lat-lng), listing + buying agents, MLS id; below: property history events, public records (APN, tax, assessed land/improvement), schools + districts, amenities groups (heating/cooling, appliances, ...), HOA dues | **Blocked from this IP**: CloudFront 403 HTML on every detail path (search `gis` still works). Parser built from the known payload shape and tested on hand-made fixtures only; field paths unverified. Run Redfin details only after the block clears, through a proxy (`REDFIN_DETAIL_PROXY`), at 1 in flight. |

Detail fetchers use their own clients (`realtor_detail`, `redfin_detail`), never the search concurrency. Defaults: 3 in
flight per source (`--inflight`, `<SOURCE>_DETAIL_MAX_INFLIGHT`), adaptive 3 -> 1 after 3 blocks in 2 min, 1 s between
request starts (`<SOURCE>_DETAIL_MIN_INTERVAL`), 10 s connect / 20 s read timeout, 3 attempts (90 s x attempt after a
403/429), and the job stops a source after 3 consecutive blocked homes (`DETAIL_MAX_BLOCKS`). Non-JSON, empty or
in-band-error responses are logged to `errors` (kind `detail`) and skipped; they are retried on the next run.
