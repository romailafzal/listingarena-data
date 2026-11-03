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
