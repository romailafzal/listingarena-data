# Provider Evaluation

Complete this table before implementing a connector. Use the provider's official API or licensed feed documentation as the source of truth.

| Provider | Exact product/API | Credentials approved | Coverage | Rate limits | License/use restrictions | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Regrid | TBD | No | TBD | TBD | TBD | Evaluate |
| RentCast | TBD | No | TBD | TBD | TBD | Evaluate |
| Inspect | TBD | No | TBD | TBD | TBD | Confirm exact provider |
| Realtor | TBD | No | TBD | TBD | TBD | Confirm official API/product before enabling |
| Redfin | TBD | No | TBD | TBD | TBD | Confirm official API/product before enabling |
| Homes.com | TBD | No | TBD | TBD | TBD | Confirm official API/product before enabling |

## Evaluation checklist

- Confirm the exact company and API product name.
- Confirm that automated access is allowed for the intended use.
- Record authentication method, endpoint, pagination, and incremental-sync support.
- Record fields, geography, freshness, rate limits, pricing, attribution, and retention rules.
- Obtain written approval for any data that will be redistributed through ListingArena.
- Store credentials only in the deployment secret manager or `.env.local`.

## Pilot approval gate

The Austin County job supports these provider environment variables, but does not imply access is approved:

- `REALTOR_API_URL` and `REALTOR_API_KEY`
- `REDFIN_API_URL` and `REDFIN_API_KEY`
- `HOMES_API_URL` and `HOMES_API_KEY`

Enable a provider only after its official API agreement confirms county search, historical records, pagination, update timestamps, owner/tax/history fields, rate limits, and redistribution rights. Website browser endpoints are not an acceptable substitute.
