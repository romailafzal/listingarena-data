# Blocking and concurrency log

What concurrency each source runs at, per route (our own IP vs the ProxyRotator gateway), which levels were
refused and which ran clean. Search-endpoint benchmarks live in `docs/rate_limits.md`; this file is the
running log for everything, detail and photo fetchers included.

Legend: **[measured]** = read from a log / counter file named next to it. **[inferred]** = our reading of the
evidence, not directly observed. **[reference]** = taken from the owner's reference pipeline
(`datarivo_pipeline-main/listings/*`), measured by them, not by us.

## ProxyRotator plan and our caps

| item | plan limit | our cap (env, default) | where enforced |
|---|---|---|---|
| concurrent threads | 10 | `PROXY_MAX_THREADS=10` (hard max 10), machine-wide flock slots in `PROXY_SLOT_DIR` (`data/proxy_slots`) | `sources/proxy.py` `GlobalSlots`, `pool_from_env` |
| bandwidth / day | 10 GB (plan pauses) | `PROXY_DAILY_GB=5`, resets at local midnight | `sources/proxy.py` `UsageMeter` |
| bandwidth / month | 300 GB | `PROXY_BUDGET_GB=150` | same |
| renewal | 2026-11-09 | `PROXY_RESET_DATE=2026-11-09` | same |
| routing per source | - | `<SOURCE>_USE_PROXYROTATOR` = `hybrid` (default for REDFIN/REALTOR when a gateway is set) / `proxy` / `off`; `<SOURCE>_PROXY` overrides | `sources/proxy.py` `mode_for` |

Counter on 2026-11-07 01:19 (`data/proxy_usage.json`) [measured]: period 5.22 GB / 3,239 requests since
2026-10-09; today 0.20 GB. The previous day ended at 5.016 GB = the daily cap, after which every process
logged `[proxy] bandwidth cap reached (period 5.02 GB, today 5.02 GB): proxied traffic stopped`.

## Knobs (all in `listingarena-orchestrator/.env` or the job's environment)

| knob | used by | meaning |
|---|---|---|
| `<SOURCE>_MAX_INFLIGHT` | `sources/http.py` `SourceClient` | adaptive cap on simultaneous calls per process (search) |
| `<SOURCE>_PAGE_PARALLEL` | `sources/http.py`, `jobs/collect.py` | parallel pages inside one county/type |
| `<SOURCE>_MIN_INTERVAL` | `sources/http.py` | spacing between request starts (0 = none) |
| `LA_<SOURCE>_WORKERS` / `realtor_workers` | orchestrator | county/type tasks in parallel |
| `<SOURCE>_DETAIL_MAX_INFLIGHT`, `<SOURCE>_DETAIL_MIN_INTERVAL` | `sources/details.py` `detail_client` (default 3 / 1.0 s) | per-home detail client |
| `--inflight` / `DETAIL_INFLIGHT` (default `DETAILS_WORKERS`=32), `DETAIL_MAX_BLOCKS` (3) | `jobs/fetch_details.py` | homes in parallel; circuit breaker |
| `REDFIN_ONLY_WORKERS`=8, `REDFIN_ONLY_RATE`=5/s | `jobs/redfin_only.py` | Realtor address-suggest lookups |
| `PHOTOS_WORKERS`=8, `PHOTOS_RATE`=15/s, `PHOTOS_PAUSE`=120 | `jobs/photos.py` | photo downloads |
| `FLOOR_PLANS_WORKERS`=8, `FLOOR_PLANS_RATE`=5/s, `LISTINGS_FLOOR_PLANS_WORKERS`=16 | `jobs/floor_plans.py` | Redfin rental floor plans |

## Per source x route

### Realtor listings (GraphQL `home_search`)
- **Own IP** [measured, `docs/rate_limits.md`, 2026-10-28]: 20 and 40 in flight: 0 x 403/429. Production
  10 workers x 20 pages = 200 in flight: 0 blocks over the full Texas run. Latency-bound, not blocked.
- **Gateway** [measured, `logs/us_refresh_20261106T1758/*.log`, 2026-11-06 17:58 -> 11-07 01:12, par=8
  states, cap/process=16, realtor_workers=30, hybrid]: 501 `ProxyError ... Tunnel connection failed: 429 Too
  Many Requests` on `/frontdoor/graphql`, 61 tasks `failed after 5 attempts`. The 429 is from the **gateway**
  (CONNECT refused), not from Realtor. Cause [inferred]: more than 10 concurrent tunnels across processes,
  and/or daily cap / plan throttling. 7 states rc=1, 7 rc=137 (idle-kill).

### Realtor details (GraphQL `home` on `/frontdoor/graphql`)
- **Own IP, clean** [measured, `logs/overnight_details/details_*.log`, `run.sh`, 2026-11-06 ~17:30 -> 11-07 00:35]:
  `--inflight 30`, `REALTOR_DETAIL_MIN_INTERVAL=0`, 8 states in parallel (`xargs -P 8`) = up to 240 in flight
  from one IP. Finished states: AZ 14,206 homes / 16,949 s, GA 12,570, IL 15,252, LA 2,285, NC 18,223,
  NY 10,888 — all `blocked: 0`, `proxied: 0`, errors 0-20 (timeouts). ~0.8-1.2 homes/s per state.
  Earlier 2026-11-03 FL run (`logs/us_refresh_20261103T1522/extras/details.log`): 7,350 homes, blocked 0.
- **Own IP, refusals** [measured]: 10 `HTTP 403 on POST .../frontdoor/graphql` across all overnight logs —
  isolated, recovered on retry. No sustained block seen at 240 in flight.
- **Gateway, unsafe** [measured]: after local midnight 2026-11-07 the daily counter reset, hybrid mode sent
  traffic through the gateway again, and CA FL MI PA SC TN TX WA (still running) logged ~7,700
  `Tunnel connection failed: 429 Too Many Requests` (+ ~40 x 502). E.g. TX: last clean line
  `16850/40449 homes, 22195s` then 429s. Proxy threads=10 (global cap). The 8 states were re-queued with
  the proxy off (`overnight_details/rerun.sh`, `det_overnight2`). [inferred] The gateway 429s even at our
  10-thread cap; either the plan counts CONNECTs per second, or slots leaked across 8 processes.

### Redfin listings (stingray `gis`, `search/rentals`)
- **Own IP** [measured, rate_limits.md 2026-10-28]: 20 in flight -> 59 x 403 (2.6%) from minute ~3, all
  recovered on 60 s retry; 10 in flight -> 20 x 403. Production 200 cap: 0 blocks in Texas tail (cap never
  reached). Rule of thumb: sporadic 403 above ~5 req/s bursts, not a hard block.
- **Gateway / hybrid** [measured, us_refresh_20261106T1758]: 167 `HTTP 403` from Redfin (159 gis, 8 rentals)
  plus several hundred gateway `Tunnel ... 429` on `/stingray/api/gis`, 54 tasks `failed after 5 attempts`.
- [reference] Owner's pipeline: search 4 workers (`collect/redfin.py:75`), floor plans 4 workers at 5/s with
  0.3 s pause: "Redfin blocks after ~1,000 requests at 8 parallel with no pause" (`collect/floor_plan_cache.py:33`).

### Redfin details (stingray `/api/home/details/aboveTheFold|belowTheFold`)
- **Own IP: blocked** [measured, rate_limits.md + `sources/details.py:305`]: CloudFront 403 since the
  2026-11-02 10 x 20 = 200-in-flight attempt; still 403 days later at 1 request. Search endpoints on the
  same IP keep working, so it is a path-level rule, not an IP ban [inferred].
- **Gateway: blocked** [reported by operator, not in these logs]: 403 from every ProxyRotator exit.
  [inferred] fingerprint (TLS/JA3 + header order of python-requests), not IP reputation.
- No clean level known. ~500K Redfin-only homes have no details from this route. See Findings.

### Redfin-only -> Realtor address match (`parser-external.geo.moveaws.com/suggest`)
- **Own IP, clean** [measured, `overnight_details/redfin_only.log`, 2026-11-06]: 8 workers, rate 5/s:
  100,000 listings in 49,130 s, found 35,284, `refusals: 0`, errors 0.

### Photos
- Ours: `PHOTOS_WORKERS=8`, `PHOTOS_RATE=15`, pause 120 s on refusal. No block evidence in our logs yet.
- [reference] `listings/photos.py:36-48`: Realtor photo host 403s one IP after ~23,000/day at ~100/s, and
  after ~2,500-4,500 *new* photos at 12-20/s even with browser image headers and 8 connections. Expect the
  same; plan a proxy or several IPs for photos.

### Zillow
Not implemented; nothing to log.

## Measured safe / unsafe summary

| source | route | clean at | blocked at | date |
|---|---|---|---|---|
| Realtor search | own IP | 200 in flight (10x20) | - | 2026-10-28 |
| Realtor search | gateway | - | 16/proc x 8 states via 10 threads: tunnel 429 | 2026-11-06/07 |
| Realtor details | own IP | 30 x 8 states = 240, MIN_INTERVAL 0 | 10 isolated 403s only | 2026-11-06 |
| Realtor details | gateway | - | 10 threads: tunnel 429 (gateway) | 2026-11-07 00:00+ |
| Redfin search | own IP | 10-200 with retries (2-3% 403) | sustained >5 req/s -> sporadic 403 | 2026-10-28 |
| Redfin search | gateway | - | 403 from Redfin + tunnel 429 | 2026-11-06/07 |
| Redfin details | own IP | none | 1 request: CloudFront 403 | since 2026-11-02 |
| Redfin details | gateway | none | 403 on every exit | 2026-11 (operator) |
| Realtor suggest | own IP | 8 workers @ 5/s | - | 2026-11-06 |

## How to log a new block

Add a row (newest first) and keep the evidence path:

| date (local, +05:00) | source / endpoint | route (own IP / gateway / explicit proxy) | concurrency (processes x inflight, MIN_INTERVAL, rate) | symptom (status, body/CDN, after N requests) | fix / next level | evidence (log path, line) |
|---|---|---|---|---|---|---|
| 2026-11-07 00:00 | Realtor `/frontdoor/graphql` | gateway (hybrid) | 8 x 30, interval 0, proxy 10 threads | `Tunnel connection failed: 429` | proxy off for Realtor details, rerun 8 states own IP | `logs/overnight_details/details_TX.log:354` |
| 2026-11-02 | Redfin `home/details/*` | own IP | 10 x 20 | CloudFront 403, persists at 1 req | stopped; see Findings | `docs/rate_limits.md`, `sources/details.py:305` |

Tips: grep `HTTP 403|HTTP 429|Tunnel connection failed|BlockedError|cap ->|bandwidth cap` in the run's logs;
print the first refusal's headers + body (the reference does this, `listings/photos.py:89-92,119`) to tell a CDN
(CloudFront/Akamai/PerimeterX) block from an app 429 or a gateway 429.

## Findings from the reference pipeline (part A)

Reference = `/home/zsquare/Desktop/projects/data/datarivo_pipeline-main (1)/datarivo_pipeline-main/`.
`datarivo_scrapper-main` is county-records scrapers only (no Redfin/Realtor code).

1. **The reference never calls Redfin's home-details endpoint.** For Redfin-only homes it looks the address
   up in Realtor's suggest API and reads the *Realtor* property page instead
   (`listings/redfin_only.py:18-33`, `URL` at :38; `listings/details.py:27-28`). Its Redfin traffic is only
   search (`collect/redfin.py:21-22`) and rental floor plans (`collect/floor_plan_cache.py:32`). We already
   ported this (`jobs/redfin_only.py`), and it ran clean (35% match rate on 100K). **Recommendation: treat the
   Realtor route as the primary source of details for Redfin-only homes and finish the lookup for all ~500K
   before spending more on Redfin details.**
2. **No TLS impersonation anywhere.** No `curl_cffi`/`impersonate` in either repo; plain `requests`. So the
   reference does not prove impersonation works, but it also never hit the details path.
3. **Header consistency.** Reference sends one fixed Chrome identity per job with the full client-hint set and
   a state-page referer (`collect/floor_plan_cache.py:45-59`: `sec-ch-ua` v155, `priority: u=1, i`,
   `sec-fetch-site: same-origin`, `referer: https://www.redfin.com/state/<State>`). Ours picks a random
   identity **per request** (`sources/http.py:30-55`, `browser_headers()` called per attempt at :209),
   mixing Chrome/Edge/Firefox/Safari UAs on one keep-alive connection whose TLS hello is always
   python-requests, and Redfin details get only `referer: https://www.redfin.com/` (`sources/details.py:309`).
   A UA that changes per request on one TLS session, and Firefox/Safari UAs over an OpenSSL hello, are classic
   bot signals [inferred].
4. **Pacing model.** Reference: one shared pace for all workers (`wait_turn`, `details.py:94-98`,
   `redfin_only.py:81-85`) and **every** worker pauses 120 s on the first 403/429 (`refused`,
   `details.py:101-109`). Ours: per-attempt backoff and an adaptive cap (`sources/http.py:197-276`), with
   jitter, but other workers keep firing during a refusal wave [inferred: prolongs CloudFront rate rules].
5. **Persistent per-thread sessions** through a proxy (`collect/floor_plan_cache.py:223-228`, comment: a new
   TLS per request costs ~7 KB over a ~0.6 KB answer). Ours also keeps thread-local sessions
   (`sources/http.py:191-195`); fine, but matters for proxy bandwidth.
6. **Separate exits per job** (`listings/proxy.py:7-12`: `DETAILS_PROXY`, `PHOTOS_PROXY`, `FLOOR_PLANS_PROXY`,
   `REDFIN_ONLY_PROXY`) so a block on one job does not reach another. Ours shares one gateway and one IP.
7. **Concurrency.** Reference Redfin: 4 workers, 5/s, 0.3 s pause. Realtor details: 32 workers, 20/s shared
   (`details.py:57-58`; "25/s for 600 pages was fine", :42). Our Redfin details attempt at 200 in flight on
   2026-11-02 is ~40x the reference's Redfin level and is the likely trigger of the CloudFront rule [inferred].

### Recommended next experiments for Redfin details (one at a time, log each in the table above)
1. Finish `jobs.redfin_only` for all Redfin-only homes, then `jobs.fetch_details --source realtor` for them
   (proven clean on own IP). Measure how many of the ~500K get Realtor details; only the remainder needs Redfin.
2. Single-request probe matrix from own IP and one gateway exit, 1 request per minute:
   (a) current headers; (b) fixed Chrome identity + full `sec-ch-ua*`, `priority`, home-page referer
   (`https://www.redfin.com/<ST>/<City>/<addr>/home/<id>`), no UA rotation; (c) (b) via `curl_cffi`
   `impersonate="chrome"` (TLS + HTTP/2 fingerprint); (d) (c) after a warm-up GET of the home page to collect
   CloudFront/`RF_*` cookies; (e) the HTML home page itself instead of the stingray API. Record status,
   `x-amz-cf-id`/`server`/body for each.
3. If any variant returns 200: run it at 1 in flight, shared pace 1/s, all-worker 120 s pause on refusal;
   raise only after a full day clean, never above the reference's 4 workers / 5 per second.
4. Give Redfin details its own exit (explicit `REDFIN_DETAIL_PROXY`), separate from search, so a block there
   cannot spill onto Redfin search.
5. Fix the gateway 429: verify slots are released (`data/proxy_slots`), and test 5 then 10 concurrent
   CONNECTs from one process before running several processes through it.
