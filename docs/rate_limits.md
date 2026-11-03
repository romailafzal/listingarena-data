# Source rate limits (search/map APIs) — measured 2026-10-28 from this PC (no proxy)

Search/map endpoints only (Redfin stingray `gis` / `search/rentals`, Realtor GraphQL `home_search`).
**Per-home detail APIs block at much lower concurrency — never apply these numbers to detail fetchers**;
give them their own `SourceClient` with their own `*_MAX_INFLIGHT` / `*_MIN_INTERVAL`.

## How concurrency works (sources/http.py, jobs/collect.py)
- `--workers` (orchestrator: `LA_<SOURCE>_WORKERS`) = county/type tasks in parallel per source.
- `<SOURCE>_PAGE_PARALLEL` = parallel page fetches inside one county/type (ordered; Redfin speculative
  window doubles when no total is reported; Realtor fetches all offsets up to the 10k cap after page 1).
- `<SOURCE>_MAX_INFLIGHT` = hard cap on simultaneous HTTP calls (AdaptiveLimiter). On sustained blocks
  (>= 8 403/429/captcha in 60 s) the cap drops cap -> cap/2 -> cap/4 (20 -> 10 -> 5 per worker) and logs it;
  after 15 min without blocks it climbs one level, never above the configured cap.
- `<SOURCE>_MIN_INTERVAL` = spacing between request starts. Slots are reserved under a lock but the sleep is
  outside it, so 0 = no serialization. Earlier defaults (1.5–2 s) capped each source at ~30–40 req/min total.
- Retries: 5 attempts; 403/429/captcha back off 60 s × attempt, 5xx/network 2^attempt s. BlockedError after all.

## Benchmark steps (6 min each, real Texas counties via jobs.collect, data kept)
| step | source | in flight | req/min | p50 / p95 latency | 403 | 429 | 5xx | captcha/HTML | BlockedError |
|---|---|---|---|---|---|---|---|---|---|
| 1 | redfin  | 20 workers × 1 = 20 | 373 (626 last min) | 0.87 / 2.09 s | 59 (2.6%, from min ~3) | 0 | 0 | 0 | 0 (all recovered on retry) |
| 1 | realtor | 20 × 1 = 20 | 276 | 3.28 / 5.71 s | 0 | 0 | 0 | 0 | 0 |
| 2 | redfin  | 10 × 1 = 10 | 336 | 0.82 / 2.09 s | 20 | 0 | 3 | 0 | 0 |
| 2 | realtor | 40 × 1 = 40 | 316 | 5.62 / 8.56 s | 0 | 0 | 0 | 0 | 0 |

Observations: Redfin returns sporadic 403s once it sees > ~5 req/s bursts, but they clear on the 60 s retry
(no hard block). Realtor latency grows with concurrency (server-side queueing) — throughput rose only
~15% from 20 to 40 in flight, so Realtor is latency-bound, not blocked.

## Production setting (user decision)
10 workers × 20 parallel pages = **200 in flight per source**, both sources at once, adaptive back-off
200 -> 100 -> 50. Set in `listingarena-orchestrator/.env` (backup `.env.bak-<ts>`).
See the full Texas run results below.

## Full Texas run `tx_full_20261028T0228` (Dagster, both sources together)
Benchmark-step data (bench_*_w20 runs + steps in this run id) was merged into this run's checkpoints, so
nothing was re-fetched. Production phase at 10 × 20 = 200 in flight per source:

| source | county-types | records | phase | requests | req/min | 403/429/5xx | cap changes |
|---|---|---|---|---|---|---|---|
| redfin  | 756/756 supported | 431,889 | 02:48–02:51 (last 26 tasks) | 357 | ~123 | 0 / 0 / 0 | none |
| realtor | 762/762 | 477,288 | 02:58–03:10 (last ~180 tasks) | 917 | ~82 | 0 / 0 / 0 | none |

The production tail was dominated by small counties (1–3 pages), so requests/min there are latency-bound
(Realtor ~3.5 s p50); the 200 cap was never reached and no back-off triggered. Big counties (Harris, Dallas,
Bexar: 130–200 pages each) completed in a few minutes each with parallel pages + parallel price-band probing.
Total wall time for Texas collection across all steps ≈ 45 min of fetching; 0 errors, 0 blocks.
