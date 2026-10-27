"""Concurrency benchmark: run jobs.collect for one source for N seconds and report per-request metrics.

python -m tools.bench_concurrency --source redfin --workers 20 --seconds 360 --states TX
Uses MIN_INTERVAL=0 (pure in-flight cap). Prints one JSON line of results, then exits.
"""
from __future__ import annotations

import argparse, json, os, statistics, sys, threading, time

import requests

LOG: list[tuple[float, float, int, bool]] = []  # (t_end, latency, status, looks_json)
_orig = requests.Session.request


def _rec(self, method, url, *a, **kw):
    t0 = time.monotonic()
    try:
        r = _orig(self, method, url, *a, **kw)
    except Exception:
        LOG.append((time.monotonic(), time.monotonic() - t0, -1, False))
        raise
    body = r.text.lstrip()[:20]
    LOG.append((time.monotonic(), time.monotonic() - t0, r.status_code,
                body.startswith(("{", "[")) or r.status_code == 404))
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--seconds", type=int, default=360)
    ap.add_argument("--states", default="TX")
    ap.add_argument("--run-id", default=None, help="shared run id so steps resume each other (checkpoints)")
    a = ap.parse_args()
    os.environ[f"{a.source.upper()}_MIN_INTERVAL"] = "0"
    requests.Session.request = _rec
    from jobs import collect
    rid = a.run_id or f"bench_{a.source}_w{a.workers}_{int(time.time())}"
    t = threading.Thread(target=collect.main, daemon=True, args=([
        "--states", a.states, "--sources", a.source, "--workers", str(a.workers),
        "--source-workers", f"{a.source}={a.workers}", "--run-id", rid],))
    start = time.monotonic()
    t.start()
    t.join(a.seconds)
    el = time.monotonic() - start
    lat = sorted(x[1] for x in LOG if x[2] > 0)
    st = [x[2] for x in LOG]
    last60 = [x for x in LOG if x[0] > start + el - 60]
    res = dict(source=a.source, workers=a.workers, run_id=rid, seconds=round(el), requests=len(LOG),
               req_per_min=round(len(LOG) / el * 60, 1),
               p50=round(statistics.median(lat), 2) if lat else None,
               p95=round(lat[int(len(lat) * .95)], 2) if lat else None,
               s403=st.count(403), s429=st.count(429), s5xx=sum(1 for s in st if 500 <= s < 600),
               neterr=st.count(-1), non_json=sum(1 for x in LOG if x[2] == 200 and not x[3]),
               last60_rpm=len(last60), last60_bad=sum(1 for x in last60 if x[2] in (403, 429) or not x[3]))
    print("BENCH " + json.dumps(res), flush=True)
    os._exit(0)


if __name__ == "__main__":
    main()
