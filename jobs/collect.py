"""Collect search results per county from listing sources.

python -m jobs.collect --states TX,FL --sources redfin,realtor --types sale,rent,sold --workers 4
    [--counties 48015,48453] [--max-pages 2] [--run-id ID] [--out exports] [--incremental]

--incremental: for sources with search_since (Realtor), fetch only listings updated since the county's watermark
(fetch_watermarks, minus LA_INCREMENTAL_OVERLAP_MIN, default 60) and stop paging there; such fetches are never
"complete", so the loader removes nothing. Counties without a watermark get a full fetch; sources without
search_since are skipped (they are refreshed by the full sweep).

Layout (run_id defaults to today's date, so re-running the same day resumes):
  exports/bronze/<source>/<st>/<fips>/<run_id>/<type>_p0001.json.gz   raw response pages
  exports/silver/<source>/<run_id>.jsonl                              normalized records (assembled from parts)
  exports/checkpoints/<run_id>/<source>/<st>/<fips>_<type>.json        per-county completion marker
  exports/replay/<run_id>/<source>/                                   every search response, replayed on resume
"""
from __future__ import annotations

import argparse
import os
import gzip
import json
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from sources.base import LISTING_TYPES, County, Source  # noqa: E402
from sources.http import BlockedError  # noqa: E402
from jobs.progress import Progress  # noqa: E402


def make_source(name: str) -> Source:
    if name == "redfin":
        from sources.redfin import RedfinSource
        return RedfinSource()
    if name == "realtor":
        from sources.realtor import RealtorSource
        return RealtorSource()
    raise SystemExit(f"unknown source {name!r}")


def load_watermarks(sources: list[str], overlap_min: float) -> dict:
    """(source, fips, type) -> fetched_through - overlap, for incremental fetches."""
    from db.migrate import connect
    with connect() as conn:
        rows = conn.execute("SELECT source, county_fips, listing_type, fetched_through FROM fetch_watermarks "
                            "WHERE source = ANY(%s)", (sources,)).fetchall()
    return {(s, f, t): ts - timedelta(minutes=overlap_min) for s, f, t, ts in rows}


def load_counties(states: list[str], fips: set[str] | None) -> list[County]:
    out = []
    for st in states:
        cfg = json.loads((ROOT / "states" / f"{st.lower()}.json").read_text())
        for c in cfg["counties"]:
            if fips and c["fips"] not in fips:
                continue
            extra = {k: v for k, v in c.items() if k not in ("name", "fips", "redfin_region_id", "realtor_location")}
            out.append(County(st.upper(), c["name"], c["fips"], c.get("redfin_region_id"), c.get("realtor_location"), extra))
    return out


def completeness_tolerance(total: int) -> int:
    """Listings may be added/removed while a big search is paged: allow max(2, 0.2%) shortfall."""
    return max(2, -(-total * 2 // 1000))


def is_complete(pages: int, max_pages: int | None, total: int | None, unique_listings: int) -> bool:
    """Complete = every page/band fetched without error (else no checkpoint is written at all), not cut by
    --max-pages, and distinct listings >= the source's reported total - tolerance. Listings are counted by the
    source's listing key (Realtor: property_id+listing_id, because its total counts MLS listings and the same
    home often appears once per MLS feed); records are then deduped per property."""
    if max_pages is not None and pages >= max_pages:
        return False
    return not total or unique_listings >= total - completeness_tolerance(total)


class Collector:
    def __init__(self, out: Path, run_id: str, max_pages: int | None, watermarks: dict | None = None):
        self.out, self.run_id, self.max_pages = out, run_id, max_pages
        self.watermarks = watermarks  # None = full fetch; else (source, fips, type) -> since (incremental)
        self.lock = threading.Lock()
        self.results: list[dict] = []
        self.progress = None

    def checkpoint(self, src: str, c: County, t: str) -> Path:
        return self.out / "checkpoints" / self.run_id / src / c.state.lower() / f"{c.fips}_{t}.json"

    def part(self, src: str, c: County, t: str) -> Path:
        return self.out / "silver" / src / self.run_id / "parts" / f"{c.state.lower()}_{c.fips}_{t}.jsonl"

    def run_one(self, source: Source, c: County, t: str) -> dict:
        cp = self.checkpoint(source.name, c, t)
        if not cp.exists():
            from jobs.memguard import wait_for_memory
            wait_for_memory(lambda m: print(m, flush=True), f"{source.name} county task")
        if cp.exists():
            res = json.loads(cp.read_text()); res["resumed"] = True
            if self.progress:
                self.progress.set(source.name, c.state, c.fips, t, "done", res.get("pages", 0), res.get("records", 0))
            return res
        pr = self.progress.set if self.progress else (lambda *a, **k: None)
        pr(source.name, c.state, c.fips, t, "running")
        bronze = self.out / "bronze" / source.name / c.state.lower() / c.fips / self.run_id
        bronze.mkdir(parents=True, exist_ok=True)
        seen, keys, rows, pages, total = set(), set(), [], 0, None
        lkey = getattr(source, "listing_key", None)
        started, started_at = time.time(), datetime.now(timezone.utc).isoformat()
        since = (self.watermarks or {}).get((source.name, c.fips, t))
        caught_up = None
        pager = source.search_since(c, t, since, self.max_pages) if since else source.search(c, t, self.max_pages)
        for page in pager:
            if since:
                caught_up = page.meta.get("caught_up", caught_up)
            pages += 1
            if total is None:  # the first (unfiltered) page's total is the search total; band pages report band totals
                total = page.meta.get("total")
            fetched = datetime.now(timezone.utc).isoformat()
            with gzip.open(bronze / f"{t}_p{page.number:04d}.json.gz", "wt", encoding="utf-8") as f:
                json.dump({"source": source.name, "listing_type": t, "county_fips": c.fips, "fetched_at": fetched,
                           "request": page.request, "meta": page.meta, "response": page.response}, f)
            for raw in page.records:
                rec = source.normalize(raw, t)
                keys.add(lkey(raw) if lkey else rec["source_id"])
                if rec["source_id"] in seen:
                    continue
                seen.add(rec["source_id"])
                rec["county_fips"], rec["fetched_at"] = c.fips, fetched
                rows.append(rec)
            pr(source.name, c.state, c.fips, t, "running", pages, len(rows))
        part = self.part(source.name, c, t)
        part.parent.mkdir(parents=True, exist_ok=True)
        part.write_text("".join(json.dumps(r) + "\n" for r in rows))
        res = {"source": source.name, "state": c.state, "county": c.name, "fips": c.fips, "type": t,
               "pages": pages, "records": len(rows), "unique_listings": len(keys), "reported_total": total,
               "complete": not since and is_complete(pages, self.max_pages,
                                                     total if getattr(source, "total_is_exact", lambda _t: True)(t) else None,
                                                     len(keys)),
               "seconds": round(time.time() - started, 1), "started_at": started_at}
        if since:
            res.update(incremental=True, since=since.isoformat(), caught_up=bool(caught_up))
        cp.parent.mkdir(parents=True, exist_ok=True)
        cp.write_text(json.dumps(res))
        cp.with_suffix(".error.json").unlink(missing_ok=True)
        pr(source.name, c.state, c.fips, t, "done", pages, len(rows))
        return res

    def record_error(self, src: str, c: County, t: str, e: Exception, blocked: bool) -> None:
        """Failed county fetch: no checkpoint (so a rerun retries) but an error marker the loader reads."""
        p = self.checkpoint(src, c, t).with_suffix(".error.json")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"source": src, "state": c.state, "fips": c.fips, "type": t, "blocked": blocked,
                                 "error": f"{type(e).__name__}: {e}"[:2000],
                                 "at": datetime.now(timezone.utc).isoformat()}))
        if self.progress:
            self.progress.set(src, c.state, c.fips, t, "blocked" if blocked else "failed")

    def assemble(self, src: str) -> Path:
        parts = sorted((self.out / "silver" / src / self.run_id / "parts").glob("*.jsonl"))
        dest = self.out / "silver" / src / f"{self.run_id}.jsonl"
        with dest.open("w") as f:
            for p in parts:
                f.write(p.read_text())
        return dest


class LiveLoader(threading.Thread):
    """Loads newly finished counties into Postgres every `every` seconds while collecting (LA_LIVE_LOAD=1),
    so the DB / map fill in as data arrives. Best effort: failures are logged; the final load reconciles."""

    def __init__(self, out: Path, run_id: str, states: list[str], every: float):
        super().__init__(daemon=True, name="live-load")
        self.out, self.run_id, self.states, self.every = out, run_id, states, every
        self.pending: set[str] = set()
        self.lock, self.stop = threading.Lock(), threading.Event()

    def add(self, fips: str) -> None:
        with self.lock:
            self.pending.add(fips)

    def flush(self) -> None:
        with self.lock:
            fips, self.pending = self.pending, set()
        if not fips:
            return
        try:
            from db.load import load_run
            from db.migrate import connect
            with connect() as conn:
                t0 = time.time()
                # one live loader at a time per run (each source's collect process has its own loader)
                conn.execute("SELECT pg_advisory_lock(hashtext(%s))", ("live:" + self.run_id,))
                conn.commit()
                r = load_run(conn, self.out, self.run_id, set(self.states), fips, trigger="live", log=lambda *_: None)
            print(f"live-load: {len(fips)} counties, {r.get('units', 0)} units, active={r.get('active', 0)} "
                  f"new={r.get('new', 0)} in {time.time() - t0:.0f}s", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"live-load failed: {type(e).__name__}: {e}", flush=True)
            with self.lock:
                self.pending |= fips

    def run(self) -> None:
        while not self.stop.wait(self.every):
            self.flush()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--states", required=True)
    ap.add_argument("--sources", default="redfin,realtor")
    ap.add_argument("--types", default="sale,rent,sold")
    ap.add_argument("--workers", type=int, default=4, help="total concurrent county tasks")
    ap.add_argument("--source-workers", default="", help="per-source caps, e.g. redfin=2,realtor=3")
    ap.add_argument("--counties", default="", help="comma-separated FIPS filter")
    ap.add_argument("--max-pages", type=int, default=None, help="pages per county/type (testing)")
    ap.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d"))
    ap.add_argument("--out", default=str(ROOT / "exports"))
    ap.add_argument("--incremental", action="store_true", default=os.environ.get("LA_INCREMENTAL", "0") == "1")
    a = ap.parse_args(argv)

    types = [t.strip() for t in a.types.split(",") if t.strip()]
    for t in types:
        if t not in LISTING_TYPES:
            raise SystemExit(f"unknown type {t!r}")
    sources = {n: make_source(n.strip()) for n in a.sources.split(",") if n.strip()}
    watermarks = None
    if a.incremental:
        skip = [n for n, s in sources.items() if not s.supports_since]
        if skip:
            print(f"incremental: skipping {', '.join(skip)} (no updated-since search; full sweep refreshes them)", flush=True)
        sources = {n: s for n, s in sources.items() if s.supports_since}
        watermarks = load_watermarks(list(sources), float(os.environ.get("LA_INCREMENTAL_OVERLAP_MIN", 60)))
    caps = dict(kv.split("=") for kv in a.source_workers.split(",") if "=" in kv)
    sems = {n: threading.BoundedSemaphore(int(caps.get(n, s.workers))) for n, s in sources.items()}
    counties = load_counties([s.strip() for s in a.states.split(",")], set(filter(None, a.counties.split(","))) or None)
    # every search response of this run is kept, so a crash or restart mid-county replays it instead of refetching
    os.environ["LA_REPLAY_DIR"] = str(Path(a.out) / "replay" / a.run_id)
    col = Collector(Path(a.out), a.run_id, a.max_pages, watermarks)

    tasks = [(s, c, t) for c in counties for s in sources.values() for t in types if s.supports(c)]
    skipped = [(s.name, c.fips) for c in counties for s in sources.values() if not s.supports(c)]
    col.progress = Progress(a.run_id)
    col.progress.queue_many([(s.name, c.state, c.fips, t) for s, c, t in tasks])
    print(f"run {a.run_id}: {len(tasks)} tasks over {len(counties)} counties; {len(skipped)} unsupported county/source pairs", flush=True)

    def job(s, c, t):
        with sems[s.name]:
            return col.run_one(s, c, t)

    live = None
    if os.environ.get("LA_LIVE_LOAD", "0") == "1":
        live = LiveLoader(Path(a.out), a.run_id, [x.strip().upper() for x in a.states.split(",")],
                          float(os.environ.get("LA_LIVE_LOAD_SECONDS", 60)))
        live.start()

    failures = 0
    pool = ThreadPoolExecutor(max_workers=a.workers)
    try:
        futs = {pool.submit(job, *task): task for task in tasks}
        for f in as_completed(futs):
            s, c, t = futs[f]
            try:
                r = f.result()
                if live and not r.get("resumed"):
                    live.add(c.fips)
                print(f"{'resumed ' if r.get('resumed') else ''}{s.name:8} {c.state} {c.fips} {c.name:22} {t:5} "
                      f"pages={r['pages']} records={r['records']} total={r['reported_total']}", flush=True)
            except Exception as e:  # noqa: BLE001 - keep other counties going; checkpoint not written
                failures += 1
                blocked = isinstance(e, BlockedError)
                print(f"{'BLOCKED' if blocked else 'FAILED'} {s.name} {c.fips} {t}: {e}", flush=True)
                if not blocked:
                    traceback.print_exc()
                col.record_error(s.name, c, t, e, blocked)
    except BaseException:
        # Ctrl-C / Dagster termination: drop queued counties, let in-flight ones finish or die with the process.
        # Finished counties are checkpointed, so rerunning the same run_id resumes where this stopped.
        print("collect: interrupted; cancelling queued counties (resume with the same --run-id)", flush=True)
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown(wait=True)
    col.progress.flush()
    if live:
        live.stop.set()
        live.flush()
    summary = {}
    for n, s in sources.items():
        print(f"{n}: silver -> {col.assemble(n)}; http {s.client.stats}", flush=True)
        summary[n] = dict(s.client.stats)
    sp = Path(a.out) / "checkpoints" / a.run_id / "http_stats.json"
    sp.parent.mkdir(parents=True, exist_ok=True)
    # every state of a run shares this file and several finish at once: a half-written or unreadable file only
    # loses the stats, never the state's result, and the write below is atomic (temp file + rename)
    try:
        prev = json.loads(sp.read_text()) if sp.exists() else {}
    except (OSError, ValueError):
        prev = {}
    for n, st in summary.items():  # accumulate across resumed invocations
        for k, v in st.items():
            prev.setdefault(n, {})[k] = round(prev.get(n, {}).get(k, 0) + v, 3)
    tmp = sp.with_name(f"{sp.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(prev))
    os.replace(tmp, sp)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
