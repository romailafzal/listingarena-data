"""Recompute checkpoint completeness from saved bronze pages under the current rule (jobs.collect.is_complete),
without refetching. Incomplete ones get their checkpoint moved aside (*.json.partial) so a rerun of the same
run id refetches ONLY those.

python -m tools.recheck_completeness RUN_ID [--states TX] [--apply]
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from jobs.collect import is_complete, make_source  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id"); ap.add_argument("--states"); ap.add_argument("--out", default="exports")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    out = ROOT / a.out
    states = {s.lower() for s in a.states.split(",")} if a.states else None
    srcs, bad, n = {}, [], 0
    for cp in sorted((out / "checkpoints" / a.run_id).glob("*/*/*.json")):
        if cp.name.endswith(".error.json"):
            continue
        src, st = cp.parts[-3], cp.parts[-2]
        if states and st not in states:
            continue
        res = json.loads(cp.read_text())
        s = srcs.setdefault(src, make_source(src))
        lkey = getattr(s, "listing_key", None)
        fips, t = cp.stem.split("_")
        total, keys, broken = None, set(), False
        for p in sorted((out / "bronze" / src / st / fips / a.run_id).glob(f"{t}_p*.json.gz")):
            try:
                d = json.load(gzip.open(p, "rt"))
            except (OSError, EOFError, ValueError):
                broken = True  # truncated bronze page (killed writer): refetch this county/type
                break
            if total is None:
                total = (d.get("meta") or {}).get("total")
            from sources import realtor, redfin
            recs = (realtor.parse_search(d["response"])[0] if src == "realtor"
                    else redfin.parse_response(d["response"], t)[0])
            for r in recs:
                keys.add(lkey(r) if lkey else s.normalize(r, t)["source_id"])
        ok = not broken and is_complete(res.get("pages", 0), None, total, len(keys))
        res.update(reported_total=total, unique_listings=len(keys), complete=ok)
        n += 1
        if not ok:
            bad.append((src, st, fips, t, total, len(keys), res.get("records")))
        if a.apply:
            cp.write_text(json.dumps(res))
            if not ok:
                cp.rename(cp.with_suffix(".json.partial"))
    print(f"checked {n}; incomplete {len(bad)}")
    for b in bad:
        print("  ", *b)


if __name__ == "__main__":
    main()
