"""Rebuild silver parts for a run from its bronze pages (after normalize() changes, e.g. adding `extra`).

python -m tools.renormalize --run-id live_test
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from jobs.collect import make_source  # noqa: E402
from sources import realtor, redfin  # noqa: E402


def records(src: str, data, t: str) -> list:
    if src == "redfin":
        return redfin.parse_response(data, t)[0]
    return realtor.parse_search(data)[0]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out", default=str(ROOT / "exports"))
    a = ap.parse_args(argv)
    out = Path(a.out)
    for src_dir in (out / "bronze").iterdir():
        src = make_source(src_dir.name)
        parts = defaultdict(dict)
        for f in sorted(src_dir.glob(f"*/*/{a.run_id}/*.json.gz")):
            st, fips = f.parts[-4], f.parts[-3]
            page = json.loads(gzip.open(f, "rt").read())
            t = page["listing_type"]
            for raw in records(src.name, page["response"], t):
                rec = src.normalize(raw, t)
                rec["county_fips"], rec["fetched_at"] = fips, page["fetched_at"]
                parts[(st, fips, t)].setdefault(rec["source_id"], rec)
        for (st, fips, t), recs in parts.items():
            p = out / "silver" / src.name / a.run_id / "parts" / f"{st}_{fips}_{t}.jsonl"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("".join(json.dumps(r) + "\n" for r in recs.values()))
            print(src.name, st, fips, t, len(recs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
