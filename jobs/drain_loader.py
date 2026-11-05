"""Single loader process that drains finished counties of a run into Postgres while collection runs
(collect runs with LA_LIVE_LOAD=0 and only writes to disk). Each pass loads checkpoints that are new or newer
than their run_county_stats row; exits after --until-file appears and a final pass finds nothing new.

python -m jobs.drain_loader --run-id RID [--every 30] [--until-file PATH]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db.load import load_run  # noqa: E402
from db.migrate import connect  # noqa: E402
from jobs.memguard import rss_mb  # noqa: E402


def _progress(msg: str) -> None:
    """Per-unit lines are dropped; the every-100-units progress line is kept so long passes show they are alive."""
    if msg.startswith(("load: unit", "LOAD FAILED")):
        print(f"{time.strftime('%H:%M:%S')} drain {msg}", flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--every", type=float, default=30)
    ap.add_argument("--until-file")
    ap.add_argument("--out", default=str(ROOT / "exports"))
    a = ap.parse_args(argv)
    while True:
        stop = a.until_file and Path(a.until_file).exists()
        t0 = time.time()
        try:
            with connect() as conn:
                r = load_run(conn, Path(a.out), a.run_id, trigger="drain", log=_progress)
            if r.get("units"):
                print(f"{time.strftime('%H:%M:%S')} drain: {r['units']} units in {time.time() - t0:.0f}s "
                      f"(skipped {r.get('skipped_loaded', 0)}) active+={r.get('active', 0)} rss {rss_mb():.0f} MB",
                      flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"drain: pass failed: {type(e).__name__}: {e}", flush=True)
        if stop:
            return 0
        time.sleep(max(1.0, a.every - (time.time() - t0)))


if __name__ == "__main__":
    sys.exit(main())
