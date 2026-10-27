"""Load a collected run into Postgres: python -m jobs.load_run --run-id live_test [--states TX] [--counties 48015]"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db.load import load_run  # noqa: E402
from db.migrate import connect, migrate  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--states", default="")
    ap.add_argument("--counties", default="")
    ap.add_argument("--out", default=str(ROOT / "exports"))
    a = ap.parse_args(argv)
    with connect() as conn:
        migrate(conn)
        res = load_run(conn, Path(a.out), a.run_id, set(filter(None, a.states.upper().split(","))) or None,
                       set(filter(None, a.counties.split(","))) or None)
    print(json.dumps(res))
    return 0


if __name__ == "__main__":
    sys.exit(main())
