"""Throwaway Postgres without Docker (pgserver wheel). Prints a DATABASE_URL; server keeps running.

uv run python -m tools.dev_postgres [--dir exports/pgdata]   # stop: uv run python -m tools.dev_postgres --stop
"""
import argparse
import subprocess
import sys
from pathlib import Path

import pgserver

ap = argparse.ArgumentParser()
ap.add_argument("--dir", default=str(Path(__file__).resolve().parent.parent / "exports" / "pgdata"))
ap.add_argument("--stop", action="store_true")
a = ap.parse_args()
if a.stop:
    bin_ = Path(pgserver.__file__).parent / "pginstall" / "bin" / "pg_ctl"
    sys.exit(subprocess.call([str(bin_), "-D", a.dir, "stop"]))
srv = pgserver.get_server(a.dir, cleanup_mode=None)
srv.psql("SELECT 1")
print(srv.get_uri())
