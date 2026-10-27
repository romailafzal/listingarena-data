"""Apply db/migrations/*.sql in order, once each (tracked in schema_migrations).

DATABASE_URL=... uv run la-migrate     (or python -m db.migrate)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

MIGRATIONS = Path(__file__).resolve().parent / "migrations"


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        env = Path(__file__).resolve().parent.parent / ".env"
        for line in env.read_text().splitlines() if env.exists() else []:
            if line.startswith("DATABASE_URL="):
                url = line.split("=", 1)[1].strip().strip('"')
    if not url:
        raise SystemExit("DATABASE_URL not set (see .env.example)")
    return url


def connect(url: str | None = None) -> psycopg.Connection:
    return psycopg.connect(url or database_url())


def migrate(conn: psycopg.Connection) -> list[str]:
    applied = []
    with conn.transaction():
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz DEFAULT now())")
        done = {r[0] for r in conn.execute("SELECT name FROM schema_migrations")}
    for f in sorted(MIGRATIONS.glob("*.sql")):
        if f.name in done:
            continue
        with conn.transaction():
            conn.execute(f.read_text())
            conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (f.name,))
        applied.append(f.name)
    return applied


def main(argv=None) -> int:
    with connect() as conn:
        applied = migrate(conn)
    print("applied:", ", ".join(applied) or "nothing (up to date)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
