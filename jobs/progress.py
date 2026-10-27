"""Best-effort live progress rows (table run_progress). Never breaks a collect run if the DB is down."""
from __future__ import annotations

import os
import threading

_SQL = """INSERT INTO run_progress (run_id, source, state, county_fips, listing_type, status, pages, records, started_at, updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s, CASE WHEN %s='running' THEN now() END, now())
ON CONFLICT (run_id, source, county_fips, listing_type) DO UPDATE SET status=EXCLUDED.status, pages=EXCLUDED.pages,
  records=EXCLUDED.records, updated_at=now(),
  started_at=CASE WHEN EXCLUDED.status='running' AND run_progress.status<>'running' THEN now() ELSE run_progress.started_at END"""


class Progress:
    def __init__(self, run_id: str):
        self.run_id, self.conn, self.lock = run_id, None, threading.Lock()
        if os.environ.get("LA_PROGRESS", "1") == "0":
            return
        try:
            from db.migrate import connect, database_url
            self.conn = connect(database_url())
            self.conn.autocommit = True
        except Exception as e:  # noqa: BLE001
            print(f"progress: disabled ({type(e).__name__}: {e})", flush=True)

    def set(self, source: str, state: str, fips: str, t: str, status: str, pages: int = 0, records: int = 0) -> None:
        if self.conn is None:
            return
        try:
            with self.lock:
                self.conn.execute(_SQL, (self.run_id, source, state, fips, t, status, pages, records, status))
        except Exception as e:  # noqa: BLE001
            print(f"progress: write failed ({type(e).__name__}: {e})", flush=True)

    def queue_many(self, rows: list[tuple[str, str, str, str]]) -> None:
        """rows: (source, state, fips, type); only inserts missing rows as queued."""
        if self.conn is None or not rows:
            return
        try:
            with self.lock, self.conn.cursor() as cur:
                # rows left 'running' by an interrupted earlier invocation of this run/source go back to queued
                cur.execute("UPDATE run_progress SET status='queued', updated_at=now() WHERE run_id=%s AND source=%s "
                            "AND status='running'", (self.run_id, rows[0][0]))
                cur.executemany("""INSERT INTO run_progress (run_id, source, state, county_fips, listing_type, status)
                                   VALUES (%s,%s,%s,%s,%s,'queued') ON CONFLICT DO NOTHING""",
                                [(self.run_id, *r) for r in rows])
        except Exception as e:  # noqa: BLE001
            print(f"progress: queue failed ({type(e).__name__}: {e})", flush=True)
