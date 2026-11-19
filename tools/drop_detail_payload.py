"""Empty listing_details.payload in small batches (safe while the pipeline runs), then plain VACUUM.

python tools/drop_detail_payload.py [--batch 5000] [--pause 0.5]
Space comes back as free space inside the table (reused by new details); migration 013 then drops the column.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db.migrate import connect  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--pause", type=float, default=0.5)
    a = ap.parse_args()
    with connect() as conn:
        conn.autocommit = True
        if not conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='listing_details' "
                            "AND column_name='payload'").fetchone():
            print("payload already dropped")
            return 0
        conn.execute("SET lock_timeout = '10s'")
        total, t0, last = 0, time.time(), ""
        while True:
            # walk by primary key so each batch is an index range, never a rescan of emptied rows
            rows = conn.execute("""WITH b AS (SELECT source, source_id FROM listing_details
                                     WHERE (source, source_id) > (split_part(%s, '|', 1), split_part(%s, '|', 2))
                                     ORDER BY source, source_id LIMIT %s)
                                   UPDATE listing_details d SET payload = NULL FROM b
                                   WHERE d.source = b.source AND d.source_id = b.source_id AND d.payload IS NOT NULL
                                   RETURNING d.source || '|' || d.source_id""", (last, last, a.batch)).fetchall()
            nxt = conn.execute("""SELECT source || '|' || source_id FROM listing_details
                                  WHERE (source, source_id) > (split_part(%s, '|', 1), split_part(%s, '|', 2))
                                  ORDER BY source, source_id OFFSET %s LIMIT 1""", (last, last, a.batch - 1)).fetchone()
            total += len(rows)
            if not nxt:
                break
            last = nxt[0]
            if total and total % 100000 < a.batch:
                print(f"{total} emptied, {time.time() - t0:.0f}s, at {last.split('|')[0]}", flush=True)
            time.sleep(a.pause)
        print(f"{total} emptied; vacuuming", flush=True)
        conn.execute("VACUUM listing_details")
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
