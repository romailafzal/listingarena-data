"""DB integration: needs Postgres. Uses TEST_DATABASE_URL, else DATABASE_URL's server with a scratch
database `listingarena_test` (dropped and recreated). Skipped when neither is reachable."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo
except ImportError:  # pragma: no cover
    psycopg = None


def _test_url():
    if os.environ.get("TEST_DATABASE_URL"):
        return os.environ["TEST_DATABASE_URL"]
    from db.migrate import database_url
    try:
        base = database_url()
    except SystemExit:
        return None
    admin = make_conninfo(**{**conninfo_to_dict(base)})
    try:
        with psycopg.connect(admin, autocommit=True, connect_timeout=3) as c:
            c.execute("DROP DATABASE IF EXISTS listingarena_test WITH (FORCE)")
            c.execute("CREATE DATABASE listingarena_test")
    except Exception:  # noqa: BLE001
        return None
    return make_conninfo(**{**conninfo_to_dict(base), "dbname": "listingarena_test"})


URL = _test_url() if psycopg else None


def write_run(root: Path, run_id: str, src: str, rows: list, complete=True, error=None, t="sale", fips="48015"):
    cp = root / "checkpoints" / run_id / src / "tx" / f"{fips}_{t}.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    if error:
        cp.with_suffix(".error.json").write_text(json.dumps({"error": error, "blocked": True}))
        return
    cp.write_text(json.dumps({"pages": 1, "records": len(rows), "complete": complete, "seconds": 1.0}))
    part = root / "silver" / src / run_id / "parts" / f"tx_{fips}_{t}.jsonl"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_text("".join(json.dumps(r) + "\n" for r in rows))


def r(src, sid, addr, price, t="sale", **kw):
    return {**kw, "source": src, "source_id": sid, "listing_type": t, "address": addr, "zip": "77474", "city": "Sealy",
            "state": "TX", "price": price, "beds": 3, "county_fips": "48015", "extra": {"mls_id": sid}}


@unittest.skipUnless(URL, "no Postgres reachable (docker compose up -d postgres, or set TEST_DATABASE_URL)")
class LoadTest(unittest.TestCase):
    def setUp(self):
        from db.migrate import migrate
        self.conn = psycopg.connect(URL)
        for t in ("listing_events", "run_county_stats", "errors", "listings", "source_records", "properties", "runs",
                  "schema_migrations"):
            self.conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        self.conn.commit()
        migrate(self.conn)
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        self.conn.close()

    def events(self, run):
        return sorted((e, k) for e, k in self.conn.execute(
            "SELECT event, property_key FROM listing_events WHERE run_id=%s", (run,)))

    def test_lifecycle(self):
        from db.load import load_run
        write_run(self.tmp, "r1", "redfin", [r("redfin", "1", "10 Oak St", 100), r("redfin", "2", "20 Elm St", 200)])
        write_run(self.tmp, "r1", "realtor", [r("realtor", "a", "10 Oak Street", 100), r("realtor", "b", "30 Pine Rd", 300)])
        res = load_run(self.conn, self.tmp, "r1", log=lambda *_: None)
        self.assertEqual((res["active"], res["multi_source"], res["new"]), (3, 1, 3))
        srcs = self.conn.execute("SELECT sources FROM listings WHERE property_key='77474|10 oak|'").fetchone()[0]
        self.assertEqual(srcs, ["realtor", "redfin"])

        # r2: redfin complete without 20 Elm and with a price change; realtor fetch failed -> its listings stay
        write_run(self.tmp, "r2", "redfin", [r("redfin", "1", "10 Oak St", 150, sqft=1500)])
        write_run(self.tmp, "r2", "realtor", [], error="HTTP 403")
        load_run(self.conn, self.tmp, "r2", log=lambda *_: None)
        self.assertEqual(self.events("r2"), [("changed", "77474|10 oak|"), ("removed", "77474|20 elm|")])
        active = {k for (k,) in self.conn.execute("SELECT property_key FROM listings WHERE active")}
        self.assertEqual(active, {"77474|10 oak|", "77474|30 pine|"})
        ch = self.conn.execute("SELECT changes FROM listing_events WHERE run_id='r2' AND event='changed'").fetchone()[0]
        # sqft: redfin has precedence; price: realtor (still active since its fetch failed) keeps precedence
        self.assertEqual(ch, {"sqft": {"before": None, "after": 1500}})
        stats = dict(self.conn.execute("SELECT source, errors FROM run_county_stats WHERE run_id='r2'").fetchall())
        self.assertEqual(stats["realtor"], 1)
        self.assertEqual(self.conn.execute("SELECT kind FROM errors WHERE run_id='r2'").fetchone()[0], "blocked")

        # r3: partial (incomplete) fetch never removes; 20 Elm comes back -> relisted
        write_run(self.tmp, "r3", "redfin", [r("redfin", "2", "20 Elm St", 200)], complete=False)
        load_run(self.conn, self.tmp, "r3", log=lambda *_: None)
        self.assertEqual(self.events("r3"), [("relisted", "77474|20 elm|")])
        self.assertEqual(self.conn.execute("SELECT count(*) FROM listings WHERE active").fetchone()[0], 3)
