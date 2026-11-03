"""Phase 2 detail fetcher: parsers on recorded fixtures, error handling, runner, and DB storage (no live network).

realtor_detail.json is a recorded live response (2026-11-02, trimmed, agent contact data replaced).
redfin_detail_*.json are hand-built in the aboveTheFold / belowTheFold shape (live endpoint 403s from this IP)."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
FIX = ROOT / "tests" / "fixtures"

from sources import details  # noqa: E402
from sources.http import BlockedError, SourceClient, parse_json  # noqa: E402
from jobs import fetch_details as fd  # noqa: E402


class Resp:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


def client_returning(*texts):
    c = mock.Mock()
    c.request.side_effect = [Resp(t) if isinstance(t, str) else t for t in texts]
    return c


class RealtorDetailTest(unittest.TestCase):
    def setUp(self):
        self.raw = (FIX / "realtor_detail.json").read_text()

    def test_normalize(self):
        raw, d = details.RealtorDetail(client_returning(self.raw)).fetch("8439071025")
        self.assertEqual(raw["data"]["home"]["property_id"], "8439071025")
        for k in (*details.DETAIL_COLUMNS, *details.DETAIL_JSON, *details.DETAIL_LISTS):
            self.assertIn(k, d)
        self.assertEqual(d["source_listing_id"], "3001931817")
        self.assertEqual(d["list_price"], 399000)
        self.assertEqual(d["year_built"], 1936)
        self.assertEqual(d["mls_id"], "HOTX")
        self.assertEqual(d["mls_listing_id"], "52066119")
        self.assertEqual(d["list_office"], "Texas Signature Realty")
        self.assertEqual(d["list_agent_email"], "agent@example.com")
        self.assertEqual(d["tax_year"], 2025)
        self.assertEqual(d["tax_annual"], 11276)
        self.assertEqual(d["school_district"], "Sealy Independent School District")
        self.assertEqual(len(d["photos"]), 3)
        self.assertTrue(d["photos"][0]["href"].startswith("http"))
        self.assertEqual(len(d["price_history"]), 5)
        self.assertRegex(d["price_history"][0]["date"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(d["mortgage"]["estimate"]["monthly_payment"], 3408)
        self.assertEqual(d["construction"], "CementSiding, WoodSiding")
        self.assertIn("Dishwasher", d["appliances"])
        self.assertEqual(len(d["comps"]), 3)
        c = d["comps"][0]
        self.assertEqual(c["rank"], 1)
        self.assertTrue(c["comp_source_id"])
        self.assertGreater(c["distance_mi"], 0)
        json.dumps(d)  # serializable

    def test_no_home_and_garbage(self):
        for text in ('{"data":{"home":null}}', "", "<html>oops</html>", "[1,2]",
                     '{"errors":[{"message":"bad"}]}'):
            with self.assertRaises(details.DetailError, msg=text):
                details.RealtorDetail(client_returning(text)).fetch("1")

    def test_empty_home_does_not_crash(self):
        d = details.normalize_realtor({})
        self.assertEqual(d["photos"], [])
        self.assertEqual(d["comps"], [])


class RedfinDetailTest(unittest.TestCase):
    def test_normalize(self):
        above, below = (FIX / "redfin_detail_above.json").read_text(), (FIX / "redfin_detail_below.json").read_text()
        c = client_returning(above, below)
        raw, d = details.RedfinDetail(c).fetch("197204706", "214029417")
        urls = [call.args[1] for call in c.request.call_args_list]
        self.assertIn("aboveTheFold?propertyId=197204706&accessLevel=1&listingId=214029417", urls[0])
        self.assertIn("belowTheFold", urls[1])
        self.assertEqual(set(raw), {"aboveTheFold", "belowTheFold"})
        self.assertEqual(d["description"], "Fixture remarks.")
        self.assertEqual(d["sqft"], 1500)
        self.assertEqual(d["year_built"], 1998)
        self.assertEqual(d["list_price"], 250000)
        self.assertEqual(d["status"], "Active")
        self.assertEqual(d["hoa_fee"], 50)
        self.assertEqual(d["assessed_value"], 200000)
        self.assertEqual(d["apn"], "R12345")
        self.assertEqual(d["mls_listing_id"], "12345678")
        self.assertEqual(d["list_agent_name"], "Test Agent")
        self.assertEqual(d["school_district"], "Brazos ISD")
        self.assertEqual(len(d["photos"]), 2)
        self.assertEqual(d["price_history"][0]["date"], "2025-10-16")
        self.assertEqual(d["heating"], "Central, Electric")
        self.assertEqual(d["lat"], 29.64)

    def test_errors(self):
        for text in ("", "not json", '{}&&{"resultCode":100,"errorMessage":"x"}', '{}&&{"resultCode":0}'):
            with self.assertRaises(details.DetailError, msg=text):
                details.RedfinDetail(client_returning(text, text)).fetch("1")
        with self.assertRaises(details.DetailError):
            details.RedfinDetail(client_returning(Resp("", 404))).fetch("1")


class ClientTest(unittest.TestCase):
    def test_detail_client_is_low_and_has_timeouts(self):
        c = details.detail_client("realtor")
        self.assertEqual(c.name, "realtor_detail")
        self.assertEqual(c.limiter.levels, [3, 1, 1])
        self.assertEqual(c.timeout, (10, 20))
        self.assertEqual(c.retries, 3)


class FakeFetcher:
    def __init__(self, outcomes):
        self.outcomes = outcomes

    def fetch(self, sid, lid=None):
        o = self.outcomes[sid]
        if isinstance(o, Exception):
            raise o
        return {"raw": sid}, o


class RunnerTest(unittest.TestCase):
    def test_errors_blocks_and_breaker(self):
        good = details.normalize_realtor(json.loads((FIX / "realtor_detail.json").read_text())["data"]["home"])
        outcomes = {"1": good, "2": details.DetailError("non-JSON"), "3": ValueError("weird"),
                    **{str(i): BlockedError("403") for i in range(10, 16)}}
        homes = [{"source": "realtor", "source_id": k, "state": "TX", "county_fips": "48015"} for k in outcomes]
        stored = []
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(fd, "store_detail", lambda conn, h, d, r, b: stored.append((h, b)) or True), \
                mock.patch.object(fd, "record_error", lambda *a: None):
            r = fd.DetailRunner(None, {"realtor": FakeFetcher(outcomes)}, Path(tmp), "det_t", inflight=1,
                                max_blocks=3, log=lambda m: None).run(homes)
            self.assertTrue((Path(tmp) / stored[0][1]).exists())
        s = r["sources"]["realtor"]
        self.assertEqual(s["fetched"], 1)
        self.assertEqual(s["errors"], 2)
        self.assertEqual(s["blocked"], 3)          # breaker opened after 3 consecutive blocks
        self.assertEqual(s["skipped"], 3)
        self.assertEqual(r["stopped"], ["realtor"])
        self.assertEqual(stored[0][1], "bronze/realtor_detail/tx/48015/det_t/1.json.gz")


try:
    from tests.test_db import URL
except Exception:  # noqa: BLE001
    URL = None


@unittest.skipUnless(URL, "no Postgres reachable")
class StoreTest(unittest.TestCase):
    def setUp(self):
        import psycopg
        from db.migrate import migrate
        self.conn = psycopg.connect(URL, autocommit=True)
        migrate(self.conn)
        for t in ("listing_details", "listing_detail_history", "listing_detail_taxes", "listing_detail_schools",
                  "listing_detail_comps", "listing_events", "listings", "properties", "runs"):
            self.conn.execute(f"TRUNCATE {t} CASCADE")

    def tearDown(self):
        self.conn.close()

    def test_store_select_checkpoint(self):
        c = self.conn
        c.execute("INSERT INTO runs (run_id, started_at) VALUES ('r1', now() - interval '1 hour')")
        c.execute("INSERT INTO properties (property_key) VALUES ('77474|1031 meyer|')")
        lid = c.execute("""INSERT INTO listings (property_key, listing_type, external_id, state, county_fips, sources,
                           source_ids, row_hash) VALUES ('77474|1031 meyer|','sale','la:x','TX','48015','{realtor}',
                           '{"realtor":"8439071025"}','h') RETURNING id""").fetchone()[0]
        c.execute("INSERT INTO listing_events (run_id, listing_id, property_key, listing_type, county_fips, event) "
                  "VALUES ('r1', %s, '77474|1031 meyer|', 'sale', '48015', 'new')", (lid,))
        homes = fd.select_homes(c, "r1", ["realtor", "redfin"], "tx", None, False)
        self.assertEqual([(h["source"], h["source_id"], h["listing_id"]) for h in homes], [("realtor", "8439071025", lid)])
        d = details.normalize_realtor(json.loads((FIX / "realtor_detail.json").read_text())["data"]["home"])
        self.assertTrue(fd.store_detail(c, homes[0], d, "det1", "bronze/x.json.gz"))
        self.assertFalse(fd.store_detail(c, homes[0], d, "det2", "bronze/x.json.gz"))   # same hash
        row = c.execute("SELECT list_price, mls_listing_id, photos->0->>'href', run_id FROM listing_details").fetchone()
        self.assertEqual(row[0], 399000)
        self.assertEqual(row[1], "52066119")
        self.assertEqual(row[3], "det2")
        n = {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in
             ("listing_detail_history", "listing_detail_taxes", "listing_detail_schools", "listing_detail_comps")}
        self.assertEqual(n, {"listing_detail_history": 5, "listing_detail_taxes": 3, "listing_detail_schools": 3,
                             "listing_detail_comps": 3})
        # checkpoint: fetched after the event -> not selected again
        self.assertEqual(fd.select_homes(c, "r1", ["realtor"], None, None, True), [])


if __name__ == "__main__":
    unittest.main()
