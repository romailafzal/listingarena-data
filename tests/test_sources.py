import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
FIX = ROOT / "tests" / "fixtures"

from normalization.schema import SILVER_FIELDS  # noqa: E402
from sources.base import County, Page, Source  # noqa: E402
from sources.http import BlockedError, SourceClient, parse_json  # noqa: E402
from sources import realtor, redfin  # noqa: E402


def fx(name):
    return json.loads((FIX / name).read_text())


TRAVIS = County("TX", "Travis County", "48453", 2866, "Travis County, TX")


class RedfinParserTest(unittest.TestCase):
    def test_sale(self):
        homes, total = redfin.parse_response(fx("redfin_sale.json"), "sale")
        self.assertEqual(len(homes), 3)
        self.assertIsNone(total)
        rec = redfin.RedfinSource(client=mock.Mock()).normalize(homes[0], "sale")
        self.assertEqual(set(rec), set(SILVER_FIELDS))
        self.assertEqual(rec["external_id"], f"redfin:{homes[0]['propertyId']}")
        self.assertEqual(rec["price"], homes[0]["price"]["value"])
        self.assertEqual(rec["address"], homes[0]["streetLine"]["value"])
        self.assertIsInstance(rec["lat"], float)
        self.assertTrue(rec["url"].startswith("https://www.redfin.com/"))
        self.assertTrue(rec["photo_url"].startswith("https://ssl.cdn-redfin.com/photo/"))

    def test_sold(self):
        homes, _ = redfin.parse_response(fx("redfin_sold.json"), "sold")
        rec = redfin.normalize_gis(homes[0], "sold")
        self.assertRegex(rec["sold_date"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertIsNone(rec["list_price"])

    def test_rent(self):
        homes, total = redfin.parse_response(fx("redfin_rent.json"), "rent")
        self.assertGreater(total, 0)
        rec = redfin.normalize_rental(homes[0])
        self.assertEqual(rec["listing_type"], "rent")
        self.assertEqual(rec["price"], homes[0]["rentalExtension"]["rentPriceRange"]["min"])
        self.assertEqual(rec["status"], "for_rent")
        self.assertEqual(rec["source_id"], str(homes[0]["homeData"]["propertyId"]))

    def test_gis_error(self):
        with self.assertRaises(ValueError):
            redfin.parse_response({"resultCode": 100, "errorMessage": "GIS Search Service timeout error"}, "sale")

    def test_urls(self):
        area = redfin.area_params(TRAVIS, "sale")[0]
        sale = redfin.build_url(area, "sale", 2)
        self.assertIn("/stingray/api/gis?", sale)
        self.assertIn("sf=1,2,3,5,6,7", sale)
        self.assertIn("start=350", sale)
        sold = redfin.build_url(area, "sold", 1)
        self.assertIn("mpt=99", sold)
        self.assertIn("sold_within_days=", sold)
        self.assertNotIn("sf=", sold)
        self.assertIn("/search/rentals?", redfin.build_url(area, "rent", 1))

    def test_polygon_fallback(self):
        c = County("WA", "King County", "53033", 118, None, {"redfin_rental_polygon": [[[-122.1, 47.5], [-122.0, 47.6]]]})
        self.assertIn("poly", redfin.area_params(c, "rent")[0])
        self.assertIn("region_id", redfin.area_params(c, "sale")[0])

    def test_search_pagination_stops_on_repeats(self):
        page = fx("redfin_sale.json")
        client = mock.Mock()
        client.request_json.return_value = page
        pages = list(redfin.RedfinSource(client=client).search(TRAVIS, "sale"))
        self.assertEqual(len(pages), 2)          # second page is all repeats -> stop
        self.assertEqual(len(pages[1].records), 0)


class RealtorParserTest(unittest.TestCase):
    def test_sale(self):
        results, total = realtor.parse_search(fx("realtor_sale.json"))
        self.assertEqual(len(results), 3)
        self.assertGreater(total, 3)
        rec = realtor.RealtorSource(client=mock.Mock()).normalize(results[0], "sale")
        self.assertEqual(set(rec), set(SILVER_FIELDS))
        self.assertEqual(rec["external_id"], f"realtor:{results[0]['property_id']}")
        self.assertEqual(rec["list_price"], results[0]["list_price"])
        self.assertEqual(rec["zip"], results[0]["location"]["address"]["postal_code"])

    def test_rent(self):
        results, _ = realtor.parse_search(fx("realtor_rent.json"))
        rec = realtor.RealtorSource(client=mock.Mock()).normalize(results[0], "rent")
        self.assertEqual(rec["status"], "for_rent")
        self.assertIsNotNone(rec["price"])

    def test_sold(self):
        results, _ = realtor.parse_search(fx("realtor_sold.json"))
        rec = realtor.RealtorSource(client=mock.Mock()).normalize(results[0], "sold")
        self.assertRegex(rec["sold_date"], r"^\d{4}-\d{2}-\d{2}$")

    def test_missing_home_search(self):
        with self.assertRaises(ValueError):
            realtor.parse_search({"errors": [{"message": "x"}]})

    def test_body(self):
        b = realtor.build_body("Travis County, TX", "sold", 400, today=date(2026, 10, 1))
        q = b["variables"]["query"]
        self.assertEqual(q["status"], ["sold"])
        self.assertIn("sold_date", q)
        self.assertEqual(b["variables"]["offset"], 400)
        self.assertEqual(b["variables"]["sort"][0]["field"], "sold_date")
        banded = realtor.build_body("X", "sale", 0, price=(0, 500000))
        self.assertEqual(banded["variables"]["query"]["list_price"], {"min": 0, "max": 500000})

    def test_search_splits_over_cap(self):
        client = mock.Mock()
        calls = []

        def answer(method, url, headers=None, json=None):
            calls.append(json["variables"])
            band = json["variables"]["query"].get("list_price")
            total = 15000 if band is None or band.get("max") is None and band["min"] == 0 else 5000
            n = 0 if json["variables"]["limit"] == 1 else 1
            return {"data": {"home_search": {"total": total, "results": [{"property_id": str(len(calls))}] * n}}}
        client.request_json.side_effect = answer
        pages = list(realtor.RealtorSource(client=client).search(TRAVIS, "sale"))
        unbanded = [p for p in pages if p.meta["price_band"] is None]
        self.assertEqual(len(unbanded), realtor.MAX_RESULTS // realtor.PAGE_SIZE)   # stops at the 10k cap
        self.assertEqual({p.meta["price_band"] for p in pages} - {None}, {(0, 1_000_000), (1_000_000, None)})


class HttpTest(unittest.TestCase):
    def test_prefix(self):
        self.assertEqual(parse_json('{}&&{"a":1}'), {"a": 1})
        self.assertEqual(parse_json('{"a":1}'), {"a": 1})

    def _client(self, statuses):
        c = SourceClient("t", min_interval=0, retries=3, sleep=lambda s: None)
        resp = [mock.Mock(status_code=s, text="{}") for s in statuses]
        session = mock.Mock(request=mock.Mock(side_effect=resp))
        c._session = lambda: session
        return c

    def test_retry_then_ok(self):
        c = self._client([429, 503, 200])
        self.assertEqual(c.request("GET", "http://x").status_code, 200)
        self.assertEqual(c.stats["retries"], 2)

    def test_blocked(self):
        with self.assertRaises(BlockedError):
            self._client([403, 403, 403]).request("GET", "http://x")

    def test_proxy_env(self):
        with mock.patch.dict("os.environ", {"REDFIN_PROXY": "http://p:1"}):
            self.assertEqual(SourceClient("redfin").proxies["https"], "http://p:1")


class FakeSource(Source):
    name = "fake"

    def __init__(self):
        self.calls = 0

    def search(self, county, listing_type, max_pages=None):
        self.calls += 1
        yield Page(1, {"url": "u"}, {"r": [1, 2]}, [{"id": 1}, {"id": 2}, {"id": 1}], {"total": 2})

    def normalize(self, raw, listing_type):
        return {"source_id": str(raw["id"]), "listing_type": listing_type}


class CollectorTest(unittest.TestCase):
    def test_checkpoint_resume(self):
        from jobs.collect import Collector
        with tempfile.TemporaryDirectory() as d:
            col = Collector(Path(d), "r1", None)
            src = FakeSource()
            res = col.run_one(src, TRAVIS, "sale")
            self.assertEqual(res["records"], 2)            # deduped
            self.assertTrue(list(Path(d).glob("bronze/fake/tx/48453/r1/sale_p0001.json.gz")))
            again = col.run_one(src, TRAVIS, "sale")
            self.assertTrue(again["resumed"])
            self.assertEqual(src.calls, 1)
            out = col.assemble("fake")
            self.assertEqual(len(out.read_text().splitlines()), 2)


class StateConfigTest(unittest.TestCase):
    def test_all_states(self):
        files = list((ROOT / "states").glob("*.json"))
        self.assertEqual(len(files), 51)
        tx = json.loads((ROOT / "states" / "tx.json").read_text())
        austin = next(c for c in tx["counties"] if c["fips"] == "48015")
        self.assertEqual(austin["redfin_region_id"], 2647)
        self.assertEqual(austin["realtor_location"], "Austin County, TX")


if __name__ == "__main__":
    unittest.main()


def test_completeness_rule_counts_listings_with_tolerance():
    from jobs.collect import completeness_tolerance, is_complete
    assert completeness_tolerance(100) == 2 and completeness_tolerance(6707) == 14
    assert is_complete(34, None, 6707, 6707)
    assert is_complete(34, None, 6707, 6693)          # within 0.2%
    assert not is_complete(34, None, 6707, 6269)      # property-deduped count would fail; listing keys must be used
    assert not is_complete(2, 2, 100, 100)            # cut by --max-pages
    assert is_complete(1, None, 0, 0) and is_complete(1, None, None, 5)


def test_realtor_listing_key_separates_mls_duplicates():
    from sources.realtor import RealtorSource
    a = {"property_id": "7278673101", "listing_id": "3001536382"}
    b = {"property_id": "7278673101", "listing_id": "3000045589"}
    assert RealtorSource.listing_key(a) != RealtorSource.listing_key(b)
