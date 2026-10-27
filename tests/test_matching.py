import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from matching.address import property_key, split_unit, zip5  # noqa: E402
from matching.changes import detect, diff  # noqa: E402
from matching.merge import merge, merge_group  # noqa: E402


def rec(source, sid, address="1200 Main St #304", zip_="78701", t="sale", **kw):
    r = {"source": source, "source_id": sid, "listing_type": t, "address": address, "zip": zip_, "city": "Austin",
         "state": "TX", "county_fips": "48453", "url": f"https://{source}/{sid}", "extra": {"mls_id": "M1"}}
    r.update(kw)
    return r


class AddressTest(unittest.TestCase):
    def test_unit_variants(self):
        keys = {property_key(a, "78701-1234") for a in
                ["1200 Main St #304", "1200 Main Street Unit 304", "1200 Main St. Apt 304", "1200 MAIN ST APT #0304",
                 "1200 Main St, Suite 304"]}
        self.assertEqual(keys, {"78701|1200 main|304"})

    def test_directions_and_suffixes(self):
        self.assertEqual(property_key("805 S Oak St", "77418"), property_key("805 Oak Street South", "77418"))
        self.assertEqual(property_key("100 North Lamar Boulevard", "78701"), property_key("100 N Lamar Blvd", "78701"))
        self.assertEqual(property_key("0 Wesley Church Lane Ln", "77833"), property_key("0 Wesley Church Ln", "77833"))

    def test_routes(self):
        self.assertEqual(property_key("141 Farm to Market Rd # 2429", "77418"), property_key("141 N FM 2429 Rd", "77418"))
        self.assertEqual(property_key("5829 TX 159", "77418"), property_key("5829 Highway 159 W", "77418"))
        self.assertEqual(property_key("9719 I-10 Frontage Rd", "77474"), property_key("9719 SW IH 10 Frontage Rd", "77474"))

    def test_different_units_differ(self):
        self.assertNotEqual(property_key("1200 Main St #304", "78701"), property_key("1200 Main St #305", "78701"))
        self.assertNotEqual(property_key("1200 Main St", "78701"), property_key("1200 Main St", "78702"))

    def test_fallbacks(self):
        self.assertEqual(zip5("78701-1234"), "78701")
        self.assertEqual(property_key("Ranch Trl Lot 5", "78701", lat=30.123456, lng=-97.1), "geo|30.1235|-97.1|5")
        self.assertIsNone(property_key(None, None))
        self.assertEqual(split_unit("12 Oak Ln Unit B"), ("12 oak ln", "b"))


class MergeTest(unittest.TestCase):
    def test_merge_two_sources(self):
        a = rec("redfin", "1", "1200 Main St Unit 304", price=500000, sqft=1000, lat=30.1, lng=-97.1, photo_url=None)
        b = rec("realtor", "9", "1200 Main St #304", price=510000, sqft=990, listed_date="2026-10-01",
                photo_url="http://p", lat=30.2, lng=-97.2)
        out, nokey = merge([a, b, rec("redfin", "2", "5 Elm Rd", price=1)])
        self.assertEqual(nokey, [])
        self.assertEqual(len(out), 2)
        m = next(x for x in out if len(x["sources"]) == 2)
        self.assertEqual(m["sources"], ["realtor", "redfin"])
        self.assertEqual(m["price"], 510000)          # realtor default precedence
        self.assertEqual(m["sqft"], 1000)             # redfin override
        self.assertEqual((m["lat"], m["lng"]), (30.1, -97.1))
        self.assertEqual(m["photo_url"], "http://p")
        self.assertEqual(m["source_ids"], {"redfin": "1", "realtor": "9"})
        self.assertEqual(set(m["extra"]), {"redfin", "realtor"})
        self.assertEqual(m["unit"], "304")
        self.assertTrue(m["external_id"].startswith("la:"))

    def test_sold_price_prefers_redfin(self):
        m = merge_group("k", "sold", [rec("realtor", "9", t="sold", price=None, list_price=300),
                                      rec("redfin", "1", t="sold", price=290)])
        self.assertEqual(m["price"], 290)

    def test_types_not_merged(self):
        out, _ = merge([rec("redfin", "1", t="sale"), rec("realtor", "2", t="rent")])
        self.assertEqual(len(out), 2)

    def test_stable_hash(self):
        a = merge_group("k", "sale", [rec("redfin", "1", price=5)])
        b = merge_group("k", "sale", [rec("redfin", "1", price=5, description="changed text only")])
        self.assertEqual(a["row_hash"], b["row_hash"])  # description is not a tracked field


class ChangeTest(unittest.TestCase):
    def test_events(self):
        cur = {"a": {"row_hash": "1", "price": 100, "active": True},
               "b": {"row_hash": "2", "price": 5, "active": True},
               "c": {"row_hash": "3", "active": False, "price": 7},
               "d": {"row_hash": "4", "active": True}}
        inc = {"a": {"row_hash": "1", "price": 100}, "b": {"row_hash": "9", "price": 6},
               "c": {"row_hash": "3", "price": 7}, "e": {"row_hash": "5"}}
        ev = {e["property_key"]: e for e in detect(cur, inc)}
        self.assertNotIn("a", ev)
        self.assertEqual(ev["b"]["event"], "changed")
        self.assertEqual(ev["b"]["changes"]["price"], {"before": 5, "after": 6})
        self.assertEqual(ev["c"]["event"], "relisted")
        self.assertEqual(ev["d"]["event"], "removed")
        self.assertEqual(ev["e"]["event"], "new")

    def test_removal_guard(self):
        cur = {"d": {"row_hash": "4", "active": True}}
        self.assertEqual(detect(cur, {}, removable=set()), [])

    def test_diff_decimal_and_dates(self):
        from datetime import date
        from decimal import Decimal
        self.assertEqual(diff({"price": Decimal("100"), "sold_date": date(2026, 1, 2)},
                              {"price": 100, "sold_date": "2026-01-02"}), {})
