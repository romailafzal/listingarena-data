import tempfile
import unittest
from pathlib import Path

from pipeline import County, StateStore, normalize_record, stable_id, write_csv


class PipelineTests(unittest.TestCase):
    def test_normalization_preserves_provider_identity_and_maps_fields(self):
        record = normalize_record("realtor", {"listing_id": "abc", "bedrooms": 3, "type": "rent"}, County(), "now")
        self.assertEqual(record["external_id"], "realtor:abc")
        self.assertEqual(record["listing_type"], "rent")
        self.assertEqual(record["beds"], 3)
        self.assertEqual(record["county_fips"], "48015")

    def test_store_upserts_records_and_restores_state(self):
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(Path(directory) / "state.sqlite3")
            record = normalize_record("homes", {"id": "1", "price": 100}, County(), "now")
            store.save("homes", [record], "cursor-1", "now")
            store.save("homes", [dict(record, price=125)], "cursor-2", "later")
            self.assertEqual(store.state("homes"), ("cursor-2", "later"))
            self.assertEqual(store.records()[0]["price"], 125)
            store.close()

    def test_stable_id_falls_back_to_address_hash(self):
        self.assertEqual(stable_id("redfin", {"address": "1 Main"}), stable_id("redfin", {"address": "1 Main"}))

    def test_sold_records_and_csv_exports_have_expected_category(self):
        with tempfile.TemporaryDirectory() as directory:
            record = normalize_record("homes", {"id": "2", "status": "closed"}, County(), "now")
            self.assertEqual(record["listing_type"], "sold")
            path = Path(directory) / "sold.csv"
            write_csv(path, [record])
            self.assertIn("external_id", path.read_text(encoding="utf-8").splitlines()[0])
            self.assertIn("homes:2", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()