import unittest

from meshmind.config import Config
from meshmind.usgs import parse_latest_gage_height
from meshmon.config import build_services


class USGSRiverMigrationTests(unittest.TestCase):
    def test_meshmind_builds_latest_continuous_url(self):
        config = Config()
        config.RIVER_GAUGE_ID = "03238000"
        config._build_urls()

        self.assertIn("/collections/latest-continuous/items", config.RIVER_API_URL)
        self.assertIn("monitoring_location_id=USGS-03238000", config.RIVER_API_URL)
        self.assertIn("parameter_code=00065", config.RIVER_API_URL)
        self.assertNotIn("api_key", config.RIVER_API_URL)
        self.assertEqual(config.RIVER_REQUEST_MIN_INTERVAL, 600)

    def test_prefixed_monitoring_location_is_not_duplicated(self):
        config = Config()
        config.RIVER_GAUGE_ID = "usgs-03238000"
        config._build_urls()

        self.assertIn("monitoring_location_id=USGS-03238000", config.RIVER_API_URL)
        self.assertNotIn("USGS-USGS-", config.RIVER_API_URL)

    def test_parser_selects_newest_valid_gage_height(self):
        payload = {
            "features": [
                {
                    "properties": {
                        "parameter_code": "00065",
                        "unit_of_measure": "ft",
                        "value": "33.50",
                        "time": "2026-08-02T15:45:00+00:00",
                    }
                },
                {
                    "properties": {
                        "parameter_code": "00065",
                        "unit_of_measure": "ft",
                        "value": "33.82",
                        "time": "2026-08-02T16:00:00+00:00",
                    }
                },
                {
                    "properties": {
                        "parameter_code": "00060",
                        "unit_of_measure": "ft3/s",
                        "value": "99999",
                        "time": "2026-08-02T16:15:00+00:00",
                    }
                },
            ]
        }

        self.assertEqual(parse_latest_gage_height(payload), 33.82)

    def test_parser_rejects_missing_or_non_foot_values(self):
        self.assertIsNone(parse_latest_gage_height({"features": []}))
        self.assertIsNone(
            parse_latest_gage_height(
                {
                    "features": [
                        {
                            "properties": {
                                "parameter_code": "00065",
                                "unit_of_measure": "m",
                                "value": "10.3",
                            }
                        }
                    ]
                }
            )
        )

    def test_meshmon_river_service_is_keyless_and_rate_limited(self):
        services = build_services(38.69, -83.60, "OHZ081", "03238000")
        river = next(s for s in services if s["name"] == "USGS River Gauge")

        self.assertIn("monitoring_location_id=USGS-03238000", river["url"])
        self.assertNotIn("api_key", river["url"])
        self.assertNotIn("requires_key", river)
        self.assertEqual(river["check_interval"], 600)
        self.assertTrue(river["enforce_check_interval"])


if __name__ == "__main__":
    unittest.main()
