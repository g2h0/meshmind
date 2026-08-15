import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from tests.helpers import BotTestCase


class FrozenDateTime(datetime):
    current = datetime(2026, 5, 1, 20, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current.astimezone(tz)


class AlertBehaviorTests(BotTestCase):
    def setUp(self):
        super().setUp()
        FrozenDateTime.current = datetime(2026, 5, 1, 20, 1, tzinfo=timezone.utc)
        self.patch_cfg(TIMEZONE=timezone.utc)
        self.bot = self.make_bot()
        self.bot._send_message = MagicMock(return_value=True)
        self.bot._save_alert_state = MagicMock()

    def noaa_feature(self, alert_id):
        return {
            "properties": {
                "id": alert_id,
                "event": "Severe Storm",
                "headline": f"Headline {alert_id}",
                "expires": "2026-05-02T01:00:00+00:00",
                "parameters": {},
                "description": "First sentence. Additional details.",
            }
        }

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_noaa_alert_id_is_sent_only_once(self):
        self.patch_cfg(NOAA_ALERTS_URL="https://alerts.test", ALERTS_CACHE_TTL=600)
        self.bot.alerts_cache = {
            "data": [self.noaa_feature("alert-1")],
            "timestamp": FrozenDateTime.current,
        }

        self.bot._check_noaa_alerts()
        self.bot._check_noaa_alerts()

        self.bot._send_message.assert_called_once()
        self.assertIn("alert-1", self.bot.seen_alert_ids)
        self.bot._save_alert_state.assert_called_once()

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_noaa_alert_broadcast_is_capped_at_three(self):
        self.patch_cfg(NOAA_ALERTS_URL="https://alerts.test", ALERTS_CACHE_TTL=600)
        self.bot.alerts_cache = {
            "data": [self.noaa_feature(f"alert-{index}") for index in range(5)],
            "timestamp": FrozenDateTime.current,
        }

        self.bot._check_noaa_alerts()

        self.assertEqual(self.bot._send_message.call_count, 3)
        self.assertEqual(len(self.bot.seen_alert_ids), 3)

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_frost_check_skips_outside_configured_season(self):
        self.patch_cfg(FROST_SEASON_MONTHS=[1], FROST_CHECK_HOURS=[20])
        self.bot._get_hourly_forecast_data = MagicMock()

        self.bot._check_frost_conditions()

        self.bot._get_hourly_forecast_data.assert_not_called()
        self.bot._send_message.assert_not_called()

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_frost_warning_uses_coldest_period_and_marks_day_sent(self):
        self.patch_cfg(
            FROST_SEASON_MONTHS=[5],
            FROST_CHECK_HOURS=[20],
            FROST_TEMP_THRESHOLD=32,
        )
        self.bot._get_hourly_forecast_data = MagicMock(return_value=[
            {"startTime": "2026-05-01T21:00:00+00:00", "temperature": 31},
            {"startTime": "2026-05-01T23:00:00+00:00", "temperature": 27},
        ])

        self.bot._check_frost_conditions()

        message = self.bot._send_message.call_args.args[0]
        self.assertIn("FROST WARNING", message)
        self.assertIn("low 27F at 23:00", message)
        self.assertEqual(self.bot.frost_alert_sent, FrozenDateTime.current.date())
        self.bot._save_alert_state.assert_called_once()

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_frost_warning_is_suppressed_after_same_day_send(self):
        self.patch_cfg(FROST_SEASON_MONTHS=[5], FROST_CHECK_HOURS=[20])
        self.bot.frost_alert_sent = FrozenDateTime.current.date()
        self.bot._get_hourly_forecast_data = MagicMock()

        self.bot._check_frost_conditions()

        self.bot._get_hourly_forecast_data.assert_not_called()
        self.bot._send_message.assert_not_called()

    def configure_flood(self):
        self.patch_cfg(
            RIVER_ENABLED=True,
            RIVER_NAME="Test River",
            FLOOD_CHECK_HOURS=[8],
            FLOOD_STAGES={"action": 8, "flood": 10, "moderate": 12, "major": 14},
        )
        FrozenDateTime.current = datetime(2026, 5, 1, 8, 1, tzinfo=timezone.utc)

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_flood_alert_escalates_to_a_higher_stage(self):
        self.configure_flood()
        self.bot.flood_alert_sent = "action"
        self.bot._get_river_level = MagicMock(return_value="10.5")

        self.bot._check_flood_conditions()

        self.assertEqual(self.bot.flood_alert_sent, "flood")
        self.assertIn("FLOOD WARNING", self.bot._send_message.call_args.args[0])

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_flood_alert_is_suppressed_at_same_stage(self):
        self.configure_flood()
        self.bot.flood_alert_sent = "moderate"
        self.bot._get_river_level = MagicMock(return_value="12.5")

        self.bot._check_flood_conditions()

        self.bot._send_message.assert_not_called()
        self.bot._save_alert_state.assert_not_called()

    @patch("meshmind.bot.datetime", FrozenDateTime)
    def test_flood_state_resets_after_level_falls_below_action(self):
        self.configure_flood()
        self.bot.flood_alert_sent = "flood"
        self.bot._get_river_level = MagicMock(return_value="7.9")

        self.bot._check_flood_conditions()

        self.assertIsNone(self.bot.flood_alert_sent)
        self.bot._send_message.assert_not_called()

    def test_aqi_only_alerts_on_escalation_and_resets_when_good(self):
        readings = [
            {"aqi": 120, "category_name": "Unhealthy", "category_number": 3, "parameter": "PM2.5"},
            {"aqi": 115, "category_name": "Unhealthy", "category_number": 3, "parameter": "PM2.5"},
            {"aqi": 35, "category_name": "Good", "category_number": 1, "parameter": "O3"},
            {"aqi": 75, "category_name": "Moderate", "category_number": 2, "parameter": "O3"},
        ]
        self.bot._get_aqi_data = MagicMock(side_effect=readings)

        for _ in readings:
            self.bot._check_aqi_alerts()

        self.assertEqual(self.bot._send_message.call_count, 2)
        self.assertEqual(self.bot.last_aqi_alert_category, 2)
        self.assertEqual(self.bot._save_alert_state.call_count, 3)


if __name__ == "__main__":
    unittest.main()
