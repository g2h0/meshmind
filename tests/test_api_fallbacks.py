import unittest
from unittest.mock import MagicMock

import requests

from tests.helpers import BotTestCase, FakeResponse


class ApiFallbackTests(BotTestCase):
    def test_noaa_conditions_timeout_returns_error_and_preserves_cache(self):
        bot = self.make_bot()
        bot._session.get = MagicMock(side_effect=requests.Timeout("slow"))

        result = bot._get_current_conditions_noaa()

        self.assertEqual(result, "Current conditions error")
        self.assertIsNone(bot.weather_cache["noaa"]["data"])
        self.assertEqual(bot.api_stats["per_endpoint"]["noaa-conditions"]["failures"], 1)

    def test_noaa_forecast_and_alert_malformed_payloads_use_fallbacks(self):
        forecast_bot = self.make_bot(WEATHER_POINTS_URL="https://points.test")
        forecast_bot._session.get = MagicMock(return_value=FakeResponse({"unexpected": {}}))
        self.assertEqual(forecast_bot._get_hourly_forecast(), "WX: Forecast unavailable")
        self.assertEqual(forecast_bot.api_stats["failed_calls"], 1)

        alert_bot = self.make_bot(NOAA_ALERTS_URL="https://alerts.test")
        alert_bot._session.get = MagicMock(return_value=FakeResponse([]))
        alert_bot._send_message = MagicMock()
        alert_bot._check_noaa_alerts()
        alert_bot._send_message.assert_not_called()
        self.assertEqual(alert_bot._noaa_consecutive_failures, 1)
        self.assertEqual(alert_bot.api_stats["per_endpoint"]["noaa-alerts"]["failures"], 1)

    def test_tomorrow_timeout_and_non_200_responses_use_fallbacks(self):
        timeout_bot = self.make_bot(TOMORROW_IO_API_KEY="key")
        timeout_bot._session.get = MagicMock(side_effect=requests.Timeout("slow"))
        self.assertEqual(timeout_bot._get_current_conditions_tomorrow(), "Weather data unavailable")
        self.assertEqual(timeout_bot.api_stats["per_endpoint"]["tomorrow-io"]["failures"], 1)

        http_bot = self.make_bot(TOMORROW_IO_API_KEY="key")
        http_bot._session.get = MagicMock(return_value=FakeResponse(status_code=503))
        self.assertEqual(http_bot._get_current_conditions_tomorrow(), "Tomorrow.io: API error")
        self.assertIsNone(http_bot.weather_cache["tomorrow"]["data"])

    def test_usgs_timeout_and_malformed_payload_return_na(self):
        for response in [requests.Timeout("slow"), FakeResponse({"features": []})]:
            with self.subTest(response=type(response).__name__):
                bot = self.make_bot(
                    RIVER_ENABLED=True,
                    RIVER_API_URL="https://river.test",
                    RIVER_REQUEST_MIN_INTERVAL=0,
                )
                if isinstance(response, BaseException):
                    bot._session.get = MagicMock(side_effect=response)
                else:
                    bot._session.get = MagicMock(return_value=response)

                self.assertEqual(bot._get_river_level(), "N/A")
                self.assertIsNone(bot.river_cache["level"])

    def test_airnow_timeout_non_200_empty_and_malformed_responses_return_none(self):
        cases = [
            requests.Timeout("slow"),
            FakeResponse(status_code=503),
            FakeResponse([]),
            FakeResponse({"wrong": "shape"}),
        ]
        for response in cases:
            with self.subTest(response=type(response).__name__):
                bot = self.make_bot(
                    AQI_ENABLED=True,
                    AIRNOW_API_URL="https://air.test",
                    AIRNOW_API_KEY="key",
                )
                if isinstance(response, BaseException):
                    bot._session.get = MagicMock(side_effect=response)
                else:
                    bot._session.get = MagicMock(return_value=response)

                self.assertIsNone(bot._get_aqi_data())
                self.assertIsNone(bot.aqi_cache["data"])
                self.assertEqual(bot.api_stats["per_endpoint"]["airnow"]["failures"], 1)

    def test_sun_api_malformed_response_leaves_existing_state_unchanged(self):
        bot = self.make_bot()
        original = dict(bot.sun_times)
        bot._session.get = MagicMock(return_value=FakeResponse({"status": "OK", "results": {}}))

        bot._update_sun_times("2026-05-01")

        self.assertEqual(bot.sun_times, original)
        self.assertEqual(bot._sun_consecutive_failures, 1)
        self.assertEqual(bot.api_stats["per_endpoint"]["sunrise-sunset"]["failures"], 1)


if __name__ == "__main__":
    unittest.main()
