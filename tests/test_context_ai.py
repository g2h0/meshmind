import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from tests.helpers import BotTestCase


class ContextSnapshotTests(BotTestCase):
    def test_empty_enabled_caches_state_that_live_data_is_unavailable(self):
        bot = self.make_bot(
            RIVER_ENABLED=True,
            RIVER_NAME="Test River",
            AQI_ENABLED=True,
        )

        snapshot = bot._build_context_snapshot()

        self.assertIn("No live weather data available right now.", snapshot)
        self.assertIn("[River: Test River]\nNo current river reading available.", snapshot)
        self.assertIn("[Air Quality]\nNo current air quality data available.", snapshot)
        self.assertNotIn("[Active Alerts]", snapshot)

    def test_disabled_optional_services_are_omitted(self):
        bot = self.make_bot(RIVER_ENABLED=False, AQI_ENABLED=False)
        snapshot = bot._build_context_snapshot()
        self.assertNotIn("[River:", snapshot)
        self.assertNotIn("[Air Quality]", snapshot)

    def test_populated_caches_are_rendered_with_alert_and_flood_context(self):
        bot = self.make_bot(
            RIVER_ENABLED=True,
            RIVER_NAME="Test River",
            FLOOD_STAGES={"action": 8, "flood": 10, "moderate": 12, "major": 14},
            AQI_ENABLED=True,
        )
        bot.weather_cache["tomorrow"]["data"] = "Clear, +70F"
        bot.alerts_cache["data"] = [{"properties": {"event": "Wind Warning"}}]
        bot.river_cache["level"] = "10.4"
        bot.aqi_cache["data"] = {
            "aqi": 82,
            "category_name": "Moderate",
            "parameter": "PM2.5",
        }

        snapshot = bot._build_context_snapshot()

        self.assertIn("Clear, +70F", snapshot)
        self.assertIn("Wind Warning", snapshot)
        self.assertIn("Level: 10.4 ft (FLOOD STAGE)", snapshot)
        self.assertIn("AQI 82 (Moderate) - PM2.5", snapshot)


class MockedLlmTests(BotTestCase):
    def setUp(self):
        super().setUp()
        self.bot = self.make_bot(
            AI_SEARCH_ENABLED=False,
            AI_BASE_URL="https://example.test/v1",
            MAX_RESPONSE_LENGTH=20,
            MAX_MESSAGE_PARTS=2,
            MAX_CHAT_HISTORY=50,
        )
        self.patch_cfg(SYSTEM_PROMPT="SYSTEM")

    def completion(self, content):
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=content))]
        )

    def test_missing_client_returns_service_unavailable_without_history(self):
        self.bot.client = None
        self.assertEqual(self.bot._get_ai_response(7, "hello"), "AI service unavailable")
        self.assertNotIn(7, self.bot.chat_histories)

    def test_chat_completion_receives_context_and_updates_history(self):
        self.bot.client = MagicMock()
        self.bot.client.chat.completions.create.return_value = self.completion("  mesh reply  ")
        self.bot._build_context_snapshot = MagicMock(return_value="LIVE CONTEXT")

        result = self.bot._get_ai_response(7, "hello")

        self.assertEqual(result, "mesh reply")
        messages = self.bot.client.chat.completions.create.call_args.kwargs["messages"]
        self.assertEqual(messages[0], {"role": "system", "content": "SYSTEM\n\nLIVE CONTEXT"})
        self.assertEqual(self.bot.chat_histories[7][-1], {"role": "assistant", "content": "mesh reply"})
        self.assertEqual(self.bot.api_stats["successful_calls"], 1)

    def test_empty_completion_is_retried_once(self):
        self.bot.client = MagicMock()
        self.bot.client.chat.completions.create.side_effect = [
            self.completion(None),
            self.completion("second try"),
        ]

        self.assertEqual(self.bot._get_ai_response(4, "retry"), "second try")
        self.assertEqual(self.bot.client.chat.completions.create.call_count, 2)

    def test_responses_api_extracts_text_and_removes_citation_links(self):
        self.patch_cfg(AI_SEARCH_ENABLED=True, AI_BASE_URL="https://api.x.ai/v1")
        self.bot.client = MagicMock()
        output_text = SimpleNamespace(
            type="output_text",
            text="Forecast [[1]](https://example.test/source)  is clear",
        )
        self.bot.client.responses.create.return_value = SimpleNamespace(
            output=[SimpleNamespace(type="message", content=[output_text])]
        )

        result = self.bot._get_ai_response(3, "weather")

        self.assertEqual(result, "Forecast is clear")
        self.bot.client.responses.create.assert_called_once()

    def test_malformed_completion_returns_processing_fallback_and_records_failure(self):
        self.bot.client = MagicMock()
        self.bot.client.chat.completions.create.return_value = SimpleNamespace(choices=[])

        result = self.bot._get_ai_response(5, "hello")

        self.assertEqual(result, "Processing error. Try again.")
        self.assertEqual(self.bot.api_stats["failed_calls"], 1)
        self.assertEqual(self.bot.api_stats["per_endpoint"]["ai"]["failures"], 1)


if __name__ == "__main__":
    unittest.main()
