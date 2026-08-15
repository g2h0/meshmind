import unittest
from unittest.mock import MagicMock, call, patch

from meshmind.config import cfg
from tests.helpers import BotTestCase


class MessageLimitTests(BotTestCase):
    def setUp(self):
        super().setUp()
        self.patch_cfg(
            MAX_RESPONSE_LENGTH=20,
            MAX_BYTE_LIMIT=20,
            MAX_MESSAGE_PARTS=2,
            MULTIPART_DELAY=0.25,
        )
        self.bot = self.make_bot()

    def test_truncate_leaves_short_ascii_unchanged(self):
        self.assertEqual(self.bot._truncate_message("mesh ready"), "mesh ready")

    def test_truncate_enforces_character_limit(self):
        result = self.bot._truncate_message("abcdefghijklmnopqrstuvwxyz")
        self.assertEqual(result, "abcdefghijklmnopq...")
        self.assertLessEqual(len(result), cfg.MAX_RESPONSE_LENGTH)

    def test_truncate_preserves_utf8_boundary_and_byte_limit(self):
        result = self.bot._truncate_message("😀" * 20)
        result.encode("utf-8")
        self.assertLessEqual(len(result.encode("utf-8")), cfg.MAX_BYTE_LIMIT)
        self.assertTrue(result.endswith("..."))

        self.bot.interface = MagicMock()
        self.bot._send_dm(7, "😀" * 20, channel=3)
        direct_text = self.bot.interface.sendText.call_args.args[0]
        self.assertLessEqual(len(direct_text.encode("utf-8")), cfg.MAX_BYTE_LIMIT)

    def test_split_prefers_sentence_boundary(self):
        parts = self.bot._split_message("Alpha sentence. Beta follows soon.")
        self.assertEqual(parts[0], "Alpha sentence.")
        self.assertEqual(parts[1], "Beta follows soon.")

    def test_split_hard_boundary_handles_multibyte_text(self):
        parts = self.bot._split_message("界" * 12)
        self.assertEqual("".join(parts), "界" * 12)
        self.assertTrue(all(len(part.encode("utf-8")) <= 20 for part in parts))

    def test_split_caps_output_at_configured_part_count(self):
        parts = self.bot._split_message("x" * 100)
        self.assertEqual(len(parts), 2)
        self.assertTrue(all(len(part.encode("utf-8")) <= 20 for part in parts))

    @patch("meshmind.bot.time.sleep")
    def test_send_message_sends_parts_in_order_with_delay(self, sleep):
        self.bot._split_message = MagicMock(return_value=["one", "two"])
        self.bot._send_single = MagicMock(return_value=True)

        self.assertTrue(self.bot._send_message("ignored", channel=3))

        self.bot._send_single.assert_has_calls([call("one", 3), call("two", 3)])
        sleep.assert_called_once_with(0.25)

    def test_send_message_stops_after_first_failed_part(self):
        self.bot._split_message = MagicMock(return_value=["one", "two"])
        self.bot._send_single = MagicMock(return_value=False)

        self.assertFalse(self.bot._send_message("ignored", channel=2))
        self.bot._send_single.assert_called_once_with("one", 2)


class PacketDispatchTests(BotTestCase):
    def setUp(self):
        super().setUp()
        self.bot = self.make_bot()
        self.bot.my_node_num = 99
        self.bot._send_message = MagicMock(return_value=True)

    def packet(self, text="ping", sender=7, channel=2, **extra):
        return {"decoded": {"text": text}, "from": sender, "channel": channel, **extra}

    def test_invalid_packet_shapes_are_ignored(self):
        invalid = [
            None,
            {},
            {"decoded": None},
            {"decoded": {}},
            {"decoded": {"text": "ping"}},
            {"decoded": {"text": None}, "from": 7},
            {"decoded": {"text": "ping"}, "from": "node-7"},
        ]
        for packet in invalid:
            with self.subTest(packet=packet):
                self.bot.on_receive(packet, None)
        self.bot._send_message.assert_not_called()
        self.assertEqual(self.bot.messages_received_count, 0)

    def test_packets_from_this_node_are_ignored(self):
        self.bot.on_receive(self.packet(sender=99), None)
        self.bot._send_message.assert_not_called()
        self.assertEqual(self.bot.messages_received_count, 0)

    def test_builtin_commands_dispatch_to_expected_handlers(self):
        self.bot._get_hourly_forecast = MagicMock(return_value="forecast")
        self.bot._get_uptime = MagicMock(return_value="3m 1s")
        self.bot._get_api_stats = MagicMock(return_value="stats")
        expected = {
            "ping": "PONG --- SNR:4.5 RSSI:-82",
            "wx": "forecast",
            "uptime": "Bot Uptime: 3m 1s",
            "api": "stats",
        }

        for command, response in expected.items():
            with self.subTest(command=command):
                self.bot._send_message.reset_mock()
                self.bot.on_receive(self.packet(command, rxSnr=4.5, rxRssi=-82), None)
                self.bot._send_message.assert_called_once_with(response, 2)

    def test_optional_commands_report_disabled_services(self):
        self.patch_cfg(RIVER_ENABLED=False, AQI_ENABLED=False)
        for command, response in {
            "river": "River monitoring not configured",
            "aqi": "Air quality monitoring not configured",
        }.items():
            with self.subTest(command=command):
                self.bot._send_message.reset_mock()
                self.bot.on_receive(self.packet(command), None)
                self.bot._send_message.assert_called_once_with(response, 2)


if __name__ == "__main__":
    unittest.main()
