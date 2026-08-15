import threading
import unittest
from unittest.mock import MagicMock, call, patch

from tests.helpers import BotTestCase


class ReconnectTests(BotTestCase):
    def disconnected_interface(self):
        interface = MagicMock()
        interface.isConnected = threading.Event()
        interface._rxThread.is_alive.return_value = True
        return interface

    @patch("meshmind.bot.time.sleep")
    def test_reconnect_short_circuits_when_stopped_or_already_connected(self, sleep):
        stopped = self.make_bot()
        stopped.is_running = False
        stopped._reconnect_interface = MagicMock()
        self.assertFalse(stopped._reconnect())
        stopped._reconnect_interface.assert_not_called()

        connected = self.make_bot()
        connected.is_running = True
        connected.interface = self.disconnected_interface()
        connected.interface.isConnected.set()
        connected._reconnect_interface = MagicMock()
        self.assertTrue(connected._reconnect())
        connected._reconnect_interface.assert_not_called()
        self.assertEqual(connected.retry_count, 0)
        sleep.assert_not_called()

    @patch("meshmind.bot.time.sleep")
    def test_successful_reconnect_closes_and_joins_old_interface(self, sleep):
        bot = self.make_bot()
        bot.is_running = True
        old_interface = self.disconnected_interface()
        bot.interface = old_interface
        bot._reconnect_interface = MagicMock(return_value=True)
        bot._notify_status_change = MagicMock()

        self.assertTrue(bot._reconnect())

        old_interface.close.assert_called_once()
        old_interface._rxThread.join.assert_called_once_with(timeout=5)
        bot._reconnect_interface.assert_called_once()
        self.assertEqual(bot.reconnect_count, 1)
        self.assertFalse(bot._closing_interface)
        sleep.assert_has_calls([call(2), call(2)])

    @patch("meshmind.bot.time.sleep")
    def test_failed_reconnect_updates_retry_state_and_applies_delay(self, sleep):
        bot = self.make_bot(RETRY_DELAY=7)
        bot.is_running = True
        bot.interface = self.disconnected_interface()
        bot._reconnect_interface = MagicMock(return_value=False)
        bot._notify_status_change = MagicMock()

        self.assertFalse(bot._reconnect())

        self.assertEqual(bot.retry_count, 1)
        self.assertEqual(bot.reconnect_count, 0)
        self.assertFalse(bot._closing_interface)
        sleep.assert_has_calls([call(2), call(7)])

    @patch("meshmind.bot.threading.Thread")
    def test_connection_loss_suppresses_intentional_close_and_starts_one_worker(self, thread_cls):
        bot = self.make_bot()
        bot.is_running = True
        bot._notify_status_change = MagicMock()

        bot._closing_interface = True
        bot._on_connection_lost(None)
        thread_cls.assert_not_called()
        bot._notify_status_change.assert_not_called()

        bot._closing_interface = False
        bot._on_connection_lost(None)
        bot._notify_status_change.assert_called_once()
        thread_cls.assert_called_once_with(
            target=bot._background_reconnect,
            name="reconnect-on-lost",
            daemon=True,
        )
        thread_cls.return_value.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
