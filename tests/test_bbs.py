import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from meshmind.utils.bbs import BbsBoard, format_age


class FrozenDateTime(datetime):
    current = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current.astimezone(tz)


class BbsBoardTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.path = Path(self.temp_dir.name) / "bbs.json"

    def test_invalid_json_recovers_as_empty_board(self):
        self.path.write_text("{not valid json", encoding="utf-8")
        self.assertEqual(BbsBoard(self.path).get_posts(), [])

    def test_non_list_json_recovers_as_empty_board(self):
        self.path.write_text('{"message": "wrong shape"}', encoding="utf-8")
        self.assertEqual(BbsBoard(self.path).get_posts(), [])

    @patch("meshmind.utils.bbs.datetime", FrozenDateTime)
    def test_add_post_truncates_message_and_persists_record(self):
        board = BbsBoard(self.path)
        self.assertEqual(board.add_post(7, "NODE", "x" * 200), 1)

        stored = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(stored[0]["node_id"], 7)
        self.assertEqual(stored[0]["node_name"], "NODE")
        self.assertEqual(stored[0]["message"], "x" * 160)
        self.assertEqual(stored[0]["timestamp"], FrozenDateTime.current.isoformat())

    @patch("meshmind.utils.bbs.datetime", FrozenDateTime)
    def test_board_retains_only_five_newest_posts(self):
        board = BbsBoard(self.path)
        for index in range(7):
            board.add_post(index, f"N{index}", f"message {index}")

        posts = board.get_posts()
        self.assertEqual([post["node_id"] for post in posts], [2, 3, 4, 5, 6])

    @patch("meshmind.utils.bbs.datetime", FrozenDateTime)
    def test_get_posts_prunes_expired_records_and_rewrites_file(self):
        old = FrozenDateTime.current - timedelta(days=8)
        fresh = FrozenDateTime.current - timedelta(days=1)
        records = [
            {"node_id": 1, "node_name": "OLD", "message": "old", "timestamp": old.isoformat()},
            {"node_id": 2, "node_name": "NEW", "message": "new", "timestamp": fresh.isoformat()},
        ]
        self.path.write_text(json.dumps(records), encoding="utf-8")

        posts = BbsBoard(self.path).get_posts()

        self.assertEqual([post["node_id"] for post in posts], [2])
        self.assertEqual(json.loads(self.path.read_text(encoding="utf-8")), posts)

    @patch("meshmind.utils.bbs.datetime", FrozenDateTime)
    def test_format_age_handles_relative_and_malformed_timestamps(self):
        cases = {
            (FrozenDateTime.current - timedelta(days=2)).isoformat(): "2d ago",
            (FrozenDateTime.current - timedelta(hours=3)).isoformat(): "3h ago",
            (FrozenDateTime.current - timedelta(minutes=9)).isoformat(): "9m ago",
            (FrozenDateTime.current + timedelta(minutes=1)).isoformat(): "just now",
            "broken": "?",
        }
        for timestamp, expected in cases.items():
            with self.subTest(timestamp=timestamp):
                self.assertEqual(format_age(timestamp), expected)


if __name__ == "__main__":
    unittest.main()
