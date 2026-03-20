"""MESHMIND BBS — Public Message Board"""

import json
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

MAX_POSTS = 5
EXPIRY_DAYS = 7
DATA_FILE = Path(__file__).parent.parent.parent / "data" / "bbs_board.json"


class BbsBoard:
    """Thread-safe persistent public message board."""

    def __init__(self, data_file: Path = DATA_FILE):
        self._path = data_file
        self._lock = threading.Lock()
        with self._lock:
            self._posts: list[dict] = self._load()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def add_post(self, node_id: int, node_name: str, message: str) -> int:
        """Add a post. Returns number of posts after pruning."""
        with self._lock:
            self._prune()
            self._posts.append({
                "node_id": node_id,
                "node_name": node_name,
                "message": message[:160],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            # Drop oldest if over cap
            if len(self._posts) > MAX_POSTS:
                self._posts = self._posts[-MAX_POSTS:]
            self._save()
            return len(self._posts)

    def get_posts(self) -> list[dict]:
        """Return current posts (pruned, oldest first)."""
        with self._lock:
            if self._prune():
                self._save()
            return list(self._posts)

    # ------------------------------------------------------------------ #
    # Internals                                                            #
    # ------------------------------------------------------------------ #

    def _prune(self) -> bool:
        """Remove posts older than EXPIRY_DAYS. Returns True if any were removed. Call inside lock."""
        before = len(self._posts)
        cutoff = datetime.now(timezone.utc) - timedelta(days=EXPIRY_DAYS)
        self._posts = [
            p for p in self._posts
            if datetime.fromisoformat(p["timestamp"]) > cutoff
        ]
        return len(self._posts) < before

    def _load(self) -> list:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else []
            except (json.JSONDecodeError, IOError):
                pass
        return []

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._posts, f, indent=2)
        except IOError:
            pass


def format_age(timestamp_str: str) -> str:
    """Return human-readable relative age string."""
    try:
        ts = datetime.fromisoformat(timestamp_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - ts
        if delta.total_seconds() < 0:
            return "just now"
        if delta.days >= 1:
            return f"{delta.days}d ago"
        hours = delta.seconds // 3600
        if hours >= 1:
            return f"{hours}h ago"
        minutes = delta.seconds // 60
        if minutes >= 1:
            return f"{minutes}m ago"
        return "just now"
    except Exception:
        return "?"
