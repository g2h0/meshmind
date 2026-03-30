"""MESHMIND Log Viewer Widget"""

import logging
import queue
from collections import deque
from datetime import datetime
from typing import Optional

from textual.events import Resize
from textual.widgets import RichLog
from rich.table import Table
from rich.text import Text

from ..log_filters import LibraryNoiseFilter
from ..themes import get_theme

MAX_LOG_ENTRIES = 2000


class TUILogHandler(logging.Handler):
    """Custom logging handler that queues logs for the TUI LogViewer.

    Uses a non-blocking queue instead of call_from_thread to avoid deadlocks
    between worker threads (holding locks while logging) and the main thread
    (acquiring those same locks in StatusPanel polling).
    """

    def __init__(self, log_viewer: "LogViewer"):
        super().__init__()
        self.log_viewer = log_viewer
        self._queue: queue.Queue = queue.Queue()
        self.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

    def emit(self, record: logging.LogRecord) -> None:
        """Queue a log record for the TUI (non-blocking)."""
        try:
            msg = self.format(record)
            self._queue.put_nowait((record.levelname, msg))
        except Exception:
            self.handleError(record)

    def drain(self) -> list:
        """Drain all pending log messages. Called from main thread timer."""
        items = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return items


class LogViewer(RichLog):
    """RichLog widget with custom styling for log levels"""

    DEFAULT_CSS = """
    LogViewer {
        background: $background;
        padding: 0 1;
        scrollbar-size: 0 0;
    }
    """

    # Maps log levels to theme color keys
    LEVEL_COLOR_KEYS = {
        "DEBUG": "text_muted",
        "INFO": "success",
        "WARNING": "warning",
        "ERROR": "error",
        "CRITICAL": "error",
    }

    def __init__(self, **kwargs):
        super().__init__(
            highlight=True,
            markup=True,
            wrap=True,
            auto_scroll=True,
            **kwargs
        )
        self._handler: Optional[TUILogHandler] = None
        self._log_entries: deque[tuple[str, str]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._last_width: Optional[int] = None
        self._reflowing: bool = False

    def _get_theme_colors(self) -> dict:
        """Get color hex values from the current theme"""
        try:
            theme_name = self.app._current_theme
            return get_theme(theme_name)["colors"]
        except Exception:
            return get_theme("tokyo-night")["colors"]

    def on_mount(self) -> None:
        """Set up the logging handler when mounted"""
        self._setup_logging()
        self.set_interval(0.1, self._drain_log_queue)

    def _drain_log_queue(self) -> None:
        """Drain queued log messages onto the widget (runs on main thread)."""
        if self._handler:
            for level, message in self._handler.drain():
                self.write_log(level, message)

    def _setup_logging(self) -> None:
        """Configure logging to output to this widget"""
        # Create and attach our custom handler
        self._handler = TUILogHandler(self)
        self._handler.setLevel(logging.DEBUG)
        self._handler.addFilter(LibraryNoiseFilter())

        # Add handler to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(self._handler)

    def write_log(self, level: str, message: str) -> None:
        """Write a log message with colored parts: timestamp, level, message"""
        if not self._reflowing:
            self._log_entries.append((level, message))
        self._write_styled(level, message)

    def _write_styled(self, level: str, message: str) -> None:
        """Render and write a styled log entry to the RichLog."""
        colors = self._get_theme_colors()

        parts = message.split(" - ", 2)
        if len(parts) >= 3:
            timestamp, _level_str, msg = parts

            prefix = Text()
            prefix.append(timestamp, style=colors["accent"])
            prefix.append(" - ")

            is_received = msg.startswith("Received")
            is_sent = msg.startswith("Sent:")
            is_bbs = msg.startswith("BBS ")
            _msg_lower = msg.lower()
            is_conn_error = any(kw in _msg_lower for kw in [
                "connection lost", "connection init failed",
                "reconnection attempt", "terminating meshtastic reader",
                "reconnection failed", "link severed", "link down",
                "link recovery attempt",
            ])
            is_conn_ok = any(kw in _msg_lower for kw in [
                "connection established", "reconnection successful",
                "mesh link reacquired", "re-establishing mesh link",
            ])
            if is_conn_error:
                msg_color = colors["error"]
            elif is_conn_ok:
                msg_color = colors["success"]
            elif is_received:
                msg_color = colors["msg_received"]
            elif is_sent:
                msg_color = colors["accent"]
            elif is_bbs:
                msg_color = colors["msg_sent"]
            elif any(kw in _msg_lower for kw in [
                "meshmind awakening", "neural cortex", "topology mapped",
                "subsystems nominal", "chronometrics synchronized",
                "heliophysics baseline", "meshmind operational",
                "mesh radio link", "voice synthesis engine online",
                "shutdown sequence", "systems dark", "systems diagnostic green",
                "voice synthesis modules", "environmental telemetry broadcast",
            ]):
                msg_color = colors["success"]
            else:
                color_key = self.LEVEL_COLOR_KEYS.get(level, "text")
                msg_color = colors[color_key]
            msg_text = Text(msg, style=msg_color)

            table = Table(
                show_header=False, show_edge=False, show_lines=False,
                box=None, padding=0, expand=True,
            )
            table.add_column(width=len(timestamp) + 3, no_wrap=True)
            table.add_column(ratio=1)
            table.add_row(prefix, msg_text)

            self.write(table)
        else:
            text = Text()
            color_key = self.LEVEL_COLOR_KEYS.get(level, "text")
            text.append(message, style=colors[color_key])
            self.write(text)

    def on_resize(self, event: Resize) -> None:
        """Re-render all log entries when the width changes."""
        new_width = event.size.width
        if self._last_width is not None and new_width != self._last_width:
            self._reflow()
        self._last_width = new_width

    def _reflow(self) -> None:
        """Clear and re-render all stored log entries at the new width."""
        self._reflowing = True
        try:
            self.clear()
            for level, message in self._log_entries:
                self._write_styled(level, message)
        finally:
            self._reflowing = False

    def clear_logs(self) -> None:
        """Clear all log entries"""
        self._log_entries.clear()
        self.clear()
        self.write_log("INFO", f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - Logs cleared")

    def on_unmount(self) -> None:
        """Clean up the logging handler"""
        if self._handler:
            root_logger = logging.getLogger()
            root_logger.removeHandler(self._handler)
