"""MESHMON Log Viewer Widget - adapted from meshmind"""

import logging
import queue
from collections import deque
from datetime import datetime
from typing import Optional

from textual.events import Resize
from textual.widgets import RichLog
from rich.table import Table
from rich.text import Text

from ..themes import get_theme

MAX_LOG_ENTRIES = 2000


class TUILogHandler(logging.Handler):
    """Custom logging handler that queues logs for the TUI LogViewer.

    Uses a non-blocking queue to avoid deadlocks between worker threads
    and the main thread.
    """

    def __init__(self):
        super().__init__()
        self._queue: queue.Queue = queue.Queue()
        self.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._queue.put_nowait((record.levelname, msg))
        except Exception:
            self.handleError(record)

    def drain(self) -> list:
        items = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return items


class LogViewer(RichLog):
    """RichLog widget with colored log output"""

    DEFAULT_CSS = """
    LogViewer {
        background: $background;
        padding: 0 1;
        scrollbar-size: 0 0;
    }
    """

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
            **kwargs,
        )
        self._handler: Optional[TUILogHandler] = None
        self._log_entries: deque[tuple[str, str]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._last_width: Optional[int] = None
        self._reflowing: bool = False

    def _get_theme_colors(self) -> dict:
        try:
            theme_name = self.app._current_theme
            return get_theme(theme_name)["colors"]
        except Exception:
            return get_theme("tokyo-night")["colors"]

    def on_mount(self) -> None:
        self._setup_logging()
        self.set_interval(0.1, self._drain_log_queue)

    def _drain_log_queue(self) -> None:
        if self._handler:
            for level, message in self._handler.drain():
                self.write_log(level, message)

    def _setup_logging(self) -> None:
        self._handler = TUILogHandler()
        self._handler.setLevel(logging.DEBUG)
        root_logger = logging.getLogger()
        root_logger.addHandler(self._handler)

    def write_log(self, level: str, message: str) -> None:
        if not self._reflowing:
            self._log_entries.append((level, message))
        self._write_styled(level, message)

    def _write_styled(self, level: str, message: str) -> None:
        colors = self._get_theme_colors()

        parts = message.split(" - ", 2)
        if len(parts) >= 3:
            timestamp, _level_str, msg = parts

            prefix = Text()
            prefix.append(timestamp, style=colors["accent"])
            prefix.append(" - ")

            # Color service status messages
            _msg_lower = msg.lower()
            if ": up " in _msg_lower or ": up(" in _msg_lower:
                msg_color = colors["success"]
            elif ": down" in _msg_lower:
                msg_color = colors["error"]
            elif ": degraded" in _msg_lower:
                msg_color = colors["warning"]
            elif "mqtt connected" in _msg_lower:
                msg_color = colors["success"]
            elif "mqtt disconnect" in _msg_lower:
                msg_color = colors["error"]
            elif "monitor engine" in _msg_lower:
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
        new_width = event.size.width
        if self._last_width is not None and new_width != self._last_width:
            self._reflow()
        self._last_width = new_width

    def _reflow(self) -> None:
        self._reflowing = True
        try:
            self.clear()
            for level, message in self._log_entries:
                self._write_styled(level, message)
        finally:
            self._reflowing = False

    def clear_logs(self) -> None:
        self._log_entries.clear()
        self.clear()
        self.write_log(
            "INFO",
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - Logs cleared",
        )

    def on_unmount(self) -> None:
        if self._handler:
            root_logger = logging.getLogger()
            root_logger.removeHandler(self._handler)
