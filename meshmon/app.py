"""MESHMON TUI Application"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

# Suppress all console logging BEFORE any other imports
logging.getLogger().handlers.clear()
logging.getLogger().setLevel(logging.INFO)

from textual.app import App, ComposeResult
from textual.command import Provider, Hit, DiscoveryHit
from textual.containers import Horizontal
from textual.theme import Theme
from textual.widgets import Static, Header

from .widgets import OverviewPanel, ServiceTable, MQTTPanel
from .themes import THEMES, get_theme
from .config import Settings
from .monitors.engine import MonitorEngine

logger = logging.getLogger(__name__)


class MeshmonCommands(Provider):
    """Command palette entries for MESHMON."""

    _COMMANDS = [
        ("Quit", "Stop monitoring and exit", "action_quit"),
        ("Refresh All", "Immediately check all services", "action_refresh_all"),
        ("Theme", "Open theme picker", "action_open_theme_picker"),
    ]

    async def discover(self) -> DiscoveryHit:
        for name, help_text, action in self._COMMANDS:
            yield DiscoveryHit(name, getattr(self.app, action), help=help_text)

    async def search(self, query: str) -> Hit:
        matcher = self.matcher(query)
        for name, help_text, action in self._COMMANDS:
            score = matcher.match(name)
            if score > 0:
                yield Hit(score, matcher.highlight(name), getattr(self.app, action), help=help_text)


class MeshmonApp(App):
    """MESHMON Service Monitor TUI Application"""

    TITLE = "MeshMon"
    SUB_TITLE = "Service Monitor"

    CSS_PATH = Path(__file__).parent / "styles" / "meshmon.tcss"

    COMMANDS = {MeshmonCommands}

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("t", "open_theme_picker", "Theme"),
        ("r", "refresh_all", "Refresh"),
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._settings = Settings()
        self._current_theme = self._settings.theme
        self._engine = MonitorEngine(self._settings)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield OverviewPanel(id="overview-panel")

        with Horizontal(id="main-content"):
            yield ServiceTable(id="service-panel")
            yield MQTTPanel(id="mqtt-panel")

        yield Static(
            " [b][q][/b]Quit  [b][t][/b]Theme  [b][r][/b]Refresh",
            id="footer-bar",
        )

    def on_mount(self) -> None:
        # Register all themes
        for name, theme_def in THEMES.items():
            colors = theme_def["colors"]
            is_light = name in ("solarized-light", "catppuccin-latte")
            self.register_theme(Theme(
                name=name,
                primary=colors["primary"],
                secondary=colors["secondary"],
                accent=colors["accent"],
                background=colors["background"],
                surface=colors["surface"],
                error=colors["error"],
                success=colors["success"],
                warning=colors["warning"],
                foreground=colors["text"],
                dark=not is_light,
            ))

        self._apply_theme(self._current_theme)

        # Start engine in background worker
        self.run_worker(self._start_engine, thread=True)

        # Set up periodic status refresh
        self.set_interval(2.0, self._refresh_status)

    def _start_engine(self) -> None:
        """Start the monitor engine in a worker thread"""
        try:
            self._engine.start()
        except Exception as e:
            logger.error(f"Engine start failed: {e}")

    def _refresh_status(self) -> None:
        """Periodically refresh all widgets from engine status"""
        try:
            status = self._engine.get_status()
        except Exception:
            return

        # Update overview
        try:
            overview = self.query_one("#overview-panel", OverviewPanel)
            overview.update_from_status(status)
        except Exception:
            pass

        # Update service table
        try:
            table = self.query_one("#service-panel", ServiceTable)
            table.update_from_status(status.get("services", []))
        except Exception:
            pass

        # Update MQTT panel
        try:
            mqtt_panel = self.query_one("#mqtt-panel", MQTTPanel)
            mqtt_panel.update_from_status(status.get("mqtt", {}))
        except Exception:
            pass

    def _apply_theme(self, theme_name: str) -> None:
        theme_def = get_theme(theme_name)
        self.theme = theme_name
        self._current_theme = theme_name
        self._settings.theme = theme_name
        logger.info(f"Theme: {theme_def['display_name']}")

    def action_quit(self) -> None:
        self._engine.stop()
        self.exit()

    def action_open_theme_picker(self) -> None:
        """Open theme picker using Textual's built-in theme command"""
        # Use a simple selection screen
        from .widgets.theme_picker import ThemePicker
        def on_theme_selected(theme_name: str | None) -> None:
            if theme_name:
                self._apply_theme(theme_name)
                self.refresh()
        self.push_screen(ThemePicker(self._current_theme), on_theme_selected)

    def action_refresh_all(self) -> None:
        self._engine.refresh_all()


def run_app():
    """Entry point for the MeshMon TUI"""
    # Clear ALL handlers from ALL loggers
    for name in list(logging.Logger.manager.loggerDict.keys()) + ['']:
        log = logging.getLogger(name)
        log.handlers.clear()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Set up persistent file logging
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    file_handler = RotatingFileHandler(
        log_dir / "meshmon.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root_logger.addHandler(file_handler)

    import sys
    sys.stdout.write("\033]0;MeshMon\007")
    sys.stdout.flush()

    app = MeshmonApp()
    app.run()


if __name__ == "__main__":
    run_app()
