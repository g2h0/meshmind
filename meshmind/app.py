"""MESHMIND TUI Application"""

import logging
from logging.handlers import RotatingFileHandler

from .log_filters import LibraryNoiseFilter
from pathlib import Path

# Suppress all console logging BEFORE any other imports
# This prevents libraries from writing to stdout/stderr which breaks Textual
logging.getLogger().handlers.clear()
logging.getLogger().setLevel(logging.INFO)

# Suppress meshtastic library logging to console
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.mesh_interface").setLevel(logging.WARNING)

from textual import on
from textual.app import App, ComposeResult
from textual.command import Provider, Hit, DiscoveryHit
from textual.containers import Horizontal, Vertical
from textual.theme import Theme
from textual.widgets import Static, Header

from .widgets import LogViewer, MessageInput, StatusPanel, ThemePicker, VoicePicker
from .themes import THEMES, get_theme
from .tts import TTSEngine
from .utils import Settings

logger = logging.getLogger(__name__)


class MeshmindCommands(Provider):
    """Command palette entries for MESHMIND."""

    _COMMANDS = [
        ("Quit", "Stop the bot and exit", "action_quit"),
        ("Clear Logs", "Clear the log viewer", "action_clear_logs"),
        ("Reconnect", "Reconnect to Meshtastic device", "action_reconnect"),
        ("Theme", "Open theme picker", "action_open_theme_picker"),
        ("Toggle AI Chat", "Pause or resume AI chat responses", "action_toggle_ai_chat"),
        ("Toggle TTS", "Enable or disable text-to-speech", "action_toggle_tts"),
        ("TTS Voice", "Select text-to-speech voice", "action_open_voice_picker"),
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


class MeshmindApp(App):
    """MESHMIND Textual TUI Application"""

    TITLE = "MeshMind"
    SUB_TITLE = "v0.3"

    CSS_PATH = Path(__file__).parent / "themes" / "styles.tcss"

    COMMANDS = {MeshmindCommands}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._settings = Settings()
        self._bot = None  # Will be lazy-loaded
        self._current_theme = self._settings.theme
        self._tts = TTSEngine(
            voice=self._settings.tts_voice,
            enabled=self._settings.tts_enabled,
        )

    def compose(self) -> ComposeResult:
        """Create the UI layout"""
        yield Header(show_clock=True)

        # Main content area
        with Horizontal(id="main-content"):
            # Log viewer (flexible width)
            with Vertical(id="log-panel"):
                yield LogViewer(id="log-viewer")

            # Status panel (fixed width)
            yield StatusPanel(id="status-panel")

        yield MessageInput(id="message-input")

    def on_mount(self) -> None:
        """Initialize the app on mount"""
        # Register all custom themes
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

        # Apply saved theme
        self._apply_theme(self._current_theme)

        # Restore TTS button state from settings
        try:
            msg_input = self.query_one("#message-input", MessageInput)
            msg_input.set_tts_active(self._settings.tts_enabled)
        except Exception:
            pass

        # Start bot in a worker thread to prevent it from blocking input
        self.run_worker(self._start_bot_worker, thread=True)

    def _start_bot_worker(self) -> None:
        """Initialize and start the bot in a worker thread"""
        import time

        try:
            from .bot import MeshmindBot

            # Meshtastic import in bot.py may add console/stream handlers that
            # write to stdout/stderr, breaking Textual's terminal control.
            # Clear them now, preserving only our TUI log handler.
            from .widgets.log_viewer import TUILogHandler
            for name in list(logging.Logger.manager.loggerDict.keys()) + ['']:
                log = logging.getLogger(name)
                log.handlers = [h for h in log.handlers if isinstance(h, (TUILogHandler, logging.FileHandler))]

            self._bot = MeshmindBot(
                on_status_change=self._on_bot_status_change,
                on_message_received=self._on_bot_message_received,
            )
            self.call_from_thread(self._set_bot_on_panel)

            # Retry connection with increasing delay
            max_attempts = 5
            for attempt in range(max_attempts):
                success = self._bot.start()
                if success:
                    return
                if attempt < max_attempts - 1:
                    delay = min(10 * (attempt + 1), 60)
                    logger.warning(
                        f"Connection attempt {attempt + 1}/{max_attempts} failed, retrying in {delay}s..."
                    )
                    time.sleep(delay)

            logger.error("Failed to start bot after all connection attempts")
        except Exception as e:
            logger.error(f"Error starting bot: {e}")

    def _set_bot_on_panel(self) -> None:
        """Set bot reference on status panel (must run on main thread)"""
        try:
            status_panel = self.query_one("#status-panel", StatusPanel)
            status_panel.set_bot(self._bot)
        except Exception as e:
            logger.error(f"Error setting bot on panel: {e}")

    def _on_bot_status_change(self, status: dict) -> None:
        """Handle bot status changes (called from bot thread)"""
        # This is called from the bot thread, so we use call_from_thread
        pass  # Status panel polls the bot directly

    def _on_bot_message_received(self, text: str, channel: int) -> None:
        """Handle incoming mesh messages for TTS (called from bot thread)."""
        from .config import cfg

        if channel == cfg.MESH_CHANNEL:
            self._tts.speak(text)

    def _apply_theme(self, theme_name: str) -> None:
        """Apply a theme to the app"""
        theme_def = get_theme(theme_name)
        self.theme = theme_name
        self._current_theme = theme_name
        self._settings.theme = theme_name
        logger.info(f"Theme changed to: {theme_def['display_name']}")

    def action_quit(self) -> None:
        """Quit the application"""
        self._tts.stop()
        if self._bot:
            self.run_worker(lambda: self._bot.stop(), thread=True)
        self.exit()

    def action_open_theme_picker(self) -> None:
        """Open the theme picker modal"""
        def on_theme_selected(theme_name: str | None) -> None:
            if theme_name:
                self._apply_theme(theme_name)
                self.refresh()

        self.push_screen(ThemePicker(self._current_theme), on_theme_selected)

    def action_clear_logs(self) -> None:
        """Clear the log viewer"""
        log_viewer = self.query_one("#log-viewer", LogViewer)
        log_viewer.clear_logs()

    def on_message_input_submitted(self, event: MessageInput.Submitted) -> None:
        """Handle manual message send from input bar."""
        if not self._bot:
            logger.warning("Cannot send message: bot not connected")
            return
        self.run_worker(
            lambda: self._bot._send_message(event.text),
            thread=True,
        )

    @on(MessageInput.AIChatToggled)
    def on_message_input_ai_chat_toggled(self, event: MessageInput.AIChatToggled) -> None:
        """Handle AI chat toggle from the control bar."""
        if self._bot:
            self._bot.chat_paused = not event.active
        state = "enabled" if event.active else "paused"
        logger.info(f"AI chat {state}")

    def action_toggle_ai_chat(self) -> None:
        """Toggle AI chat responses on/off via command palette."""
        try:
            msg_input = self.query_one("#message-input", MessageInput)
            msg_input.toggle_ai()
        except Exception:
            pass

    def action_reconnect(self) -> None:
        """Reconnect the bot (runs in worker thread to avoid blocking input)"""
        if self._bot:
            logger.info("Re-establishing mesh link...")
            self.run_worker(self._reconnect_worker, thread=True)

    def _reconnect_worker(self) -> None:
        """Reconnect bot in a worker thread"""
        if self._bot:
            success = self._bot.reconnect()
            if success:
                logger.info("Mesh link reacquired")
            else:
                logger.error("Reconnection failed")

    @on(MessageInput.TTSToggled)
    def on_message_input_tts_toggled(self, event: MessageInput.TTSToggled) -> None:
        """Handle TTS toggle from the control bar."""
        self._tts.enabled = event.active
        self._settings.tts_enabled = event.active
        state = "enabled" if event.active else "disabled"
        logger.info(f"TTS {state}")

    def action_toggle_tts(self) -> None:
        """Toggle TTS on/off via command palette."""
        try:
            msg_input = self.query_one("#message-input", MessageInput)
            msg_input.toggle_tts()
        except Exception:
            pass

    def action_open_voice_picker(self) -> None:
        """Open the voice picker modal."""
        _FALLBACK_VOICES = [
            "af_alloy", "af_aoede", "af_bella", "af_heart", "af_jessica",
            "af_kore", "af_nicole", "af_nova", "af_river", "af_sarah",
            "af_sky", "am_adam", "am_echo", "am_eric", "am_fenrir",
            "am_liam", "am_michael", "am_onyx", "am_puck", "am_santa",
            "bf_alice", "bf_emma", "bf_isabella", "bf_lily",
            "bm_daniel", "bm_fable", "bm_george", "bm_lewis",
        ]

        def _load_and_show():
            voices = self._tts.list_voices()
            if not voices:
                voices = _FALLBACK_VOICES

            def _show(voices_list):
                def on_voice_selected(voice_name):
                    if voice_name:
                        self._tts.voice = voice_name
                        self._settings.tts_voice = voice_name
                        logger.info(f"TTS voice changed to: {voice_name}")

                self.push_screen(
                    VoicePicker(voices_list, self._tts.voice),
                    on_voice_selected,
                )

            self.call_from_thread(_show, voices)

        self.run_worker(_load_and_show, thread=True)


def run_app():
    """Entry point for the TUI application"""
    # Aggressively clear ALL handlers from ALL loggers
    # This ensures no library can write to stdout/stderr and break Textual
    for name in list(logging.Logger.manager.loggerDict.keys()) + ['']:
        log = logging.getLogger(name)
        log.handlers.clear()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Set up persistent file logging so logs survive after the app exits
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    file_handler = RotatingFileHandler(
        log_dir / "meshmind.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB per file
        backupCount=5,              # keep up to 5 rotated files (25 MB total)
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    file_handler.addFilter(LibraryNoiseFilter())
    root_logger.addHandler(file_handler)

    import sys
    sys.stdout.write("\033]0;MeshMind\007")
    sys.stdout.flush()

    app = MeshmindApp()
    app.run()


if __name__ == "__main__":
    run_app()
