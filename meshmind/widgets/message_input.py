"""MESHMIND Message Input Widget"""

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Input, Button
from textual.message import Message


class MessageInput(Horizontal):
    """Control bar with message input, send button, and AI chat toggle"""

    DEFAULT_CSS = """
    MessageInput {
        height: auto;
        padding: 1 1 1 0;
        background: $surface;
        border-top: solid $primary;
        align: left middle;
    }

    MessageInput Input {
        width: 1fr;
        margin: 0 1;
        background: $background;
        border: solid $primary;
        color: $text;
    }

    MessageInput Input:focus {
        border: solid $accent;
    }

    MessageInput Button {
        width: auto;
        min-width: 8;
        height: 3;
        margin: 0 0 0 1;
        padding: 0 1;
        border: solid $primary;
        background: $background;
        color: $text;
    }

    MessageInput #msg-send {
        background: $background;
        color: $primary;
        text-style: bold;
    }

    MessageInput #msg-send:hover {
        background: $primary;
        color: $background;
    }

    MessageInput #ai-toggle.ai-on {
        border: solid $success;
        color: $success;
        text-style: bold;
    }

    MessageInput #ai-toggle.ai-off {
        border: solid $error;
        color: $error;
    }

    MessageInput #tts-toggle.tts-on {
        border: solid $success;
        color: $success;
        text-style: bold;
    }

    MessageInput #tts-toggle.tts-off {
        border: solid $surface-lighten-2;
        color: $text 50%;
    }
    """

    class Submitted(Message):
        """Posted when a message is submitted."""

        def __init__(self, text: str) -> None:
            super().__init__()
            self.text = text

    class AIChatToggled(Message):
        """Posted when AI chat is toggled on or off."""

        def __init__(self, active: bool) -> None:
            super().__init__()
            self.active = active

    class TTSToggled(Message):
        """Posted when TTS is toggled on or off."""

        def __init__(self, active: bool) -> None:
            super().__init__()
            self.active = active

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ai_active = True
        self._tts_active = False

    @property
    def ai_active(self) -> bool:
        """Whether AI chat responses are active."""
        return self._ai_active

    def compose(self) -> ComposeResult:
        yield Input(placeholder="Type a message...", id="msg-input")
        yield Button("Send", id="msg-send")
        yield Button("AI \u25cf", id="ai-toggle", classes="ai-on")
        yield Button("TTS \u25cb", id="tts-toggle", classes="tts-off")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter key in input field."""
        self._submit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        if event.button.id == "msg-send":
            self._submit()
        elif event.button.id == "ai-toggle":
            self.toggle_ai()
        elif event.button.id == "tts-toggle":
            self.toggle_tts()

    def _submit(self) -> None:
        """Submit the current input text."""
        input_widget = self.query_one("#msg-input", Input)
        text = input_widget.value.strip()
        if text:
            self.post_message(self.Submitted(text))
            input_widget.value = ""

    def toggle_ai(self) -> None:
        """Toggle AI chat on/off and notify the app."""
        self._ai_active = not self._ai_active
        toggle_btn = self.query_one("#ai-toggle", Button)
        if self._ai_active:
            toggle_btn.label = "AI \u25cf"
            toggle_btn.remove_class("ai-off")
            toggle_btn.add_class("ai-on")
        else:
            toggle_btn.label = "AI \u25cb"
            toggle_btn.remove_class("ai-on")
            toggle_btn.add_class("ai-off")
        self.post_message(self.AIChatToggled(self._ai_active))

    def set_ai_active(self, active: bool) -> None:
        """Set AI chat state programmatically."""
        if self._ai_active != active:
            self.toggle_ai()

    def toggle_tts(self) -> None:
        """Toggle TTS on/off and notify the app."""
        self._tts_active = not self._tts_active
        toggle_btn = self.query_one("#tts-toggle", Button)
        if self._tts_active:
            toggle_btn.label = "TTS \u25cf"
            toggle_btn.remove_class("tts-off")
            toggle_btn.add_class("tts-on")
        else:
            toggle_btn.label = "TTS \u25cb"
            toggle_btn.remove_class("tts-on")
            toggle_btn.add_class("tts-off")
        self.post_message(self.TTSToggled(self._tts_active))

    def set_tts_active(self, active: bool) -> None:
        """Set TTS state programmatically."""
        if self._tts_active != active:
            self.toggle_tts()
