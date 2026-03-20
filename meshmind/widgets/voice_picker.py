"""MESHMIND Voice Picker Modal"""

from typing import List

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Static, OptionList
from textual.widgets.option_list import Option


class VoicePicker(ModalScreen[str]):
    """Modal screen for selecting a TTS voice"""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "select", "Select"),
    ]

    def __init__(self, voices: List[str], current_voice: str = "af_heart", **kwargs):
        super().__init__(**kwargs)
        self._voices = voices
        self._current_voice = current_voice

    def compose(self) -> ComposeResult:
        with Vertical(id="voice-picker-container"):
            yield Static("Select TTS Voice", id="voice-picker-title")

            option_list = OptionList(id="voice-list")
            yield option_list

            yield Static("Enter: Select | Escape: Cancel", id="voice-picker-hint")

    def on_mount(self) -> None:
        """Populate voice list on mount"""
        option_list = self.query_one("#voice-list", OptionList)

        for voice_name in self._voices:
            display_name = voice_name
            if voice_name == self._current_voice:
                display_name = f"* {voice_name}"
            option_list.add_option(Option(display_name, id=voice_name))

        if self._current_voice in self._voices:
            index = self._voices.index(self._current_voice)
            option_list.highlighted = index

        option_list.focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Handle voice selection"""
        if event.option.id:
            self.dismiss(event.option.id)

    def action_cancel(self) -> None:
        """Cancel voice selection"""
        self.dismiss(None)

    def action_select(self) -> None:
        """Select the highlighted voice"""
        option_list = self.query_one("#voice-list", OptionList)
        if option_list.highlighted is not None:
            option = option_list.get_option_at_index(option_list.highlighted)
            if option and option.id:
                self.dismiss(option.id)
