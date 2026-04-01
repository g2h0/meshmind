"""MESHMON Theme Picker Modal"""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Static, OptionList
from textual.widgets.option_list import Option

from ..themes import THEMES, get_theme_names


class ThemePicker(ModalScreen[str]):
    """Modal screen for selecting a theme"""

    DEFAULT_CSS = """
    ThemePicker {
        align: center middle;
    }

    #theme-picker-container {
        width: 40;
        height: 20;
        border: solid $primary;
        background: $surface;
        padding: 1;
    }

    #theme-picker-title {
        text-align: center;
        text-style: bold;
        color: $accent;
        padding: 0 0 1 0;
    }

    #theme-picker-hint {
        text-align: center;
        color: $text-muted;
        padding: 1 0 0 0;
    }

    #theme-list {
        height: 1fr;
    }
    """

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "select", "Select"),
    ]

    def __init__(self, current_theme: str = "tokyo-night", **kwargs):
        super().__init__(**kwargs)
        self._current_theme = current_theme

    def compose(self) -> ComposeResult:
        with Vertical(id="theme-picker-container"):
            yield Static("Select Theme", id="theme-picker-title")
            yield OptionList(id="theme-list")
            yield Static("Enter: Select | Escape: Cancel", id="theme-picker-hint")

    def on_mount(self) -> None:
        option_list = self.query_one("#theme-list", OptionList)
        for theme_name in get_theme_names():
            theme = THEMES[theme_name]
            display_name = theme["display_name"]
            if theme_name == self._current_theme:
                display_name = f"* {display_name}"
            option_list.add_option(Option(display_name, id=theme_name))

        theme_names = get_theme_names()
        if self._current_theme in theme_names:
            option_list.highlighted = theme_names.index(self._current_theme)
        option_list.focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option.id:
            self.dismiss(event.option.id)

    def action_cancel(self) -> None:
        self.dismiss(None)

    def action_select(self) -> None:
        option_list = self.query_one("#theme-list", OptionList)
        if option_list.highlighted is not None:
            option = option_list.get_option_at_index(option_list.highlighted)
            if option and option.id:
                self.dismiss(option.id)
