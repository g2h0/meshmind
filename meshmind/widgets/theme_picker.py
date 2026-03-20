"""MESHMIND Theme Picker Modal"""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Static, OptionList
from textual.widgets.option_list import Option

from meshmind.themes import THEMES, get_theme_names


class ThemePicker(ModalScreen[str]):
    """Modal screen for selecting a theme"""

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

            option_list = OptionList(id="theme-list")
            yield option_list

            yield Static("Enter: Select | Escape: Cancel", id="theme-picker-hint")

    def on_mount(self) -> None:
        """Populate theme list on mount"""
        option_list = self.query_one("#theme-list", OptionList)

        # Add all themes as options
        for theme_name in get_theme_names():
            theme = THEMES[theme_name]
            display_name = theme["display_name"]

            # Mark current theme
            if theme_name == self._current_theme:
                display_name = f"* {display_name}"

            option_list.add_option(Option(display_name, id=theme_name))

        # Highlight current theme
        theme_names = get_theme_names()
        if self._current_theme in theme_names:
            index = theme_names.index(self._current_theme)
            option_list.highlighted = index

        option_list.focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Handle theme selection"""
        if event.option.id:
            self.dismiss(event.option.id)

    def action_cancel(self) -> None:
        """Cancel theme selection"""
        self.dismiss(None)

    def action_select(self) -> None:
        """Select the highlighted theme"""
        option_list = self.query_one("#theme-list", OptionList)
        if option_list.highlighted is not None:
            option = option_list.get_option_at_index(option_list.highlighted)
            if option and option.id:
                self.dismiss(option.id)
