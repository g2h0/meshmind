"""MESHMON Themes - imports from meshmind with fallback"""

try:
    from meshmind.themes import THEMES, get_theme, get_theme_names
except ImportError:
    THEMES = {
        "tokyo-night": {
            "display_name": "Tokyo Night",
            "colors": {
                "primary": "#7aa2f7",
                "secondary": "#bb9af7",
                "accent": "#7dcfff",
                "background": "#1a1b26",
                "surface": "#24283b",
                "error": "#f7768e",
                "success": "#9ece6a",
                "warning": "#e0af68",
                "text": "#c0caf5",
                "text_muted": "#565f89",
                "msg_received": "#73daca",
                "msg_sent": "#ff9e64",
            },
        },
    }

    def get_theme(name: str) -> dict:
        return THEMES.get(name, THEMES["tokyo-night"])

    def get_theme_names() -> list:
        return list(THEMES.keys())

__all__ = ["THEMES", "get_theme", "get_theme_names"]
