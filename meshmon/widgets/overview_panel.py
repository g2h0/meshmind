"""MESHMON Overview Panel - Top-level summary bar"""

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Static
from rich.text import Text


class SummaryCard(Static):
    """A single summary metric in the overview bar"""

    DEFAULT_CSS = """
    SummaryCard {
        height: 1;
        padding: 0 1;
        content-align: center middle;
    }
    """

    COLOR_MAP = {
        "success": "green",
        "warning": "yellow",
        "error": "red",
        "accent": "cyan",
    }

    def __init__(self, label: str, value: str = "--", style_class: str = "", **kwargs):
        super().__init__(self._format(label, value, style_class), **kwargs)
        self._label = label

    @staticmethod
    def _format(label: str, value: str, style_class: str = "") -> Text:
        text = Text()
        color = SummaryCard.COLOR_MAP.get(style_class, "")
        style = f"bold {color}".strip()
        text.append(f" {label}: ", style="dim")
        text.append(value, style=style)
        text.append(" ")
        return text

    def update_value(self, value: str, style_class: str = "") -> None:
        self.update(self._format(self._label, value, style_class))


class OverviewPanel(Horizontal):
    """Top bar showing aggregate service status"""

    DEFAULT_CSS = """
    OverviewPanel {
        height: 1;
        dock: top;
        background: $surface;
        border-bottom: solid $primary;
    }
    """

    def compose(self) -> ComposeResult:
        yield SummaryCard("Services", "--", id="overview-services")
        yield SummaryCard("Avg", "--", id="overview-avg")
        yield SummaryCard("Uptime", "--", id="overview-uptime")
        yield SummaryCard("MQTT", "--", id="overview-mqtt")

    def update_from_status(self, status: dict) -> None:
        """Update all summary cards from engine status"""
        summary = status.get("summary", {})
        mqtt = status.get("mqtt", {})

        # Services count
        up = summary.get("up", 0)
        total = summary.get("total", 0)
        down = summary.get("down", 0)
        svc_style = "success" if down == 0 else "error" if up == 0 else "warning"
        try:
            self.query_one("#overview-services", SummaryCard).update_value(
                f"{up}/{total} UP", svc_style
            )
        except Exception:
            pass

        # Avg response
        avg = summary.get("avg_response_ms", 0)
        avg_style = "success" if avg < 1000 else "warning" if avg < 3000 else "error"
        try:
            self.query_one("#overview-avg", SummaryCard).update_value(
                f"{avg:.0f}ms" if avg else "--", avg_style
            )
        except Exception:
            pass

        # Uptime
        uptime = summary.get("overall_uptime", 0)
        up_style = "success" if uptime >= 99 else "warning" if uptime >= 95 else "error"
        try:
            self.query_one("#overview-uptime", SummaryCard).update_value(
                f"{uptime:.1f}%" if uptime else "--", up_style
            )
        except Exception:
            pass

        # MQTT
        mqtt_connected = mqtt.get("connected", False)
        try:
            self.query_one("#overview-mqtt", SummaryCard).update_value(
                "Connected" if mqtt_connected else "Disconnected",
                "success" if mqtt_connected else "error",
            )
        except Exception:
            pass
