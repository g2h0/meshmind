"""MESHMON MQTT Panel Widget - MQTT broker stats display"""

from typing import List

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Static, Sparkline
from rich.text import Text


class MetricCard(Static):
    """A single-line metric display: label + value"""

    DEFAULT_CSS = """
    MetricCard {
        height: auto;
        max-height: 1;
        padding: 0;
        margin: 0;
    }
    """

    COLOR_MAP = {
        "success": "green",
        "warning": "yellow",
        "error": "red",
    }

    def __init__(self, label: str, value: str = "-", value_class: str = "", **kwargs):
        super().__init__(self._format(label, value, value_class), **kwargs)
        self._label = label

    @staticmethod
    def _format(label: str, value: str, value_class: str = "") -> Text:
        text = Text()
        text.append(f"{label}: ", style="dim")
        color = MetricCard.COLOR_MAP.get(value_class, "")
        style = f"bold {color}".strip()
        text.append(value, style=style)
        return text

    def update_value(self, value: str, value_class: str = "") -> None:
        self.update(self._format(self._label, value, value_class))


class SectionHeader(Static):
    """A styled section header"""

    DEFAULT_CSS = """
    SectionHeader {
        color: $accent;
        text-style: bold;
        text-align: center;
        padding: 0;
        margin: 0;
        border-top: solid $primary;
        border-bottom: solid $primary;
    }

    SectionHeader:first-child {
        border-top: none;
    }
    """


class MQTTPanel(Vertical):
    """Panel showing MQTT broker statistics"""

    DEFAULT_CSS = """
    MQTTPanel {
        width: 50;
        border: solid $primary;
        background: $surface;
        padding: 0;
        overflow-y: auto;
        scrollbar-size: 0 0;
    }
    """

    def compose(self) -> ComposeResult:
        yield MetricCard("Status", "Connecting...", id="mqtt-status")
        yield MetricCard("Broker", "--", id="mqtt-broker")
        yield MetricCard("Msgs/hr", "--", id="mqtt-rate")
        yield MetricCard("Topics", "--", id="mqtt-topics")
        yield MetricCard("Reconnects", "0", id="mqtt-reconnects")
        yield MetricCard("Since", "--", id="mqtt-since")
        yield MetricCard("Last Msg", "--", id="mqtt-last-msg")
        yield MetricCard("Avg Rate", "--", id="mqtt-avg-rate")
        yield MetricCard("Uptime", "--", id="mqtt-uptime")
        yield MetricCard("Avg Conn", "--", id="mqtt-avg-conn")
        yield MetricCard("Stale", "--", id="mqtt-stale")
        yield Static(" ", id="mqtt-spacer-1")
        yield Sparkline([], id="mqtt-sparkline")
        yield Static("  msgs/hr", classes="sparkline-label")
        yield Static(" ", id="mqtt-spacer-2")
        yield Static("  Waiting...", id="mqtt-recent-topics")

    def update_from_status(self, mqtt: dict) -> None:
        """Update all MQTT metrics from status dict"""
        if not mqtt:
            return

        connected = mqtt.get("connected", False)
        status_str = "Connected" if connected else "Disconnected"
        status_class = "success" if connected else "error"

        self._update_metric("mqtt-status", status_str, status_class)
        self._update_metric("mqtt-broker", mqtt.get("broker", "--"))

        # Msgs/hr with trend arrow
        rate = mqtt.get("msgs_per_hour", 0)
        trend = mqtt.get("rate_trend", "=")
        trend_arrow = {"^": " ^", "v": " v", "=": ""}[trend]
        avg_rate = mqtt.get("avg_rate", 0)
        if avg_rate > 0 and rate < avg_rate * 0.5:
            rate_class = "warning"
        elif rate > 0:
            rate_class = "success"
        else:
            rate_class = "error" if connected else ""
        self._update_metric("mqtt-rate", f"{rate:.0f}/hr{trend_arrow}", rate_class)

        self._update_metric("mqtt-topics", str(mqtt.get("active_topics", 0)))
        self._update_metric("mqtt-reconnects", str(mqtt.get("reconnects", 0)),
                           "warning" if mqtt.get("reconnects", 0) > 0 else "")
        self._update_metric("mqtt-since", mqtt.get("connected_since", "--"))

        # Health metrics
        msg_age = mqtt.get("last_msg_age")
        if msg_age is None:
            last_msg_class = ""
        elif msg_age > 60:
            last_msg_class = "error"
        elif msg_age > 30:
            last_msg_class = "warning"
        else:
            last_msg_class = "success"
        self._update_metric("mqtt-last-msg", mqtt.get("last_msg", "--"), last_msg_class)

        self._update_metric("mqtt-avg-rate", f"{avg_rate:.0f}/hr" if avg_rate else "--")

        uptime_secs = mqtt.get("uptime_secs")
        if uptime_secs is None:
            uptime_class = ""
        elif uptime_secs > 600:
            uptime_class = "success"
        elif uptime_secs > 60:
            uptime_class = "warning"
        else:
            uptime_class = "error"
        self._update_metric("mqtt-uptime", mqtt.get("uptime", "--"), uptime_class)

        self._update_metric("mqtt-avg-conn", mqtt.get("avg_conn", "--"))

        stale = mqtt.get("stale_topics", 0)
        stale_class = "warning" if stale > 0 else ""
        self._update_metric("mqtt-stale", f"{stale} topics", stale_class)

        # Update sparkline
        rate_history = mqtt.get("rate_history", [])
        if rate_history:
            try:
                sparkline = self.query_one("#mqtt-sparkline", Sparkline)
                sparkline.data = rate_history
            except Exception:
                pass

        # Update recent topics
        topics = mqtt.get("recent_topics", [])
        if topics:
            lines = []
            for topic in topics[-6:]:
                if len(topic) > 46:
                    topic = topic[:43] + "..."
                lines.append(f"  {topic}")
            try:
                widget = self.query_one("#mqtt-recent-topics", Static)
                widget.update("\n".join(lines))
            except Exception:
                pass

    def _update_metric(self, card_id: str, value: str, value_class: str = "") -> None:
        try:
            card = self.query_one(f"#{card_id}", MetricCard)
            card.update_value(value, value_class)
        except Exception:
            pass
