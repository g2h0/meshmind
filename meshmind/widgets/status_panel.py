"""MESHMIND Status Panel Widget - Modern Dashboard Design"""

from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING, List

from textual.app import ComposeResult
from textual.widgets import Static, Sparkline, ProgressBar
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from rich.text import Text

if TYPE_CHECKING:
    from meshmind.bot import MeshmindBot


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

    # Map class names to Rich color names
    COLOR_MAP = {
        "success": "green",
        "warning": "yellow",
        "error": "red",
    }

    def __init__(self, label: str, value: str = "-", value_class: str = "", **kwargs):
        super().__init__(self._format(label, value, value_class), **kwargs)
        self._label = label
        self._value = value
        self._value_class = value_class

    @staticmethod
    def _format(label: str, value: str, value_class: str = "") -> Text:
        text = Text()
        text.append(f"{label}: ", style="dim")
        color = MetricCard.COLOR_MAP.get(value_class, "")
        style = f"bold {color}".strip()
        text.append(value, style=style)
        return text

    def update_value(self, value: str, value_class: str = "") -> None:
        """Update the metric value"""
        self._value = value
        self._value_class = value_class
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


class StatusPanel(Vertical):
    """Modern dashboard status panel"""

    DEFAULT_CSS = """
    StatusPanel {
        padding: 0 1;
        background: $surface;
        scrollbar-size: 0 0;
    }

    StatusPanel ProgressBar {
        padding: 0;
        margin: 0;
    }

    StatusPanel ProgressBar Bar {
        width: 100%;
    }

    StatusPanel Sparkline {
        height: 1;
        margin: 0;
    }

    StatusPanel .cache-label {
        color: $text-muted;
        text-style: dim;
        height: 1;
        padding: 0;
        margin: 0;
    }

    StatusPanel .cache-row {
        height: auto;
        margin: 0;
    }

    StatusPanel .cache-columns {
        height: auto;
        margin: 0;
        padding: 0;
        align: center top;
    }

    StatusPanel .cache-col {
        width: 1fr;
        height: auto;
        margin: 0;
        padding: 0 1;
        align: center top;
    }

    StatusPanel .cache-col .cache-label {
        text-align: center;
        width: 100%;
    }

    StatusPanel .cache-col ProgressBar {
        width: 100%;
    }

    StatusPanel Rule {
        margin: 0;
        color: $primary 30%;
    }
    """

    # Reactive properties
    connection_status = reactive("Disconnected")

    def __init__(self, bot: Optional["MeshmindBot"] = None, **kwargs):
        super().__init__(**kwargs)
        self._bot = bot
        self._response_times: List[float] = [0.0] * 20

    @staticmethod
    def _format_duration(seconds: int) -> str:
        """Format seconds into a human-readable duration string."""
        days, remainder = divmod(int(seconds), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, _ = divmod(remainder, 60)
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"

    def compose(self) -> ComposeResult:
        # Connection Status - prominent at top
        yield SectionHeader("CONNECTION")
        yield MetricCard("Status", "Disconnected", id="card-status")
        yield MetricCard("AI Chat", "Active", value_class="success", id="card-chat")
        yield MetricCard("Reconnects", "0", id="card-reconnects")
        yield MetricCard("Uptime", "0s", id="card-uptime")

        yield SectionHeader("DEVICE")
        yield MetricCard("Name", "-", id="card-long-name")
        yield MetricCard("Node ID", "-", id="card-node")
        yield MetricCard("Hardware", "-", id="card-hw-model")
        yield MetricCard("Host", "-", id="card-host")
        yield MetricCard("Node Uptime", "-", id="card-node-uptime")

        yield SectionHeader("RADIO")
        yield MetricCard("Battery", "-", id="card-battery")
        yield MetricCard("Voltage", "-", id="card-voltage")
        yield MetricCard("Ch Util", "-", id="card-ch-util")
        yield MetricCard("Air Util", "-", id="card-air-util")

        yield SectionHeader("ACTIVITY")
        yield MetricCard("Sent", "0", id="card-messages")
        yield MetricCard("Received", "0", id="card-received")
        yield MetricCard("Nodes", "0", id="card-nodes")
        yield MetricCard("Active Chats", "0", id="card-active-chats")
        yield MetricCard("BBS Posts", "-", id="card-bbs-posts")
        yield MetricCard("Last Msg", "-", id="card-lastmsg")

        yield SectionHeader("CACHE")
        with Horizontal(classes="cache-columns"):
            with Vertical(classes="cache-col"):
                yield Static("Weather", classes="cache-label")
                yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="cache-weather")
            with Vertical(classes="cache-col"):
                yield Static("Alerts", classes="cache-label")
                yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="cache-alerts")
        with Horizontal(classes="cache-columns", id="cache-row-optional"):
            with Vertical(classes="cache-col"):
                yield Static("River", classes="cache-label")
                yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="cache-river")
            with Vertical(classes="cache-col"):
                yield Static("AQI", classes="cache-label")
                yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="cache-aqi")
        with Horizontal(classes="cache-columns", id="cache-row-optional2"):
            with Vertical(classes="cache-col"):
                yield Static("Space", classes="cache-label")
                yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="cache-space-weather")
            with Vertical(classes="cache-col"):
                yield Static("Quake", classes="cache-label")
                yield ProgressBar(total=100, show_eta=False, show_percentage=False, id="cache-earthquake")

        yield SectionHeader("API STATS")
        yield Static("", id="ai-provider-label", classes="cache-label")
        yield MetricCard("Calls", "0", id="card-calls")
        yield MetricCard("Success", "-%", id="card-success")
        yield MetricCard("Avg Time", "-ms", id="card-avgtime")
        yield MetricCard("Errors", "0", id="card-errors")

        yield Static("Response Times", classes="cache-label")
        yield Sparkline(self._response_times, id="sparkline-response")

    def on_mount(self) -> None:
        """Initialize and start refresh"""
        self.set_interval(1.0, self._refresh_status)

    def _refresh_status(self) -> None:
        """Refresh all status data"""
        if self._bot:
            status = self._bot.get_status()
            self._update_bot_stats(status)

    def _update_bot_stats(self, status: dict) -> None:
        """Update bot-related stats"""
        # Connection
        conn = status.get("connection", "Disconnected")
        self.connection_status = conn
        self._update_metric("card-status", conn, "success" if conn == "Connected" else "error")
        chat_paused = status.get("chat_paused", False)
        self._update_metric("card-chat", "Paused" if chat_paused else "Active",
                            "error" if chat_paused else "success")
        self._update_metric("card-uptime", status.get("uptime", "0s"))
        reconnects = status.get("reconnect_count", 0)
        self._update_metric("card-reconnects", str(reconnects),
                            "success" if reconnects == 0 else "warning" if reconnects < 5 else "error")

        # Device
        self._update_metric("card-host", status.get("device_host", "-"))
        node_id = status.get("node_id")
        self._update_metric("card-node", f"!{node_id:08x}" if node_id else "-")
        device_user = status.get("device_user", {})
        long_name = device_user.get("long_name")
        if long_name:
            self._update_metric("card-long-name", long_name)
        hw_model = device_user.get("hw_model")
        if hw_model:
            self._update_metric("card-hw-model", hw_model)
        node_uptime = status.get("device_telemetry", {}).get("uptime_seconds")
        if node_uptime is not None:
            self._update_metric("card-node-uptime", self._format_duration(node_uptime))

        # Radio telemetry
        telem = status.get("device_telemetry", {})
        battery = telem.get("battery")
        if battery is not None:
            bat_class = "success" if battery > 50 else "warning" if battery > 20 else "error"
            self._update_metric("card-battery", f"{int(battery)}%", bat_class)
        voltage = telem.get("voltage")
        if voltage is not None:
            self._update_metric("card-voltage", f"{voltage:.1f}V")
        ch_util = telem.get("ch_util")
        if ch_util is not None:
            cu_class = "success" if ch_util < 25 else "warning" if ch_util < 50 else "error"
            self._update_metric("card-ch-util", f"{ch_util:.1f}%", cu_class)
        air_util = telem.get("air_util")
        if air_util is not None:
            self._update_metric("card-air-util", f"{air_util:.1f}%")

        # API Stats
        provider = status.get("ai_provider_name", "AI")
        try:
            self.query_one("#ai-provider-label", Static).update(f"Provider: {provider}")
        except Exception:
            pass

        api_stats = status.get("api_stats", {})
        total_calls = api_stats.get("total_calls", 0)
        successful = api_stats.get("successful_calls", 0)
        failed = api_stats.get("failed_calls", 0)
        response_times = api_stats.get("response_times", [])
        errors = api_stats.get("errors", {})

        success_rate = (successful / total_calls * 100) if total_calls > 0 else 0
        avg_time = sum(response_times) / len(response_times) * 1000 if response_times else 0
        total_errors = sum(errors.values()) if errors else 0

        self._update_metric("card-calls", str(total_calls))
        self._update_metric("card-success", f"{success_rate:.0f}%",
                          "success" if success_rate > 90 else "warning" if success_rate > 70 else "error")
        self._update_metric("card-avgtime", f"{avg_time:.0f}ms",
                          "success" if avg_time < 1500 else "warning" if avg_time < 3000 else "error")
        self._update_metric("card-errors", str(total_errors),
                          "success" if total_errors == 0 else "warning" if total_errors < 5 else "error")

        # Update sparkline with response times
        if response_times:
            # Normalize to last 20 values, scale to 0-100
            times = response_times[-20:] if len(response_times) > 20 else response_times
            max_time = max(times) if times else 1
            normalized = [t / max_time * 100 for t in times]
            # Pad with zeros if needed
            while len(normalized) < 20:
                normalized.insert(0, 0)
            self._response_times = normalized
            try:
                self.query_one("#sparkline-response", Sparkline).data = self._response_times
            except Exception:
                pass

        # Cache freshness (inverted - 100% = fresh, 0% = stale)
        weather_age = status.get("weather_cache_age")
        weather_ttl = status.get("weather_cache_ttl", 7200)
        alerts_age = status.get("alerts_cache_age")
        alerts_ttl = status.get("alerts_cache_ttl", 600)

        if weather_age is not None:
            freshness = max(0, 100 - (weather_age / weather_ttl * 100))
            try:
                self.query_one("#cache-weather", ProgressBar).progress = freshness
            except Exception:
                pass

        if alerts_age is not None:
            freshness = max(0, 100 - (alerts_age / alerts_ttl * 100))
            try:
                self.query_one("#cache-alerts", ProgressBar).progress = freshness
            except Exception:
                pass

        # Optional cache bars — hide row if neither feature is enabled
        for cache_id, age_key, ttl_key, enabled_key in [
            ("cache-river", "river_cache_age", "river_cache_ttl", "river_enabled"),
            ("cache-aqi", "aqi_cache_age", "aqi_cache_ttl", "aqi_enabled"),
            ("cache-space-weather", "space_weather_cache_age", "space_weather_cache_ttl", "space_weather_enabled"),
            ("cache-earthquake", "earthquake_cache_age", "earthquake_cache_ttl", "earthquake_enabled"),
        ]:
            age = status.get(age_key)
            ttl = status.get(ttl_key, 1)
            if age is not None:
                freshness = max(0, 100 - (age / ttl * 100))
                try:
                    self.query_one(f"#{cache_id}", ProgressBar).progress = freshness
                except Exception:
                    pass

        # Hide optional cache rows if features are disabled
        try:
            row1 = self.query_one("#cache-row-optional")
            row1.display = status.get("river_enabled") or status.get("aqi_enabled")
        except Exception:
            pass
        try:
            row2 = self.query_one("#cache-row-optional2")
            row2.display = status.get("space_weather_enabled") or status.get("earthquake_enabled")
        except Exception:
            pass

        # Activity
        self._update_metric("card-messages", str(status.get("messages_sent", 0)))
        self._update_metric("card-received", str(status.get("messages_received", 0)))
        self._update_metric("card-nodes", str(status.get("nodes", 0)))
        self._update_metric("card-active-chats", str(status.get("active_chats", 0)))
        bbs_posts = status.get("bbs_posts")
        if bbs_posts is not None:
            self._update_metric("card-bbs-posts", str(bbs_posts))
        elif not status.get("bbs_enabled"):
            self._update_metric("card-bbs-posts", "Off", "")

        last_msg_time = status.get("last_message_time")
        if last_msg_time:
            from meshmind.config import cfg
            local_time = last_msg_time.astimezone(cfg.TIMEZONE)
            self._update_metric("card-lastmsg", local_time.strftime("%H:%M:%S"))

    def _update_metric(self, card_id: str, value: str, value_class: str = "") -> None:
        """Update a metric card's value"""
        try:
            card = self.query_one(f"#{card_id}", MetricCard)
            card.update_value(value, value_class)
        except Exception:
            pass

    def set_bot(self, bot: "MeshmindBot") -> None:
        """Set the bot instance"""
        self._bot = bot

    def watch_connection_status(self, value: str) -> None:
        """React to connection changes"""
        pass
