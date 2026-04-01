"""MESHMON Detail Panel Widget - Expanded info for selected service"""

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Static, Sparkline
from rich.text import Text


class DetailPanel(Vertical):
    """Shows expanded details for the currently selected service"""

    DEFAULT_CSS = """
    DetailPanel {
        height: 5;
        border: solid $primary;
        background: $surface;
        padding: 0 1;
    }

    DetailPanel .detail-header {
        color: $accent;
        text-style: bold;
        text-align: center;
        padding: 0;
        margin: 0;
        border-bottom: solid $primary;
    }

    DetailPanel .detail-info {
        height: 1;
        padding: 0;
        margin: 0;
    }

    DetailPanel .detail-stats {
        height: 1;
        padding: 0;
        margin: 0;
    }

    DetailPanel Sparkline {
        height: 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static(" SELECT A SERVICE ", classes="detail-header", id="detail-title")
        yield Static("", classes="detail-info", id="detail-url")
        with Horizontal(classes="detail-stats"):
            yield Static("", id="detail-stats-text")
            yield Sparkline([], id="detail-sparkline")

    def update_for_service(self, service: dict) -> None:
        """Update the detail panel for a selected service"""
        name = service.get("name", "")
        url = service.get("url", "")
        status = service.get("status", "UNKNOWN")
        status_code = service.get("status_code")
        avg_rt = service.get("avg_response_time", 0)
        p95_rt = service.get("p95_response_time", 0)
        check_count = service.get("check_count", 0)
        response_times = service.get("response_times", [])

        # Title
        try:
            title = self.query_one("#detail-title", Static)
            title.update(f" DETAIL: {name} ")
        except Exception:
            pass

        # URL line
        try:
            url_widget = self.query_one("#detail-url", Static)
            text = Text()
            text.append(" URL: ", style="dim")
            text.append(url, style="")
            url_widget.update(text)
        except Exception:
            pass

        # Stats line
        try:
            stats = self.query_one("#detail-stats-text", Static)
            text = Text()

            # HTTP status code
            if status_code:
                code_style = "green" if status_code < 400 else "red"
                text.append(f" HTTP {status_code}", style=f"bold {code_style}")
            else:
                text.append(f" {status}", style="dim")

            text.append("  |  ", style="dim")
            text.append(f"Checks: {check_count}", style="")
            text.append("  |  ", style="dim")
            text.append(f"Avg: {avg_rt:.0f}ms", style="")
            text.append("  |  ", style="dim")
            text.append(f"P95: {p95_rt:.0f}ms", style="")
            text.append("  ", style="")

            stats.update(text)
        except Exception:
            pass

        # Sparkline
        if response_times:
            try:
                sparkline = self.query_one("#detail-sparkline", Sparkline)
                sparkline.data = response_times
            except Exception:
                pass

    def clear_detail(self) -> None:
        """Clear the detail panel"""
        try:
            self.query_one("#detail-title", Static).update(" SELECT A SERVICE ")
            self.query_one("#detail-url", Static).update("")
            self.query_one("#detail-stats-text", Static).update("")
            self.query_one("#detail-sparkline", Sparkline).data = []
        except Exception:
            pass
