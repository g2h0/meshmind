"""MESHMON Service Table Widget - DataTable showing all monitored services"""

from datetime import datetime, timezone
from typing import Optional

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import DataTable, Static


STATUS_ICONS = {
    "UP": ("[green]\u25cf[/]", "green"),
    "DOWN": ("[red]\u25cf[/]", "red"),
    "DEGRADED": ("[yellow]\u25cf[/]", "yellow"),
    "UNKNOWN": ("[dim]\u25cb[/]", "dim"),
    "DISABLED": ("[dim]\u25cb[/]", "dim"),
}


class ServiceTable(Vertical):
    """Table showing status of all monitored services"""

    DEFAULT_CSS = """
    ServiceTable {
        height: 1fr;
    }

    ServiceTable DataTable {
        height: 1fr;
    }

    ServiceTable .section-header {
        color: $accent;
        text-style: bold;
        text-align: center;
        padding: 0;
        margin: 0;
        border-bottom: solid $primary;
    }
    """

    class ServiceSelected(Message):
        """Emitted when a service row is highlighted"""
        def __init__(self, service_name: str) -> None:
            super().__init__()
            self.service_name = service_name

    def compose(self) -> ComposeResult:
        yield Static(" SERVICES ", classes="section-header")
        table = DataTable(id="service-datatable", cursor_type="row")
        table.add_column("St", width=2)
        table.add_column("Service", width=18)
        table.add_column("Time", width=7)
        table.add_column("Last", width=6)
        table.add_column("Uptime", width=8)
        yield table

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """When a row is highlighted, post service selection"""
        if event.row_key and event.row_key.value:
            self.post_message(self.ServiceSelected(event.row_key.value))

    def update_from_status(self, services: list) -> None:
        """Update table rows from service status list"""
        try:
            table = self.query_one("#service-datatable", DataTable)
        except Exception:
            return

        existing_keys = set(str(k.value) for k in table.rows.keys())
        service_names = set()

        for svc in services:
            name = svc["name"]
            service_names.add(name)
            status = svc.get("status", "UNKNOWN")
            icon_markup, _ = STATUS_ICONS.get(status, STATUS_ICONS["UNKNOWN"])

            # Response time
            rt = svc.get("response_time_ms")
            if rt is not None and rt > 0:
                time_str = f"{rt:.0f}ms"
            elif status == "DISABLED":
                time_str = "--"
            else:
                time_str = "--"

            # Last check
            last_check = svc.get("last_check")
            if last_check:
                try:
                    dt = datetime.fromisoformat(last_check)
                    last_str = dt.astimezone().strftime("%H:%M")
                except Exception:
                    last_str = "--"
            else:
                last_str = "--"

            # Uptime
            check_count = svc.get("check_count", 0)
            if status == "DISABLED":
                uptime_str = "(no key)" if svc.get("error", "").startswith("Missing") else "disabled"
            elif check_count > 0:
                uptime_str = f"{svc.get('uptime_percent', 0):.1f}%"
            else:
                uptime_str = "--"

            if name in existing_keys:
                # Update existing row
                row_key = name
                try:
                    idx = list(str(k.value) for k in table.rows.keys()).index(name)
                    table.update_cell_at((idx, 0), icon_markup, update_width=False)
                    table.update_cell_at((idx, 1), name, update_width=False)
                    table.update_cell_at((idx, 2), time_str, update_width=False)
                    table.update_cell_at((idx, 3), last_str, update_width=False)
                    table.update_cell_at((idx, 4), uptime_str, update_width=False)
                except Exception:
                    pass
            else:
                # Add new row
                table.add_row(
                    icon_markup, name, time_str, last_str, uptime_str,
                    key=name,
                )
