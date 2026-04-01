"""MESHMON Monitor Engine - orchestrates all service monitors"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

from .base import ServiceStatus
from .http_monitor import HTTPMonitor
from .mqtt_monitor import MQTTMonitor
from ..config import Settings

logger = logging.getLogger(__name__)


class MonitorEngine:
    """Orchestrates all service monitors and provides aggregated status"""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._lock = threading.RLock()
        self._running = False
        self._start_time: Optional[datetime] = None

        # Build monitors from config
        self._http_monitors: list[HTTPMonitor] = []
        self._mqtt_monitor: Optional[MQTTMonitor] = None

        self._build_monitors()

    def _build_monitors(self) -> None:
        """Create monitor instances from settings"""
        timeout = self._settings.http_timeout
        degraded = self._settings.degraded_threshold
        interval = self._settings.check_interval

        for svc in self._settings.services:
            monitor = HTTPMonitor(
                name=svc["name"],
                url=svc["url"],
                enabled=svc.get("enabled", True),
                timeout=timeout,
                degraded_threshold=degraded,
                requires_key=svc.get("requires_key"),
                dynamic_params=svc.get("dynamic_params", False),
                check_interval=interval,
            )
            self._http_monitors.append(monitor)

        if self._settings.mqtt_enabled:
            self._mqtt_monitor = MQTTMonitor(
                broker=self._settings.mqtt_broker,
                port=self._settings.mqtt_port,
                topic=self._settings.mqtt_topic,
                enabled=True,
            )

    def start(self) -> None:
        """Start all monitors"""
        self._running = True
        self._start_time = datetime.now(timezone.utc)

        logger.info("Monitor engine starting...")

        # Start MQTT monitor
        if self._mqtt_monitor:
            self._mqtt_monitor.start()

        # Start HTTP check loop in background thread
        self._check_thread = threading.Thread(
            target=self._check_loop, daemon=True
        )
        self._check_thread.start()

        logger.info(
            f"Monitoring {len(self._http_monitors)} HTTP services"
            + (", MQTT broker" if self._mqtt_monitor else "")
        )

    def stop(self) -> None:
        """Stop all monitors"""
        self._running = False
        if self._mqtt_monitor:
            self._mqtt_monitor.stop()
        logger.info("Monitor engine stopped")

    def _check_loop(self) -> None:
        """Background loop that runs HTTP checks periodically"""
        # Staggered initial checks
        with ThreadPoolExecutor(max_workers=4) as executor:
            # First pass: run checks with 1s stagger
            for i, monitor in enumerate(self._http_monitors):
                if not self._running:
                    return
                if not monitor.enabled:
                    # Check once to set DISABLED status
                    result = monitor.check()
                    monitor.record_result(result)
                    continue
                if i > 0:
                    time.sleep(1)
                future = executor.submit(self._run_check, monitor)

            # Then loop at configured interval
            while self._running:
                time.sleep(self._settings.check_interval)
                if not self._running:
                    return

                # Also update MQTT status
                if self._mqtt_monitor and self._mqtt_monitor.enabled:
                    result = self._mqtt_monitor.check()
                    self._mqtt_monitor.record_result(result)

                # Run all HTTP checks in parallel
                futures = []
                for monitor in self._http_monitors:
                    if monitor.enabled or monitor.status == ServiceStatus.UNKNOWN:
                        futures.append(
                            executor.submit(self._run_check, monitor)
                        )
                # Wait for all to complete
                for f in futures:
                    try:
                        f.result(timeout=30)
                    except Exception:
                        pass

    def _run_check(self, monitor: HTTPMonitor) -> None:
        """Run a single check and log the result"""
        result = monitor.check()
        monitor.record_result(result)

        if result.status == ServiceStatus.DISABLED:
            return

        status_str = result.status.value
        time_str = f" ({result.response_time_ms:.0f}ms)" if result.response_time_ms else ""
        error_str = f" - {result.error}" if result.error else ""

        if result.status == ServiceStatus.UP:
            logger.info(f"{monitor.name}: {status_str}{time_str}")
        elif result.status == ServiceStatus.DEGRADED:
            logger.warning(f"{monitor.name}: {status_str}{time_str}")
        else:
            logger.error(f"{monitor.name}: {status_str}{error_str}")

    def refresh_all(self) -> None:
        """Trigger an immediate check of all services"""
        logger.info("Refreshing all services...")

        def _do_refresh():
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for monitor in self._http_monitors:
                    if monitor.enabled or monitor.status == ServiceStatus.UNKNOWN:
                        futures.append(
                            executor.submit(self._run_check, monitor)
                        )
                if self._mqtt_monitor and self._mqtt_monitor.enabled:
                    result = self._mqtt_monitor.check()
                    self._mqtt_monitor.record_result(result)
                for f in futures:
                    try:
                        f.result(timeout=30)
                    except Exception:
                        pass

        threading.Thread(target=_do_refresh, daemon=True).start()

    def get_status(self) -> dict:
        """Get aggregated status of all monitors (thread-safe)"""
        services = []
        for monitor in self._http_monitors:
            services.append(monitor.get_status_dict())

        # MQTT status
        mqtt_status = {}
        if self._mqtt_monitor:
            mqtt_status = self._mqtt_monitor.get_mqtt_status()
            # Also include MQTT in the service list
            services.append(self._mqtt_monitor.get_status_dict())

        # Summary
        total = len(services)
        up = sum(1 for s in services if s["status"] == "UP")
        down = sum(1 for s in services if s["status"] == "DOWN")
        degraded = sum(1 for s in services if s["status"] == "DEGRADED")
        disabled = sum(1 for s in services if s["status"] == "DISABLED")
        enabled_services = [s for s in services if s["status"] not in ("DISABLED",)]

        avg_response = 0.0
        response_times = [s["response_time_ms"] for s in enabled_services if s["response_time_ms"]]
        if response_times:
            avg_response = sum(response_times) / len(response_times)

        overall_uptime = 0.0
        uptimes = [s["uptime_percent"] for s in enabled_services if s["check_count"] > 0]
        if uptimes:
            overall_uptime = sum(uptimes) / len(uptimes)

        return {
            "services": services,
            "mqtt": mqtt_status,
            "summary": {
                "total": total,
                "up": up,
                "down": down,
                "degraded": degraded,
                "disabled": disabled,
                "avg_response_ms": round(avg_response, 1),
                "overall_uptime": round(overall_uptime, 1),
            },
        }
