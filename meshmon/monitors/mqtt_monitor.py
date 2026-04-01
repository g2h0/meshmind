"""MESHMON MQTT Broker Monitor"""

import logging
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Optional

from .base import BaseMonitor, CheckResult, ServiceStatus

logger = logging.getLogger(__name__)


class MQTTMonitor(BaseMonitor):
    """Monitors a Meshtastic MQTT broker for connectivity and message flow"""

    def __init__(
        self,
        broker: str = "mqtt.meshtastic.org",
        port: int = 1883,
        topic: str = "msh/#",
        username: str = "meshdev",
        password: str = "large4cats",
        keepalive: int = 60,
        enabled: bool = True,
    ):
        super().__init__(
            name="Meshtastic MQTT",
            url=f"mqtt://{broker}:{port}",
            enabled=enabled,
        )
        self.broker = broker
        self.port = port
        self.topic = topic
        self.username = username
        self.password = password
        self.keepalive = keepalive

        self._lock = threading.Lock()
        self._client = None
        self._connected = False
        self._total_messages: int = 0
        self._message_timestamps: deque = deque(maxlen=50000)
        self._topic_counts: Counter = Counter()
        self._recent_topics: deque = deque(maxlen=8)
        self._rate_history: deque = deque(maxlen=60)
        self._reconnect_count: int = 0
        self._connected_since: Optional[datetime] = None
        self._running = False

        # Health metrics
        self._last_message_time: Optional[float] = None
        self._rate_samples_total: float = 0.0
        self._rate_samples_count: int = 0
        self._connection_start: Optional[float] = None
        self._connection_durations: list[float] = []
        self._topic_last_seen: dict[str, float] = {}

    def start(self) -> None:
        """Start the MQTT client in its own thread"""
        if not self.enabled:
            self.status = ServiceStatus.DISABLED
            return

        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            logger.error("paho-mqtt not installed. Run: pip install paho-mqtt")
            self.status = ServiceStatus.DOWN
            self.last_error = "paho-mqtt not installed"
            return

        self._running = True
        import uuid
        client_id = f"meshmon-{uuid.uuid4().hex[:8]}"
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
        self._client.username_pw_set(self.username, self.password)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        try:
            self._client.connect(self.broker, self.port, self.keepalive)
            self._client.loop_start()
            logger.info(f"MQTT connecting to {self.broker}:{self.port}")
        except Exception as e:
            logger.error(f"MQTT connection failed: {e}")
            self.status = ServiceStatus.DOWN
            self.last_error = str(e)

        # Start rate sampling thread
        self._rate_thread = threading.Thread(
            target=self._sample_rate_loop, daemon=True
        )
        self._rate_thread.start()

    def stop(self) -> None:
        """Stop the MQTT client"""
        self._running = False
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        with self._lock:
            self._connected = True
            self._connected_since = datetime.now(timezone.utc)
            self._connection_start = time.monotonic()
        self.status = ServiceStatus.UP
        self.last_error = None
        client.subscribe(self.topic)
        logger.info(f"MQTT connected to {self.broker}, subscribed to {self.topic}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        was_connected = False
        with self._lock:
            was_connected = self._connected
            self._connected = False
            # Record connection duration
            if self._connection_start is not None:
                duration = time.monotonic() - self._connection_start
                self._connection_durations.append(duration)
                self._connection_start = None
        if was_connected:
            self._reconnect_count += 1
        self.status = ServiceStatus.DOWN
        self.last_error = f"Disconnected (rc={reason_code})"
        logger.warning(f"MQTT disconnected (reason={reason_code}, name={reason_code.getName() if hasattr(reason_code, 'getName') else reason_code})")

    def _on_message(self, client, userdata, msg) -> None:
        """Lightweight callback - just increment counters"""
        now = time.monotonic()
        topic = msg.topic
        with self._lock:
            self._total_messages += 1
            self._message_timestamps.append(now)
            self._topic_counts[topic] += 1
            self._recent_topics.append(topic)
            self._last_message_time = now
            self._topic_last_seen[topic] = now

    def _sample_rate_loop(self) -> None:
        """Periodically sample the message rate for the sparkline"""
        while self._running:
            rate = self.messages_per_hour
            with self._lock:
                self._rate_history.append(rate)
                self._rate_samples_total += rate
                self._rate_samples_count += 1
            time.sleep(5)

    @property
    def messages_per_hour(self) -> float:
        """Count actual messages received in the last 60 minutes"""
        now = time.monotonic()
        window = 3600.0
        with self._lock:
            return sum(1 for ts in self._message_timestamps if now - ts <= window)

    @property
    def active_topics(self) -> int:
        with self._lock:
            return len(self._topic_counts)

    @property
    def total_messages(self) -> int:
        with self._lock:
            return self._total_messages

    @property
    def recent_topics(self) -> list:
        with self._lock:
            return list(self._recent_topics)

    @property
    def rate_history(self) -> list:
        with self._lock:
            return list(self._rate_history)

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def connected_since(self) -> Optional[datetime]:
        with self._lock:
            return self._connected_since

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    @property
    def last_message_age(self) -> Optional[float]:
        """Seconds since last message received, or None if no messages yet"""
        with self._lock:
            if self._last_message_time is None:
                return None
            return time.monotonic() - self._last_message_time

    @property
    def avg_rate(self) -> float:
        """Session-average messages/sec"""
        with self._lock:
            if self._rate_samples_count == 0:
                return 0.0
            return self._rate_samples_total / self._rate_samples_count

    @property
    def connection_uptime(self) -> Optional[float]:
        """Seconds the current connection has been alive"""
        with self._lock:
            if self._connection_start is None:
                return None
            return time.monotonic() - self._connection_start

    @property
    def avg_connection_duration(self) -> Optional[float]:
        """Average connection duration in seconds across all past connections"""
        with self._lock:
            if not self._connection_durations:
                return None
            return sum(self._connection_durations) / len(self._connection_durations)

    @property
    def stale_topic_count(self) -> int:
        """Count of topics active in last 10m but silent for > 5m"""
        now = time.monotonic()
        with self._lock:
            count = 0
            for topic, last_seen in self._topic_last_seen.items():
                age = now - last_seen
                # Was active within 10 minutes, but silent for over 5 minutes
                if 300 < age < 600:
                    count += 1
            return count

    @property
    def rate_trend(self) -> str:
        """Compare recent rate to previous rate: ^ rising, v falling, = stable"""
        with self._lock:
            history = list(self._rate_history)
        if len(history) < 12:
            return "="
        # Last 30s = last 6 samples (5s each), previous 30s = 6 before that
        recent = history[-6:]
        previous = history[-12:-6]
        recent_avg = sum(recent) / len(recent)
        prev_avg = sum(previous) / len(previous)
        if prev_avg == 0:
            return "^" if recent_avg > 0 else "="
        ratio = recent_avg / prev_avg
        if ratio > 1.2:
            return "^"
        elif ratio < 0.8:
            return "v"
        return "="

    def check(self) -> CheckResult:
        """Return current MQTT status with a TCP ping for latency"""
        if not self.enabled:
            return CheckResult(status=ServiceStatus.DISABLED)

        # TCP ping to measure broker latency
        import socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            start = time.monotonic()
            sock.connect((self.broker, self.port))
            elapsed_ms = (time.monotonic() - start) * 1000
            sock.close()

            if not self.is_connected:
                return CheckResult(
                    status=ServiceStatus.DOWN,
                    response_time_ms=elapsed_ms,
                    error=self.last_error or "Not connected",
                )
            return CheckResult(
                status=ServiceStatus.UP,
                response_time_ms=elapsed_ms,
            )
        except (socket.timeout, socket.error) as e:
            return CheckResult(
                status=ServiceStatus.DOWN,
                error=f"TCP ping failed: {e}",
            )

    def get_mqtt_status(self) -> dict:
        """Get MQTT-specific status dict for the MQTT panel"""
        connected_since = self.connected_since
        if connected_since:
            since_str = connected_since.strftime("%H:%M:%S")
        else:
            since_str = "--"

        # Format last message age
        msg_age = self.last_message_age
        if msg_age is None:
            last_msg_str = "No messages"
        elif msg_age < 60:
            last_msg_str = f"{msg_age:.0f}s ago"
        elif msg_age < 3600:
            last_msg_str = f"{msg_age / 60:.0f}m ago"
        else:
            last_msg_str = f"{msg_age / 3600:.0f}h ago"

        # Format connection uptime
        uptime = self.connection_uptime
        if uptime is None:
            uptime_str = "--"
        elif uptime < 60:
            uptime_str = f"{uptime:.0f}s"
        elif uptime < 3600:
            uptime_str = f"{uptime / 60:.0f}m {uptime % 60:.0f}s"
        else:
            hours = int(uptime // 3600)
            mins = int((uptime % 3600) // 60)
            uptime_str = f"{hours}h {mins}m"

        # Format avg connection duration
        avg_conn = self.avg_connection_duration
        if avg_conn is None:
            avg_conn_str = "--"
        elif avg_conn < 60:
            avg_conn_str = f"{avg_conn:.0f}s"
        elif avg_conn < 3600:
            avg_conn_str = f"{avg_conn / 60:.0f}m"
        else:
            avg_conn_str = f"{avg_conn / 3600:.1f}h"

        return {
            "connected": self.is_connected,
            "broker": f"{self.broker}:{self.port}",
            "msgs_per_hour": round(self.messages_per_hour),
            "total_messages": self.total_messages,
            "active_topics": self.active_topics,
            "reconnects": self.reconnect_count,
            "connected_since": since_str,
            "recent_topics": self.recent_topics,
            "rate_history": self.rate_history,
            # Health metrics
            "last_msg": last_msg_str,
            "last_msg_age": msg_age,
            "avg_rate": round(self.avg_rate),
            "uptime": uptime_str,
            "uptime_secs": uptime,
            "avg_conn": avg_conn_str,
            "stale_topics": self.stale_topic_count,
            "rate_trend": self.rate_trend,
        }
