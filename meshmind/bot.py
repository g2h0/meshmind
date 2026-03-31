"""MESHMIND Bot - Refactored for TUI integration"""

import json
import re
import socket
import sys
import time
import logging
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Callable

from dotenv import load_dotenv

load_dotenv()

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import meshtastic
from meshtastic.tcp_interface import TCPInterface
from meshtastic.mesh_interface import MeshInterface
from pubsub import pub
from openai import OpenAI

from .config import cfg, Config
from .utils.bbs import BbsBoard, format_age

logger = logging.getLogger(__name__)
ALERT_STATE_FILE = Path(__file__).parent.parent / "data" / "alert_state.json"


class MeshmindBot:
    """Meshtastic AI Bot - Non-blocking version for TUI integration"""

    def __init__(
        self,
        on_status_change: Optional[Callable[[Dict], None]] = None,
        on_message_received: Optional[Callable[[str, int], None]] = None,
    ):
        self.interface: Optional[TCPInterface] = None
        self.client: Optional[OpenAI] = None
        self.my_node_num: Optional[int] = None

        # Thread safety - use RLock to allow reentrant locking
        self.lock = threading.RLock()

        # State tracking
        self.retry_count = 0
        self.last_hour_check = -1
        self.seen_alert_ids = {}
        self.last_alert_check = None
        self.sun_times = {"sunrise": None, "sunset": None, "last_update": None}
        self.alert_sent = {"sunrise": None, "sunset": None}
        self.frost_alert_sent = None
        self.flood_alert_sent = None
        self.known_nodes = set()
        self.is_running = False
        self.bbs: Optional[BbsBoard] = BbsBoard() if cfg.BBS_ENABLED else None
        self.start_time = datetime.now(timezone.utc)
        self.last_cleanup_date = None

        # Message tracking
        self.messages_sent_count = 0
        self.messages_received_count = 0
        self.last_message_time: Optional[datetime] = None
        self.chat_paused = False
        self.reconnect_count = 0
        self._reconnect_lock = threading.Lock()
        self._closing_interface = False
        self._recent_reconnects: deque = deque(maxlen=10)

        # Caching for API responses
        self.river_cache = {"level": None, "timestamp": None}
        self.weather_cache = {
            "tomorrow": {"data": None, "timestamp": None},
            "noaa": {"data": None, "timestamp": None},
        }
        self.alerts_cache = {"data": None, "timestamp": None}
        self.hourly_forecast_cache = {"data": None, "timestamp": None}
        self.forecast_cache = {"data": None, "timestamp": None}
        self.moon_cache = {"data": None, "timestamp": None}
        self._noaa_points_cache = {"forecast_url": None, "forecast_hourly_url": None}
        self._noaa_station_cache = {"station_url": None}

        # AQI cache and state
        self.aqi_cache = {"data": None, "timestamp": None}
        self.last_aqi_check = None
        self.last_aqi_alert_category = 0

        # Space Weather cache and state
        self.space_weather_cache = {"data": None, "timestamp": None}
        self.last_space_weather_check = None
        self.seen_storm_events = {}

        # Earthquake cache and state
        self.earthquake_cache = {"data": None, "timestamp": None}
        self.last_earthquake_check = None
        self.seen_earthquake_ids = {}

        # Restore persisted alert state (prevents duplicate alerts on restart)
        self._load_alert_state()

        # Chat history
        self.chat_histories: Dict[int, List[Dict[str, str]]] = {}
        self.last_activity: Dict[int, datetime] = {}

        # Performance monitoring
        self.api_stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "response_times": [],
            "errors": {},
            "per_endpoint": {},
        }

        # Callback for status changes
        self._on_status_change = on_status_change

        # Callback for incoming messages (text, channel)
        self._on_message_received = on_message_received

        # Rate limiting for AI chat
        self._last_ai_response: Dict[int, datetime] = {}

        # NOAA backoff tracking
        self._noaa_consecutive_failures = 0

        # Sun times backoff tracking
        self._last_sun_attempt = None
        self._sun_consecutive_failures = 0

        # HTTP session with retry on transient errors.  read=1 allows a single
        # retry on read failures (covers stale keep-alive ConnectionResetError)
        # without compounding blocking on genuinely slow/down servers.
        _retry = Retry(
            total=3,
            connect=3,
            read=1,
            backoff_factor=0.5,
            raise_on_status=False,
        )
        _adapter = HTTPAdapter(max_retries=_retry)
        self._session = requests.Session()
        self._session.mount("https://", _adapter)
        self._session.mount("http://", _adapter)

        # Suppress noisy urllib3 retry warnings (retries still happen)
        logging.getLogger("urllib3.util.retry").setLevel(logging.ERROR)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

        # Duplicate broadcast guard
        self._last_conditions_sent: Optional[datetime] = None

        # Threading
        self._periodic_thread: Optional[threading.Thread] = None

        # Load system prompt
        cfg.SYSTEM_PROMPT = Config.load_system_prompt()

    def _notify_status_change(self) -> None:
        """Notify listeners of status change"""
        if self._on_status_change:
            self._on_status_change(self.get_status())

    # ------------------------------------------------------------------ #
    # Alert state persistence                                              #
    # ------------------------------------------------------------------ #

    def _load_alert_state(self) -> None:
        """Restore alert tracking state from disk so restarts don't re-fire alerts."""
        try:
            if not ALERT_STATE_FILE.exists():
                return
            with open(ALERT_STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)

            today = datetime.now(cfg.TIMEZONE).date() if cfg.TIMEZONE else date.today()

            # sunrise/sunset sent dates — only restore if same day
            for event in ("sunrise", "sunset"):
                d = state.get("alert_sent", {}).get(event)
                if d:
                    saved_date = date.fromisoformat(d)
                    if saved_date == today:
                        self.alert_sent[event] = saved_date

            # frost alert sent date
            d = state.get("frost_alert_sent")
            if d:
                saved_date = date.fromisoformat(d)
                if saved_date == today:
                    self.frost_alert_sent = saved_date

            # flood alert level (string)
            self.flood_alert_sent = state.get("flood_alert_sent")

            # AQI category
            self.last_aqi_alert_category = state.get("last_aqi_alert_category", 0)

            # last conditions broadcast
            ts = state.get("last_conditions_sent")
            if ts:
                self._last_conditions_sent = datetime.fromisoformat(ts)

            # Seen IDs (NOAA alerts, storms, earthquakes) — restore with datetime values
            for attr, key in [
                ("seen_alert_ids", "seen_alert_ids"),
                ("seen_storm_events", "seen_storm_events"),
                ("seen_earthquake_ids", "seen_earthquake_ids"),
            ]:
                saved = state.get(key, {})
                restored = {}
                for k, v in saved.items():
                    try:
                        restored[k] = datetime.fromisoformat(v)
                    except (ValueError, TypeError):
                        pass
                setattr(self, attr, restored)

        except (json.JSONDecodeError, IOError, KeyError, ValueError, TypeError):
            pass  # corrupted or missing — start fresh

    def _save_alert_state(self) -> None:
        """Persist alert tracking state to disk."""
        try:
            state = {}

            with self.lock:
                # sunrise/sunset
                state["alert_sent"] = {
                    k: v.isoformat() if isinstance(v, date) else None
                    for k, v in self.alert_sent.items()
                }

                # frost
                state["frost_alert_sent"] = (
                    self.frost_alert_sent.isoformat()
                    if isinstance(self.frost_alert_sent, date) else None
                )

                # flood
                state["flood_alert_sent"] = self.flood_alert_sent

                # AQI
                state["last_aqi_alert_category"] = self.last_aqi_alert_category

                # last conditions
                state["last_conditions_sent"] = (
                    self._last_conditions_sent.isoformat()
                    if self._last_conditions_sent else None
                )

                # Seen IDs
                for attr, key in [
                    ("seen_alert_ids", "seen_alert_ids"),
                    ("seen_storm_events", "seen_storm_events"),
                    ("seen_earthquake_ids", "seen_earthquake_ids"),
                ]:
                    state[key] = {
                        k: v.isoformat() for k, v in getattr(self, attr).items()
                    }

            ALERT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(ALERT_STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except (IOError, TypeError):
            pass

    def get_status(self) -> Dict[str, Any]:
        """Get current bot status for UI display"""
        with self.lock:
            is_connected = (
                self.interface is not None
                and hasattr(self.interface, "isConnected")
                and self.interface.isConnected.is_set()
            )

            # Calculate cache ages in seconds
            now = datetime.now(timezone.utc)
            weather_age = None
            alerts_age = None

            if self.weather_cache["tomorrow"]["timestamp"]:
                weather_age = (now - self.weather_cache["tomorrow"]["timestamp"]).total_seconds()
            if self.alerts_cache["timestamp"]:
                alerts_age = (now - self.alerts_cache["timestamp"]).total_seconds()

            # Sync known_nodes from interface
            if self.interface and hasattr(self.interface, 'nodes') and self.interface.nodes:
                self.known_nodes.update(self.interface.nodes.keys())

            # Local device telemetry
            device_telemetry = {}
            device_user = {}
            if self.interface and hasattr(self.interface, 'nodesByNum') and self.my_node_num:
                local_node = self.interface.nodesByNum.get(self.my_node_num, {})
                metrics = local_node.get("deviceMetrics", {})
                if metrics:
                    device_telemetry = {
                        "battery": metrics.get("batteryLevel"),
                        "voltage": metrics.get("voltage"),
                        "ch_util": metrics.get("channelUtilization"),
                        "air_util": metrics.get("airUtilTx"),
                        "uptime_seconds": metrics.get("uptimeSeconds"),
                    }
                user = local_node.get("user", {})
                if user:
                    device_user = {
                        "long_name": user.get("longName"),
                        "hw_model": user.get("hwModel"),
                    }

            # Optional cache ages
            river_age = None
            if self.river_cache.get("timestamp"):
                river_age = (now - self.river_cache["timestamp"]).total_seconds()
            aqi_age = None
            if self.aqi_cache.get("timestamp"):
                aqi_age = (now - self.aqi_cache["timestamp"]).total_seconds()
            space_weather_age = None
            if self.space_weather_cache.get("timestamp"):
                space_weather_age = (now - self.space_weather_cache["timestamp"]).total_seconds()
            earthquake_age = None
            if self.earthquake_cache.get("timestamp"):
                earthquake_age = (now - self.earthquake_cache["timestamp"]).total_seconds()

            return {
                "connection": "Connected" if is_connected else "Disconnected",
                "uptime": self._get_uptime(),
                "nodes": len(self.known_nodes),
                "messages_sent": self.messages_sent_count,
                "messages_received": self.messages_received_count,
                "last_message_time": self.last_message_time,
                "api_stats": self.api_stats.copy(),
                "device_host": cfg.DEVICE_HOST,
                "node_id": self.my_node_num,
                "weather_cache_age": weather_age,
                "alerts_cache_age": alerts_age,
                "weather_cache_ttl": cfg.CONDITIONS_UPDATE_INTERVAL_HOURS * 3600,
                "alerts_cache_ttl": cfg.ALERTS_CACHE_TTL,
                "river_cache_age": river_age,
                "river_cache_ttl": cfg.CONDITIONS_UPDATE_INTERVAL_HOURS * 3600,
                "aqi_cache_age": aqi_age,
                "aqi_cache_ttl": cfg.AQI_CACHE_TTL,
                "space_weather_cache_age": space_weather_age,
                "space_weather_cache_ttl": cfg.SPACE_WEATHER_CACHE_TTL,
                "earthquake_cache_age": earthquake_age,
                "earthquake_cache_ttl": cfg.EARTHQUAKE_CACHE_TTL,
                "device_telemetry": device_telemetry,
                "device_user": device_user,
                "chat_paused": self.chat_paused,
                "reconnect_count": self.reconnect_count,
                "ai_provider_name": cfg.ai_provider_display_name,
                "active_chats": len(self.chat_histories),
                "bbs_posts": len(self.bbs._posts) if self.bbs else None,
                "river_enabled": cfg.RIVER_ENABLED,
                "aqi_enabled": cfg.AQI_ENABLED,
                "space_weather_enabled": cfg.SPACE_WEATHER_ENABLED,
                "earthquake_enabled": cfg.EARTHQUAKE_ENABLED,
                "bbs_enabled": cfg.BBS_ENABLED,
            }

    def _get_uptime(self) -> str:
        """Get formatted uptime string"""
        try:
            now = datetime.now(timezone.utc)
            uptime_delta = now - self.start_time

            days = uptime_delta.days
            hours, remainder = divmod(uptime_delta.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)

            if days > 0:
                return f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                return f"{hours}h {minutes}m {seconds}s"
            else:
                return f"{minutes}m {seconds}s"

        except (ValueError, TypeError, AttributeError):
            return "N/A"

    def _validate_config(self) -> None:
        """Validate configuration values on startup"""
        errors = []

        if not cfg.AI_API_KEY and cfg.AI_PROVIDER not in ("ollama", "lmstudio"):
            logger.warning("Neural cortex offline — no API key detected")

        if not cfg.TOMORROW_IO_API_KEY:
            logger.warning(
                "Primary weather telemetry unavailable — NOAA fallback active"
            )

        if not (-90 <= cfg.LAT <= 90):
            errors.append("Invalid latitude")

        if not (-180 <= cfg.LON <= 180):
            errors.append("Invalid longitude")

        if not cfg.DEVICE_HOST:
            errors.append("DEVICE_HOST not set")

        if cfg.LAT == 0.0 and cfg.LON == 0.0:
            logger.warning("Coordinates not set — environmental sensors disabled")

        if cfg.RIVER_ENABLED:
            if not cfg.RIVER_GAUGE_ID:
                logger.warning("Hydrological monitor enabled but no gauge ID specified")
            if not any(cfg.FLOOD_STAGES.get(k, 0) for k in ["action", "flood", "moderate", "major"]):
                logger.warning("Flood thresholds at zero — alert triggers disarmed")

        if errors:
            logger.error("Configuration errors: " + ", ".join(errors))
            raise ValueError("Configuration validation failed")

    def _validate_local_model(self) -> None:
        """Warn if the configured model isn't available on the local provider."""
        try:
            available = [m.id for m in self.client.models.list().data]
            if cfg.MODEL not in available:
                logger.warning(
                    f"Model '{cfg.MODEL}' not found in {cfg.ai_provider_display_name} "
                    f"model list. Available: {', '.join(available[:8]) or 'none listed'}"
                )
            else:
                logger.info(f"Neural cortex locked on — model '{cfg.MODEL}' verified via {cfg.ai_provider_display_name}")
        except Exception as e:
            logger.warning(f"Neural cortex unverified — {cfg.ai_provider_display_name} handshake failed: {e}")

    def _send_dm(self, node_id: int, text: str, channel: int = 0) -> None:
        """Send a direct message to a specific node."""
        if not self.interface:
            return
        try:
            self.interface.sendText(
                text[:cfg.MAX_RESPONSE_LENGTH],
                destinationId=node_id,
                channelIndex=channel,
                wantAck=True,
            )
        except Exception as e:
            logger.warning(f"Transmission to {self._get_node_name(node_id)} failed: {e}")

    def _get_node_name(self, node_id: int) -> str:
        """Return 'longName (shortName)' for a node ID, or hex fallback."""
        try:
            if self.interface and self.interface.nodes:
                info = self.interface.nodes.get(f"!{node_id:08x}", {})
                user = info.get("user", {})
                long_name = user.get("longName", "")
                short_name = user.get("shortName", "")
                if long_name and short_name:
                    return f"{long_name} ({short_name})"
                elif long_name:
                    return long_name
                elif short_name:
                    return short_name
        except Exception:
            pass
        return f"!{node_id:08x}"

    def _build_context_snapshot(self) -> str:
        """Build a fresh context block from cached data for AI system prompt injection."""
        sections = []

        # Current date/time
        now = datetime.now(cfg.TIMEZONE)
        sections.append(f"[Date/Time]\n{now.strftime('%A, %B %d, %Y %I:%M %p %Z')}")

        # Current conditions (prefer Tomorrow.io, fall back to NOAA)
        conditions = (
            self.weather_cache.get("tomorrow", {}).get("data")
            or self.weather_cache.get("noaa", {}).get("data")
        )
        if conditions:
            sections.append(f"[Current Conditions]\n{conditions}")

        # Active NOAA alerts
        alert_data = self.alerts_cache.get("data")
        if alert_data:
            lines = []
            for feat in alert_data[:3]:
                props = feat.get("properties", {})
                event = props.get("event", "Alert")
                expires_str = props.get("expires", "")
                try:
                    exp_dt = datetime.fromisoformat(expires_str).astimezone(cfg.TIMEZONE)
                    expires_fmt = exp_dt.strftime("%a %I:%M %p").lstrip("0")
                    lines.append(f"{event} until {expires_fmt}")
                except Exception:
                    lines.append(event)
            if lines:
                sections.append("[Active Alerts]\n" + "\n".join(lines))

        # River level
        if cfg.RIVER_ENABLED:
            level_str = self.river_cache.get("level")
            if level_str is not None:
                try:
                    level_ft = float(level_str)
                    flood = cfg.FLOOD_STAGES.get("flood", 0)
                    action = cfg.FLOOD_STAGES.get("action", 0)
                    stage = ""
                    if flood and level_ft >= flood:
                        stage = " (FLOOD STAGE)"
                    elif action and level_ft >= action:
                        stage = " (Action stage)"
                    sections.append(
                        f"[River: {cfg.RIVER_NAME}]\nLevel: {level_ft:.1f} ft{stage}"
                    )
                except (ValueError, TypeError):
                    pass

        # AQI
        if cfg.AQI_ENABLED:
            aqi_data = self.aqi_cache.get("data")
            if aqi_data:
                aqi = aqi_data.get("aqi", "")
                cat = aqi_data.get("category_name", "")
                param = aqi_data.get("parameter", "")
                line = f"AQI {aqi} ({cat})"
                if param:
                    line += f" — {param}"
                sections.append(f"[Air Quality]\n{line}")

        return "\n\n".join(sections)

    @staticmethod
    def _configure_socket(sock: socket.socket) -> None:
        """Tune the mesh TCP socket for low-latency delivery.

        TCP_NODELAY disables Nagle so frames ship immediately.
        SO_KEEPALIVE is NOT enabled — the ESP32 lwIP stack RSTs the
        connection when it receives TCP keepalive probes.
        """
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            logger.info("Socket configured: TCP_NODELAY")
        except OSError as e:
            logger.warning(f"Could not set socket options: {e}")

    def _init_connections(self) -> bool:
        """Initialize all connections with proper error handling"""
        try:
            logger.info("Neural cortex initializing")
            is_local = cfg.AI_PROVIDER in ("ollama", "lmstudio")

            if not is_local and not cfg.AI_API_KEY:
                logger.error("AI_API_KEY not set — required for cloud providers")
                return False

            api_key = cfg.AI_API_KEY if cfg.AI_API_KEY else "local"
            self.client = OpenAI(base_url=cfg.AI_BASE_URL, api_key=api_key, timeout=60.0)

            # Model validation for local providers
            if is_local:
                self._validate_local_model()

            if not cfg.TOMORROW_IO_API_KEY:
                logger.warning(
                    "Primary weather telemetry unavailable — NOAA fallback active"
                )

            self.interface = TCPInterface(cfg.DEVICE_HOST)
            if self.interface and self.interface.myInfo:
                self.my_node_num = self.interface.myInfo.my_node_num
            else:
                logger.error("Failed to get myInfo from interface")
                return False

            if self.interface.socket:
                self._configure_socket(self.interface.socket)

            # Cancel the library's 300s heartbeat — sendHeartbeat() causes
            # the ESP32 to RST the TCP connection after device reboots.
            if self.interface.heartbeatTimer:
                self.interface.heartbeatTimer.cancel()
                self.interface.heartbeatTimer = None

            pub.subscribe(self.on_receive, "meshtastic.receive.text")
            pub.subscribe(self.on_node_discovered, "meshtastic.receive.nodeinfo")
            pub.subscribe(self._on_connection_established, "meshtastic.connection.established")
            pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")

            # Seed known_nodes from interface's node database
            if hasattr(self.interface, 'nodes') and self.interface.nodes:
                with self.lock:
                    self.known_nodes = set(self.interface.nodes.keys())
                logger.info(f"Mesh topology mapped — {len(self.known_nodes)} nodes acquired")

            logger.info("All subsystems nominal")
            self.retry_count = 0
            self._notify_status_change()
            return True

        except (
            ConnectionError,
            OSError,
            ValueError,
            TypeError,
            AttributeError,
        ) as e:
            logger.error(f"Connection init failed: {e}")
            return False

    def _reconnect_interface(self) -> bool:
        """Reconnect only the mesh radio (skips OpenAI/session/pub.subscribe)."""
        try:
            logger.info("Neural cortex initializing")
            self.interface = TCPInterface(cfg.DEVICE_HOST)
            if self.interface and self.interface.myInfo:
                self.my_node_num = self.interface.myInfo.my_node_num
            else:
                logger.error("Failed to get myInfo from interface")
                return False

            if self.interface.socket:
                self._configure_socket(self.interface.socket)

            # Cancel the library's 300s heartbeat — sendHeartbeat() causes
            # the ESP32 to RST the TCP connection after device reboots.
            if self.interface.heartbeatTimer:
                self.interface.heartbeatTimer.cancel()
                self.interface.heartbeatTimer = None

            if hasattr(self.interface, 'nodes') and self.interface.nodes:
                with self.lock:
                    self.known_nodes = set(self.interface.nodes.keys())
                logger.info(f"Mesh topology mapped — {len(self.known_nodes)} nodes acquired")

            logger.info("All subsystems nominal")
            self.retry_count = 0
            self._notify_status_change()
            return True

        except (
            ConnectionError,
            OSError,
            ValueError,
            TypeError,
            AttributeError,
        ) as e:
            logger.error(f"Connection init failed: {e}")
            return False

    def _reconnect(self, blocking=True) -> bool:
        """Reconnect the mesh radio interface."""
        acquired = self._reconnect_lock.acquire(blocking=blocking)
        if not acquired:
            return False
        try:
            if not self.is_running:
                return False

            # Connection may have been restored while waiting for lock
            if (self.interface
                    and hasattr(self.interface, "isConnected")
                    and self.interface.isConnected.is_set()):
                return True

            self.retry_count += 1
            logger.warning(f"Mesh link recovery attempt {self.retry_count}/{cfg.MAX_RETRIES}")
            self._notify_status_change()

            old_interface = self.interface
            if old_interface:
                try:
                    self._closing_interface = True
                    old_interface.close()
                except (ConnectionError, OSError) as e:
                    logger.warning(f"Error closing interface during reconnect: {e}")
                    # close() failed partway — force cleanup so the ESP32
                    # doesn't keep a zombie connection eating a client slot.
                    try:
                        old_interface._wantExit = True
                        if old_interface.socket:
                            old_interface.socket.close()
                            old_interface.socket = None
                    except Exception:
                        pass
                finally:
                    self._closing_interface = False

                # Wait for the old reader thread to fully exit before opening
                # a new connection — avoids two sockets to the ESP32 at once.
                try:
                    if hasattr(old_interface, '_rxThread') and old_interface._rxThread.is_alive():
                        old_interface._rxThread.join(timeout=5)
                except Exception:
                    pass

            # Give the device time to release the TCP slot.
            time.sleep(2)

            if self._reconnect_interface():
                with self.lock:
                    self.reconnect_count += 1
                logger.info("Mesh link reacquired")
                time.sleep(2)  # Let connection stabilize before sending
                return True

            time.sleep(cfg.RETRY_DELAY)
            return False
        finally:
            self._reconnect_lock.release()

    def _truncate_message(self, text: str) -> str:
        """Safely truncate message to byte limit while preserving UTF-8 boundaries"""
        if not text:
            return text

        original_len = len(text)

        if len(text) <= cfg.MAX_RESPONSE_LENGTH:
            encoded = text.encode("utf-8")
            if len(encoded) <= cfg.MAX_BYTE_LIMIT:
                return text

        truncated = text[: cfg.MAX_RESPONSE_LENGTH]
        while (
            len(truncated.encode("utf-8")) > cfg.MAX_BYTE_LIMIT and len(truncated) > 0
        ):
            truncated = truncated[:-1]

        if len(truncated) < len(text) * 0.8:
            if len(truncated) > 3:
                truncated = truncated[:-3] + "..."

        return truncated

    def _split_message(self, text: str) -> list:
        """Split a message into parts that each fit within the byte limit."""
        if not text:
            return []
        if len(text.encode("utf-8")) <= cfg.MAX_BYTE_LIMIT:
            return [text]

        parts = []
        remaining = text

        while remaining and len(parts) < cfg.MAX_MESSAGE_PARTS:
            if len(remaining.encode("utf-8")) <= cfg.MAX_BYTE_LIMIT:
                parts.append(remaining)
                break

            # Find max chars that fit in byte limit
            cut = min(len(remaining), cfg.MAX_RESPONSE_LENGTH)
            while cut > 0 and len(remaining[:cut].encode("utf-8")) > cfg.MAX_BYTE_LIMIT:
                cut -= 1

            if cut == 0:
                break

            # Try to split at a natural boundary
            chunk = remaining[:cut]
            split = -1

            # Prefer sentence boundaries
            for sep in [". ", "? ", "! "]:
                idx = chunk.rfind(sep)
                if idx > len(chunk) // 3:
                    split = max(split, idx + len(sep))

            # Fall back to clause boundaries
            if split == -1:
                for sep in [", ", "; "]:
                    idx = chunk.rfind(sep)
                    if idx > len(chunk) // 3:
                        split = max(split, idx + len(sep))

            # Fall back to word boundary
            if split == -1:
                idx = chunk.rfind(" ")
                if idx > len(chunk) // 3:
                    split = idx + 1

            if split > 0:
                cut = split

            parts.append(remaining[:cut].rstrip())
            remaining = remaining[cut:].lstrip()

        return parts if parts else [text[:cfg.MAX_RESPONSE_LENGTH]]

    def _send_message(self, text: str, channel: int = None) -> bool:
        """Send message, splitting into multiple parts if needed."""
        parts = self._split_message(text)

        for i, part in enumerate(parts):
            if not self._send_single(part, channel if channel is not None else cfg.MESH_CHANNEL):
                return False
            if i < len(parts) - 1:
                time.sleep(cfg.MULTIPART_DELAY)

        return True

    def _send_single(self, text: str, channel: int = None) -> bool:
        """Send a single message with retry mechanism."""
        safe_text = self._truncate_message(text)

        if (
            not self.interface
            or not hasattr(self.interface, "isConnected")
            or not self.interface.isConnected.is_set()
        ):
            logger.warning("Mesh link down — initiating recovery")
            if not self._reconnect():
                return False

        for attempt in range(cfg.MAX_RETRIES):
            try:
                self.interface.sendText(safe_text, channelIndex=channel if channel is not None else cfg.MESH_CHANNEL)
                logger.info(f"Sent: {safe_text}")
                with self.lock:
                    self.messages_sent_count += 1
                self._notify_status_change()
                return True
            except (
                ConnectionError,
                OSError,
                ValueError,
                TypeError,
                AttributeError,
                MeshInterface.MeshInterfaceError,
            ) as e:
                logger.error(f"Send failed (attempt {attempt + 1}/{cfg.MAX_RETRIES}): {e}")
                if attempt < cfg.MAX_RETRIES - 1:
                    if not self._reconnect():
                        logger.error("Reconnect failed during send retry, aborting")
                        return False
                    time.sleep(cfg.RETRY_DELAY)
                else:
                    return False
        return False

    def on_receive(self, packet: Dict[str, Any], interface):
        """Handle incoming messages"""
        if (
            not isinstance(packet, dict)
            or "decoded" not in packet
            or not isinstance(packet["decoded"], dict)
            or "text" not in packet["decoded"]
            or "from" not in packet
        ):
            logger.error(f"Invalid packet structure received: {packet}")
            return

        try:
            message = packet["decoded"]["text"]
            sender = packet["from"]

            if sender == self.my_node_num:
                return

            with self.lock:
                self.messages_received_count += 1
                self.last_message_time = datetime.now(timezone.utc)

            hop_start = packet.get("hopStart", 0)
            hop_limit = packet.get("hopLimit", 0)
            hops = hop_start - hop_limit if hop_start else 0
            hop_str = f" [{hops} hop{'s' if hops != 1 else ''}]" if hops > 0 else " [direct]"
            sender_name = self._get_node_name(sender)
            logger.info(f"Received{hop_str}: {message} from {sender_name}")

            msg_channel = packet.get("channel", 0)
            if self._on_message_received:
                try:
                    self._on_message_received(message, msg_channel)
                except Exception:
                    pass

            cmd = message.lower().strip()

            if cmd == "ping":
                response = f"PONG --- SNR:{packet.get('rxSnr', 'N/A')} RSSI:{packet.get('rxRssi', 'N/A')}"
            elif cmd == "wx":
                response = self._get_hourly_forecast()
            elif cmd == "uptime":
                uptime = self._get_uptime()
                response = f"Bot Uptime: {uptime}"
            elif cmd == "api":
                response = self._get_api_stats()
            elif cmd == "river":
                if not cfg.RIVER_ENABLED:
                    response = "River monitoring not configured"
                else:
                    level = self._get_river_level()
                    if level == "N/A":
                        response = "RIVER: Data unavailable"
                    else:
                        response = f"{cfg.RIVER_NAME}: {level}ft"
            elif cmd == "aqi":
                if not cfg.AQI_ENABLED:
                    response = "Air quality monitoring not configured"
                else:
                    aqi_data = self._get_aqi_data()
                    if aqi_data:
                        response = f"Air Quality: AQI {aqi_data['aqi']} - {aqi_data['category_name']} ({aqi_data['parameter']}) [EPA AirNow - preliminary]"
                    else:
                        response = "AQI: Data unavailable"
            elif cmd == "help":
                ai_status = "OFF" if self.chat_paused else "ON"
                cmds = ["ping", "wx"]
                if cfg.RIVER_ENABLED:
                    cmds.append("river")
                if cfg.AQI_ENABLED:
                    cmds.append("aqi")
                cmds.extend(["uptime", "api", "help"])
                if cfg.BBS_ENABLED:
                    cmds.extend(["bbspost", "bbsread"])
                response = f"Commands: {', '.join(cmds)} | AI: {ai_status}"
            elif cfg.BBS_ENABLED and (cmd == "bbspost" or cmd.startswith("bbspost ")):
                if self.bbs:
                    body = message.strip()[8:].strip()
                    if not body:
                        self._send_dm(sender, "BBS: Usage: bbspost <message>", msg_channel)
                    else:
                        try:
                            node_info = (self.interface.nodes or {}).get(f"!{sender:08x}", {}) if self.interface else {}
                            name = node_info.get("user", {}).get("shortName", "") or f"!{sender:08x}"
                        except Exception:
                            name = f"!{sender:08x}"
                        count = self.bbs.add_post(sender, name, body)
                        self._send_dm(sender, f"BBS: Posted. Board has {count} message(s).", msg_channel)
                        logger.info(f"BBS write: {name} posted ({len(body)} chars) — board has {count} message(s)")
                return
            elif cfg.BBS_ENABLED and cmd == "bbsread":
                if self.bbs:
                    posts = self.bbs.get_posts()
                    if not posts:
                        self._send_dm(sender, "BBS: No messages on the board.", msg_channel)
                    else:
                        total = len(posts)
                        for i, post in enumerate(posts, 1):
                            age = format_age(post["timestamp"])
                            line = f"[BBS {i}/{total}] {post['node_name']} ({age}): {post['message']}"
                            self._send_dm(sender, line, msg_channel)
                            if i < total:
                                time.sleep(cfg.MULTIPART_DELAY)
                        logger.info(f"BBS read: {total} message(s) delivered to {self._get_node_name(sender)}")
                return
            else:
                if self.chat_paused:
                    return
                # Rate limit AI chat: 1 response per 10 seconds per user
                now_utc = datetime.now(timezone.utc)
                with self.lock:
                    last = self._last_ai_response.get(sender)
                    if last and (now_utc - last).total_seconds() < 10:
                        logger.debug(f"Rate limited AI for {self._get_node_name(sender)}")
                        return
                now = datetime.now(timezone.utc)
                with self.lock:
                    if sender in self.last_activity and (
                        now - self.last_activity[sender]
                    ) > timedelta(minutes=cfg.CHAT_TIMEOUT_MINUTES):
                        self.chat_histories[sender] = []
                        logger.info(f"Memory banks cleared for {sender_name} — session expired")
                response = self._get_ai_response(sender, message)

            self._send_message(response, packet.get("channel", 0))

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Message handling error: {e}")

    def on_node_discovered(self, packet: Dict[str, Any], interface):
        """Handle node discovery events"""
        try:
            node_info = packet.get("decoded", {})
            node_id = packet.get("from")

            if node_id in self.known_nodes:
                return

            self.known_nodes.add(node_id)
            self._notify_status_change()

            user_info = node_info.get("user", {})
            snr = packet.get("rxSnr", "N/A")
            rssi = packet.get("rxRssi", "N/A")

            logger.info(f"NEW NODE: {self._get_node_name(node_id)} | SNR:{snr}dB RSSI:{rssi}dBm")

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Node discovery error: {e}")

    def _on_connection_established(self, interface, topic=None):
        """Handle meshtastic connection established event."""
        logger.info("Mesh radio link established")
        self._notify_status_change()

    def _on_connection_lost(self, interface, topic=None):
        """Handle meshtastic connection lost event."""
        if self._closing_interface:
            return  # Intentional close during reconnect — ignore
        logger.warning("Mesh radio link severed")
        self._notify_status_change()
        if self.is_running:
            threading.Thread(
                target=self._background_reconnect,
                name="reconnect-on-lost",
                daemon=True,
            ).start()

    def _background_reconnect(self):
        """Attempt reconnection with backoff on failure.

        The circuit breaker only fires when reconnection *fails* repeatedly,
        not when the link drops and recovers quickly.  Frequent successful
        reconnects are a symptom — delaying them only makes it worse.
        """
        delay = 5
        max_delay = 60
        consecutive_failures = 0

        while self.is_running:
            time.sleep(delay)
            if not self.is_running:
                break
            if (self.interface
                    and hasattr(self.interface, "isConnected")
                    and self.interface.isConnected.is_set()):
                return
            if self._reconnect(blocking=False):
                self._recent_reconnects.append(time.time())
                return
            # Only escalate delay when reconnection *fails*
            consecutive_failures += 1
            if consecutive_failures >= 3:
                delay = min(delay * 2, max_delay)
                logger.warning(
                    f"Mesh link recovery stalled — next attempt in {delay}s"
                )

    def _get_api_stats(self) -> str:
        """Get API performance statistics"""
        try:
            with self.lock:
                total = self.api_stats["total_calls"]
                success = self.api_stats["successful_calls"]
                times = self.api_stats["response_times"]
                errors = self.api_stats["errors"]
                per_endpoint = {k: dict(v) for k, v in self.api_stats["per_endpoint"].items()}

            if total == 0:
                return "No API calls yet"

            success_rate = (success / total) * 100 if total > 0 else 0
            avg_time = sum(times) / len(times) if times else 0

            top_errors = sorted(errors.items(), key=lambda x: x[1], reverse=True)[:3]
            error_str = ", ".join(f"{k}:{v}" for k, v in top_errors) if top_errors else "None"

            # Per-endpoint breakdown sorted by average response time descending
            ep_parts = []
            for ep, stats in sorted(per_endpoint.items(), key=lambda x: (sum(x[1]["times"]) / len(x[1]["times"])) if x[1]["times"] else 0, reverse=True):
                ep_times = stats["times"]
                ep_avg = sum(ep_times) / len(ep_times) if ep_times else 0
                ep_calls = stats["calls"]
                ep_parts.append(f"{ep}:{ep_avg:.2f}s({ep_calls})")
            ep_str = " | ".join(ep_parts) if ep_parts else "none"

            return (
                f"API Stats: Total:{total} Success:{success_rate:.1f}% Avg:{avg_time:.2f}s Errors:{error_str}\n"
                f"By endpoint (avg/calls): {ep_str}"
            )

        except (ValueError, TypeError, AttributeError):
            return "Stats error"

    def _record_api_call(self, success: bool, response_time: float, error: Exception = None, endpoint: str = "unknown") -> None:
        """Record an API call's outcome for performance monitoring"""
        with self.lock:
            self.api_stats["total_calls"] += 1
            if success:
                self.api_stats["successful_calls"] += 1
            else:
                self.api_stats["failed_calls"] += 1
                if error:
                    error_type = type(error).__name__
                    self.api_stats["errors"][error_type] = self.api_stats["errors"].get(error_type, 0) + 1
            self.api_stats["response_times"].append(response_time)
            if len(self.api_stats["response_times"]) > 100:
                self.api_stats["response_times"].pop(0)

            # Per-endpoint tracking
            ep = self.api_stats["per_endpoint"].setdefault(endpoint, {"calls": 0, "failures": 0, "times": []})
            ep["calls"] += 1
            if not success:
                ep["failures"] += 1
            ep["times"].append(response_time)
            if len(ep["times"]) > 100:
                ep["times"].pop(0)

    def _get_ai_response(self, sender: int, query: str) -> str:
        """Get AI response with chat history"""
        if not self.client:
            return "AI service unavailable"

        now = datetime.now(timezone.utc)

        context_snapshot = self._build_context_snapshot()

        with self.lock:
            if sender not in self.chat_histories:
                self.chat_histories[sender] = []

            self.chat_histories[sender].append({"role": "user", "content": query})

            system_content = cfg.SYSTEM_PROMPT
            if context_snapshot:
                system_content = cfg.SYSTEM_PROMPT + "\n\n" + context_snapshot

            messages = [
                {"role": "system", "content": system_content}
            ] + self.chat_histories[sender]

        try:
            start_time = time.time()
            for attempt in range(2):
                attempt_start = time.time()
                use_search = cfg.AI_SEARCH_ENABLED and "x.ai" in cfg.AI_BASE_URL
                if use_search:
                    response = self.client.responses.create(
                        model=cfg.MODEL, input=messages, max_output_tokens=2000,
                        temperature=cfg.AI_TEMPERATURE,
                        tools=[{"type": "web_search"}],
                    )
                    # Extract text from Responses API output
                    content = ""
                    for block in response.output:
                        if getattr(block, "type", None) == "message":
                            for part in block.content:
                                if getattr(part, "type", None) == "output_text":
                                    content += part.text
                    content = content.strip() if content else "RETRY"
                    # Strip citation links — not useful over LoRa
                    content = re.sub(r'\[\[\d+\]\]\([^)]*\)', '', content)
                    content = re.sub(r'  +', ' ', content).strip()
                    logger.debug("Grok responded via Responses API (web search enabled)")
                else:
                    completion = self.client.chat.completions.create(
                        model=cfg.MODEL, messages=messages, max_tokens=2000,
                        temperature=cfg.AI_TEMPERATURE,
                    )
                    content = completion.choices[0].message.content
                    content = content.strip() if content else "RETRY"

                if content != "RETRY":
                    break
                logger.warning(f"AI returned RETRY on attempt {attempt + 1}, retrying")

            response_time = time.time() - attempt_start

            self._record_api_call(success=True, response_time=response_time, endpoint="ai")

            with self.lock:
                self.chat_histories[sender].append({"role": "assistant", "content": content})

                if len(self.chat_histories[sender]) > cfg.MAX_CHAT_HISTORY:
                    self.chat_histories[sender] = self.chat_histories[sender][-cfg.MAX_CHAT_HISTORY:]

                self.last_activity[sender] = now
                self._last_ai_response[sender] = datetime.now(timezone.utc)

            return content[: cfg.MAX_RESPONSE_LENGTH * cfg.MAX_MESSAGE_PARTS]

        except (ConnectionError, requests.RequestException, ValueError, TypeError, AttributeError, KeyError) as e:
            logger.error(f"AI response error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="ai")
            return "Processing error. Try again."

    def _degrees_to_cardinal(self, degrees: float) -> str:
        """Convert wind direction degrees to cardinal direction"""
        directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                      "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        index = round(degrees / 22.5) % 16
        return directions[index]

    def _weather_code_to_description(self, code: int) -> str:
        """Convert Tomorrow.io weather code to description"""
        weather_codes = {
            0: "Unknown", 1000: "Clear", 1100: "Mostly Clear", 1101: "Partly Cloudy",
            1102: "Mostly Cloudy", 1001: "Cloudy", 2000: "Fog", 2100: "Light Fog",
            4000: "Drizzle", 4001: "Rain", 4200: "Light Rain", 4201: "Heavy Rain",
            5000: "Snow", 5001: "Flurries", 5100: "Light Snow", 5101: "Heavy Snow",
            6000: "Freezing Drizzle", 6001: "Freezing Rain", 6200: "Light Freezing Rain",
            6201: "Heavy Freezing Rain", 7000: "Ice Pellets", 7101: "Heavy Ice Pellets",
            7102: "Light Ice Pellets", 8000: "Thunderstorm",
        }
        return weather_codes.get(code, f"Unknown ({code})")

    def _get_current_conditions_noaa(self) -> str:
        """Get current weather conditions from NOAA stations API"""
        cache_key = "noaa"
        with self.lock:
            if (
                self.weather_cache[cache_key]["timestamp"]
                and (datetime.now(timezone.utc) - self.weather_cache[cache_key]["timestamp"]).total_seconds()
                < cfg.WEATHER_CACHE_TTL
            ):
                return self.weather_cache[cache_key]["data"]

        start_time = time.time()
        try:
            station_url = self._noaa_station_cache["station_url"]
            if not station_url:
                stations_url = f"https://api.weather.gov/points/{cfg.LAT},{cfg.LON}/stations"
                resp = self._session.get(stations_url, headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
                if resp.status_code != 200:
                    logger.warning(f"NOAA stations HTTP {resp.status_code}")
                    self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-conditions")
                    return "Current conditions unavailable"

                stations = resp.json()["features"]
                if not stations:
                    logger.warning("NOAA stations returned empty list")
                    self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-conditions")
                    return "No weather stations found"

                station_props = stations[0]["properties"]
                station_url = station_props.get("@id", "N/A")
                self._noaa_station_cache["station_url"] = station_url
                logger.info(f"NOAA station cached: {station_url}")

            resp = self._session.get(f"{station_url}/observations/latest",
                                headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"NOAA observations HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-conditions")
                return "Current observations unavailable"

            obs = resp.json()["properties"]

            temp_f = obs.get("temperature", {}).get("value")
            if temp_f is not None:
                temp_f = round(temp_f * 9 / 5 + 32, 1)

            humidity = obs.get("relativeHumidity", {}).get("value")
            wind_speed_mph = obs.get("windSpeed", {}).get("value")
            if wind_speed_mph is not None:
                wind_speed_mph = round(wind_speed_mph * 2.237, 1)

            wind_dir = obs.get("windDirection", {}).get("value")
            conditions = obs.get("textDescription", "Unknown")

            parts = []
            if conditions:
                parts.append(conditions)
            if temp_f is not None:
                parts.append(f"+{temp_f}F")
            if wind_speed_mph is not None and wind_dir is not None:
                wind_dir_cardinal = self._degrees_to_cardinal(wind_dir)
                parts.append(f"WIND {wind_dir_cardinal}{wind_speed_mph}mph")
            if humidity is not None:
                parts.append(f"HUMIDITY {humidity}%")

            result = ", ".join(parts) if parts else "Conditions data incomplete"
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-conditions")
            with self.lock:
                self.weather_cache[cache_key]["data"] = result
                self.weather_cache[cache_key]["timestamp"] = datetime.now(timezone.utc)
            return result

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"NOAA current conditions error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-conditions")
            return "Current conditions error"

    def _get_current_conditions_tomorrow(self) -> str:
        """Get current weather conditions from Tomorrow.io API"""
        cache_key = "tomorrow"
        with self.lock:
            if (
                self.weather_cache[cache_key]["timestamp"]
                and (datetime.now(timezone.utc) - self.weather_cache[cache_key]["timestamp"]).total_seconds()
                < cfg.WEATHER_CACHE_TTL
            ):
                return self.weather_cache[cache_key]["data"]

        try:
            start_time = time.time()
            url = "https://api.tomorrow.io/v4/weather/realtime"

            params = {
                "location": f"{cfg.LAT},{cfg.LON}",
                "apikey": cfg.TOMORROW_IO_API_KEY,
                "fields": ["temperature", "temperatureApparent", "humidity", "windSpeed",
                           "windDirection", "windGust", "precipitationIntensity",
                           "precipitationProbability", "cloudCover", "visibility",
                           "uvIndex", "pressureSeaLevel", "dewPoint", "weatherCode"],
            }

            resp = self._session.get(url, params=params, timeout=cfg.API_TIMEOUT)
            response_time = time.time() - start_time

            self._record_api_call(success=(resp.status_code == 200), response_time=response_time, endpoint="tomorrow-io")

            if resp.status_code != 200:
                return "Tomorrow.io: API error"

            data = resp.json()["data"]["values"]

            temp_c = data.get("temperature", 0)
            temp_f = round(temp_c * 9 / 5 + 32, 1)
            feels_c = data.get("temperatureApparent", 0)
            feels_f = round(feels_c * 9 / 5 + 32, 1)
            humidity = data.get("humidity", 0)
            uv_index = data.get("uvIndex", 0)
            wind_speed_ms = data.get("windSpeed", 0)
            wind_speed_mph = round(wind_speed_ms * 2.237, 1)
            wind_dir = data.get("windDirection", 0)
            wind_cardinal = self._degrees_to_cardinal(wind_dir)
            wind_gust_ms = data.get("windGust", 0)
            wind_gust_mph = round(wind_gust_ms * 2.237, 1)
            precip_mm = data.get("precipitationIntensity", 0)
            precip_prob = data.get("precipitationProbability", 0)
            pressure_hpa = data.get("pressureSeaLevel", 0)
            pressure_mb = round(pressure_hpa, 1)
            cloud_cover = data.get("cloudCover", 0)
            visibility_km = data.get("visibility", 0)
            visibility_mi = round(visibility_km * 0.621371, 1)
            dew_point_c = data.get("dewPoint", 0)
            dew_point_f = round(dew_point_c * 9 / 5 + 32, 1)
            weather_code = data.get("weatherCode", 0)
            weather_desc = self._weather_code_to_description(weather_code)

            parts = [weather_desc, f"+{temp_f}F", f"FEELS {feels_f}F", f"UV {uv_index}",
                     f"WIND {wind_cardinal}{wind_speed_mph}mph"]

            if wind_gust_mph > wind_speed_mph * 1.25:
                parts.append(f"GUST {wind_gust_mph}mph")

            parts.extend([f"HUMIDITY {humidity}%", f"PRESSURE {pressure_mb}mb",
                          f"DEWPOINT {dew_point_f}F", f"CLOUDS {cloud_cover}%", f"VIS {visibility_mi}mi"])

            if precip_mm > 0:
                parts.append(f"PRECIP {precip_mm}mm")
            if precip_prob > 0:
                parts.append(f"RAIN {precip_prob}%")

            result = ", ".join(parts)
            with self.lock:
                self.weather_cache[cache_key]["data"] = result
                self.weather_cache[cache_key]["timestamp"] = datetime.now(timezone.utc)
            return result

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Tomorrow.io current conditions error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="tomorrow-io")
            return "Weather data unavailable"

    def _get_noaa_points_urls(self) -> dict:
        """Get NOAA points-derived forecast URLs, fetching once then caching."""
        if self._noaa_points_cache["forecast_url"]:
            return self._noaa_points_cache
        start_time = time.time()
        try:
            resp = self._session.get(cfg.WEATHER_POINTS_URL, headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"NOAA points HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-points")
                return {}
            props = resp.json()["properties"]
            self._noaa_points_cache = {
                "forecast_url": props["forecast"],
                "forecast_hourly_url": props["forecastHourly"],
            }
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-points")
            return self._noaa_points_cache
        except (requests.RequestException, KeyError, ValueError, TypeError) as e:
            logger.error(f"NOAA points lookup error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-points")
            return {}

    def _get_hourly_forecast(self) -> str:
        """Get next 6 hours of hourly forecast for WX command"""
        start_time = time.time()
        try:
            urls = self._get_noaa_points_urls()
            if not urls:
                return "WX: Forecast unavailable"

            start_time = time.time()
            resp = self._session.get(urls["forecast_hourly_url"], headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"NOAA hourly forecast HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-forecast")
                return "WX: Hourly data unavailable"

            periods = resp.json()["properties"]["periods"][:6]

            condition_abbrevs = {"Partly Cloudy": "P Cloudy", "Mostly Clear": "M Clear", "Mostly Cloudy": "M Cloudy"}

            hourly_parts = []
            for period in periods:
                try:
                    local_time = datetime.fromisoformat(period["startTime"].replace("Z", "+00:00")).astimezone(cfg.TIMEZONE)
                    time_str = local_time.strftime("%I%p").lstrip("0")
                except (ValueError, AttributeError):
                    time_str = "N/A"

                temp = period.get("temperature", "N/A")
                condition = period.get("shortForecast", "Unknown")
                condition = condition_abbrevs.get(condition, condition)
                hourly_parts.append(f"{time_str} {temp}F {condition}")

            result = "WX: " + ", ".join(hourly_parts)
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-forecast")
            return result[: cfg.MAX_RESPONSE_LENGTH]

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Hourly forecast error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-forecast")
            return "WX: Error fetching forecast"

    def _get_hourly_forecast_data(self) -> list:
        """Get cached hourly forecast data"""
        with self.lock:
            if (
                self.hourly_forecast_cache["timestamp"]
                and (datetime.now(timezone.utc) - self.hourly_forecast_cache["timestamp"]).total_seconds()
                < cfg.HOURLY_FORECAST_CACHE_TTL
            ):
                return self.hourly_forecast_cache["data"] or []

        start_time = time.time()
        try:
            urls = self._get_noaa_points_urls()
            if not urls:
                return []

            start_time = time.time()
            resp = self._session.get(urls["forecast_hourly_url"], headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"NOAA hourly forecast data HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-forecast")
                return []

            periods = resp.json()["properties"]["periods"]
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-forecast")
            with self.lock:
                self.hourly_forecast_cache["data"] = periods
                self.hourly_forecast_cache["timestamp"] = datetime.now(timezone.utc)

            return periods

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Hourly forecast data error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-forecast")
            return []

    def _fetch_weather_forecast(self, is_daily: bool = True) -> str:
        """Fetch weather forecast with caching"""
        with self.lock:
            if (self.forecast_cache["timestamp"]
                    and (datetime.now(timezone.utc) - self.forecast_cache["timestamp"]).total_seconds()
                    < cfg.WEATHER_CACHE_TTL
                    and self.forecast_cache["data"] is not None):
                periods = self.forecast_cache["data"]
                for p in periods:
                    if p["isDaytime"] == is_daily:
                        return f"{p['detailedForecast']}"
                return "No forecast data"

        start_time = time.time()
        try:
            urls = self._get_noaa_points_urls()
            if not urls:
                return "Forecast unavailable"

            start_time = time.time()
            resp = self._session.get(urls["forecast_url"], headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"NOAA daily forecast HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-forecast")
                return "Forecast unavailable"

            periods = resp.json()["properties"]["periods"]
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-forecast")
            with self.lock:
                self.forecast_cache = {"data": periods, "timestamp": datetime.now(timezone.utc)}
            for p in periods:
                if p["isDaytime"] == is_daily:
                    return f"{p['detailedForecast']}"

            return "No forecast data"

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Forecast error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-forecast")
            return "Forecast error"

    def _update_sun_times(self, date):
        """Update sunrise/sunset times"""
        start_time = time.time()
        try:
            url = f"{cfg.SUN_API_URL}?lat={cfg.LAT}&lng={cfg.LON}&date={date}&formatted=0"
            resp = self._session.get(url, timeout=cfg.API_TIMEOUT)
            if not resp.ok or not resp.text.strip():
                logger.error(f"Sun times API returned HTTP {resp.status_code} with empty body")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="sunrise-sunset")
                self._sun_consecutive_failures += 1
                return
            data = resp.json()

            if data["status"] == "OK":
                results = data["results"]
                try:
                    sunrise = datetime.fromisoformat(results["sunrise"].replace("Z", "+00:00")).astimezone(cfg.TIMEZONE)
                    sunset = datetime.fromisoformat(results["sunset"].replace("Z", "+00:00")).astimezone(cfg.TIMEZONE)
                    self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="sunrise-sunset")
                    with self.lock:
                        self.sun_times["sunrise"] = sunrise
                        self.sun_times["sunset"] = sunset
                        self.sun_times["last_update"] = date
                    self._sun_consecutive_failures = 0
                    logger.info("Solar chronometrics synchronized")
                except (ValueError, AttributeError) as e:
                    logger.error(f"Invalid sunrise/sunset datetime format: {e}")
                    self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="sunrise-sunset")
                    self._sun_consecutive_failures += 1
            else:
                logger.warning(f"Sunrise-sunset API status: {data.get('status', 'unknown')}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="sunrise-sunset")
                self._sun_consecutive_failures += 1

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Sun times error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="sunrise-sunset")
            self._sun_consecutive_failures += 1

    def _check_noaa_alerts(self) -> None:
        """Check for NOAA weather alerts and send messages for new ones"""
        if not cfg.NOAA_ALERTS_URL:
            return

        try:
            # Check cache with lock, but do NOT hold lock during HTTP or send
            cache_hit = False
            with self.lock:
                if (
                    self.alerts_cache["timestamp"]
                    and (datetime.now(timezone.utc) - self.alerts_cache["timestamp"]).total_seconds()
                    < cfg.ALERTS_CACHE_TTL
                ):
                    features = self.alerts_cache["data"] or []
                    cache_hit = True

            if not cache_hit:
                # HTTP call outside the lock to avoid blocking the main thread
                start_time = time.time()
                resp = self._session.get(cfg.NOAA_ALERTS_URL, headers={"User-Agent": "MeshtasticBot", "Connection": "close"}, timeout=cfg.API_TIMEOUT)
                if resp.status_code != 200:
                    logger.warning(f"NOAA alerts HTTP {resp.status_code}")
                    self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-alerts")
                    self._noaa_consecutive_failures += 1
                    return

                features = resp.json().get("features", [])
                self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-alerts")
                self._noaa_consecutive_failures = 0
                with self.lock:
                    self.alerts_cache["data"] = features
                    self.alerts_cache["timestamp"] = datetime.now(timezone.utc)

            if not features:
                return

            # Collect messages to send while holding the lock briefly
            messages_to_send = []
            now = datetime.now(timezone.utc)
            with self.lock:
                for feature in features:
                    if len(messages_to_send) >= 3:
                        break

                    alert = feature["properties"]
                    alert_id = alert.get("id")

                    if alert_id and alert_id not in self.seen_alert_ids:
                        self.seen_alert_ids[alert_id] = now
                        event = alert.get("event", "Alert")
                        headline = alert.get("headline", "")
                        expires = alert.get("expires", "").split("T")[1][:5] if "T" in alert.get("expires", "") else ""

                        # Build header with issuing info
                        alert_msg = f"WARNING {event}{f' til {expires}' if expires else ''}: {headline}"

                        # Add detail from NWSheadline or description
                        params = alert.get("parameters", {})
                        nws_headline = params.get("NWSheadline", [""])[0] if params.get("NWSheadline") else ""
                        if nws_headline:
                            alert_msg += f" | {nws_headline}"
                        else:
                            desc = alert.get("description", "").strip()
                            if desc:
                                # Take first sentence of description
                                for sep in [". ", ".\n", "\n\n"]:
                                    idx = desc.find(sep)
                                    if idx > 0:
                                        desc = desc[:idx + 1]
                                        break
                                alert_msg += f" | {desc}"

                        messages_to_send.append(alert_msg)

            # Send messages outside the lock
            for msg in messages_to_send:
                self._send_message(msg)
            if messages_to_send:
                self._save_alert_state()

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            self._noaa_consecutive_failures += 1
            logger.error(f"NOAA alert error: {e}")
            if not cache_hit:
                self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-alerts")

    def _check_frost_conditions(self) -> None:
        """Check for frost conditions and send warnings"""
        try:
            now = datetime.now(cfg.TIMEZONE)

            if now.month not in cfg.FROST_SEASON_MONTHS:
                return
            if now.hour not in cfg.FROST_CHECK_HOURS:
                return

            with self.lock:
                if self.frost_alert_sent == now.date():
                    return

            periods = self._get_hourly_forecast_data()
            if not periods:
                return

            # Only scan the current overnight window: now → noon tomorrow
            overnight_cutoff = (now + timedelta(days=1)).replace(
                hour=12, minute=0, second=0, microsecond=0
            )

            frost_onset_time = None
            frost_low_time = None
            min_temp = float("inf")

            for period in periods:
                try:
                    period_dt = datetime.fromisoformat(
                        period["startTime"].replace("Z", "+00:00")
                    ).astimezone(cfg.TIMEZONE)
                except (ValueError, AttributeError, KeyError):
                    continue

                if period_dt < now or period_dt > overnight_cutoff:
                    continue

                temp = period.get("temperature", float("inf"))
                if temp < cfg.FROST_TEMP_THRESHOLD:
                    if frost_onset_time is None:
                        frost_onset_time = period_dt      # first frost period
                    if temp < min_temp:
                        min_temp = temp
                        frost_low_time = period_dt         # coldest period

            if not frost_onset_time:
                return

            with self.lock:
                sunrise = self.sun_times.get("sunrise")

            if sunrise:
                sunrise_time = sunrise.strftime("%H:%M")
                if sunrise > now:
                    sunrise_label = f"sunrise at {sunrise_time}"
                else:
                    sunrise_label = f"tomorrow's sunrise (~{sunrise_time})"
            else:
                sunrise_label = "sunrise"

            # Day context label
            if frost_onset_time.date() > now.date():
                when = "tonight" if now.hour >= 18 else "tomorrow morning"
            else:
                when = "tonight" if frost_onset_time.hour >= 18 else "this morning"

            onset_str = frost_onset_time.strftime("%H:%M")

            if frost_low_time and frost_low_time != frost_onset_time:
                low_str = frost_low_time.strftime("%H:%M")
                frost_msg = (
                    f"FROST WARNING: Frost arrives {when} at {onset_str} "
                    f"(low {int(min_temp)}F at {low_str}). "
                    f"Protect plants before {sunrise_label}."
                )
            else:
                frost_msg = (
                    f"FROST WARNING: Frost arrives {when} at {onset_str}, "
                    f"low of {int(min_temp)}F. "
                    f"Protect plants before {sunrise_label}."
                )

            self._send_message(frost_msg)
            with self.lock:
                self.frost_alert_sent = now.date()
            self._save_alert_state()
            logger.info(f"Cryogenic hazard advisory transmitted — onset {frost_onset_time}, nadir {min_temp}°F")

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Frost check error: {e}")

    def _get_river_level(self) -> str:
        """Get current river level from USGS gauge"""
        if not cfg.RIVER_ENABLED or not cfg.RIVER_API_URL:
            return "N/A"

        with self.lock:
            if (
                self.river_cache["timestamp"]
                and (datetime.now(timezone.utc) - self.river_cache["timestamp"]).total_seconds()
                < cfg.RIVER_CACHE_TTL
            ):
                return self.river_cache["level"]

        start_time = time.time()
        try:
            resp = self._session.get(cfg.RIVER_API_URL, headers={"User-Agent": "MeshtasticBot"}, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"USGS river HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="usgs-river")
                return "N/A"

            data = resp.json()
            response_time = time.time() - start_time
            self._record_api_call(success=True, response_time=response_time, endpoint="usgs-river")

            if "value" not in data or "timeSeries" not in data["value"]:
                return "N/A"

            time_series = data["value"]["timeSeries"]
            if not time_series:
                return "N/A"

            values = time_series[0]["values"][0]["value"]
            if not values:
                return "N/A"

            latest_reading = values[-1]
            try:
                level = float(latest_reading["value"])
            except (ValueError, TypeError):
                return "N/A"

            level_str = f"{level:.1f}"
            with self.lock:
                self.river_cache["level"] = level_str
                self.river_cache["timestamp"] = datetime.now(timezone.utc)

            return level_str

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"River level error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="usgs-river")
            return "N/A"

    def _get_aqi_data(self) -> Optional[dict]:
        """Get current AQI from AirNow API. Returns dict with aqi, category_name, category_number, parameter."""
        if not cfg.AQI_ENABLED or not cfg.AIRNOW_API_URL:
            return None

        with self.lock:
            if (
                self.aqi_cache["timestamp"]
                and (datetime.now(timezone.utc) - self.aqi_cache["timestamp"]).total_seconds()
                < cfg.AQI_CACHE_TTL
                and self.aqi_cache["data"] is not None
            ):
                return self.aqi_cache["data"]

        start_time = time.time()
        try:
            resp = self._session.get(
                cfg.AIRNOW_API_URL,
                params={"API_KEY": cfg.AIRNOW_API_KEY},
                headers={"User-Agent": "MeshtasticBot"},
                timeout=cfg.AQI_API_TIMEOUT,
            )
            if resp.status_code != 200:
                logger.warning(f"AirNow HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="airnow")
                return None

            observations = resp.json()
            if not observations:
                logger.warning("AirNow returned empty observations")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="airnow")
                return None

            # Pick the observation with the highest AQI
            worst = max(observations, key=lambda o: o.get("AQI", 0))
            result = {
                "aqi": worst.get("AQI", 0),
                "category_name": worst.get("Category", {}).get("Name", "Unknown"),
                "category_number": worst.get("Category", {}).get("Number", 0),
                "parameter": worst.get("ParameterName", ""),
            }

            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="airnow")
            with self.lock:
                self.aqi_cache["data"] = result
                self.aqi_cache["timestamp"] = datetime.now(timezone.utc)

            return result

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            if isinstance(e, requests.RequestException):
                logger.error(f"AQI data error: {type(e).__name__}")
            else:
                logger.error(f"AQI data error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="airnow")
            return None

    def _check_aqi_alerts(self) -> None:
        """Check if AQI has crossed a threshold and send alert."""
        aqi_data = self._get_aqi_data()
        if not aqi_data:
            return

        category = aqi_data["category_number"]

        with self.lock:
            last_cat = self.last_aqi_alert_category

        # Reset tracking when AQI drops back to Good
        if category <= 1:
            with self.lock:
                self.last_aqi_alert_category = 0
            self._save_alert_state()
            return

        # Only alert when category escalates
        if category <= last_cat:
            return

        msg = f"AIR QUALITY ALERT: AQI {aqi_data['aqi']} - {aqi_data['category_name']} ({aqi_data['parameter']}) [EPA AirNow - preliminary]"
        self._send_message(msg)
        logger.info(f"AQI alert sent: {msg}")

        with self.lock:
            self.last_aqi_alert_category = category
        self._save_alert_state()

    def _check_space_weather(self) -> None:
        """Check planetary K-index and alert on geomagnetic storms (Kp >= 5)."""
        url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"

        start_time = time.time()
        try:
            resp = self._session.get(
                url,
                headers={"User-Agent": "MeshtasticBot"},
                timeout=cfg.API_TIMEOUT,
            )
            if resp.status_code != 200:
                logger.warning(f"NOAA space weather HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-space-weather")
                return

            data = resp.json()
            if not data or len(data) < 2:
                logger.warning("NOAA space weather returned insufficient data")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="noaa-space-weather")
                return

            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="noaa-space-weather")

            with self.lock:
                self.space_weather_cache["data"] = data
                self.space_weather_cache["timestamp"] = datetime.now(timezone.utc)

            # Skip header row; only consider the most recent entry
            # (avoids flooding alerts with historical data on first run)
            g_scale = {5: "G1", 6: "G2", 7: "G3", 8: "G4", 9: "G5"}

            # Only alert on the latest entry if it's new and Kp >= 5
            latest = data[-1]
            latest_tag = latest[0]

            with self.lock:
                already_seen = latest_tag in self.seen_storm_events
                first_run = self.last_space_weather_check is None

            # Mark all existing entries as seen (backfill)
            for entry in data[1:]:
                time_tag = entry[0]
                with self.lock:
                    if time_tag not in self.seen_storm_events:
                        self.seen_storm_events[time_tag] = datetime.now(timezone.utc)
            self._save_alert_state()

            if first_run:
                logger.info(f"Heliophysics baseline captured — {len(data) - 1} K-index observations")
                return

            if already_seen:
                return   # this observation was already alerted

            try:
                kp = float(latest[1])
            except (ValueError, TypeError, IndexError):
                return

            if kp >= 5:
                kp_int = min(int(kp), 9)
                g_level = g_scale.get(kp_int, f"G{kp_int - 4}")
                msg = f"GEOMAGNETIC STORM: Kp={kp:.0f} ({g_level}). Possible aurora visible, GPS/radio disruption."
                self._send_message(msg)
                logger.info(f"Space weather alert sent: Kp={kp}")

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Space weather check error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="noaa-space-weather")

    def _check_earthquakes(self) -> None:
        """Check USGS for recent significant earthquakes nearby."""
        url = (
            f"https://earthquake.usgs.gov/fdsnws/event/1/query"
            f"?format=geojson"
            f"&starttime={(datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')}"
            f"&minmagnitude={cfg.EARTHQUAKE_MIN_MAGNITUDE}"
            f"&latitude={cfg.LAT}&longitude={cfg.LON}"
            f"&maxradiuskm={cfg.EARTHQUAKE_RADIUS_KM}"
        )

        start_time = time.time()
        try:
            resp = self._session.get(
                url,
                headers={"User-Agent": "MeshtasticBot"},
                timeout=cfg.API_TIMEOUT,
            )
            if resp.status_code != 200:
                logger.warning(f"USGS earthquakes HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="usgs-earthquakes")
                return

            data = resp.json()
            features = data.get("features", [])
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="usgs-earthquakes")

            with self.lock:
                self.earthquake_cache["data"] = data
                self.earthquake_cache["timestamp"] = datetime.now(timezone.utc)

            alerts_sent = 0
            for feature in features:
                eq_id = feature.get("id", "")

                with self.lock:
                    if eq_id in self.seen_earthquake_ids:
                        continue

                props = feature.get("properties", {})
                geom = feature.get("geometry", {})
                coords = geom.get("coordinates", [0, 0, 0])

                mag = props.get("mag", 0)
                place = props.get("place", "Unknown location")
                depth = coords[2] if len(coords) > 2 else 0

                eq_time_ms = props.get("time")
                if eq_time_ms:
                    eq_dt = datetime.fromtimestamp(eq_time_ms / 1000, tz=timezone.utc).astimezone(cfg.TIMEZONE)
                    time_str = eq_dt.strftime("%I:%M%p").lstrip("0")
                    msg = f"EARTHQUAKE M{mag:.1f} - {place} @ {time_str} (depth {depth:.0f}km)"
                else:
                    msg = f"EARTHQUAKE M{mag:.1f} - {place} (depth {depth:.0f}km)"
                if len(msg) > cfg.MAX_RESPONSE_LENGTH:
                    msg = msg[:cfg.MAX_RESPONSE_LENGTH]
                self._send_message(msg)
                logger.info(f"Earthquake alert sent: {msg}")

                with self.lock:
                    self.seen_earthquake_ids[eq_id] = datetime.now(timezone.utc)

                alerts_sent += 1
                if alerts_sent >= 3:
                    break

            if alerts_sent:
                self._save_alert_state()

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Earthquake check error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="usgs-earthquakes")

    def _get_moon_phase(self) -> str:
        """Get current moon phase from Tomorrow.io API"""
        if not cfg.TOMORROW_IO_API_KEY:
            return ""

        with self.lock:
            if (self.moon_cache["timestamp"]
                    and (datetime.now(timezone.utc) - self.moon_cache["timestamp"]).total_seconds()
                    < 21600  # 6-hour TTL
                    and self.moon_cache["data"] is not None):
                return self.moon_cache["data"]

        start_time = time.time()
        try:
            url = "https://api.tomorrow.io/v4/weather/realtime"
            params = {"location": f"{cfg.LAT},{cfg.LON}", "apikey": cfg.TOMORROW_IO_API_KEY, "fields": ["moonPhase"]}

            resp = self._session.get(url, params=params, timeout=cfg.API_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"Tomorrow.io moon phase HTTP {resp.status_code}")
                self._record_api_call(success=False, response_time=time.time() - start_time, endpoint="tomorrow-moon")
                return ""

            data = resp.json()["data"]["values"]
            moon_phase_val = data.get("moonPhase", 0)
            self._record_api_call(success=True, response_time=time.time() - start_time, endpoint="tomorrow-moon")

            if moon_phase_val < 0.03 or moon_phase_val > 0.97:
                phase = "New Moon"
            elif 0.03 <= moon_phase_val < 0.22:
                phase = "Waxing Crescent"
            elif 0.22 <= moon_phase_val < 0.28:
                phase = "First Quarter"
            elif 0.28 <= moon_phase_val < 0.47:
                phase = "Waxing Gibbous"
            elif 0.47 <= moon_phase_val < 0.53:
                phase = "Full Moon"
            elif 0.53 <= moon_phase_val < 0.72:
                phase = "Waning Gibbous"
            elif 0.72 <= moon_phase_val < 0.78:
                phase = "Last Quarter"
            else:
                phase = "Waning Crescent"
            with self.lock:
                self.moon_cache = {"data": phase, "timestamp": datetime.now(timezone.utc)}
            return phase

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Moon phase error: {e}")
            self._record_api_call(success=False, response_time=time.time() - start_time, error=e, endpoint="tomorrow-moon")
            return ""

    def _get_daylight_hours(self) -> str:
        """Calculate daylight hours from sunrise/sunset times"""
        try:
            with self.lock:
                sunrise = self.sun_times.get("sunrise")
                sunset = self.sun_times.get("sunset")

            if not sunrise or not sunset:
                return ""

            daylight_duration = sunset - sunrise
            total_seconds = int(daylight_duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60

            if minutes == 0:
                return f"{hours}h daylight"
            else:
                return f"{hours}h {minutes}m daylight"

        except (ValueError, TypeError, AttributeError):
            return ""

    def _cleanup_chat_histories(self) -> None:
        """Daily cleanup routine to remove inactive chat histories"""
        try:
            now = datetime.now(timezone.utc)
            cutoff_time = now - timedelta(days=cfg.CHAT_HISTORY_RETENTION_DAYS)

            with self.lock:
                inactive_users = [sender_id for sender_id, last_activity in self.last_activity.items()
                                  if last_activity < cutoff_time]

                cleanup_count = 0
                for sender_id in inactive_users:
                    if sender_id in self.chat_histories:
                        del self.chat_histories[sender_id]
                        cleanup_count += 1
                    if sender_id in self.last_activity:
                        del self.last_activity[sender_id]

                self.last_cleanup_date = now.date()

            if cleanup_count > 0:
                logger.info(f"Memory bank purge — {cleanup_count} stale sessions cleared")

        except (ValueError, TypeError, AttributeError, KeyError) as e:
            logger.error(f"Chat history cleanup error: {e}")

    def _cleanup_seen_alerts(self) -> None:
        """Cleanup old seen alert IDs to prevent memory leaks"""
        try:
            now = datetime.now(timezone.utc)
            cutoff_time = now - timedelta(days=7)

            with self.lock:
                old_alerts = [alert_id for alert_id, ts in self.seen_alert_ids.items() if ts < cutoff_time]
                for alert_id in old_alerts:
                    del self.seen_alert_ids[alert_id]

            if old_alerts:
                logger.info(f"Alert archive pruned — {len(old_alerts)} expired entries cleared")

            # Clean earthquake IDs
            with self.lock:
                old_eqs = [eid for eid, ts in self.seen_earthquake_ids.items() if ts < cutoff_time]
                for eid in old_eqs:
                    del self.seen_earthquake_ids[eid]

            if old_eqs:
                logger.info(f"Seismic archive pruned — {len(old_eqs)} expired entries cleared")

            # Clean storm events
            with self.lock:
                old_storms = [key for key, ts in self.seen_storm_events.items() if ts < cutoff_time]
                for key in old_storms:
                    del self.seen_storm_events[key]

            if old_storms:
                logger.info(f"Storm archive pruned — {len(old_storms)} expired entries cleared")

            if old_alerts or old_eqs or old_storms:
                self._save_alert_state()

        except (ValueError, TypeError, AttributeError, KeyError) as e:
            logger.error(f"Alert cleanup error: {e}")

    def _check_flood_conditions(self) -> None:
        """Check for flood conditions and send warnings"""
        if not cfg.RIVER_ENABLED:
            return

        try:
            now = datetime.now(cfg.TIMEZONE)

            if now.hour not in cfg.FLOOD_CHECK_HOURS or now.minute > 5:
                return

            level_str = self._get_river_level()
            if level_str == "N/A":
                return

            try:
                level = float(level_str)
            except (ValueError, TypeError):
                return

            current_level = None
            if level >= cfg.FLOOD_STAGES["major"]:
                current_level = "major"
            elif level >= cfg.FLOOD_STAGES["moderate"]:
                current_level = "moderate"
            elif level >= cfg.FLOOD_STAGES["flood"]:
                current_level = "flood"
            elif level >= cfg.FLOOD_STAGES["action"]:
                current_level = "action"

            # Build message and update state with lock, but send outside
            alert_msg = None
            with self.lock:
                if current_level:
                    if self.flood_alert_sent == current_level:   # already warned at this stage
                        return
                    level_names = {
                        "action": "ACTION STAGE", "flood": "FLOOD WARNING",
                        "moderate": "MODERATE FLOOD", "major": "MAJOR FLOOD"
                    }
                    threshold = cfg.FLOOD_STAGES[current_level]
                    alert_msg = f"{cfg.RIVER_NAME}: {level_names[current_level]} - Level at {level:.1f}ft (above {threshold}ft threshold)"
                    self.flood_alert_sent = current_level
                else:
                    self.flood_alert_sent = None   # reset so next flood event fires

            if alert_msg:
                self._send_message(alert_msg)
                self._save_alert_state()
                logger.info(f"Flood alert sent: {current_level} level at {level:.1f}ft")

        except (requests.RequestException, KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(f"Flood check error: {e}")

    def _periodic_tasks(self):
        """Handle all periodic tasks"""
        while self.is_running:
            try:
                now = datetime.now(cfg.TIMEZONE)

                # --- Sun times update ---
                try:
                    if not self.sun_times["last_update"] or self.sun_times["last_update"] != now.date():
                        backoff = min(2 ** self._sun_consecutive_failures, 60)
                        sun_interval = 60 * backoff
                        if (self._last_sun_attempt is None
                                or (datetime.now(timezone.utc) - self._last_sun_attempt).total_seconds() >= sun_interval):
                            self._last_sun_attempt = datetime.now(timezone.utc)
                            self._update_sun_times(now.date())
                            with self.lock:
                                self.alert_sent = {"sunrise": None, "sunset": None}
                            self._save_alert_state()
                except Exception as e:
                    logger.error(f"Sun times update error: {e}")

                # --- Chat/alert cleanup ---
                try:
                    if self.last_cleanup_date is None or self.last_cleanup_date != now.date():
                        self._cleanup_chat_histories()
                        self._cleanup_seen_alerts()
                except Exception as e:
                    logger.error(f"Chat/alert cleanup error: {e}")

                # --- Conditions broadcast (already has own try/except) ---
                if (now.minute == 0 and now.hour % cfg.CONDITIONS_UPDATE_INTERVAL_HOURS == 0
                        and now.hour != self.last_hour_check
                        and (self._last_conditions_sent is None
                             or (datetime.now(timezone.utc) - self._last_conditions_sent).total_seconds() > 120)):
                    try:
                        # Fetch weather, river, and AQI in parallel
                        weather = None
                        river_level = "N/A"
                        aqi_data = None

                        with ThreadPoolExecutor(max_workers=3) as executor:
                            futures = {"weather": executor.submit(self._get_current_conditions_tomorrow)}
                            if cfg.RIVER_ENABLED:
                                futures["river"] = executor.submit(self._get_river_level)
                            if cfg.AQI_ENABLED:
                                futures["aqi"] = executor.submit(self._get_aqi_data)

                            for key, future in futures.items():
                                try:
                                    if key == "weather":
                                        weather = future.result(timeout=cfg.API_TIMEOUT + 5)
                                    elif key == "river":
                                        river_level = future.result(timeout=cfg.API_TIMEOUT + 5)
                                    elif key == "aqi":
                                        aqi_data = future.result(timeout=cfg.API_TIMEOUT + 5)
                                except Exception as e:
                                    logger.error(f"Parallel {key} fetch error: {e}")

                        if weather == "Weather data unavailable" or not weather:
                            logger.warning("Primary weather source offline — switching to NOAA backup")
                            weather = self._get_current_conditions_noaa()

                        timestamp = now.strftime("%b %d %I:%M%p")

                        # Build suffix segments
                        suffixes = []
                        if cfg.RIVER_ENABLED and river_level != "N/A":
                            suffixes.append(f"RIVER: {river_level}ft")

                        if cfg.AQI_ENABLED and aqi_data:
                            suffixes.append(f"AQI {aqi_data['aqi']} ({aqi_data['category_name']})")

                        suffix_str = " | ".join(suffixes)
                        if suffix_str:
                            base_msg = f"{timestamp} CONDITIONS --- {weather} | {suffix_str}"
                            if len(base_msg) > cfg.MAX_RESPONSE_LENGTH:
                                overhead = len(f"{timestamp} CONDITIONS ---  | {suffix_str}")
                                max_weather_len = cfg.MAX_RESPONSE_LENGTH - overhead
                                weather = weather[:max_weather_len].rstrip(", ")
                                base_msg = f"{timestamp} CONDITIONS --- {weather} | {suffix_str}"
                        else:
                            base_msg = f"{timestamp} CONDITIONS --- {weather}"
                            if len(base_msg) > cfg.MAX_RESPONSE_LENGTH:
                                max_weather_len = cfg.MAX_RESPONSE_LENGTH - len(f"{timestamp} CONDITIONS --- ")
                                weather = weather[:max_weather_len].rstrip(", ")
                                base_msg = f"{timestamp} CONDITIONS --- {weather}"

                        self._send_message(base_msg)
                        self.last_hour_check = now.hour
                        self._last_conditions_sent = datetime.now(timezone.utc)
                        self._save_alert_state()
                        logger.info(f"Environmental telemetry broadcast at {timestamp}")
                    except (requests.RequestException, ValueError, TypeError, AttributeError) as e:
                        logger.error(f"Weather fetch failed: {e}")

                # --- NOAA alert check (exponential backoff on consecutive failures) ---
                try:
                    backoff = min(2 ** self._noaa_consecutive_failures, 6)
                    alert_interval = cfg.ALERT_CHECK_INTERVAL_SECONDS * backoff
                    if (self.last_alert_check is None
                            or (datetime.now(timezone.utc) - self.last_alert_check).total_seconds()
                            >= alert_interval):
                        self._check_noaa_alerts()
                        self.last_alert_check = datetime.now(timezone.utc)
                except Exception as e:
                    logger.error(f"NOAA alert check error: {e}")

                # --- AQI alert check (non-blocking) ---
                try:
                    if cfg.AQI_ENABLED and (
                        self.last_aqi_check is None
                        or (datetime.now(timezone.utc) - self.last_aqi_check).total_seconds()
                        >= cfg.AQI_CHECK_INTERVAL_SECONDS
                    ):
                        self.last_aqi_check = datetime.now(timezone.utc)
                        threading.Thread(
                            target=self._check_aqi_alerts, daemon=True
                        ).start()
                except Exception as e:
                    logger.error(f"AQI check error: {e}")

                # --- Space weather check ---
                try:
                    if cfg.SPACE_WEATHER_ENABLED and (
                        self.last_space_weather_check is None
                        or (datetime.now(timezone.utc) - self.last_space_weather_check).total_seconds()
                        >= cfg.SPACE_WEATHER_CHECK_INTERVAL_SECONDS
                    ):
                        self._check_space_weather()
                        self.last_space_weather_check = datetime.now(timezone.utc)
                except Exception as e:
                    logger.error(f"Space weather check error: {e}")

                # --- Earthquake check ---
                try:
                    if cfg.EARTHQUAKE_ENABLED and (
                        self.last_earthquake_check is None
                        or (datetime.now(timezone.utc) - self.last_earthquake_check).total_seconds()
                        >= cfg.EARTHQUAKE_CHECK_INTERVAL_SECONDS
                    ):
                        self._check_earthquakes()
                        self.last_earthquake_check = datetime.now(timezone.utc)
                except Exception as e:
                    logger.error(f"Earthquake check error: {e}")

                # --- Frost + flood checks ---
                try:
                    if now.minute == 0:
                        self._check_frost_conditions()
                        self._check_flood_conditions()
                except Exception as e:
                    logger.error(f"Frost/flood check error: {e}")

                # --- Health check ---
                try:
                    if now.minute == 0 and now.hour % cfg.HEALTH_CHECK_INTERVAL_HOURS == 0:
                        uptime = self._get_uptime()
                        logger.info(f"Systems diagnostic green — uptime: {uptime}")
                except Exception as e:
                    logger.error(f"Health check error: {e}")

                # --- Sunrise/sunset alerts ---
                try:
                    for event in ["sunrise", "sunset"]:
                        event_time = self.sun_times.get(event)

                        should_send_alert = False
                        with self.lock:
                            should_send_alert = (
                                event_time
                                and self.alert_sent[event] != now.date()
                                and event_time - timedelta(hours=1) <= now < event_time
                            )

                        if should_send_alert:
                            forecast = self._fetch_weather_forecast(event == "sunrise")

                            if event == "sunset":
                                moon_phase = self._get_moon_phase()
                                if moon_phase:
                                    suffix = f" | {moon_phase}"
                                    prefix = f"Sunset @ {event_time.strftime('%I:%M%p')} --- "
                                    max_forecast = cfg.MAX_RESPONSE_LENGTH - len(prefix) - len(suffix)
                                    trimmed = forecast[:max_forecast].rstrip()
                                    msg = f"{prefix}{trimmed}{suffix}"
                                else:
                                    msg = f"{event.title()} @ {event_time.strftime('%I:%M%p')} --- {forecast}"
                            else:
                                daylight_hours = self._get_daylight_hours()
                                if daylight_hours:
                                    suffix = f" | {daylight_hours}"
                                    prefix = f"Sunrise @ {event_time.strftime('%I:%M%p')} --- "
                                    max_forecast = cfg.MAX_RESPONSE_LENGTH - len(prefix) - len(suffix)
                                    trimmed = forecast[:max_forecast].rstrip()
                                    msg = f"{prefix}{trimmed}{suffix}"
                                else:
                                    msg = f"{event.title()} @ {event_time.strftime('%I:%M%p')} --- {forecast}"

                            self._send_message(msg)
                            with self.lock:
                                self.alert_sent[event] = now.date()
                            self._save_alert_state()
                except Exception as e:
                    logger.error(f"Sunrise/sunset alert error: {e}")

                time.sleep(60)

            except Exception as e:
                logger.error(f"Periodic task error (unexpected): {e}")
                time.sleep(60)

    def start(self) -> bool:
        """Start the bot (non-blocking)"""
        logger.info("MeshMind awakening...")

        self._validate_config()
        cfg.auto_derive_location_info()

        if not self._init_connections():
            logger.error("Failed to initialize connections")
            return False

        self.is_running = True
        self.start_time = datetime.now(timezone.utc)

        # Pre-warm caches so status panel bars appear immediately on startup
        try:
            if cfg.TOMORROW_IO_API_KEY:
                self._get_current_conditions_tomorrow()
            else:
                self._get_current_conditions_noaa()
        except Exception as e:
            logger.warning(f"Environmental sensor cache warm-up failed: {e}")

        if cfg.RIVER_ENABLED:
            try:
                self._get_river_level()
            except Exception as e:
                logger.warning(f"Hydrological sensor cache warm-up failed: {e}")

        self._periodic_thread = threading.Thread(target=self._periodic_tasks, daemon=True)
        self._periodic_thread.start()

        logger.info("All systems online — MeshMind operational")
        self._notify_status_change()
        return True

    def stop(self) -> None:
        """Stop the bot"""
        logger.info("Initiating shutdown sequence...")
        self.is_running = False

        if self._periodic_thread and self._periodic_thread.is_alive():
            self._periodic_thread.join(timeout=5)

        if self.interface:
            try:
                self._closing_interface = True
                self.interface.close()
            except Exception as e:
                logger.warning(f"Mesh link disconnect error during shutdown: {e}")
            finally:
                self._closing_interface = False

        self._notify_status_change()
        logger.info("All systems dark — MeshMind offline")

    def reconnect(self) -> bool:
        """Attempt to reconnect the bot"""
        logger.info("Re-establishing mesh link...")
        return self._reconnect()
