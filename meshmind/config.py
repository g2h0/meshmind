"""MESHMIND Configuration"""

import os
import logging
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

from .utils.settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Centralized configuration — loads user values from settings.json"""

    # --- User-configured (loaded from settings.json in __post_init__) ---
    DEVICE_HOST: str = ""
    BOT_NAME: str = "MeshBot"
    LOCATION_NAME: str = ""
    LAT: float = 0.0
    LON: float = 0.0
    TIMEZONE: Any = None
    NOAA_ZONE: str = ""

    # AI provider (any OpenAI-compatible API)
    AI_BASE_URL: str = "https://api.x.ai/v1"
    AI_API_KEY: str = ""
    MODEL: str = "grok-3-mini"
    AI_TEMPERATURE: float = 1.0
    AI_PROVIDER: str = "cloud"   # "cloud" | "ollama" | "lmstudio"
    AI_SEARCH_ENABLED: bool = False
    BBS_ENABLED: bool = True

    # Weather API key
    TOMORROW_IO_API_KEY: str = ""

    # Meshtastic channel
    MESH_CHANNEL: int = 0

    # River monitoring (optional)
    RIVER_ENABLED: bool = False
    RIVER_GAUGE_ID: str = ""
    RIVER_NAME: str = ""
    FLOOD_STAGES: dict = field(default_factory=dict)

    # Air Quality (optional)
    AQI_ENABLED: bool = False
    AQI_DISTANCE_MILES: int = 25

    # Space Weather (optional, no API key needed)
    SPACE_WEATHER_ENABLED: bool = False

    # Earthquakes (optional, no API key needed)
    EARTHQUAKE_ENABLED: bool = False
    EARTHQUAKE_MIN_MAGNITUDE: float = 4.0
    EARTHQUAKE_RADIUS_KM: int = 500

    # --- Derived (computed from user values) ---
    RIVER_API_URL: str = ""
    AIRNOW_API_URL: str = ""
    NOAA_ALERTS_URL: str = ""
    WEATHER_POINTS_URL: str = ""

    # --- Static defaults (operational, not user-configurable) ---
    MAX_RESPONSE_LENGTH = 230
    MAX_BYTE_LIMIT = 233
    MAX_MESSAGE_PARTS = 2
    MULTIPART_DELAY = 3
    MAX_RETRIES = 5
    RETRY_DELAY = 10
    API_TIMEOUT = 30
    AQI_API_TIMEOUT = 10
    CHAT_TIMEOUT_MINUTES = 30
    MAX_CHAT_HISTORY = 50
    CHAT_HISTORY_RETENTION_DAYS = 7

    # Frost Warning Settings
    FROST_TEMP_THRESHOLD = 32
    FROST_CHECK_HOURS = [20, 21, 22, 23, 0, 1, 2, 3, 4, 5]
    FROST_FORECAST_HOURS = 24
    FROST_SEASON_MONTHS = [3, 4, 5, 9, 10, 11]

    # Cache TTL Settings (in seconds)
    WEATHER_CACHE_TTL = 7200
    ALERTS_CACHE_TTL = 600
    HOURLY_FORECAST_CACHE_TTL = 3600
    RIVER_CACHE_TTL = 1800
    AQI_CACHE_TTL = 14400                       # 4 hours
    AQI_CHECK_INTERVAL_SECONDS = 14400          # 4 hours
    SPACE_WEATHER_CACHE_TTL = 3600              # 1 hour
    SPACE_WEATHER_CHECK_INTERVAL_SECONDS = 3600  # 1 hour
    EARTHQUAKE_CACHE_TTL = 1800                  # 30 min
    EARTHQUAKE_CHECK_INTERVAL_SECONDS = 1800     # 30 min

    # Periodic Task Intervals
    CONDITIONS_UPDATE_INTERVAL_HOURS = 3
    ALERT_CHECK_INTERVAL_SECONDS = 600
    HEALTH_CHECK_INTERVAL_HOURS = 6
    FLOOD_CHECK_HOURS = [8, 9, 10]

    # URLs
    SUN_API_URL = "https://api.sunrise-sunset.org/json"

    # Dynamic year for system prompt
    CURRENT_YEAR = datetime.now().year

    # System prompt file path
    SYSTEM_PROMPT_FILE = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "system_prompt.txt"
    )

    # System Prompt - will be loaded after logger is available
    SYSTEM_PROMPT = None

    def __post_init__(self):
        settings = Settings()

        # Load user values from settings.json
        self.DEVICE_HOST = settings.get("device_host", "")
        self.BOT_NAME = settings.get("bot_name", "MeshBot")
        self.LOCATION_NAME = settings.get("location_name", "")
        self.LAT = float(settings.get("lat", 0.0))
        self.LON = float(settings.get("lon", 0.0))

        tz_str = settings.get("timezone", "")
        self.TIMEZONE = ZoneInfo(tz_str) if tz_str else ZoneInfo("UTC")

        self.NOAA_ZONE = settings.get("noaa_zone", "")

        # AI provider
        self.AI_BASE_URL = settings.get("ai_base_url", "https://api.x.ai/v1")
        self.MODEL = settings.get("ai_model", "grok-3-mini")
        self.AI_TEMPERATURE = float(settings.get("ai_temperature", 1.0))
        self.AI_PROVIDER = settings.get("ai_provider", "cloud")
        self.AI_SEARCH_ENABLED = settings.get("ai_search_enabled", False)
        self.BBS_ENABLED = settings.get("bbs_enabled", True)
        self.AI_API_KEY = os.getenv("AI_API_KEY", "")
        self.TOMORROW_IO_API_KEY = os.getenv("TOMORROW_IO_API_KEY", "")

        # Meshtastic channel
        self.MESH_CHANNEL = int(settings.get("mesh_channel", 0))

        # River (optional)
        self.RIVER_ENABLED = settings.get("river_enabled", False)
        self.RIVER_GAUGE_ID = settings.get("river_gauge_id", "")
        self.RIVER_NAME = settings.get("river_name", "")
        self.FLOOD_STAGES = settings.get(
            "flood_stages", {"action": 0, "flood": 0, "moderate": 0, "major": 0}
        )

        # AQI (optional)
        self.AQI_ENABLED = settings.get("aqi_enabled", False)
        self.AIRNOW_API_KEY = os.getenv("AIRNOW_API_KEY", "")
        self.AQI_DISTANCE_MILES = int(settings.get("aqi_distance_miles", 25))

        # Space Weather (optional)
        self.SPACE_WEATHER_ENABLED = settings.get("space_weather_enabled", False)

        # Earthquakes (optional)
        self.EARTHQUAKE_ENABLED = settings.get("earthquake_enabled", False)
        self.EARTHQUAKE_MIN_MAGNITUDE = float(settings.get("earthquake_min_magnitude", 4.0))
        self.EARTHQUAKE_RADIUS_KM = int(settings.get("earthquake_radius_km", 500))

        # Broadcast & check intervals (optional tuning)
        self.CONDITIONS_UPDATE_INTERVAL_HOURS = int(settings.get("conditions_update_interval_hours", 3))
        self.ALERT_CHECK_INTERVAL_SECONDS = int(settings.get("alert_check_interval_seconds", 600))
        self.HEALTH_CHECK_INTERVAL_HOURS = int(settings.get("health_check_interval_hours", 6))

        # Chat behavior
        self.CHAT_TIMEOUT_MINUTES = int(settings.get("chat_timeout_minutes", 30))
        self.MAX_CHAT_HISTORY = int(settings.get("max_chat_history", 50))

        # Frost warning threshold
        self.FROST_TEMP_THRESHOLD = int(settings.get("frost_temp_threshold", 32))

        # Build derived URLs
        self._build_urls()

    @property
    def ai_provider_display_name(self) -> str:
        """Human-readable provider name for TUI display."""
        if self.AI_PROVIDER == "ollama":
            return "Ollama"
        if self.AI_PROVIDER == "lmstudio":
            return "LM Studio"
        # Cloud: derive from hostname
        try:
            from urllib.parse import urlparse
            host = urlparse(self.AI_BASE_URL).hostname or ""
            known = {
                "x.ai": "xAI",
                "openai.com": "OpenAI",
                "groq.com": "Groq",
                "openrouter.ai": "OpenRouter",
                "anthropic.com": "Anthropic",
                "together.xyz": "Together",
                "mistral.ai": "Mistral",
            }
            for domain, name in known.items():
                if domain in host:
                    return name
            # fallback: second-to-last part of hostname, capitalized
            parts = host.split(".")
            return parts[-2].capitalize() if len(parts) >= 2 else host
        except Exception:
            return "Cloud"

    def _build_urls(self):
        """Build derived URLs from user-configured values"""
        if self.RIVER_GAUGE_ID:
            self.RIVER_API_URL = (
                f"https://waterservices.usgs.gov/nwis/iv/?format=json"
                f"&sites={self.RIVER_GAUGE_ID}"
                f"&parameterCd=00065&siteStatus=all"
            )
        if self.AQI_ENABLED and self.AIRNOW_API_KEY and self.LAT and self.LON:
            self.AIRNOW_API_URL = (
                f"https://www.airnowapi.org/aq/observation/latLong/current/"
                f"?format=application/json"
                f"&latitude={self.LAT}&longitude={self.LON}"
                f"&distance={self.AQI_DISTANCE_MILES}"
            )
        self.NOAA_ALERTS_URL = (
            f"https://api.weather.gov/alerts/active?zone={self.NOAA_ZONE}"
            if self.NOAA_ZONE
            else ""
        )
        self.WEATHER_POINTS_URL = (
            f"https://api.weather.gov/points/{self.LAT},{self.LON}"
        )

    def auto_derive_location_info(self):
        """Call NOAA points API to get zone and timezone from lat/lon.
        Called during bot startup if zone/timezone are empty."""
        if (not self.NOAA_ZONE or self.TIMEZONE == ZoneInfo("UTC")) and self.LAT and self.LON:
            import requests

            try:
                resp = requests.get(
                    f"https://api.weather.gov/points/{self.LAT},{self.LON}",
                    headers={"User-Agent": "MeshMind Bot"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    props = resp.json()["properties"]
                    if not self.NOAA_ZONE:
                        zone_url = props.get("forecastZone", "")
                        self.NOAA_ZONE = (
                            zone_url.rsplit("/", 1)[-1] if zone_url else ""
                        )
                    tz_str = props.get("timeZone", "")
                    if tz_str and self.TIMEZONE == ZoneInfo("UTC"):
                        self.TIMEZONE = ZoneInfo(tz_str)
                    self._build_urls()
                    logger.info(
                        f"Auto-derived: zone={self.NOAA_ZONE}, "
                        f"timezone={self.TIMEZONE}"
                    )
            except Exception as e:
                logger.warning(f"Could not auto-derive location info: {e}")

    def reload(self):
        """Reload config from settings.json"""
        self.__post_init__()

    @classmethod
    def load_system_prompt(cls) -> str:
        """Load system prompt from external file with error handling"""
        try:
            with open(cls.SYSTEM_PROMPT_FILE, "r", encoding="utf-8") as f:
                prompt_template = f.read().strip()

            commands = "river, " if cfg.RIVER_ENABLED else ""
            if cfg.AQI_ENABLED:
                commands += "aqi, "
            if cfg.BBS_ENABLED:
                commands += "bbspost, bbsread, "

            broadcasts = "flood warnings, " if cfg.RIVER_ENABLED else ""
            if cfg.AQI_ENABLED:
                broadcasts += "air quality alerts, "
            if cfg.EARTHQUAKE_ENABLED:
                broadcasts += "earthquake alerts, "
            if cfg.SPACE_WEATHER_ENABLED:
                broadcasts += "geomagnetic storm alerts, "

            system_prompt = prompt_template.format(
                BOT_NAME=cfg.BOT_NAME,
                LOCATION_NAME=cfg.LOCATION_NAME or "your area",
                CURRENT_YEAR=cls.CURRENT_YEAR,
                COMMANDS=commands,
                RIVER_BROADCASTS=broadcasts,
            )
            return system_prompt

        except FileNotFoundError:
            logger.error(f"System prompt file not found: {cls.SYSTEM_PROMPT_FILE}")
            return (
                f"You are {cfg.BOT_NAME}, an AI assistant on a Meshtastic mesh "
                f"network in {cfg.LOCATION_NAME or 'your area'}. Current year: "
                f"{cls.CURRENT_YEAR}. Keep responses under 200 characters."
            )

        except Exception as e:
            logger.error(f"Error loading system prompt: {e}")
            return (
                f"You are {cfg.BOT_NAME}, an AI assistant on a Meshtastic mesh "
                f"network in {cfg.LOCATION_NAME or 'your area'}. Current year: "
                f"{cls.CURRENT_YEAR}. Keep responses under 200 characters."
            )


cfg = Config()
