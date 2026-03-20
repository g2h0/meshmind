"""MESHMIND Settings Persistence"""

import json
import os
from pathlib import Path
from typing import Any, Optional


class Settings:
    """Manages user settings persistence to JSON file"""

    DEFAULT_SETTINGS = {
        "theme": "tokyo-night",
        "device_host": "",
        "mesh_channel": 0,
        "bot_name": "MeshBot",
        "location_name": "",
        "lat": 0.0,
        "lon": 0.0,
        "timezone": "",
        "noaa_zone": "",
        "ai_base_url": "https://api.x.ai/v1",
        "ai_model": "grok-3-mini",
        "ai_temperature": 1.0,
        "ai_provider": "cloud",
        "ai_search_enabled": False,
        "bbs_enabled": True,
        "river_enabled": False,
        "river_gauge_id": "",
        "river_name": "",
        "flood_stages": {"action": 0, "flood": 0, "moderate": 0, "major": 0},
        "tts_enabled": False,
        "tts_voice": "af_heart",
        "aqi_enabled": False,
        "aqi_distance_miles": 25,
        "space_weather_enabled": False,
        "earthquake_enabled": False,
        "earthquake_min_magnitude": 4.0,
        "earthquake_radius_km": 500,
        "conditions_update_interval_hours": 3,
        "alert_check_interval_seconds": 600,
        "health_check_interval_hours": 6,
        "chat_timeout_minutes": 30,
        "max_chat_history": 50,
        "frost_temp_threshold": 32,
    }

    def __init__(self, settings_path: Optional[str] = None):
        if settings_path:
            self.settings_path = Path(settings_path)
        else:
            # Default to settings.json in project root
            self.settings_path = Path(__file__).parent.parent.parent / "settings.json"

        self._settings = self._load()

    def _load(self) -> dict:
        """Load settings from JSON file"""
        if self.settings_path.exists():
            try:
                with open(self.settings_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    # Merge with defaults to ensure all keys exist
                    return {**self.DEFAULT_SETTINGS, **loaded}
            except (json.JSONDecodeError, IOError):
                return self.DEFAULT_SETTINGS.copy()
        return self.DEFAULT_SETTINGS.copy()

    def _save(self) -> None:
        """Save settings to JSON file"""
        try:
            with open(self.settings_path, "w", encoding="utf-8") as f:
                json.dump(self._settings, f, indent=2)
        except IOError as e:
            # Log but don't crash
            pass

    def get(self, key: str, default: Any = None) -> Any:
        """Get a setting value"""
        return self._settings.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a setting value and persist"""
        self._settings[key] = value
        self._save()

    @property
    def theme(self) -> str:
        """Get current theme name"""
        return self.get("theme", "tokyo-night")

    @theme.setter
    def theme(self, value: str) -> None:
        """Set theme and persist"""
        self.set("theme", value)

    @property
    def tts_enabled(self) -> bool:
        """Get TTS enabled state"""
        return self.get("tts_enabled", False)

    @tts_enabled.setter
    def tts_enabled(self, value: bool) -> None:
        """Set TTS enabled state and persist"""
        self.set("tts_enabled", value)

    @property
    def tts_voice(self) -> str:
        """Get TTS voice name"""
        return self.get("tts_voice", "af_heart")

    @tts_voice.setter
    def tts_voice(self, value: str) -> None:
        """Set TTS voice and persist"""
        self.set("tts_voice", value)
