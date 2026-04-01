"""MESHMON Configuration and Settings Persistence"""

import json
import os
from pathlib import Path
from typing import Any, Optional


def build_services(lat: float, lon: float, noaa_zone: str,
                   river_gauge_id: str = "", aqi_distance_miles: int = 25,
                   earthquake_min_magnitude: float = 4.0,
                   earthquake_radius_km: int = 500) -> list:
    """Build the service list dynamically from location settings"""
    services = [
        {
            "name": "NOAA Weather",
            "url": f"https://api.weather.gov/points/{lat},{lon}",
            "enabled": True,
        },
        {
            "name": "NOAA Alerts",
            "url": f"https://api.weather.gov/alerts/active?zone={noaa_zone}",
            "enabled": bool(noaa_zone),
        },
        {
            "name": "USGS Earthquakes",
            "url": (
                "https://earthquake.usgs.gov/fdsnws/event/1/query"
                f"?format=geojson&starttime={{yesterday}}"
                f"&minmagnitude={earthquake_min_magnitude}"
                f"&latitude={lat}&longitude={lon}"
                f"&maxradiuskm={earthquake_radius_km}"
            ),
            "enabled": True,
            "dynamic_params": True,
        },
        {
            "name": "USGS River Gauge",
            "url": f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={river_gauge_id}&parameterCd=00065&siteStatus=all",
            "enabled": bool(river_gauge_id),
        },
        {
            "name": "Space Weather",
            "url": "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
            "enabled": True,
        },
        {
            "name": "Sunrise-Sunset",
            "url": f"https://api.sunrise-sunset.org/json?lat={lat}&lng={lon}&formatted=0",
            "enabled": True,
        },
        {
            "name": "Tomorrow.io",
            "url": f"https://api.tomorrow.io/v4/weather/realtime?location={lat},{lon}&fields=temperature",
            "enabled": True,
            "requires_key": "TOMORROW_IO_API_KEY",
        },
        {
            "name": "AirNow",
            "url": (
                "https://www.airnowapi.org/aq/observation/latLong/current/"
                f"?format=application/json&latitude={lat}&longitude={lon}"
                f"&distance={aqi_distance_miles}"
            ),
            "enabled": True,
            "requires_key": "AIRNOW_API_KEY",
        },
    ]
    return services


DEFAULT_SETTINGS = {
    "theme": "tokyo-night",
    "check_interval": 60,
    "http_timeout": 10,
    "degraded_threshold": 3.0,
    "lat": 0.0,
    "lon": 0.0,
    "noaa_zone": "",
    "river_gauge_id": "",
    "aqi_distance_miles": 25,
    "earthquake_min_magnitude": 4.0,
    "earthquake_radius_km": 500,
    "mqtt_enabled": True,
    "mqtt_broker": "mqtt.meshtastic.org",
    "mqtt_port": 1883,
    "mqtt_topic": "msh/#",
}


class Settings:
    """Manages meshmon settings persistence to JSON file.

    On first load, if no meshmon settings.json exists, pulls location
    settings from the parent meshmind settings.json automatically.
    """

    def __init__(self, settings_path: Optional[str] = None):
        if settings_path:
            self.settings_path = Path(settings_path)
        else:
            self.settings_path = Path(__file__).parent / "settings.json"

        self._meshmind_settings_path = Path(__file__).parent.parent / "settings.json"
        self._settings = self._load()

    def _load(self) -> dict:
        settings = DEFAULT_SETTINGS.copy()

        # If meshmon settings exist, load them
        if self.settings_path.exists():
            try:
                with open(self.settings_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    settings.update(loaded)
            except (json.JSONDecodeError, IOError):
                pass
        else:
            # First run: pull location from meshmind settings.json
            settings = self._import_from_meshmind(settings)
            # Save so the user has their own meshmon settings file
            self._settings = settings
            self._save()

        return settings

    def _import_from_meshmind(self, settings: dict) -> dict:
        """Import location settings from parent meshmind settings.json"""
        if not self._meshmind_settings_path.exists():
            return settings

        try:
            with open(self._meshmind_settings_path, "r", encoding="utf-8") as f:
                mm = json.load(f)
        except (json.JSONDecodeError, IOError):
            return settings

        # Pull relevant keys from meshmind
        for key in ("lat", "lon", "noaa_zone", "river_gauge_id",
                     "aqi_distance_miles", "earthquake_min_magnitude",
                     "earthquake_radius_km", "theme"):
            if key in mm:
                settings[key] = mm[key]

        return settings

    def _save(self) -> None:
        try:
            with open(self.settings_path, "w", encoding="utf-8") as f:
                json.dump(self._settings, f, indent=2)
        except IOError:
            pass

    def get(self, key: str, default: Any = None) -> Any:
        return self._settings.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._settings[key] = value
        self._save()

    @property
    def theme(self) -> str:
        return self.get("theme", "tokyo-night")

    @theme.setter
    def theme(self, value: str) -> None:
        self.set("theme", value)

    @property
    def check_interval(self) -> int:
        return self.get("check_interval", 60)

    @property
    def http_timeout(self) -> int:
        return self.get("http_timeout", 10)

    @property
    def degraded_threshold(self) -> float:
        return self.get("degraded_threshold", 3.0)

    @property
    def mqtt_enabled(self) -> bool:
        return self.get("mqtt_enabled", True)

    @property
    def mqtt_broker(self) -> str:
        return self.get("mqtt_broker", "mqtt.meshtastic.org")

    @property
    def mqtt_port(self) -> int:
        return self.get("mqtt_port", 1883)

    @property
    def mqtt_topic(self) -> str:
        return self.get("mqtt_topic", "msh/#")

    @property
    def services(self) -> list:
        """Build service list dynamically from location settings"""
        return build_services(
            lat=self.get("lat", 0.0),
            lon=self.get("lon", 0.0),
            noaa_zone=self.get("noaa_zone", ""),
            river_gauge_id=self.get("river_gauge_id", ""),
            aqi_distance_miles=self.get("aqi_distance_miles", 25),
            earthquake_min_magnitude=self.get("earthquake_min_magnitude", 4.0),
            earthquake_radius_km=self.get("earthquake_radius_km", 500),
        )
