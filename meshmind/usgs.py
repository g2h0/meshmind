"""Helpers for the modern USGS Water Data API."""

from datetime import datetime, timezone
from typing import Optional


def parse_latest_gage_height(data: dict) -> Optional[float]:
    """Extract the newest gage-height reading in feet from USGS GeoJSON."""
    if not isinstance(data, dict):
        return None

    features = data.get("features")
    if not isinstance(features, list):
        return None

    candidates = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        if str(properties.get("parameter_code", "")) != "00065":
            continue
        if str(properties.get("unit_of_measure", "")).lower() not in {
            "ft", "foot", "feet"
        }:
            continue

        try:
            level = float(properties["value"])
        except (KeyError, ValueError, TypeError):
            continue

        observed_at = datetime.min.replace(tzinfo=timezone.utc)
        timestamp = properties.get("time")
        if isinstance(timestamp, str):
            try:
                observed_at = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )
                if observed_at.tzinfo is None:
                    observed_at = observed_at.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        candidates.append((observed_at, level))

    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]
