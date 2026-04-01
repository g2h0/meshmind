"""MESHMON Base Monitor Classes"""

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ServiceStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    DEGRADED = "DEGRADED"
    UNKNOWN = "UNKNOWN"
    DISABLED = "DISABLED"


@dataclass
class CheckResult:
    status: ServiceStatus
    response_time_ms: float = 0.0
    status_code: Optional[int] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class BaseMonitor(ABC):
    """Abstract base class for service monitors"""

    def __init__(
        self,
        name: str,
        url: str = "",
        enabled: bool = True,
        check_interval: float = 60.0,
        timeout: float = 10.0,
    ):
        self.name = name
        self.url = url
        self.enabled = enabled
        self.check_interval = check_interval
        self.timeout = timeout

        self.status = ServiceStatus.UNKNOWN
        self.last_check: Optional[datetime] = None
        self.last_response_time: Optional[float] = None
        self.last_error: Optional[str] = None
        self.last_status_code: Optional[int] = None
        self.consecutive_failures: int = 0

        self.response_times: deque = deque(maxlen=60)
        self.uptime_history: deque = deque(maxlen=1000)

    @abstractmethod
    def check(self) -> CheckResult:
        ...

    def record_result(self, result: CheckResult) -> None:
        """Record a check result and update state"""
        self.status = result.status
        self.last_check = result.timestamp
        self.last_error = result.error
        self.last_status_code = result.status_code

        if result.response_time_ms > 0:
            self.last_response_time = result.response_time_ms
            self.response_times.append(result.response_time_ms)

        is_up = result.status in (ServiceStatus.UP, ServiceStatus.DEGRADED)
        self.uptime_history.append(is_up)

        if is_up:
            self.consecutive_failures = 0
        else:
            self.consecutive_failures += 1

    @property
    def uptime_percent(self) -> float:
        if not self.uptime_history:
            return 0.0
        return (sum(self.uptime_history) / len(self.uptime_history)) * 100.0

    @property
    def avg_response_time(self) -> float:
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)

    @property
    def p95_response_time(self) -> float:
        if not self.response_times:
            return 0.0
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[min(idx, len(sorted_times) - 1)]

    def get_status_dict(self) -> dict:
        return {
            "name": self.name,
            "url": self.url,
            "enabled": self.enabled,
            "status": self.status.value,
            "response_time_ms": self.last_response_time,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "uptime_percent": round(self.uptime_percent, 1),
            "error": self.last_error,
            "status_code": self.last_status_code,
            "consecutive_failures": self.consecutive_failures,
            "response_times": list(self.response_times),
            "avg_response_time": round(self.avg_response_time, 1),
            "p95_response_time": round(self.p95_response_time, 1),
            "check_count": len(self.uptime_history),
        }
