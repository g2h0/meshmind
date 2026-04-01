"""MESHMON HTTP Endpoint Monitor"""

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseMonitor, CheckResult, ServiceStatus

logger = logging.getLogger(__name__)


class HTTPMonitor(BaseMonitor):
    """Monitors HTTP endpoints for availability and response time"""

    def __init__(
        self,
        name: str,
        url: str,
        enabled: bool = True,
        timeout: float = 10.0,
        degraded_threshold: float = 3.0,
        requires_key: Optional[str] = None,
        dynamic_params: bool = False,
        check_interval: float = 60.0,
    ):
        super().__init__(
            name=name,
            url=url,
            enabled=enabled,
            check_interval=check_interval,
            timeout=timeout,
        )
        self.degraded_threshold = degraded_threshold
        self.requires_key = requires_key
        self.dynamic_params = dynamic_params

        # Shared session with retry logic
        self._session = requests.Session()
        retry = Retry(total=2, backoff_factor=0.3, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.headers.update({"User-Agent": "MeshMon/0.1"})

    def _prepare_url(self) -> str:
        """Replace dynamic parameters in the URL"""
        url = self.url
        if self.dynamic_params:
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
            url = url.replace("{yesterday}", yesterday)
        if self.requires_key:
            api_key = os.getenv(self.requires_key, "")
            if api_key:
                sep = "&" if "?" in url else "?"
                if self.requires_key == "AIRNOW_API_KEY":
                    url = f"{url}{sep}API_KEY={api_key}"
                elif self.requires_key == "TOMORROW_IO_API_KEY":
                    url = f"{url}{sep}apikey={api_key}"
        return url

    def check(self) -> CheckResult:
        """Perform an HTTP health check"""
        if not self.enabled:
            return CheckResult(status=ServiceStatus.DISABLED)

        # Check if required API key is available
        if self.requires_key and not os.getenv(self.requires_key):
            return CheckResult(
                status=ServiceStatus.DISABLED,
                error=f"Missing env var: {self.requires_key}",
            )

        url = self._prepare_url()

        try:
            start = time.monotonic()
            response = self._session.get(url, timeout=self.timeout)
            elapsed_ms = (time.monotonic() - start) * 1000

            if response.status_code >= 500:
                return CheckResult(
                    status=ServiceStatus.DOWN,
                    response_time_ms=elapsed_ms,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code}",
                )

            if response.status_code >= 400:
                return CheckResult(
                    status=ServiceStatus.DOWN,
                    response_time_ms=elapsed_ms,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code}",
                )

            # Determine if degraded (slow but successful)
            elapsed_sec = elapsed_ms / 1000
            if elapsed_sec > self.degraded_threshold:
                return CheckResult(
                    status=ServiceStatus.DEGRADED,
                    response_time_ms=elapsed_ms,
                    status_code=response.status_code,
                )

            return CheckResult(
                status=ServiceStatus.UP,
                response_time_ms=elapsed_ms,
                status_code=response.status_code,
            )

        except requests.Timeout:
            return CheckResult(
                status=ServiceStatus.DOWN,
                error="Request timed out",
            )
        except requests.ConnectionError as e:
            return CheckResult(
                status=ServiceStatus.DOWN,
                error=f"Connection error: {type(e).__name__}",
            )
        except Exception as e:
            return CheckResult(
                status=ServiceStatus.DOWN,
                error=str(e),
            )
