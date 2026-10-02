"""Async HTTP client for public, read-only APIs: per-host rate limiting, retries, backoff."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import ssl
import time
from typing import Any

import httpx

from bosona.config import Config

log = logging.getLogger(__name__)

RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


class ApiError(RuntimeError):
    pass


def _ssl_context() -> ssl.SSLContext | bool:
    """Honour a custom CA bundle (corporate / sandbox proxies) given via SSL_CERT_FILE or REQUESTS_CA_BUNDLE."""
    cafile = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    return ssl.create_default_context(cafile=cafile) if cafile else True


class RateLimiter:
    """Evenly spaces request starts: at most `rate` requests per second."""

    def __init__(self, rate: float) -> None:
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if not self.interval:
            return
        async with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            await asyncio.sleep(start - now)


class ApiClient:
    def __init__(self, cfg: Config) -> None:
        http = cfg.http
        self.max_retries = int(http.get("max_retries", 6))
        self.backoff_base = float(http.get("backoff_base_s", 1.0))
        self.backoff_max = float(http.get("backoff_max_s", 60.0))
        self._client = httpx.AsyncClient(
            timeout=float(http.get("timeout_s", 30)),
            headers={"User-Agent": http.get("user_agent", "bosona-tracker"), "Accept": "application/json"},
            follow_redirects=True,
            verify=_ssl_context(),
        )
        self._limiters = {host: RateLimiter(rate) for host, rate in cfg.rate_limits.items()}
        self.requests = 0

    async def __aenter__(self) -> ApiClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    def _backoff(self, attempt: int) -> float:
        return min(self.backoff_max, self.backoff_base * 2**attempt) * (0.5 + random.random() / 2)

    async def get_json(self, url: str, params: dict[str, Any] | list[tuple[str, Any]] | None = None) -> Any:
        limiter = self._limiters.get(httpx.URL(url).host)
        last_error = ""
        for attempt in range(self.max_retries + 1):
            if limiter:
                await limiter.wait()
            self.requests += 1
            try:
                resp = await self._client.get(url, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                delay = self._backoff(attempt)
            else:
                if resp.status_code == 200:
                    return resp.json()
                last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                if resp.status_code not in RETRY_STATUS:
                    raise ApiError(f"GET {resp.request.url} -> {last_error}")
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else self._backoff(attempt)
                except ValueError:
                    delay = self._backoff(attempt)
            if attempt < self.max_retries:
                log.warning("GET %s failed (%s), retry %d/%d in %.1fs", url, last_error, attempt + 1, self.max_retries, delay)
                await asyncio.sleep(delay)
        raise ApiError(f"GET {url} params={params} failed after {self.max_retries + 1} attempts: {last_error}")

    async def rpc(self, url: str, method: str, params: list[Any]) -> Any:
        """JSON-RPC call (public Polygon node) with the same rate limiting and backoff; returns `result`."""
        limiter = self._limiters.get(httpx.URL(url).host)
        last_error = ""
        for attempt in range(self.max_retries + 1):
            if limiter:
                await limiter.wait()
            self.requests += 1
            try:
                resp = await self._client.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
                if resp.status_code == 200:
                    d = resp.json()
                    if "error" not in d:
                        return d.get("result")
                    last_error = f"RPC error {d['error']}"
                else:
                    last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    if resp.status_code not in RETRY_STATUS:
                        raise ApiError(f"{method} -> {last_error}")
            except (httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if attempt < self.max_retries:
                delay = self._backoff(attempt)
                log.warning("%s failed (%s), retry %d/%d in %.1fs", method, last_error, attempt + 1, self.max_retries, delay)
                await asyncio.sleep(delay)
        raise ApiError(f"{method} {params} failed after {self.max_retries + 1} attempts: {last_error}")
