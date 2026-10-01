"""Reconnecting WebSocket client shared by price providers, detectors and the order-book feed."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import ssl
import time
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

log = logging.getLogger(__name__)


def now_ms() -> float:
    return time.time() * 1000.0


_BACKGROUND: set[asyncio.Task[Any]] = set()


def spawn(coro: Any) -> asyncio.Task[Any]:
    """create_task that keeps a reference until the task is done (the loop only holds weak references)."""
    task = asyncio.create_task(coro)
    _BACKGROUND.add(task)
    task.add_done_callback(_BACKGROUND.discard)
    return task


def ssl_context() -> ssl.SSLContext:
    """Same CA handling as the HTTP client (SSL_CERT_FILE / REQUESTS_CA_BUNDLE for intercepting proxies)."""
    cafile = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
    return ssl.create_default_context(cafile=cafile) if cafile else ssl.create_default_context()


class Backoff:
    def __init__(self, base: float = 1.0, cap: float = 60.0) -> None:
        self.base, self.cap, self.attempt = base, cap, 0

    def reset(self) -> None:
        self.attempt = 0

    def next(self) -> float:
        delay = min(self.cap, self.base * 2**self.attempt) * (0.5 + random.random() / 2)
        self.attempt += 1
        return delay


class WsClient:
    """Keeps one WebSocket open forever: reconnects with jittered backoff, sends an application-level
    heartbeat when the protocol needs one, and drops connections that go silent (`idle_timeout_s`).

    `on_open(client)` (re)subscribes after every connect; `on_message(text, recv_ms)` handles frames.
    """

    def __init__(
        self,
        name: str,
        url: str | Callable[[], str],
        *,
        heartbeat_s: float | None = None,
        heartbeat_msg: str = "PING",
        idle_timeout_s: float = 60.0,
        headers: Callable[[], dict[str, str]] | None = None,
        max_size: int = 2**24,
        max_queue: int = 4096,
    ) -> None:
        self.name = name
        self._url = url
        self.heartbeat_s = heartbeat_s
        self.heartbeat_msg = heartbeat_msg
        self.idle_timeout_s = idle_timeout_s
        self._headers = headers
        self.max_size = max_size
        self.max_queue = max_queue  # frames buffered while the handler is busy (a full buffer makes the CLOB close 1013)
        self.ws: ClientConnection | None = None
        self.connected = False
        self.connects = 0
        self.messages = 0
        self.last_msg_ms = 0.0
        self.last_error = ""

    @property
    def url(self) -> str:
        return self._url() if callable(self._url) else self._url

    async def send(self, obj: Any) -> bool:
        """Send now if connected; otherwise the next on_open resubscribes from state."""
        ws = self.ws
        if ws is None or not self.connected:
            return False
        try:
            await ws.send(obj if isinstance(obj, str) else json.dumps(obj))
            return True
        except Exception as exc:  # noqa: BLE001 - a broken socket is handled by the reader loop
            log.debug("%s: send failed: %s", self.name, exc)
            return False

    async def run(
        self,
        on_open: Callable[[WsClient], Awaitable[None]],
        on_message: Callable[[str, float], None],
    ) -> None:
        backoff = Backoff()
        while True:
            try:
                async with connect(
                    self.url,
                    ssl=ssl_context(),
                    additional_headers=self._headers() if self._headers else None,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=3,
                    max_size=self.max_size,
                    max_queue=self.max_queue,
                ) as ws:
                    self.ws, self.connected = ws, True
                    self.connects += 1
                    log.info("%s: connected (#%d)", self.name, self.connects)
                    await on_open(self)
                    beat = asyncio.create_task(self._heartbeat(ws)) if self.heartbeat_s else None
                    try:
                        await self._read(ws, on_message, backoff)
                    finally:
                        if beat:
                            beat.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - every failure means "reconnect"
                self.last_error = f"{type(exc).__name__}: {exc}"
            finally:
                self.ws, self.connected = None, False
            delay = backoff.next()
            log.warning("%s: disconnected (%s), reconnect in %.1fs", self.name, self.last_error or "closed", delay)
            await asyncio.sleep(delay)

    async def _read(self, ws: ClientConnection, on_message: Callable[[str, float], None], backoff: Backoff) -> None:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
            except asyncio.TimeoutError:
                raise ConnectionError(f"no message for {self.idle_timeout_s:.0f}s") from None
            recv = now_ms()
            self.messages += 1
            self.last_msg_ms = recv
            if self.messages % 1000 == 0:
                backoff.reset()  # a long healthy session earns a fast reconnect next time
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "replace")
            try:
                on_message(raw, recv)
            except Exception:  # noqa: BLE001 - a bad frame must not kill the feed
                log.exception("%s: handler failed on %r", self.name, raw[:300])

    async def _heartbeat(self, ws: ClientConnection) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_s or 10)
            try:
                await ws.send(self.heartbeat_msg)
            except Exception:  # noqa: BLE001
                return
