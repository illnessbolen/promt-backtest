"""Pluggable spot / reference price providers and the in-memory price book.

Every provider turns its feed into `PriceTick`s tagged with `source` and `kind`, so each stored value
says where it came from. Providers are independent tasks: one failing (e.g. the legacy RTDS Chainlink
topic being switched off) never stops the others, and Binance is always on.

Built-in providers (enable / order in config.yaml `live.price_providers`):
  * `binance_ws`             Binance aggTrade stream on data-stream.binance.vision (source "binance", kind "spot");
  * `chainlink_rtds`         Polymarket RTDS legacy topics crypto_prices_chainlink / crypto_prices_twap_sixty
                             (source "chainlink", kinds "spot" / "twap60"), public, no auth;
  * `chainlink_data_streams` Chainlink Data Streams WebSocket (source "chainlink_ds"); switches itself on
                             when CHAINLINK_DS_API_KEY and CHAINLINK_DS_USER_SECRET are set (.env), no code change.
A new provider only needs a subclass of `PriceProvider` decorated with `@register`.
"""

from __future__ import annotations

import abc
import hashlib
import hmac
import json
import logging
import os
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar
from urllib.parse import urlsplit

from bosona.live.wsconn import WsClient, now_ms

log = logging.getLogger(__name__)

BINANCE_SYMBOLS = {"btc": "btcusdt", "eth": "ethusdt", "sol": "solusdt", "xrp": "xrpusdt", "doge": "dogeusdt",
                   "bnb": "bnbusdt", "hype": "hypeusdt", "zec": "zecusdt"}


@dataclass(slots=True)
class PriceTick:
    source: str       # binance | chainlink | chainlink_ds | ...
    kind: str         # spot | twap60 | twap30
    asset: str        # btc | eth | ...
    price: float
    ts_ms: float      # timestamp assigned by the source (trade time / observation time)
    recv_ms: float    # local receive time


Emit = Callable[[PriceTick], None]


class PriceProvider(abc.ABC):
    name: ClassVar[str]

    def __init__(self, settings: dict[str, Any], assets: Sequence[str]) -> None:
        self.settings = settings
        self.assets = list(assets)
        self.ticks = 0
        self.last_tick_ms = 0.0
        self.ws: WsClient | None = None

    @classmethod
    def unavailable_reason(cls, settings: dict[str, Any]) -> str | None:
        """None when the provider can run; otherwise why it is skipped (e.g. missing credentials)."""
        return None

    @abc.abstractmethod
    async def run(self, emit: Emit) -> None:
        """Run forever (reconnecting on errors); cancelled on shutdown."""

    def _emit(self, emit: Emit, tick: PriceTick) -> None:
        self.ticks += 1
        self.last_tick_ms = tick.recv_ms
        if self.ws is not None:
            self.ws.mark_data()
        emit(tick)

    def status(self) -> dict[str, Any]:
        ws = self.ws
        return {
            "connected": bool(ws and ws.connected),
            "connects": ws.connects if ws else 0,
            "ticks": self.ticks,
            "age_s": round((now_ms() - self.last_tick_ms) / 1000, 1) if self.last_tick_ms else None,
            "last_error": ws.last_error if ws else "",
        }


REGISTRY: dict[str, type[PriceProvider]] = {}


def register(cls: type[PriceProvider]) -> type[PriceProvider]:
    REGISTRY[cls.name] = cls
    return cls


def build_providers(names: Iterable[str], settings: dict[str, dict[str, Any]], assets: Sequence[str]) -> list[PriceProvider]:
    out = []
    for name in names:
        cls = REGISTRY.get(name)
        if cls is None:
            raise ValueError(f"unknown price provider {name!r}; known: {sorted(REGISTRY)}")
        conf = settings.get(name) or {}
        reason = cls.unavailable_reason(conf)
        if reason:
            log.info("price provider %s disabled: %s", name, reason)
            continue
        out.append(cls(conf, assets))
    return out


# ---------------------------------------------------------------------------------------------- Binance
@register
class BinanceWs(PriceProvider):
    """Binance spot aggTrade stream: price and trade time (ms) of every aggregated trade."""

    name = "binance_ws"

    async def run(self, emit: Emit) -> None:
        base = self.settings.get("url", "wss://data-stream.binance.vision")
        symbols = {BINANCE_SYMBOLS[a]: a for a in self.assets if a in BINANCE_SYMBOLS}
        url = f"{base}/stream?streams=" + "/".join(f"{s}@aggTrade" for s in symbols)

        def on_message(raw: str, recv: float) -> None:
            msg = json.loads(raw)
            data = msg.get("data") or {}
            asset = symbols.get(str(data.get("s", "")).lower())
            if data.get("e") == "aggTrade" and asset:
                self._emit(emit, PriceTick("binance", "spot", asset, float(data["p"]), float(data["T"]), recv))

        async def on_open(_: WsClient) -> None:
            return None

        self.ws = WsClient("binance_ws", url, idle_timeout_s=float(self.settings.get("idle_timeout_s", 30)))
        await self.ws.run(on_open, on_message)


# -------------------------------------------------------------------------------- Chainlink via RTDS
RTDS_TOPICS = {"crypto_prices_chainlink": "spot", "crypto_prices_twap_sixty": "twap60", "crypto_prices_twap_thirty": "twap30"}


def parse_rtds_price(msg: dict[str, Any], recv: float, assets: set[str]) -> PriceTick | None:
    """RTDS legacy price frame -> tick. `full_accuracy_value` is an E18 integer string for the Chainlink
    and TWAP topics (decimal string for crypto_prices); `value` is the same number as a float."""
    kind = RTDS_TOPICS.get(msg.get("topic", ""))
    payload = msg.get("payload") or {}
    if kind is None or msg.get("type") != "update" or not isinstance(payload, dict):
        return None
    asset = str(payload.get("symbol", "")).split("/")[0].lower()
    if asset not in assets or payload.get("timestamp") is None:
        return None
    full = payload.get("full_accuracy_value")
    if isinstance(full, str) and full.lstrip("-").isdigit():
        price = int(full) / 1e18
    else:
        price = float(payload["value"])
    return PriceTick("chainlink", kind, asset, price, float(payload["timestamp"]), recv)


@register
class ChainlinkRtds(PriceProvider):
    """Chainlink spot and TWAP from Polymarket's public RTDS (legacy, planned removal ~2026-10-23)."""

    name = "chainlink_rtds"

    async def run(self, emit: Emit) -> None:
        url = self.settings.get("url", "wss://ws-live-data.polymarket.com")
        topics = list(self.settings.get("topics", ["crypto_prices_chainlink", "crypto_prices_twap_sixty"]))
        assets = set(self.assets)

        async def on_open(client: WsClient) -> None:
            await client.send({"action": "subscribe", "subscriptions": [{"topic": t, "type": "update"} for t in topics]})

        def on_message(raw: str, recv: float) -> None:
            if not raw or raw[0] != "{":
                return  # PONG / empty keep-alives
            tick = parse_rtds_price(json.loads(raw), recv, assets)
            if tick:
                self._emit(emit, tick)

        # a removed topic may leave a socket that only answers PING: recycle it after a minute without ticks
        self.ws = WsClient("chainlink_rtds", url, heartbeat_s=5, idle_timeout_s=float(self.settings.get("idle_timeout_s", 30)),
                           data_timeout_s=float(self.settings.get("data_timeout_s", 60)))
        await self.ws.run(on_open, on_message)


# ------------------------------------------------------------------------ Chainlink Data Streams (keys)
def ds_auth_headers(api_key: str, user_secret: str, method: str, url: str, body: bytes = b"", ts_ms: int | None = None) -> dict[str, str]:
    """HMAC headers of the Data Streams API (same algorithm as @chainlink/data-streams-sdk utils/auth.js):
    signature = HMAC_SHA256(secret, f"{METHOD} {path?query} {sha256(body)} {api_key} {ts_ms}")."""
    ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    parts = urlsplit(url)
    path = parts.path + (f"?{parts.query}" if parts.query else "")
    base = f"{method} {path} {hashlib.sha256(body).hexdigest()} {api_key} {ts}"
    sig = hmac.new(user_secret.encode(), base.encode(), hashlib.sha256).hexdigest()
    return {"Authorization": api_key, "X-Authorization-Timestamp": str(ts), "X-Authorization-Signature-SHA256": sig}


# word index of the main price inside the report blob, by schema version (first 2 bytes of the feed id);
# words 0-5 are feedId, validFrom, observationsTimestamp, nativeFee, linkFee, expiresAt in every schema
DS_PRICE_WORD = {2: 6, 3: 6, 4: 6, 5: 6, 6: 6, 7: 6, 8: 7, 10: 7, 11: 6, 14: 6}


def _word(buf: bytes, i: int) -> int:
    return int.from_bytes(buf[32 * i : 32 * i + 32], "big")


def _signed(v: int) -> int:
    return v - (1 << 256) if v >> 255 else v


def decode_ds_report(full_report_hex: str) -> dict[str, Any]:
    """ABI-decode a Data Streams `fullReport`: (bytes32[3] ctx, bytes reportBlob, bytes32[] rs, bytes32[] ss, bytes32 vs),
    then read the static head of the report blob. Price fields are int192 with 18 decimals."""
    raw = bytes.fromhex(full_report_hex[2:] if full_report_hex.startswith("0x") else full_report_hex)
    off = _word(raw, 3)  # offset of reportBlob (head: 3 words ctx, then the bytes offset)
    length = _word(raw[off:], 0)
    blob = raw[off + 32 : off + 32 + length]
    feed_id = "0x" + blob[:32].hex()
    version = int(feed_id[2:6], 16)
    word = DS_PRICE_WORD.get(version)
    if word is None:
        raise ValueError(f"unsupported Data Streams report version {version}")
    return {
        "feed_id": feed_id,
        "version": version,
        "valid_from_ts": _word(blob, 1),
        "observations_ts": _word(blob, 2),
        "price": _signed(_word(blob, word)) / 1e18,
    }


def parse_feed_map(spec: str | dict[str, str] | None) -> dict[str, tuple[str, str]]:
    """{"btc:spot": "0x...", "btc:twap60": "0x..."} or "btc:spot=0x...,btc:twap60=0x..." -> {feed_id: (asset, kind)}."""
    if not spec:
        return {}
    items = spec.items() if isinstance(spec, dict) else (p.split("=", 1) for p in spec.split(",") if "=" in p)
    out = {}
    for key, feed in items:
        asset, _, kind = key.strip().partition(":")
        out[feed.strip().lower()] = (asset.lower(), kind or "spot")
    return out


@register
class ChainlinkDataStreams(PriceProvider):
    """Chainlink Data Streams WebSocket. Needs CHAINLINK_DS_API_KEY + CHAINLINK_DS_USER_SECRET and feed ids
    (config `live.providers.chainlink_data_streams.feeds` or env CHAINLINK_DS_FEEDS="btc:spot=0x...,...")."""

    name = "chainlink_data_streams"

    @classmethod
    def unavailable_reason(cls, settings: dict[str, Any]) -> str | None:
        if not (os.environ.get("CHAINLINK_DS_API_KEY") and os.environ.get("CHAINLINK_DS_USER_SECRET")):
            return "CHAINLINK_DS_API_KEY / CHAINLINK_DS_USER_SECRET not set"
        if not cls._feeds(settings):
            return "no feed ids configured"
        return None

    @staticmethod
    def _feeds(settings: dict[str, Any]) -> dict[str, tuple[str, str]]:
        return parse_feed_map(os.environ.get("CHAINLINK_DS_FEEDS") or settings.get("feeds"))

    async def run(self, emit: Emit) -> None:
        feeds = {f: ak for f, ak in self._feeds(self.settings).items() if ak[0] in self.assets}
        base = os.environ.get("CHAINLINK_DS_WS_URL") or self.settings.get("ws_url", "wss://ws.dataengine.chain.link")
        url = f"{base.rstrip('/')}/api/v1/ws?feedIDs={','.join(feeds)}"
        key, secret = os.environ["CHAINLINK_DS_API_KEY"], os.environ["CHAINLINK_DS_USER_SECRET"]

        def on_message(raw: str, recv: float) -> None:
            report = (json.loads(raw) or {}).get("report") or {}
            if not report.get("fullReport"):
                return
            dec = decode_ds_report(report["fullReport"])
            asset, kind = feeds.get(dec["feed_id"], (None, None))
            if asset:
                self._emit(emit, PriceTick("chainlink_ds", kind, asset, dec["price"], dec["observations_ts"] * 1000.0, recv))

        async def on_open(_: WsClient) -> None:
            return None

        self.ws = WsClient("chainlink_ds", url, idle_timeout_s=float(self.settings.get("idle_timeout_s", 30)),
                           headers=lambda: ds_auth_headers(key, secret, "GET", url))
        await self.ws.run(on_open, on_message)


# ---------------------------------------------------------------------------------------- price book
class PriceBook:
    """Latest tick and a short history per (source, kind, asset).

    `at(key, ts_ms)` returns the last tick with source time <= ts_ms (the state of that source at that moment).
    Closed seconds are handed to `on_second` once per key and second (last tick of the second = 1s close),
    which is what gets persisted.
    """

    def __init__(self, keep_s: float = 900.0, on_second: Callable[[PriceTick], None] | None = None) -> None:
        self.keep_ms = keep_s * 1000.0
        self.on_second = on_second
        self.latest: dict[tuple[str, str, str], PriceTick] = {}
        self.history: dict[tuple[str, str, str], deque[PriceTick]] = {}
        self._flushed: dict[tuple[str, str, str], int] = {}   # second already handed over early (flush_second)

    def update(self, tick: PriceTick) -> None:
        key = (tick.source, tick.kind, tick.asset)
        prev = self.latest.get(key)
        if prev is not None and tick.ts_ms < prev.ts_ms:
            return  # out-of-order frame: keep the newest state
        if prev is not None and self.on_second and int(tick.ts_ms // 1000) > int(prev.ts_ms // 1000) \
                and self._flushed.get(key) != int(prev.ts_ms // 1000):
            self.on_second(prev)
        self.latest[key] = tick
        hist = self.history.setdefault(key, deque())
        hist.append(tick)
        horizon = tick.ts_ms - self.keep_ms
        while hist and hist[0].ts_ms < horizon:
            hist.popleft()

    def last(self, source: str, kind: str, asset: str) -> PriceTick | None:
        return self.latest.get((source, kind, asset))

    def at(self, source: str, kind: str, asset: str, ts_ms: float) -> PriceTick | None:
        hist = self.history.get((source, kind, asset))
        if not hist:
            return None
        for tick in reversed(hist):  # targets are recent: a short walk from the right end
            if tick.ts_ms <= ts_ms:
                return tick
        return None

    def flush_second(self, source: str, kind: str, asset: str) -> None:
        """Hand the current second of one key to `on_second` now (it is complete when the source sends one tick
        per second, e.g. Chainlink). The same second is not handed over twice."""
        tick = self.latest.get((source, kind, asset))
        if tick is not None and self.on_second and self._flushed.get((source, kind, asset)) != int(tick.ts_ms // 1000):
            self._flushed[(source, kind, asset)] = int(tick.ts_ms // 1000)
            self.on_second(tick)

    def flush_seconds(self) -> None:
        """Hand the current (unfinished) second of every key to `on_second` (used on shutdown)."""
        if self.on_second:
            for tick in self.latest.values():
                self.on_second(tick)
