"""Order books of the tracked markets from the public CLOB market channel, plus REST /book snapshots.

wss://ws-subscriptions-clob.polymarket.com/ws/market (docs.polymarket.com/market-data/realtime-data):
`book` = full snapshot, `price_change` = new aggregate size of a level (+ best bid/ask), `last_trade_price`
= every match with its transaction_hash and match timestamp (ms). The match time of the wallet's fills
comes from there: on-chain and RTDS see the fill only after the block (~2 s later).
"""

from __future__ import annotations

import json
import logging
from collections import deque
from collections.abc import Iterable
from typing import Any

from bosona.live.wsconn import WsClient

log = logging.getLogger(__name__)

TOB_KEEP_MS = 15 * 60 * 1000
TRADES_KEEP_MS = 15 * 60 * 1000


class TokenBook:
    __slots__ = ("bids", "asks", "server_ts_ms", "recv_ms", "tob")

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.server_ts_ms = 0.0
        self.recv_ms = 0.0
        self.tob: deque[tuple[float, float | None, float | None]] = deque()  # (server ts, best bid, best ask)

    def best(self) -> tuple[float | None, float | None]:
        return (max(self.bids) if self.bids else None, min(self.asks) if self.asks else None)

    def note_tob(self, ts_ms: float, bid: float | None, ask: float | None) -> None:
        if self.tob and self.tob[-1][1] == bid and self.tob[-1][2] == ask:
            return
        self.tob.append((ts_ms, bid, ask))
        while self.tob and self.tob[0][0] < ts_ms - TOB_KEEP_MS:
            self.tob.popleft()

    def tob_before(self, ts_ms: float) -> tuple[float | None, float | None] | None:
        """Top of book in force just before ts_ms (strictly earlier update)."""
        for t, bid, ask in reversed(self.tob):
            if t < ts_ms:
                return bid, ask
        return None


def _levels(raw: Iterable[dict[str, Any]]) -> dict[float, float]:
    out = {}
    for lv in raw or []:
        size = float(lv["size"])
        if size > 0:
            out[float(lv["price"])] = size
    return out


def summarize(bids: dict[float, float], asks: dict[float, float], depth: int) -> dict[str, Any]:
    """Best levels first; sizes in shares."""
    b = sorted(bids.items(), key=lambda kv: -kv[0])[:depth]
    a = sorted(asks.items(), key=lambda kv: kv[0])[:depth]
    return {
        "best_bid": b[0][0] if b else None,
        "best_ask": a[0][0] if a else None,
        "bid_size": b[0][1] if b else None,
        "ask_size": a[0][1] if a else None,
        "bids_json": json.dumps([[p, s] for p, s in b]),
        "asks_json": json.dumps([[p, s] for p, s in a]),
    }


def copy_cost(side: str, size: float, price: float | None, bids: dict[float, float], asks: dict[float, float]) -> dict[str, Any]:
    """What copying `side` x `size` right now would cost: best price, VWAP over the book, depth at his price."""
    if side == "BUY":
        levels = sorted(asks.items())  # cheapest first
        at_px = sum(s for p, s in levels if price is not None and p <= price + 1e-9)
    else:
        levels = sorted(bids.items(), reverse=True)
        at_px = sum(s for p, s in levels if price is not None and p >= price - 1e-9)
    best = levels[0][0] if levels else None
    need, cost = size, 0.0
    for p, s in levels:
        take = min(need, s)
        cost += take * p
        need -= take
        if need <= 1e-9:
            break
    vwap = cost / size if size > 0 and need <= 1e-9 else None
    slip = None
    if vwap is not None and price is not None:
        slip = vwap - price if side == "BUY" else price - vwap
    return {"copy_px": best, "copy_vwap": vwap, "copy_slip": slip, "size_at_px": at_px if price is not None else None}


class BookFeed:
    """Keeps local books for a dynamic set of tokens and a short cache of matches by transaction hash."""

    def __init__(self, url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market") -> None:
        self.ws = WsClient("clob_market", url, heartbeat_s=10, idle_timeout_s=60)
        self.books: dict[str, TokenBook] = {}
        self.wanted: set[str] = set()
        self.trades_by_tx: dict[str, list[dict[str, Any]]] = {}
        self._trade_order: deque[tuple[float, str]] = deque()
        self.events = 0

    # ---------------------------------------------------------------------------------- subscriptions
    async def set_tokens(self, tokens: set[str]) -> None:
        new, old = tokens - self.wanted, self.wanted - tokens
        self.wanted = set(tokens)
        if new:
            await self.ws.send({"assets_ids": sorted(new), "operation": "subscribe", "custom_feature_enabled": True})
        if old:
            await self.ws.send({"assets_ids": sorted(old), "operation": "unsubscribe"})
            for t in old:
                self.books.pop(t, None)

    async def _on_open(self, client: WsClient) -> None:
        self.books.clear()  # fresh snapshots follow the subscription
        if self.wanted:
            await client.send({"assets_ids": sorted(self.wanted), "type": "market", "custom_feature_enabled": True})

    async def run(self) -> None:
        await self.ws.run(self._on_open, self.on_message)

    # --------------------------------------------------------------------------------------- messages
    def on_message(self, raw: str, recv: float) -> None:
        if not raw or raw[0] not in "[{":
            return  # PONG
        msg = json.loads(raw)
        for ev in msg if isinstance(msg, list) else (msg,):
            self.events += 1
            et = ev.get("event_type")
            if et == "price_change":
                ts = float(ev.get("timestamp") or 0)
                for ch in ev.get("price_changes") or ():
                    book = self.books.get(ch.get("asset_id"))
                    if book is None:
                        continue
                    side = book.bids if ch.get("side") == "BUY" else book.asks
                    p, s = float(ch["price"]), float(ch["size"])
                    if s > 0:
                        side[p] = s
                    else:
                        side.pop(p, None)
                    book.server_ts_ms, book.recv_ms = ts, recv
                    if "best_bid" in ch:
                        book.note_tob(ts, _price(ch.get("best_bid")), _price(ch.get("best_ask")))
            elif et == "book":
                asset = ev.get("asset_id")
                if asset not in self.wanted:
                    continue
                book = self.books.setdefault(asset, TokenBook())
                book.bids, book.asks = _levels(ev.get("bids")), _levels(ev.get("asks"))
                book.server_ts_ms, book.recv_ms = float(ev.get("timestamp") or 0), recv
                book.note_tob(book.server_ts_ms, *book.best())
            elif et == "best_bid_ask":
                book = self.books.get(ev.get("asset_id"))
                if book is not None:
                    book.note_tob(float(ev.get("timestamp") or 0), _price(ev.get("best_bid")), _price(ev.get("best_ask")))
            elif et == "last_trade_price":
                tx = (ev.get("transaction_hash") or "").lower()
                if tx:
                    self.trades_by_tx.setdefault(tx, []).append({
                        "asset_id": ev.get("asset_id"), "price": _price(ev.get("price")), "size": _price(ev.get("size")),
                        "side": ev.get("side"), "ts_ms": float(ev.get("timestamp") or 0), "recv_ms": recv})
                    self._trade_order.append((recv, tx))
                    while self._trade_order and self._trade_order[0][0] < recv - TRADES_KEEP_MS:
                        self.trades_by_tx.pop(self._trade_order.popleft()[1], None)

    # -------------------------------------------------------------------------------------- queries
    def match_ms(self, tx_hash: str) -> float | None:
        evs = self.trades_by_tx.get(tx_hash.lower())
        return min(e["ts_ms"] for e in evs) if evs else None

    def snapshot(self, token: str, depth: int) -> dict[str, Any] | None:
        """Local book (None when the token is not subscribed or the feed is down: use REST instead).
        A quiet book can be minutes old and still exact: the channel pushes every change."""
        book = self.books.get(token)
        if book is None or not self.ws.connected:
            return None
        return {"server_ts_ms": book.server_ts_ms, **summarize(book.bids, book.asks, depth)}

    def levels(self, token: str) -> tuple[dict[float, float], dict[float, float]] | None:
        book = self.books.get(token)
        return None if book is None else (dict(book.bids), dict(book.asks))

    def tob_before(self, token: str, ts_ms: float) -> tuple[float | None, float | None] | None:
        book = self.books.get(token)
        return None if book is None else book.tob_before(ts_ms)

    def status(self) -> dict[str, Any]:
        return {"connected": self.ws.connected, "connects": self.ws.connects, "tokens": len(self.wanted),
                "books": len(self.books), "events": self.events, "last_error": self.ws.last_error}


def _price(v: Any) -> float | None:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def parse_rest_book(resp: dict[str, Any]) -> tuple[dict[float, float], dict[float, float], float]:
    """GET {clob}/book?token_id= -> (bids, asks, server timestamp ms)."""
    return _levels(resp.get("bids")), _levels(resp.get("asks")), float(resp.get("timestamp") or 0)
