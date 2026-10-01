"""Channels that report the wallet's new fills. All three run at once; the tracker keeps the first sighting
and records every channel's own latency (that comparison is part of the stage 3 answer).

  * rtds_activity  public RTDS `activity/trades` firehose (every Polymarket fill with proxyWallet,
                   maker fills included), filtered by wallet. Push, ~2 s after the match.
  * chain_logs     OrderFilled logs of the CTF exchanges with maker topic = wallet, via eth_subscribe on a
                   public Polygon node; gives exact amounts, role, fee and log index. Push, ~2 s after the match.
                   After a reconnect the gap is back-filled with eth_getLogs.
  * data_api       /v2/activity polling: the canonical rows of the stage 1 history, but ~10 s behind.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

import httpx

from bosona.http import ApiClient, _ssl_context
from bosona.live.wsconn import Backoff, WsClient, now_ms, spawn

log = logging.getLogger(__name__)

ORDER_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
# CLOB V2 exchanges: CTF Exchange and Neg Risk CTF Exchange (docs.polymarket.com, contract addresses)
EXCHANGES = ("0xe111180000d2663c0091e4f400237545b87b996b", "0xe2222d279d744050d28e00520010520000310f59")


@dataclass
class FillEvent:
    channel: str
    recv_ms: float
    tx_hash: str
    token_id: str
    side: str
    size: float
    seq: int = 0                       # occurrence of identical (tx, token, side, size) within the channel
    price: float | None = None
    usdc: float | None = None
    src_ts_ms: float | None = None     # timestamp attached by the channel
    block_ts: int | None = None
    block_number: int | None = None
    log_index: int | None = None
    role: str | None = None
    fee_usdc: float | None = None
    order_hash: str | None = None
    condition_id: str | None = None
    slug: str | None = None
    outcome: str | None = None
    update_only: bool = False          # late field update (e.g. block timestamp) of an earlier sighting
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def base_key(self) -> str:
        return f"{self.tx_hash.lower()}:{self.token_id}:{self.side}:{round(self.size * 1e6)}"

    @property
    def fill_key(self) -> str:
        return f"{self.base_key}:{self.seq}"

    def as_json(self) -> str:
        d = {k: v for k, v in asdict(self).items() if v is not None and k not in ("raw", "update_only")}
        d["raw"] = self.raw
        return json.dumps(d, separators=(",", ":"), default=str)


Emit = Callable[[FillEvent], None]


class Occurrences:
    """Assigns `seq` to identical fills inside one channel (bounded memory)."""

    def __init__(self, keep: int = 50_000) -> None:
        self.keep = keep
        self._seen: OrderedDict[str, int] = OrderedDict()

    def next(self, base_key: str) -> int:
        n = self._seen.get(base_key, 0)
        self._seen[base_key] = n + 1
        self._seen.move_to_end(base_key)
        while len(self._seen) > self.keep:
            self._seen.popitem(last=False)
        return n


# --------------------------------------------------------------------------------------- RTDS activity
def parse_rtds_trade(msg: dict[str, Any], user: str, recv: float) -> FillEvent | None:
    if msg.get("topic") != "activity" or msg.get("type") != "trades":
        return None
    p = msg.get("payload") or {}
    if str(p.get("proxyWallet", "")).lower() != user:
        return None
    size = float(p["size"])
    price = float(p["price"]) if p.get("price") is not None else None
    return FillEvent(
        channel="rtds_activity", recv_ms=recv, tx_hash=str(p["transactionHash"]).lower(), token_id=str(p["asset"]),
        side=str(p["side"]), size=size, price=price, usdc=round(size * price, 6) if price is not None else None,
        src_ts_ms=float(p["timestamp"]) * 1000 if p.get("timestamp") is not None else None,
        fee_usdc=float(p["fee"]) if p.get("fee") is not None else None,
        condition_id=str(p.get("conditionId") or "").lower() or None, slug=p.get("slug"), outcome=p.get("outcome"),
        raw={"envelope_ts": msg.get("timestamp"), **{k: p.get(k) for k in ("timestamp", "price", "size", "fee", "eventSlug", "outcomeIndex")}},
    )


class RtdsActivityDetector:
    name = "rtds_activity"

    def __init__(self, user: str, url: str = "wss://ws-live-data.polymarket.com") -> None:
        self.user = user.lower()
        self.ws = WsClient(self.name, url, heartbeat_s=5, idle_timeout_s=30, data_timeout_s=60)
        self.occ = Occurrences()
        self.messages = 0
        self.fills = 0

    async def run(self, emit: Emit) -> None:
        async def on_open(client: WsClient) -> None:
            await client.send({"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades"}]})

        def on_message(raw: str, recv: float) -> None:
            if not raw or raw[0] != "{":
                return
            self.messages += 1
            msg = json.loads(raw)
            if msg.get("topic") == "activity":
                self.ws.mark_data()  # the firehose carries ~40 trades/s: a minute of silence means a dead subscription
            ev = parse_rtds_trade(msg, self.user, recv)
            if ev:
                ev.seq = self.occ.next(ev.base_key)
                self.fills += 1
                emit(ev)

        await self.ws.run(on_open, on_message)

    def status(self) -> dict[str, Any]:
        return {"connected": self.ws.connected, "connects": self.ws.connects, "messages": self.messages,
                "fills": self.fills, "last_error": self.ws.last_error}


# ------------------------------------------------------------------------------------------ chain logs
def _words(data: str) -> list[int]:
    h = data[2:]
    return [int(h[i : i + 64], 16) for i in range(0, len(h), 64)]


def parse_order_filled(lg: dict[str, Any], recv: float) -> FillEvent:
    """OrderFilled(orderHash, maker, taker, side, tokenId, makerAmountFilled, takerAmountFilled, fee, builder, metadata)
    of an order owned by the wallet (topic2). BUY: maker amount = USDC paid, taker amount = shares; SELL reversed.
    taker topic == exchange address <=> the wallet's order was the taker order of the match."""
    w = _words(lg["data"])
    side = "BUY" if w[0] == 0 else "SELL"
    block_ts = int(lg["blockTimestamp"], 16) if lg.get("blockTimestamp") else None  # publicnode (Bor) includes it
    maker_amt, taker_amt = w[2], w[3]
    shares, usdc = (taker_amt, maker_amt) if side == "BUY" else (maker_amt, taker_amt)
    taker_topic = "0x" + lg["topics"][3][-40:]
    return FillEvent(
        channel="chain_logs", recv_ms=recv, tx_hash=lg["transactionHash"].lower(), token_id=str(w[1]), side=side,
        size=shares / 1e6, usdc=usdc / 1e6, price=usdc / shares if shares else None,
        block_number=int(lg["blockNumber"], 16), log_index=int(lg["logIndex"], 16), block_ts=block_ts,
        src_ts_ms=block_ts * 1000.0 if block_ts else None,
        role="taker" if taker_topic.lower() in EXCHANGES else "maker", fee_usdc=w[4] / 1e6, order_hash=lg["topics"][1],
        raw={"address": lg.get("address"), "removed": lg.get("removed", False)},
    )


class ChainLogsDetector:
    name = "chain_logs"

    def __init__(self, user: str, ws_url: str, http_url: str, max_backfill_blocks: int = 2_000) -> None:
        self.user = user.lower()
        self.ws = WsClient(self.name, ws_url, idle_timeout_s=30)
        self.http_url = http_url
        self.max_backfill = max_backfill_blocks
        self.topic_user = "0x" + "0" * 24 + self.user[2:]
        self.block_ts: OrderedDict[int, int] = OrderedDict()
        self.seen_logs: OrderedDict[tuple[str, int], None] = OrderedDict()
        # log indexes per (tx, token, side, size): seq = rank of the log index, so a fill re-added by a reorg
        # (removed=true, then again in another block) gets its old seq back and merges instead of duplicating
        self.same_fill_logs: OrderedDict[str, list[int]] = OrderedDict()
        self.last_block = 0
        self.fills = 0
        self.heads = 0
        self._emit: Emit | None = None
        self._http: httpx.AsyncClient | None = None

    def _filter(self) -> dict[str, Any]:
        return {"address": list(EXCHANGES), "topics": [ORDER_FILLED, None, self.topic_user]}

    async def rpc(self, method: str, params: list[Any]) -> Any:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=15, verify=_ssl_context())
        backoff = Backoff(0.5, 8)
        for attempt in range(5):
            try:
                r = await self._http.post(self.http_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
                d = r.json()
                if "error" not in d:
                    return d["result"]
                err = d["error"]
            except (httpx.HTTPError, ValueError) as exc:
                err = f"{type(exc).__name__}: {exc}"
            if attempt == 4:
                raise RuntimeError(f"{method} failed: {err}")
            await asyncio.sleep(backoff.next())

    async def run(self, emit: Emit) -> None:
        self._emit = emit

        async def on_open(client: WsClient) -> None:
            await client.send({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]})
            await client.send({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe", "params": ["logs", self._filter()]})
            if self.last_block:
                spawn(self._backfill(self.last_block + 1))

        try:
            await self.ws.run(on_open, self._on_message)
        finally:
            if self._http is not None:
                await self._http.aclose()
                self._http = None

    def _on_message(self, raw: str, recv: float) -> None:
        msg = json.loads(raw)
        if "params" not in msg:
            if "error" in msg:
                log.warning("chain_logs: %s", msg["error"])
            return
        res = msg["params"]["result"]
        if "logIndex" in res:
            self._handle_log(res, recv)
        elif "number" in res:
            n, ts = int(res["number"], 16), int(res["timestamp"], 16)
            self.heads += 1
            self.block_ts[n] = ts
            while len(self.block_ts) > 5_000:
                self.block_ts.popitem(last=False)
            self.last_block = max(self.last_block, n)

    def _handle_log(self, lg: dict[str, Any], recv: float, backfill: bool = False) -> None:
        ident = (lg["transactionHash"].lower(), int(lg["logIndex"], 16))
        ev = parse_order_filled(lg, recv)
        if lg.get("removed"):
            log.warning("chain_logs: log removed by a reorg: %s", ident)
            self.seen_logs.pop(ident, None)
            if ident[1] in self.same_fill_logs.get(ev.base_key, []):
                self.same_fill_logs[ev.base_key].remove(ident[1])
            return
        if ident in self.seen_logs:
            return
        self.seen_logs[ident] = None
        while len(self.seen_logs) > 50_000:
            self.seen_logs.popitem(last=False)
        idx = self.same_fill_logs.setdefault(ev.base_key, [])
        idx.append(ident[1])
        idx.sort()
        ev.seq = idx.index(ident[1])
        while len(self.same_fill_logs) > 50_000:
            self.same_fill_logs.popitem(last=False)
        if ev.block_ts is None:
            ev.block_ts = self.block_ts.get(ev.block_number)
            ev.src_ts_ms = ev.block_ts * 1000.0 if ev.block_ts else None
        ev.raw["backfill"] = backfill
        self.last_block = max(self.last_block, ev.block_number or 0)
        self.fills += 1
        assert self._emit is not None
        self._emit(ev)
        if ev.block_ts is None:
            spawn(self._late_block_ts(ev))

    async def _late_block_ts(self, ev: FillEvent) -> None:
        """The log came before its newHeads notification: fetch the timestamp and send an update."""
        await asyncio.sleep(0.5)
        ts = self.block_ts.get(ev.block_number)
        if ts is None:
            try:
                blk = await self.rpc("eth_getBlockByNumber", [hex(ev.block_number), False])
                ts = int(blk["timestamp"], 16)
                self.block_ts[ev.block_number] = ts
            except Exception as exc:  # noqa: BLE001
                log.warning("chain_logs: block %s timestamp unavailable: %s", ev.block_number, exc)
                return
        upd = FillEvent(**{**ev.__dict__, "block_ts": ts, "src_ts_ms": ts * 1000.0, "update_only": True})
        assert self._emit is not None
        self._emit(upd)

    async def _backfill(self, from_block: int) -> None:
        """eth_getLogs over the blocks missed while disconnected (bounded; the Data API detector is the backstop)."""
        try:
            head = int(await self.rpc("eth_blockNumber", []), 16)
            start = max(from_block, head - self.max_backfill)
            for a in range(start, head + 1, 1_000):  # publicnode caps ranges at 10k blocks
                b = min(head, a + 999)
                logs = await self.rpc("eth_getLogs", [{**self._filter(), "fromBlock": hex(a), "toBlock": hex(b)}])
                recv = now_ms()
                for lg in logs:
                    n = int(lg["blockNumber"], 16)
                    if n not in self.block_ts and not lg.get("blockTimestamp"):
                        blk = await self.rpc("eth_getBlockByNumber", [hex(n), False])
                        self.block_ts[n] = int(blk["timestamp"], 16)
                    self._handle_log(lg, recv, backfill=True)
            log.info("chain_logs: back-filled blocks %d..%d", start, head)
        except Exception as exc:  # noqa: BLE001
            log.warning("chain_logs: backfill from %d failed: %s", from_block, exc)

    def status(self) -> dict[str, Any]:
        return {"connected": self.ws.connected, "connects": self.ws.connects, "heads": self.heads,
                "last_block": self.last_block, "fills": self.fills, "last_error": self.ws.last_error}


# -------------------------------------------------------------------------------------------- Data API
def data_api_events(rows: list[dict[str, Any]], recv: float) -> list[FillEvent]:
    """/v2/activity rows -> fill events (TRADE rows only); identical rows of one tx get seq 0, 1, ..."""
    occ = Occurrences()
    out = []
    for r in rows:
        if r.get("type") != "TRADE":
            continue
        ev = FillEvent(
            channel="data_api", recv_ms=recv, tx_hash=str(r["transaction_hash"]).lower(), token_id=str(r["token_id"]),
            side=str(r["side"]), size=float(r["size"]), price=float(r["price"]) if r.get("price") is not None else None,
            usdc=float(r["usdc_size"]) if r.get("usdc_size") is not None else None,
            block_ts=int(r["timestamp"]), src_ts_ms=int(r["timestamp"]) * 1000.0,
            condition_id=str(r.get("condition_id") or "").lower() or None, slug=r.get("slug"), outcome=r.get("outcome"),
        )
        ev.seq = occ.next(ev.base_key)
        out.append(ev)
    return out


class DataApiDetector:
    name = "data_api"

    def __init__(self, user: str, client: ApiClient, base_url: str, poll_s: float = 3.0, limit: int = 100) -> None:
        self.user = user.lower()
        self.client = client
        self.url = f"{base_url}/v2/activity"
        self.poll_s = poll_s
        self.limit = limit
        self.emitted: OrderedDict[str, None] = OrderedDict()
        self.polls = 0
        self.fills = 0
        self.last_error = ""

    async def run(self, emit: Emit) -> None:
        baseline = True
        while True:
            t0 = now_ms()
            try:
                payload = await self.client.get_json(self.url, {"user": self.user, "limit": self.limit})
                recv = now_ms()
                self.polls += 1
                for ev in data_api_events(payload.get("data") or [], recv):
                    if ev.fill_key in self.emitted:
                        continue
                    self.emitted[ev.fill_key] = None
                    if not baseline:  # rows already there at start-up are history, not live detections
                        self.fills += 1
                        emit(ev)
                while len(self.emitted) > 20_000:
                    self.emitted.popitem(last=False)
                baseline = False
            except Exception as exc:  # noqa: BLE001 - keep polling; ApiClient already retried
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("data_api: poll failed: %s", self.last_error)
            await asyncio.sleep(max(0.0, self.poll_s - (now_ms() - t0) / 1000))

    def status(self) -> dict[str, Any]:
        return {"polls": self.polls, "fills": self.fills, "last_error": self.last_error}
