"""Stage 3 live tracker: `python -m bosona track`.

Wiring:
  detectors (RTDS activity, chain logs, Data API) --FillEvent--> Tracker.on_fill
  price providers (Binance, Chainlink RTDS, Chainlink Data Streams) --PriceTick--> PriceBook (+ per-second rows)
  CLOB market channel --> local books of the open windows + match time of every trade (by tx hash)

On the first sighting of a fill the tracker freezes what a copier would see at that instant: every source's
latest spot tick and the books of both tokens (local book, then REST /book). `finalize_after_s` later, when the
slower channels have reported, it computes the reference values at the trade itself (match time, else block
time): spot then vs at detection (shift in bps) and his token's top of book then vs at detection.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import statistics
import time
from collections import OrderedDict, deque
from typing import Any

from bosona import parse
from bosona.config import Config
from bosona.http import ApiClient
from bosona.live.books import BookFeed, copy_cost, parse_rest_book, summarize
from bosona.live.detect import ChainLogsDetector, DataApiDetector, FillEvent, RtdsActivityDetector
from bosona.live.markets import MarketRegistry
from bosona.live.outcomes import resolve_pending
from bosona.live.prices import PriceBook, PriceTick, build_providers
from bosona.live.store import FILL_COLUMNS, LiveStore
from bosona.live.wsconn import now_ms, spawn
from bosona.windows import slug_for, window_start

log = logging.getLogger(__name__)

DEFAULTS: dict[str, Any] = {
    "assets": ["btc", "eth", "sol", "xrp", "doge", "bnb"],
    "timeframes": ["5m", "15m", "1h", "4h", "1d"],
    "book_series": ["btc:5m", "btc:15m", "btc:1h", "eth:5m"],
    "close_timeframes": ["5m", "15m", "4h"],
    "window_lead_s": 60,
    "book_depth": 10,
    "finalize_after_s": 30,
    "close_delay_s": 3,
    "price_ring_s": 1200,
    "detectors": ["rtds_activity", "chain_logs", "data_api"],
    "data_api_poll_s": 3,
    "rpc_http": "https://polygon-bor-rpc.publicnode.com",
    "rpc_ws": "wss://polygon-bor-rpc.publicnode.com",
    "rtds_url": "wss://ws-live-data.polymarket.com",
    "clob_ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "price_providers": ["binance_ws", "chainlink_rtds", "chainlink_data_streams"],
    "providers": {},
    "stale_s": 10,
    "block_minus_match_ms": 2300,   # prior for block ts - match time (two 2026-10-01 races: median 2.0-2.3 s)
}
BACKFILL_MS = 60_000
CHAINLINK_REGIMES = ("chainlink_spot", "chainlink_twap30", "chainlink_twap60")


def _bps(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None or b == 0 else (a / b - 1) * 1e4


class LiveFill:
    """Everything known about one fill; merged from every channel that reports it."""

    def __init__(self, ev: FillEvent) -> None:
        self.key = ev.fill_key
        self.f: dict[str, Any] = {c: None for c in FILL_COLUMNS}
        self.f.update(fill_key=self.key, first_channel=ev.channel, first_seen_ms=ev.recv_ms, backfill=0)
        self.channels: list[str] = []
        self.src_ts: dict[str, float] = {}                       # channel -> timestamp it attached (ms)
        self.detect_ticks: dict[tuple[str, str, str], PriceTick] = {}
        self.books: dict[tuple[str, str], dict[str, Any]] = {}   # (token, src) -> live_books row
        self.levels: dict[str, tuple[dict[float, float], dict[float, float]]] = {}  # his token's detection book
        self.finalized = False
        self.completion: asyncio.Task[None] | None = None   # market lookup + REST books after detection
        self.merge(ev)

    def merge(self, ev: FillEvent) -> bool:
        """Fill missing fields; chain values (exact amounts, role, fee) win over API floats. True if new channel."""
        f = self.f
        for k in ("tx_hash", "token_id", "side", "size", "condition_id", "slug", "outcome", "block_ts", "block_number",
                  "log_index", "order_hash"):
            v = getattr(ev, k)
            if v is not None and f.get(k) is None:
                f[k] = v
        if ev.channel == "chain_logs":
            f.update(size=ev.size, price=ev.price, usdc=ev.usdc, role=ev.role, fee_usdc=ev.fee_usdc)
        else:
            for k in ("price", "usdc", "fee_usdc"):
                if f.get(k) is None and getattr(ev, k) is not None:
                    f[k] = getattr(ev, k)
        if ev.src_ts_ms is not None:
            self.src_ts[ev.channel] = ev.src_ts_ms
        if ev.update_only or ev.channel in self.channels:
            return False
        self.channels.append(ev.channel)
        f["channels"] = ",".join(self.channels)
        return True


class Tracker:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.s = {**DEFAULTS, **(cfg.live or {})}
        self.assets: list[str] = list(self.s["assets"])
        self.store = LiveStore(cfg.live_db_path)
        self.client = ApiClient(cfg)
        self.prices = PriceBook(keep_s=float(self.s["price_ring_s"]), on_second=self.store.add_spot)
        self.providers = build_providers(self.s["price_providers"], self.s.get("providers") or {}, self.assets)
        self.feed = BookFeed(self.s["clob_ws_url"])
        self.registry = MarketRegistry(self.store, self.client, cfg.gamma_api, self.assets, list(self.s["timeframes"]),
                                       lead_s=float(self.s["window_lead_s"]))
        self.book_series = {tuple(x.split(":", 1)) for x in self.s["book_series"]}
        self.detectors = self._build_detectors()
        self.fills: OrderedDict[str, LiveFill] = OrderedDict()
        self.pending: dict[str, LiveFill] = {}          # not finalized yet
        self.recent_tokens: dict[str, float] = {}   # token -> last fill time: keep its book streamed for a while
        self.closes_done: set[tuple[str, str, int]] = set()
        # block timestamp - CLOB match time of fills that have both: used to estimate the match time of the others
        self.block_minus_match: deque[float] = deque([float(self.s["block_minus_match_ms"])], maxlen=500)
        self.started_ms = now_ms()
        self.stop = asyncio.Event()

    def _build_detectors(self) -> list[Any]:
        out: list[Any] = []
        for name in self.s["detectors"]:
            if name == "rtds_activity":
                out.append(RtdsActivityDetector(self.cfg.user, self.s["rtds_url"]))
            elif name == "chain_logs":
                out.append(ChainLogsDetector(self.cfg.user, self.s["rpc_ws"], self.s["rpc_http"]))
            elif name == "data_api":
                out.append(DataApiDetector(self.cfg.user, self.client, self.cfg.data_api, float(self.s["data_api_poll_s"])))
            else:
                raise ValueError(f"unknown detector {name!r}")
        return out

    # ------------------------------------------------------------------------------------------- run
    async def run(self, duration_s: float | None = None) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop.set)
            except (NotImplementedError, RuntimeError):
                pass
        log.info("live tracker: wallet %s, db %s, assets %s, detectors %s, price providers %s",
                 self.cfg.user, self.store.path, ",".join(self.assets), ",".join(d.name for d in self.detectors),
                 ",".join(p.name for p in self.providers) or "-")
        await self._refresh_windows()
        tasks = [asyncio.create_task(self._guard(p.name, p.run(self.prices.update))) for p in self.providers]
        tasks += [asyncio.create_task(self._guard(d.name, d.run(self.on_fill))) for d in self.detectors]
        tasks += [asyncio.create_task(self._guard(n, c)) for n, c in (
            ("clob_market", self.feed.run()), ("windows", self._every(5, self._refresh_windows)),
            ("flush", self._every(0.5, self._flush)), ("finalize", self._every(1, self._finalize_due)),
            ("closes", self._every(1, self._record_closes)), ("resolve", self._every(60, self._resolve_closes, 60)),
            ("outcomes", self._every(60, self._resolve_fills, 90)), ("health", self._every(60, self._health, 60)))]
        try:
            async with asyncio.timeout(duration_s):
                await self.stop.wait()
        except TimeoutError:
            pass
        log.info("live tracker: stopping")
        for t in tasks:
            t.cancel()
        _, stuck = await asyncio.wait(tasks, timeout=15)
        if stuck:
            log.warning("live tracker: %d task(s) did not stop within 15 s; closing anyway", len(stuck))
        for lf in list(self.pending.values()):
            await self._finalize(lf)
        self.prices.flush_seconds()
        self._health_snapshot()
        self.store.close()
        await self.client.aclose()
        summary = {"fills": len(self.fills), "rows_written": self.store.rows_written,
                   "run_s": round((now_ms() - self.started_ms) / 1000)}
        log.info("live tracker stopped: %s", summary)
        return summary

    async def _guard(self, name: str, coro: Any) -> None:
        """A component that crashes is logged and restarted; the others keep running."""
        while True:
            try:
                await coro
                return
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("%s crashed; restarting in 5 s", name)
                await asyncio.sleep(5)
                coro = self._restart(name)
                if coro is None:
                    return

    def _restart(self, name: str) -> Any:
        for p in self.providers:
            if p.name == name:
                return p.run(self.prices.update)
        for d in self.detectors:
            if d.name == name:
                return d.run(self.on_fill)
        if name == "clob_market":
            return self.feed.run()
        loops = {"windows": (5, self._refresh_windows), "flush": (0.5, self._flush), "finalize": (1, self._finalize_due),
                 "closes": (1, self._record_closes), "resolve": (60, self._resolve_closes, 60),
                 "outcomes": (60, self._resolve_fills, 90), "health": (60, self._health, 60)}
        if name in loops:
            return self._every(*loops[name])
        return None

    async def _every(self, period: float, fn: Any, initial_delay: float = 0.0) -> None:
        await asyncio.sleep(initial_delay)
        while True:
            try:
                res = fn()
                if asyncio.iscoroutine(res):
                    await res
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one failed iteration must not stop the loop
                log.exception("%s failed", getattr(fn, "__name__", fn))
            await asyncio.sleep(period)

    def _flush(self) -> None:
        self.store.flush()

    # ----------------------------------------------------------------------------------------- fills
    def on_fill(self, ev: FillEvent) -> None:
        lf = self.fills.get(ev.fill_key)
        if lf is None:
            if ev.update_only:
                return
            lf = LiveFill(ev)
            self.fills[ev.fill_key] = lf
            self.pending[ev.fill_key] = lf
            while len(self.fills) > 20_000:
                self.fills.popitem(last=False)
            self._capture_detection(lf)
            lf.completion = spawn(self._complete_detection(lf))
            new_channel = True
        else:
            new_channel = lf.merge(ev)
        if new_channel:
            self.store.insert_ignore("live_detections", [{
                "fill_key": lf.key, "channel": ev.channel, "recv_ms": ev.recv_ms, "src_ts_ms": ev.src_ts_ms,
                "raw_json": ev.as_json()}])
        if lf.finalized:
            self._timing(lf)
            self._save_fill(lf)

    def _market(self, lf: LiveFill) -> dict[str, Any] | None:
        return self.registry.get(lf.f["token_id"])

    def _capture_detection(self, lf: LiveFill) -> None:
        """Freeze the state at the moment of detection (synchronously, before any await)."""
        t = lf.f["first_seen_ms"]
        lf.detect_ticks = dict(self.prices.latest)
        token = lf.f["token_id"]
        self.recent_tokens[token] = t
        m = self._market(lf)
        for tok in self._tokens_of(m, token):
            snap = self.feed.snapshot(tok, int(self.s["book_depth"]))
            if snap:
                lf.books[(tok, "ws")] = self._book_row(lf, tok, "ws", t, snap, m)
                if tok == token:
                    lf.levels[tok] = self.feed.levels(tok) or ({}, {})
        log.info("fill %s via %s: %s %s %.2f @ %s %s", lf.key[:18], lf.f["first_channel"], lf.f["side"],
                 lf.f.get("outcome") or "?", lf.f["size"], _fmt(lf.f.get("price"), 4), lf.f.get("slug") or (m or {}).get("slug") or token[:12])

    def _tokens_of(self, m: dict[str, Any] | None, token: str) -> list[str]:
        if m and m.get("up_token_id") and m.get("down_token_id"):
            return [m["up_token_id"], m["down_token_id"]]
        return [token]

    def _book_row(self, lf: LiveFill, tok: str, src: str, taken: float, snap: dict[str, Any], m: dict[str, Any] | None) -> dict[str, Any]:
        outcome = None
        if m:
            outcome = "Up" if tok == m.get("up_token_id") else "Down" if tok == m.get("down_token_id") else None
        return {"fill_key": lf.key, "token_id": tok, "src": src, "outcome": outcome, "taken_ms": taken, **snap}

    async def _complete_detection(self, lf: LiveFill) -> None:
        """Market metadata (Gamma, if this token was not pre-fetched) and REST books of both tokens."""
        token = lf.f["token_id"]
        m = self._market(lf)
        if m is None:
            try:
                m = await self.registry.lookup(token)
            except Exception as exc:  # noqa: BLE001
                log.warning("market lookup for %s failed: %s", token, exc)
        tokens = self._tokens_of(m, token)
        results = await asyncio.gather(*(self.client.get_json(f"{self.cfg.clob_api}/book", {"token_id": tok}) for tok in tokens),
                                       return_exceptions=True)
        taken = now_ms()
        for tok, res in zip(tokens, results):
            if isinstance(res, Exception):
                log.info("REST book %s unavailable: %s", tok[:12], res)
                continue
            bids, asks, server_ts = parse_rest_book(res)
            lf.books[(tok, "rest")] = self._book_row(lf, tok, "rest", taken, {"server_ts_ms": server_ts,
                                                     **summarize(bids, asks, int(self.s["book_depth"]))}, m)
            if tok == token and tok not in lf.levels:
                lf.levels[tok] = (bids, asks)

    async def _finalize_due(self) -> None:
        now = now_ms()
        due = float(self.s["finalize_after_s"]) * 1000
        for lf in list(self.pending.values()):
            age = now - lf.f["first_seen_ms"]
            busy = lf.completion is not None and not lf.completion.done() and age < 2 * due
            if not busy and (age >= due or len(lf.channels) >= len(self.detectors)):
                await self._finalize(lf)

    async def _finalize(self, lf: LiveFill) -> None:
        f = lf.f
        m = self._market(lf)
        if m:
            f.update(condition_id=f["condition_id"] or m["condition_id"], slug=f["slug"] or m["slug"], asset=m["asset"],
                     timeframe=m["timeframe"], window_start_ts=m["window_start_ts"], window_end_ts=m["window_end_ts"])
            if f["outcome"] is None:
                f["outcome"] = "Up" if f["token_id"] == m.get("up_token_id") else "Down"
        elif f["slug"]:
            f["asset"], f["timeframe"] = parse.parse_slug(f["slug"])
        match = self.feed.match_ms(f["tx_hash"])
        if match is not None:
            f["match_ms"] = match
            if f["block_ts"]:
                self.block_minus_match.append(f["block_ts"] * 1000 - match)
        self._timing(lf)
        ref_ms = f["ref_ms"]
        asset = f["asset"]
        snaps = []
        if asset and ref_ms:
            regime = (m or {}).get("resolution_regime") or ""
            for (source, kind, a), det in lf.detect_ticks.items():
                if a != asset:
                    continue
                ref = self.prices.at(source, kind, a, ref_ms)
                snaps.append({"fill_key": lf.key, "source": source, "kind": kind,
                              "ref_ts_ms": ref.ts_ms if ref else None, "ref_price": ref.price if ref else None,
                              "det_ts_ms": det.ts_ms, "det_price": det.price, "det_age_ms": f["first_seen_ms"] - det.recv_ms,
                              "shift_bps": _bps(det.price, ref.price if ref else None)})
            head = self._headline(snaps, regime)
            if head:
                f.update(spot_source=f"{head['source']}:{head['kind']}", spot_ref=head["ref_price"],
                         spot_detect=head["det_price"], spot_shift_bps=head["shift_bps"])
        token = f["token_id"]
        if ref_ms:
            tob = self.feed.tob_before(token, ref_ms)
            if tob:
                f["bid_ref"], f["ask_ref"] = tob
        det_book = lf.books.get((token, "ws")) or lf.books.get((token, "rest"))
        if det_book:
            f["bid_detect"], f["ask_detect"] = det_book["best_bid"], det_book["best_ask"]
        if token in lf.levels and f["size"]:
            bids, asks = lf.levels[token]
            f.update(copy_cost(f["side"], float(f["size"]), f.get("price"), bids, asks))
        f["finalized_ms"] = now_ms()
        lf.finalized = True
        self.pending.pop(lf.key, None)
        self.store.upsert("live_spot_snaps", snaps)
        self.store.upsert("live_books", list(lf.books.values()))
        self._save_fill(lf)
        log.info("fill %s final: %s %s %s role=%s first=%s lat_block=%s ms lat_match=%s ms spot %s shift=%s bps "
                 "ask %s->%s copy_slip=%s", lf.key[:18], f.get("slug"), f.get("outcome"), _fmt(f.get("price"), 4), f.get("role"),
                 f["first_channel"], _fmt(f["lat_block_ms"]), _fmt(f["lat_match_ms"]), f.get("spot_source"),
                 _fmt(f["spot_shift_bps"], 2), f.get("ask_ref"), f.get("ask_detect"), _fmt(f.get("copy_slip"), 4))

    @staticmethod
    def _headline(snaps: list[dict[str, Any]], regime: str) -> dict[str, Any] | None:
        """The spot that matters for the market: Chainlink for Chainlink-resolved windows when it was live at
        detection (fresh tick), else Binance."""
        by = {(s["source"], s["kind"]): s for s in snaps if s["det_price"] is not None}
        prefs = [("chainlink_ds", "spot"), ("chainlink", "spot"), ("binance", "spot")] if regime in CHAINLINK_REGIMES \
            else [("binance", "spot")]
        for p in prefs:
            s = by.get(p)
            if s and s["det_age_ms"] is not None and s["det_age_ms"] < 10_000 and s["ref_price"] is not None:
                return s
        return by.get(("binance", "spot"))

    def _timing(self, lf: LiveFill) -> None:
        f = lf.f
        first = f["first_seen_ms"]
        f["lat_block_ms"] = first - f["block_ts"] * 1000 if f["block_ts"] else None
        f["lat_match_ms"] = first - f["match_ms"] if f["match_ms"] else None
        f["backfill"] = int(f["lat_block_ms"] is not None and f["lat_block_ms"] > BACKFILL_MS)
        # reference time of the trade: the CLOB match when the market was streamed; otherwise the block time
        # minus the typical block-match gap measured on this run (Polygon block timestamps run ~1 s ahead of
        # real time, so the raw block time can even be later than our detection)
        if f["match_ms"]:
            f["ref_ms"], f["ref_kind"] = f["match_ms"], "match"
        elif f["block_ts"]:
            f["ref_ms"] = f["block_ts"] * 1000 - statistics.median(self.block_minus_match)
            f["ref_kind"] = "block_est"
        elif lf.src_ts.get("rtds_activity"):
            f["ref_ms"], f["ref_kind"] = lf.src_ts["rtds_activity"], "rtds_ts"
        if f["window_end_ts"] and f["ref_ms"]:
            f["secs_to_close"] = f["window_end_ts"] - f["ref_ms"] / 1000

    def _save_fill(self, lf: LiveFill) -> None:
        lf.f["updated_ms"] = now_ms()
        self.store.upsert("live_fills", [{c: lf.f.get(c) for c in FILL_COLUMNS}])

    # ------------------------------------------------------------------------------- books / windows
    async def _refresh_windows(self) -> None:
        now = time.time()
        rows = await self.registry.refresh(now)
        tokens = {t for r in rows if (r.get("asset"), r.get("timeframe")) in self.book_series
                  for t in (r.get("up_token_id"), r.get("down_token_id")) if t}
        horizon = now * 1000 - 15 * 60 * 1000
        for tok, t in list(self.recent_tokens.items()):
            m = self.registry.get(tok)
            if t < horizon or (m and m.get("window_end_ts") and m["window_end_ts"] < now - 60):
                self.recent_tokens.pop(tok, None)
            elif m:
                tokens.update(x for x in (m.get("up_token_id"), m.get("down_token_id")) if x)
        await self.feed.set_tokens(tokens)

    # ------------------------------------------------------------------- Binance vs Chainlink at closes
    def _record_closes(self) -> None:
        now = time.time()
        delay = float(self.s["close_delay_s"])
        for tf in self.s["close_timeframes"]:
            we = window_start(tf, int(now - delay))  # the latest boundary at least `delay` seconds ago
            ws = window_start(tf, we - 1)
            if we * 1000 < self.started_ms + 61_000:
                continue  # need 60 s of Binance seconds for the TWAP
            for asset in self.assets:
                key = (asset, tf, we)
                if key in self.closes_done:
                    continue
                self.closes_done.add(key)
                self.store.flush()
                row = self._close_row(asset, tf, ws, we)
                self.store.upsert("live_window_close", [row])
                if row["cl_start"] is None or row["bn_start"] is None:
                    continue  # tracker started inside this window: no start values
                log.info("close %s %s %s: chainlink twap %s -> %s (%s bps) | binance-chainlink spot %s bps, twap %s bps "
                         "(basis %s) | winner chainlink=%s binance=%s", asset, tf, time.strftime("%H:%M", time.gmtime(we)),
                         row["cl_start"], row["cl_end"], _fmt(row["move_cl_bps"], 2), _fmt(row["div_spot_bps"], 2),
                         _fmt(row["div_twap_bps"], 2), _fmt(row["basis_bps"], 2), row["winner_cl"], row["winner_bn"])

    def _bn_twap(self, asset: str, t_end: int, lookback: int = 60) -> float | None:
        """Mean of Binance 1 s closes over [t_end - lookback, t_end), forward-filled (stage 2 proxy rule)."""
        series = self.store.spot_series("binance", "spot", asset, t_end - lookback - 30, t_end - 1)
        if not series:
            return None
        vals, j, last = [], 0, None
        for t in range(t_end - lookback, t_end):
            while j < len(series) and series[j][0] <= t:
                last = series[j][1]
                j += 1
            if last is not None:
                vals.append(last)
        return sum(vals) / len(vals) if len(vals) >= lookback * 0.9 else None

    def _close_row(self, asset: str, tf: str, ws: int, we: int) -> dict[str, Any]:
        st = self.store
        cl_start, cl_end = st.spot_at("chainlink", "twap60", asset, ws), st.spot_at("chainlink", "twap60", asset, we)
        cl_spot = st.spot_at("chainlink", "spot", asset, we)
        bn_spot = self.prices.at("binance", "spot", asset, we * 1000)
        bn_start, bn_end = self._bn_twap(asset, ws), self._bn_twap(asset, we)
        cl_series = dict(st.spot_series("chainlink", "spot", asset, we - 600, we))
        bn_series = st.spot_series("binance", "spot", asset, we - 600, we)
        diffs = [_bps(p, cl_series[t]) for t, p in bn_series if t in cl_series]
        diffs = [d for d in diffs if d is not None]
        return {
            "asset": asset, "timeframe": tf, "window_end_ts": we, "window_start_ts": ws, "slug": slug_for(asset, tf, ws),
            "cl_start": cl_start, "cl_end": cl_end, "cl_spot_end": cl_spot,
            "bn_start": bn_start, "bn_end": bn_end, "bn_spot_end": bn_spot.price if bn_spot else None,
            "div_spot_bps": _bps(bn_spot.price if bn_spot else None, cl_spot),
            "div_twap_bps": _bps(bn_end, cl_end),
            "basis_bps": statistics.median(diffs) if len(diffs) >= 60 else None,
            "move_cl_bps": _bps(cl_end, cl_start),
            "winner_cl": None if cl_start is None or cl_end is None else ("Up" if cl_end >= cl_start else "Down"),
            "winner_bn": None if bn_start is None or bn_end is None else ("Up" if bn_end >= bn_start else "Down"),
            "official_strike": None, "official_final": None, "official_winner": None,
            "recorded_ms": now_ms(), "resolved_ms": None,
        }

    async def _resolve_closes(self) -> None:
        """Official strike / final / winner from Gamma for the recorded closes.

        Right after resolution Gamma shows the winner and `priceToBeat` but not yet `finalPrice`; the final of a
        window is the strike of the next one (priceToBeat(N) = finalPrice(N-1), stage 0), so it is taken from
        there once the next window has closed too. Gamma answers are CDN-cached for up to 5 min, hence the retries.
        """
        now = time.time()
        rows = self.store.conn.execute(
            "SELECT asset, timeframe, window_end_ts, slug FROM live_window_close "
            "WHERE (official_final IS NULL OR official_winner IS NULL) AND window_end_ts BETWEEN ? AND ? "
            "ORDER BY window_end_ts LIMIT 100", (now - 8 * 3600, now - 90)).fetchall()
        if not rows:
            return
        nxt = {r["slug"]: slug_for(r["asset"], r["timeframe"], r["window_end_ts"]) for r in rows}
        names = sorted(set(nxt) | set(nxt.values()))
        meta: dict[str, dict[str, Any]] = {}
        for i in range(0, len(names), 50):
            chunk = names[i : i + 50]
            try:
                found = await self.client.get_json(f"{self.cfg.gamma_api}/markets",
                                                   [("slug", s) for s in chunk] + [("closed", "true"), ("limit", 50)])
            except Exception as exc:  # noqa: BLE001
                log.warning("close resolution lookup failed: %s", exc)
                return
            for mk in found:
                _, res = parse.parse_market(mk, (mk.get("events") or [{}])[0], int(now))
                if res:
                    meta[mk["slug"]] = res
        for r in rows:
            cur, after = meta.get(r["slug"]) or {}, meta.get(nxt[r["slug"]]) or {}
            final = cur.get("final_price") if cur.get("final_price") is not None else after.get("price_to_beat")
            winner = cur.get("winner")
            if cur.get("price_to_beat") is None and final is None and winner is None:
                continue
            self.store.conn.execute(
                "UPDATE live_window_close SET official_strike = COALESCE(?, official_strike), "
                "official_final = COALESCE(?, official_final), official_winner = COALESCE(?, official_winner), "
                "resolved_ms = CASE WHEN ? IS NOT NULL AND ? IS NOT NULL THEN ? ELSE resolved_ms END "
                "WHERE asset = ? AND timeframe = ? AND window_end_ts = ?",
                (cur.get("price_to_beat"), final, winner, final, winner, now_ms(), r["asset"], r["timeframe"], r["window_end_ts"]))
        self.store.conn.commit()

    async def _resolve_fills(self) -> None:
        """Winner of every market traded live (for the PnL of his fills and of copying them)."""
        self.store.flush()
        n = await resolve_pending(self.client, self.cfg.gamma_api, self.store.conn)
        if n:
            log.debug("resolved %d traded market(s)", n)

    # ------------------------------------------------------------------------------------------ health
    def _health_snapshot(self) -> dict[str, Any]:
        state: dict[str, Any] = {p.name: p.status() for p in self.providers}
        state.update({d.name: d.status() for d in self.detectors})
        state["clob_market"] = self.feed.status()
        state["fills"] = len(self.fills)
        ts = int(time.time())
        for comp, st in state.items():
            self.store.health(ts, comp, st if isinstance(st, dict) else {"value": st})
        return state

    def _health(self) -> None:
        state = self._health_snapshot()
        parts = []
        for p in self.providers:
            st = state[p.name]
            parts.append(f"{p.name} {'up' if st['connected'] else 'DOWN'} age={st['age_s']}s")
            if st["age_s"] is None or st["age_s"] > float(self.s["stale_s"]):
                log.warning("price provider %s is stale or down (%s); snapshots fall back to the other sources",
                            p.name, st.get("last_error") or "no ticks")
        for d in self.detectors:
            st = state[d.name]
            parts.append(f"{d.name} fills={st.get('fills')}" + ("" if st.get("connected", True) else " DOWN"))
        div = []
        for a in self.assets[:3]:
            bn, cl = self.prices.last("binance", "spot", a), self.prices.last("chainlink", "spot", a)
            if bn and cl:
                div.append(f"{a} {_fmt(_bps(bn.price, cl.price), 1)}")
        log.info("health: %s | clob books %d/%d | binance-chainlink bps: %s", "; ".join(parts),
                 state["clob_market"]["books"], state["clob_market"]["tokens"], ", ".join(div) or "-")


def _fmt(v: float | None, nd: int = 0) -> str:
    return "-" if v is None else f"{v:.{nd}f}"


async def run_tracker(cfg: Config, duration_s: float | None = None) -> dict[str, Any]:
    return await Tracker(cfg).run(duration_s)

