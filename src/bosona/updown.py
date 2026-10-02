"""Run bosona strategies inside updown (github.com/illnessbolen/updown, package `latarb`).

updown supplies the market-data path: WebSocket feeds or recorded ticks, its parsers, the hub with local L2 books,
the Chainlink reference of each window, the replay clock and scheduler, and its paper exchange. bosona supplies the
strategy (bosona.strategies) and the fair value (bosona.backtest.pricing), so the same strategy object runs on the
tape backtest, on updown recordings and live in paper (DRY_RUN) mode with updown's risk profiles. Nothing in updown
is modified: it is imported from a checkout (`updown.path` in config.yaml or UPDOWN_PATH).

Two execution changes, made in a subclass of updown's PaperExchange:
  * quotes rest until cancelled or the window ends. updown cancels makers after MAKER_TIMEOUT_MS (<= 10 s): right
    for a latency signal, wrong for quoting, which would lose its queue position every 10 s;
  * a print on the OTHER outcome fills our bid too. The market channel reports a trade once, on the taker's token
    (stage 5: 4 323 of 4 323 trades), and 87% of his maker fills came from takers buying the other outcome, minted
    against the bids at 1 - price. updown ignores those prints, which on these markets misses most maker fills.
"""

from __future__ import annotations

import dataclasses
import importlib
import logging
import math
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bosona.backtest.pricing import TWAP_LOOKBACK_S, fair_up
from bosona.strategies.base import (
    Book,
    Cancel,
    Inventory,
    Order,
    PlaceBid,
    State,
    TakerBuy,
    Window,
)
from bosona.strategies.profiles import RiskProfile

log = logging.getLogger(__name__)

EPS = 1e-9
SIDES = ("up", "down")


def import_updown(path: str | os.PathLike[str] | None) -> Any:
    """Import updown's `latarb` package from a checkout; returns a namespace of the modules used here."""
    p = Path(path or os.environ.get("UPDOWN_PATH", "../updown")).expanduser().resolve()
    if not (p / "latarb" / "__init__.py").exists():
        raise FileNotFoundError(f"updown checkout not found at {p} (set updown.path in config.yaml or UPDOWN_PATH)")
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
    names = ["config", "clock", "scheduler", "data.hub", "data.markets", "data.parsers", "data.recorder",
             "data.reference", "data.events", "execution.paper", "risk.limits", "fastjson"]
    mods = {n: importlib.import_module(f"latarb.{n}") for n in names}
    return type("Updown", (), {n.replace(".", "_"): m for n, m in mods.items()})


def updown_profile(U: Any, settings: dict[str, Any] | None, name: str | None, bankroll: float) -> RiskProfile:
    """The risk profile as updown resolves it (risk/limits.py: its PROFILES, RISK_* overrides from the environment
    or `settings`, hard bounds). `name` overrides updown's RISK_PROFILE (default there: conservative)."""
    over = dict(settings or {})
    if name:
        over["RISK_PROFILE"] = name
    lim = U.risk_limits.resolve_limits(U.config.load_settings(dotenv=False, **over))
    log.info("risk: %s", lim.describe(bankroll))
    return RiskProfile(lim.profile, bankroll, lim.bet_pct, lim.exposure_pct, lim.daily_stop_pct)


def regime_for(label: str, resolution: str, start_ts: float) -> str:
    """Settlement rule of a window (stage 0, §4): Chainlink TWAP-60 since 2026-08-07 (5m: TWAP-30 until 08-14)."""
    if resolution != "chainlink":
        return "binance"
    if label == "5m":
        return "chainlink_twap60" if start_ts >= 1786665600 else ("chainlink_twap30" if start_ts >= 1786060800 else "chainlink_spot")
    return "chainlink_twap60" if start_ts >= 1786060800 else "chainlink_spot"


def make_exchange_class(U: Any) -> type:
    PaperExchange = U.execution_paper.PaperExchange

    class QuotingPaperExchange(PaperExchange):
        """updown's paper exchange with resting quotes and fills from the other outcome's prints."""

        def __init__(self, *a, on_fill: Callable[[Any, float, float, bool], None] | None = None, **kw) -> None:
            super().__init__(*a, **kw)
            self.on_fill = on_fill
            self._cancelled: set[str] = set()

        def cancel(self, order_id: str, reason: str) -> None:
            if order_id not in self.resting:             # still in flight: drop it when it arrives
                self._cancelled.add(order_id)
            super().cancel(order_id, reason)

        def _arrive(self, o) -> None:
            if o.order_id in self._cancelled:
                self._cancelled.discard(o.order_id)
                return self._finish(o, "cancelled")
            return super()._arrive(o)

        def _fill(self, o, qty: float, price: float, maker: bool) -> None:
            super()._fill(o, qty, price, maker)
            if self.on_fill is not None:
                self.on_fill(o, qty, price, maker)

        def _expire(self, o) -> None:
            now = self.clock.now()
            if o.status == "resting" and now < o.window.end_ts - 0.01:
                self.scheduler.call_at(min(now + 10.0, o.window.end_ts - 0.001), lambda: self._expire(o))
                return
            super()._expire(o)

        def _on_trade(self, t) -> None:
            super()._on_trade(t)                     # prints on our own token
            if t.side != "BUY":
                return
            w = self.hub.by_token.get(t.token)
            if w is None:
                return
            eff = 1.0 - t.price                      # a buyer of the other outcome minted against bids at 1 - p
            size = t.size
            for o in sorted(self.resting_for(w.slug), key=lambda x: -x.limit):
                if o.token == t.token or o.remaining <= EPS or size <= EPS:
                    continue
                if eff < o.limit - EPS:
                    q = min(o.remaining, size)
                elif abs(eff - o.limit) <= EPS:
                    used = min(size, max(o.queue_ahead, 0.0))
                    o.queue_ahead -= used
                    size -= used
                    q = min(o.remaining, size)
                else:
                    continue
                if q > EPS:
                    size -= q
                    self._fill(o, q, o.limit, maker=True)
                if o.remaining <= EPS:
                    self._finish(o, "filled")

    return QuotingPaperExchange


@dataclass
class WinCtx:
    w: Any                                   # updown MarketWindow
    strategy: Any
    window: Window
    inv: Inventory = field(default_factory=Inventory)
    orders: dict[str, tuple[Order, Any]] = field(default_factory=dict)   # our oid -> (Order, PaperOrder)
    fills: list[dict] = field(default_factory=list)
    winner: str | None = None


class StrategyHost:
    """Drives one strategy instance per window from updown's hub; routes its intents to the paper exchange."""

    def __init__(self, U: Any, cfg: Any, hub: Any, clock: Any, scheduler: Any, refs: Any,
                 make_strategy: Callable[[], Any], profile: RiskProfile, assets: tuple[str, ...] = ("btc",),
                 labels: tuple[str, ...] = ("5m", "15m"), react_s: float | None = None,
                 cancel_latency_s: float = 0.1) -> None:
        """react_s: also re-evaluate a window on every Binance quote of its asset, at most every react_s seconds
        (None: only on the 1 s timer). cancel_latency_s: a cancel reaches the venue this much later."""
        self.U, self.cfg, self.hub, self.clock, self.refs = U, cfg, hub, clock, refs
        self.make_strategy, self.profile, self.assets, self.labels = make_strategy, profile, assets, labels
        self.scheduler = scheduler
        self.exchange = make_exchange_class(U)(cfg, hub, clock, scheduler, on_fill=self._on_fill)
        self.ctx: dict[str, WinCtx] = {}
        self._ids = 0
        self.evaluations = 0
        self.react_s = react_s
        self.cancel_latency_s = cancel_latency_s
        self._last_eval: dict[str, float] = {}
        if react_s is not None:
            hub.spot_listeners.append(self._on_spot)

    # ------------------------------------------------------------------ state
    def _window(self, w) -> Window:
        return Window(key=w.slug, asset=w.asset, timeframe=w.label, start=w.start_ts, end=w.end_ts, strike=None,
                      regime=regime_for(w.label, w.resolution, w.start_ts), tick=w.tick_size,
                      min_size=w.min_order_size,
                      fee_rate=w.taker_fee_rate if w.taker_fee_rate is not None else self.cfg.TAKER_FEE_RATE)

    def _books(self, w, now: float) -> tuple[Book, Book] | None:
        up, dn = self.hub.books.get(w.up_token), self.hub.books.get(w.down_token)
        if up is None or dn is None or not (up.synced and dn.synced):
            return None
        feed = self.hub.feed_last_msg.get("polymarket", 0.0)
        out = []
        for own, other in ((up, dn), (dn, up)):
            bb, ba, ob, oa = own.best_bid(), own.best_ask(), other.best_bid(), other.best_ask()
            bids = [x for x in (bb[0] if bb else None, 1.0 - oa[0] if oa else None) if x is not None]
            asks = [x for x in (ba[0] if ba else None, 1.0 - ob[0] if ob else None) if x is not None]
            out.append(Book(bid=round(max(bids), 6) if bids else None, ask=round(min(asks), 6) if asks else None,
                            bid_size=bb[1] if bb else None, ask_size=ba[1] if ba else None,
                            age_s=max(0.0, now - max(own.updated_ts, feed))))
        return out[0], out[1]

    def _spot(self, w, now: float) -> tuple[float | None, float | None, float | None, float | None]:
        """(spot in resolution units, 10 s move in bps, sigma per sqrt(s), TWAP of the oracle so far)."""
        st = self.hub.assets.get(w.asset)
        if st is None or st.fast is None or now - st.fast.recv_ts > self.cfg.SPOT_STALE_S:
            return None, None, None, None
        mid = st.fast.mid
        adj = math.exp(st.oracle_basis.mean) if (w.resolution == "chainlink" and st.oracle_basis.ready) else 1.0
        prev = st.fast_hist.at(now - 10.0)
        ret = (math.log(mid / prev[1]) * 1e4) if prev is not None else None
        sg = st.vol.sigmas()
        sigma = sg[1] if sg else None
        twap = None
        lb = TWAP_LOOKBACK_S.get(regime_for(w.label, w.resolution, w.start_ts))
        if lb and now > w.end_ts - lb:                  # per-second TWAP of the oracle over the elapsed part
            pts = [st.oracle_hist.at(t) for t in range(int(w.end_ts - lb), int(now))]
            pts = [x[1] for x in pts if x is not None]
            twap = sum(pts) / len(pts) if pts else None
        return mid * adj, ret, sigma, twap

    def state(self, c: WinCtx, now: float) -> State | None:
        w = c.w
        books = self._books(w, now)
        if books is None:
            return None
        ref, _ = self.refs.get(w, now)
        c.window.strike = ref.price if ref is not None else None
        spot, ret, sigma, twap = self._spot(w, now)
        fair = None
        if spot and c.window.strike and sigma:
            fair = fair_up(spot, c.window.strike, w.end_ts - now, sigma, c.window.regime, twap)
        orders = [o for o, po in c.orders.values() if po.status != "done" and not po.meta.get("cancelling")]
        return State(now=now, window=c.window, books=books, spot=spot, ret_10s_bps=ret, sigma=sigma, fair_up=fair,
                     inventory=c.inv, orders=orders, max_order_usdc=self.profile.max_order_usdc,
                     max_window_usdc=self.profile.max_window_usdc)

    # ------------------------------------------------------------------ loop
    def on_timer(self) -> None:
        self._evaluate(None)

    def _on_spot(self, asset: str, _ts: float) -> None:
        self._evaluate(asset, throttle=True)

    def _evaluate(self, asset: str | None, throttle: bool = False) -> None:
        now = self.clock.now()
        for w in list(self.hub.markets.values()):
            if w.asset not in self.assets or w.label not in self.labels or not (w.start_ts <= now < w.end_ts):
                continue
            if asset is not None and w.asset != asset:
                continue
            if throttle and now - self._last_eval.get(w.slug, -1e18) < (self.react_s or 0.0):
                continue
            c = self.ctx.get(w.slug)
            if c is None:
                c = self.ctx[w.slug] = WinCtx(w=w, strategy=self.make_strategy(), window=self._window(w))
            s = self.state(c, now)
            if s is None:
                continue
            self._last_eval[w.slug] = now
            self.evaluations += 1
            for it in c.strategy.on_state(s):
                self._apply(c, it, now)

    def _apply(self, c: WinCtx, it, now: float) -> None:
        P = self.U.execution_paper.PaperOrder
        w = c.w
        if isinstance(it, Cancel):
            pair = c.orders.get(it.oid)
            if pair is not None and pair[1].status != "done" and not pair[1].meta.get("cancelling"):
                pair[1].meta["cancelling"] = True
                oid = pair[1].order_id
                self.scheduler.call_at(now + self.cancel_latency_s, lambda: self.exchange.cancel(oid, "strategy"))
            return
        if not isinstance(it, PlaceBid | TakerBuy) or it.shares <= 0:
            return
        k = it.outcome
        token, other = (w.up_token, w.down_token) if k == 0 else (w.down_token, w.up_token)
        kind = "maker" if isinstance(it, PlaceBid) else "taker"
        limit = it.price if kind == "maker" else it.limit
        self._ids += 1
        order = Order(oid=f"b{self._ids}", outcome=k, price=limit, shares=it.shares, posted=now, tag=it.tag)
        po = P(window=w, side=SIDES[k], token=token, other_token=other, kind=kind, limit=limit, shares=it.shares,
               t_send=now, fee_rate=c.window.fee_rate, fee_exp=self.cfg.FEE_EXPONENT, on_done=lambda o: None,
               meta={"slug": w.slug, "oid": order.oid})
        c.orders[order.oid] = (order, po)
        self.exchange.submit(po)

    def _on_fill(self, po, qty: float, price: float, maker: bool) -> None:
        c = self.ctx.get(po.meta.get("slug"))
        if c is None:
            return
        k = 0 if po.side == "up" else 1
        fee = 0.0 if maker else qty * self.cfg.TAKER_FEE_RATE * price * (1 - price)
        c.inv.shares[k] += qty
        c.inv.cost[k] += qty * price + fee
        c.inv.fees += fee
        pair = c.orders.get(po.meta.get("oid"))
        if pair is not None:
            pair[0].filled += qty
        c.fills.append({"t": self.clock.now(), "outcome": k, "price": price, "shares": qty, "fee": fee,
                        "role": "maker" if maker else "taker", "tag": pair[0].tag if pair else ""})

    def results(self, payouts: dict[str, tuple[float, float]]) -> list[dict]:
        out = []
        for slug, c in self.ctx.items():
            pay = payouts.get(slug)
            pnl = (sum(c.inv.shares[k] * pay[k] for k in (0, 1)) - sum(c.inv.cost)) if pay else None
            out.append({"slug": slug, "label": c.w.label, "start": c.w.start_ts, "fills": len(c.fills),
                        "maker_fills": sum(f["role"] == "maker" for f in c.fills),
                        "taker_fills": sum(f["role"] == "taker" for f in c.fills),
                        "usdc": sum(c.inv.cost), "fees": c.inv.fees, "shares_up": c.inv.shares[0],
                        "shares_down": c.inv.shares[1], "pnl": pnl, "resolved": pay is not None})
        return out


def replay(U: Any, paths: list[str], make_strategy: Callable[[], Any], profile: RiskProfile,
           assets: tuple[str, ...] = ("btc",), labels: tuple[str, ...] = ("5m", "15m"),
           settings: dict[str, Any] | None = None, react_s: float | None = None,
           cancel_latency_s: float = 0.1) -> tuple[StrategyHost, dict[str, str]]:
    """Feed recorded updown ticks through updown's hub and our strategy host (virtual time)."""
    cfg = U.config.load_settings(dotenv=False, **(settings or {}))
    cfg = dataclasses.replace(cfg, ASSETS=list(assets))
    clock = U.clock.ReplayClock()
    sched = U.scheduler.ReplayScheduler(clock)
    hub = U.data_hub.MarketDataHub(cfg, clock)
    refs = U.data_reference.ReferenceResolver(cfg, hub)
    host = StrategyHost(U, cfg, hub, clock, sched, refs, make_strategy, profile, assets, labels, react_s,
                        cancel_latency_s)
    parsers = U.data_parsers.build_parsers(cfg.BINANCE_SYMBOLS, cfg.COINBASE_PRODUCTS, cfg.CHAINLINK_SYMBOLS)
    MW = U.data_markets.MarketWindow
    winners: dict[str, str] = {}
    first = None
    for ts, src, raw in U.data_recorder.iter_recording(paths):
        if first is None:
            first = ts
            sched.every(1.0, host.on_timer, ts)
        sched.run_until(ts)
        clock.advance_to(ts)
        if src == "@markets":
            hub.set_markets(MW.from_dict(d) for d in U.fastjson.loads(raw))
            refs.prune(set(hub.markets))
        elif src == "@open":
            hub.on_feed_open(raw, ts)
        elif src == "@close":
            hub.on_feed_close(raw, ts)
        elif src == "@outcome":
            d = U.fastjson.loads(raw)
            winners[d["slug"]] = d["winner"]
        else:
            parser = parsers.get(src)
            if parser is None:
                continue
            hub.note_message(src, ts)
            events = U.data_parsers.parse_safely(parser, raw, ts, [])
            if events:
                hub.apply_many(events)
    return host, winners


async def run_paper(U: Any, make_strategy: Callable[[], Any], profile: RiskProfile, out_dir: Path,
                    assets: tuple[str, ...] = ("btc",), labels: tuple[str, ...] = ("5m", "15m"),
                    settings: dict[str, Any] | None = None, duration_s: float | None = None,
                    react_s: float | None = 0.05, record: bool = False) -> list[dict]:
    """Live paper trading (DRY_RUN: no order can leave this process) with updown's feeds and discovery.

    Stops on Ctrl+C / SIGTERM, after `duration_s`, or when the kill-switch file (updown's KILL_SWITCH_FILE,
    default STOP) appears; new orders also stop for the UTC day once the day's realized loss reaches the
    profile's daily stop. Results per window go to out_dir/paper_windows.json."""
    import asyncio
    import json
    import signal as _signal
    import time

    gamma_mod = importlib.import_module("latarb.data.gamma")
    feeds_mod = importlib.import_module("latarb.data.feeds")
    cfg = U.config.load_settings(dotenv=False, **(settings or {}))
    cfg = dataclasses.replace(cfg, ASSETS=list(assets))
    clock = U.clock.WallClock()
    sched = U.scheduler.LoopScheduler(clock)
    hub = U.data_hub.MarketDataHub(cfg, clock)
    refs = U.data_reference.ReferenceResolver(cfg, hub)
    host = StrategyHost(U, cfg, hub, clock, sched, refs, make_strategy, profile, assets, labels, react_s)
    out_dir.mkdir(parents=True, exist_ok=True)
    recorder = U.data_recorder.TickRecorder(str(out_dir / "ticks"), cfg.RECORD_ROTATE_S) if record else None
    feeds = feeds_mod.FeedSet(cfg, clock, hub, recorder)
    gamma = gamma_mod.GammaClient(cfg)
    discovery = gamma_mod.Discovery(cfg, gamma, clock)
    payouts: dict[str, tuple[float, float]] = {}
    day_pnl: dict[str, float] = {}
    stop = asyncio.Event()
    if os.path.exists(cfg.KILL_SWITCH_FILE):
        log.critical("kill-switch file %r exists - refusing to start", cfg.KILL_SWITCH_FILE)
        return []
    log.warning("PAPER (DRY_RUN): orders are simulated against live books with a virtual bankroll of %.0f; "
                "no real order can be sent. profile=%s", profile.bankroll, profile.name)

    def halted() -> bool:
        day = time.strftime("%Y-%m-%d", time.gmtime(clock.now()))
        return os.path.exists(cfg.KILL_SWITCH_FILE) or day_pnl.get(day, 0.0) <= -profile.daily_stop_usdc

    strategy_eval = host._evaluate

    def guarded(asset, throttle=False):
        if halted():
            for c in host.ctx.values():               # pull every quote, place nothing new
                for oid, (_o, po) in list(c.orders.items()):
                    if po.status != "done":
                        host.exchange.cancel(po.order_id, "halt")
            return
        strategy_eval(asset, throttle)

    host._evaluate = guarded

    async def every(period: float, fn) -> None:
        while not stop.is_set():
            try:
                await fn()
            except Exception as e:  # noqa: BLE001 - a periodic job must survive transient failures
                log.warning("paper job failed: %s", e)
            await asyncio.sleep(period)

    async def discovery_job() -> None:
        await asyncio.to_thread(discovery.refresh)
        relevant = discovery.relevant(clock.now())
        hub.set_markets(relevant)
        refs.prune(set(hub.markets))
        await feeds.poly.set_tokens(t for w in relevant for t in w.tokens())
        feeds.poly.check_snapshots(clock.now())

    async def timer_job() -> None:
        host.on_timer()

    async def outcome_job() -> None:
        now = clock.now()
        for slug, c in list(host.ctx.items()):
            if slug in payouts or now < c.w.end_ts + 60 or not c.fills:
                continue
            winner = await asyncio.to_thread(gamma_mod.fetch_winner, gamma, slug)
            if winner:
                payouts[slug] = (1.0, 0.0) if winner == "up" else (0.0, 1.0)
                pnl = sum(c.inv.shares[k] * payouts[slug][k] for k in (0, 1)) - sum(c.inv.cost)
                day = time.strftime("%Y-%m-%d", time.gmtime(now))
                day_pnl[day] = day_pnl.get(day, 0.0) + pnl
                log.info("settled %s: winner %s pnl %+.2f (day %+.2f)", slug, winner, pnl, day_pnl[day])

    async def status_job() -> None:
        res = host.results(payouts)
        done = [r for r in res if r["resolved"]]
        log.info("paper: windows %d, settled %d, pnl %+.2f, open usdc %.2f, evaluations %d", len(res), len(done),
                 sum(r["pnl"] for r in done), sum(r["usdc"] for r in res if not r["resolved"]), host.evaluations)
        (out_dir / "paper_windows.json").write_text(json.dumps(res, indent=1, default=float), encoding="utf-8")

    tasks = [asyncio.create_task(f.run(), name=f.name) for f in feeds.all()]
    tasks += [asyncio.create_task(every(cfg.DISCOVERY_TICK_S, discovery_job)),
              asyncio.create_task(every(1.0, timer_job)),
              asyncio.create_task(every(cfg.RESOLVE_INTERVAL_S, outcome_job)),
              asyncio.create_task(every(30.0, status_job))]
    loop = asyncio.get_running_loop()
    for sig in (_signal.SIGINT, _signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        if duration_s:
            try:
                await asyncio.wait_for(stop.wait(), timeout=duration_s)
            except TimeoutError:
                pass
        else:
            await stop.wait()
    finally:
        stop.set()
        discovery.stop()
        host.exchange.cancel_all("shutdown")
        for f in feeds.all():
            await f.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if recorder is not None:
            recorder.close()
        await status_job()
    return host.results(payouts)
