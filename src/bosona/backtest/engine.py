"""Tape backtest: replays one window against its trade tape, second by second.

Decisions every `step_s` on what was known then: the book reconstructed from prints matched before t (proxy.py),
Binance closes of the last full second in resolution-source units, his fills once we could have seen them
(CLOB match + `detect_after_match_s`, stage 3). Executions:

  taker  arrives `order_latency_s` after the decision and pays the ask at that moment (median of the buy prints
         within 1 s, + `ask_bias`; validated on live books), walking `depth_per_tick` shares per 1c level up to the limit
         (BTC 5m: median 222 shares at the touch, stage 3 books); taker fee rate * p * (1 - p) per level;
  maker  a bid is live from `order_latency_s` after the decision until `cancel_latency_s` after its cancel or the
         window end. Prints that sold to the bids fill it: taker sells of the token at q and taker buys of the
         other token at q, minted against the bids at 1 - q (87% of his maker fills came this way). With e the
         effective price of a print and p our bid:
             e < p   filled (a better bid is hit first), up to the print's size;
             e == p  filled after the queue ahead is used up — queue_mode: front = nothing ahead,
                     touch = `queue_shares` ahead when joining the best bid (0 when improving it),
                     through = only prints below the bid fill;
             e > p   not filled.
         Our fills do not remove liquidity from the tape (small sizes; documented optimism).

Positions: Up and Down shares and their cost; a pair is worth $1 whether merged or held. PnL at resolution =
shares x payout - cost. Mode "ideal" books his own fills as they happened (variant (a)).
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from bosona.backtest.data import HisFill, WindowData
from bosona.backtest.pricing import TWAP_LOOKBACK_S, fair_up_array, taker_fee
from bosona.strategies.base import (
    Book,
    Cancel,
    Inventory,
    Order,
    PlaceBid,
    State,
    TakerBuy,
)
from bosona.strategies.profiles import RiskProfile

EPS = 1e-9


@dataclass
class ExecParams:
    order_latency_s: float = 0.3
    cancel_latency_s: float = 0.3
    detect_after_match_s: float = 2.0
    queue_mode: str = "touch"            # front | touch | through
    queue_shares: float = 222.0
    depth_per_tick: float = 222.0
    max_quote_age_s: float = 10.0
    ask_bias: float = 0.0                # added to the reconstructed ask (calibration against live books)
    step_s: float = 1.0
    vol_window_s: int = 300
    oracle_noise: float = 1e-4


@dataclass
class Fill:
    t: float
    outcome: int
    price: float
    shares: float
    fee: float
    role: str
    tag: str


@dataclass
class WindowResult:
    variant: str
    key: str
    timeframe: str
    start: int
    sample: str
    winner: str | None
    fills: list[Fill] = field(default_factory=list)
    shares: tuple[float, float] = (0.0, 0.0)
    cost: tuple[float, float] = (0.0, 0.0)
    fees: float = 0.0
    pnl: float = 0.0

    @property
    def usdc(self) -> float:
        return self.cost[0] + self.cost[1]

    def summary(self) -> dict[str, Any]:
        maker = [f for f in self.fills if f.role == "maker"]
        taker = [f for f in self.fills if f.role == "taker"]
        copied = [f for f in self.fills if f.tag.startswith("copy@")]
        return {"copy_shares": sum(f.shares for f in copied),
                "copy_premium_usdc": sum(f.shares * (f.price - float(f.tag[5:])) + f.fee for f in copied),"variant": self.variant, "key": self.key, "timeframe": self.timeframe, "start": self.start,
                "sample": self.sample, "winner": self.winner, "fills": len(self.fills), "maker_fills": len(maker),
                "taker_fills": len(taker), "usdc": self.usdc, "fees": self.fees, "pnl": self.pnl,
                "maker_usdc": sum(f.price * f.shares for f in maker),
                "taker_usdc": sum(f.price * f.shares + f.fee for f in taker),
                "shares_up": self.shares[0], "shares_down": self.shares[1],
                "paired": min(self.shares), "net": self.shares[0] - self.shares[1]}


@dataclass
class _Live:
    order: Order
    active_from: float
    cancel_at: float = math.inf
    queue_ahead: float | None = None


class WindowSim:
    def __init__(self, wd: WindowData, strategy, ep: ExecParams, profile: RiskProfile | None, variant: str) -> None:
        self.wd, self.s, self.ep, self.variant = wd, strategy, ep, variant
        self.w = wd.window
        self.max_order = profile.max_order_usdc if profile else math.inf
        self.max_window = profile.max_window_usdc if profile else math.inf
        self.inv = Inventory()
        self.orders: dict[str, _Live] = {}
        self.fills: list[Fill] = []
        self._ids = itertools.count(1)
        self._prints = [wd.proxy.effective_sells(k) for k in (0, 1)]
        self._ptr = [0, 0]
        self._precompute()

    # ------------------------------------------------------------------ inputs on the decision grid
    def _precompute(self) -> None:
        w, wd, ep = self.w, self.wd, self.ep
        self.grid = np.arange(w.start, w.end, ep.step_s, dtype=float)
        g = self.grid
        self.bk = []
        for k in (0, 1):
            a, aa = wd.proxy.ask(k, g)
            b, ba = wd.proxy.bid(k, g)
            self.bk.append((a, aa, b, ba))
        idx = g.astype(np.int64) - 1 - wd.spot_t0
        spot = wd.spot[idx]
        self.spot = spot
        self.ret10 = (np.log(spot) - np.log(wd.spot[idx - 10])) * 1e4
        lr = np.diff(np.log(wd.spot), prepend=np.nan)
        lr[0] = 0.0
        c2 = np.concatenate([[0.0], np.cumsum(lr * lr)])
        n = ep.vol_window_s
        self.sigma = np.sqrt((c2[idx + 1] - c2[np.clip(idx + 1 - n, 0, None)]) / n)
        twap = None
        lb = TWAP_LOOKBACK_S.get(w.regime or "")
        if lb:
            cs = np.concatenate([[0.0], np.cumsum(wd.spot)])
            a0 = int(w.end) - lb - wd.spot_t0
            k_now = idx + 1                                   # seconds [end - lb, t) are known
            cnt = k_now - a0
            twap = np.where(cnt > 0, (cs[np.clip(k_now, 0, None)] - cs[a0]) / np.maximum(cnt, 1), np.nan)
        self.fair = fair_up_array(spot, w.strike, w.end - g, self.sigma, w.regime, twap, ep.oracle_noise)

    def _i(self, t: float) -> int:
        return min(len(self.grid) - 1, max(0, int((t - self.w.start) // self.ep.step_s)))

    def _state(self, t: float) -> State:
        i = self._i(t)
        books = []
        for k in (0, 1):
            a, aa, b, ba = (x[i] for x in self.bk[k])
            books.append(Book(bid=None if np.isnan(b) else float(b), ask=None if np.isnan(a) else float(a),
                              age_s=float(max(aa, ba))))
        f = self.fair[i]
        orders = [lv.order for lv in self.orders.values() if lv.cancel_at == math.inf]   # not being cancelled
        return State(now=t, window=self.w, books=(books[0], books[1]),
                     spot=float(self.spot[i]), ret_10s_bps=float(self.ret10[i]), sigma=float(self.sigma[i]),
                     fair_up=None if not np.isfinite(f) else float(f), inventory=self.inv, orders=orders,
                     max_order_usdc=self.max_order, max_window_usdc=self.max_window)

    # ------------------------------------------------------------------ execution
    def _book(self, t: float, k: int, price: float, shares: float, fee: float, role: str, tag: str) -> None:
        self.inv.shares[k] += shares
        self.inv.cost[k] += price * shares + fee
        self.inv.fees += fee
        self.fills.append(Fill(t, k, price, shares, fee, role, tag))

    def _taker(self, t: float, it: TakerBuy) -> None:
        ta = t + self.ep.order_latency_s
        if ta >= self.w.end:
            return
        ask, dist = self.wd.proxy.ask_exec(it.outcome, ta, before=self.w.end)
        if not np.isfinite(ask) or dist > self.ep.max_quote_age_s:
            return
        ask = round(min(0.99, ask + self.ep.ask_bias), 6)
        if ask > it.limit + EPS:
            return
        left, level, tick = it.shares, 0, self.w.tick
        while left > EPS:
            p = round(ask + level * tick, 6)
            if p > it.limit + EPS or p >= 1.0:
                break
            q = min(left, self.ep.depth_per_tick)
            self._book(ta, it.outcome, p, q, q * taker_fee(p, self.w.fee_rate), "taker", it.tag)
            left -= q
            level += 1

    def _place(self, t: float, it: PlaceBid) -> None:
        if it.shares <= 0 or t + self.ep.order_latency_s >= self.w.end:
            return
        o = Order(oid=f"o{next(self._ids)}", outcome=it.outcome, price=it.price, shares=it.shares, posted=t, tag=it.tag)
        self.orders[o.oid] = _Live(o, active_from=t + self.ep.order_latency_s)

    def _cancel(self, t: float, oid: str) -> None:
        lv = self.orders.get(oid)
        if lv is not None and lv.cancel_at == math.inf:
            lv.cancel_at = t + self.ep.cancel_latency_s

    def _queue(self, lv: _Live) -> float:
        mode = self.ep.queue_mode
        if mode == "front":
            return 0.0
        if mode == "through":
            return math.inf
        bid, _ = self.wd.proxy.bid(lv.order.outcome, lv.active_from)
        if np.isfinite(bid[0]) and lv.order.price > bid[0] + EPS:
            return 0.0                                   # improves the touch: nobody ahead
        return self.ep.queue_shares

    def _match(self, until: float) -> None:
        """Feed prints matched up to `until` to the live bids, in time order."""
        end = min(until, self.w.end)
        for k in (0, 1):
            t, e, z = self._prints[k]
            i = self._ptr[k]
            while i < len(t) and t[i] <= end:
                self._hit(k, t[i], e[i], z[i])
                i += 1
            self._ptr[k] = i
        for oid in [o for o, lv in self.orders.items() if lv.cancel_at <= until or lv.order.remaining <= EPS]:
            del self.orders[oid]

    def _hit(self, k: int, tp: float, e: float, size: float) -> None:
        live = [lv for lv in self.orders.values()
                if lv.order.outcome == k and lv.active_from <= tp < lv.cancel_at and lv.order.remaining > EPS]
        live.sort(key=lambda lv: -lv.order.price)        # the best bid is hit first
        for lv in live:
            if size <= EPS:
                return
            o = lv.order
            if lv.queue_ahead is None:
                lv.queue_ahead = self._queue(lv)
            if e < o.price - EPS:
                q = min(o.remaining, size)
            elif abs(e - o.price) <= EPS:
                used = min(size, lv.queue_ahead)
                lv.queue_ahead -= used
                size -= used
                q = min(o.remaining, size)
            else:
                continue
            if q > EPS:
                o.filled += q
                size -= q
                self._book(tp, k, o.price, q, 0.0, "maker", o.tag)

    # ------------------------------------------------------------------ run
    def run(self, mode: str = "strategy") -> WindowResult:
        w, wd = self.w, self.wd
        if mode == "ideal":
            for f in wd.his:
                self.inv.shares[f.outcome] += f.size
                self.inv.cost[f.outcome] += f.usdc
                self.inv.fees += f.fee
                self.fills.append(Fill(f.match_t, f.outcome, f.price, f.size, f.fee, f.role, "his"))
            return self._result()
        events: list[tuple[float, int, Any]] = [(float(t), 1, None) for t in self.grid]
        for f in wd.his:
            td = f.match_t + self.ep.detect_after_match_s
            if w.start <= td < w.end:
                events.append((td, 0, f))
        events.sort(key=lambda x: (x[0], x[1]))
        reset = getattr(self.s, "reset", None)
        if reset:
            reset()
        for t, kind, payload in events:
            self._match(t)
            st = self._state(t)
            intents = self.s.on_signal(st, payload) if kind == 0 else self.s.on_state(st)
            for it in intents:
                if isinstance(it, Cancel):
                    self._cancel(t, it.oid)
                elif isinstance(it, PlaceBid):
                    self._place(t, it)
                elif isinstance(it, TakerBuy):
                    self._taker(t, it)
        self._match(w.end)
        return self._result()

    def _result(self) -> WindowResult:
        wd = self.wd
        pnl = sum(self.inv.shares[k] * wd.payout[k] for k in (0, 1)) - sum(self.inv.cost)
        return WindowResult(variant=self.variant, key=self.w.key, timeframe=self.w.timeframe, start=int(self.w.start),
                            sample=wd.sample, winner=wd.winner, fills=self.fills,
                            shares=(self.inv.shares[0], self.inv.shares[1]),
                            cost=(self.inv.cost[0], self.inv.cost[1]), fees=self.inv.fees, pnl=pnl)


def simulate(wd: WindowData, strategy, ep: ExecParams, profile: RiskProfile | None, variant: str,
             mode: str = "strategy") -> WindowResult:
    return WindowSim(wd, strategy, ep, profile, variant).run(mode)


def his_fill_signal(f: HisFill) -> HisFill:      # signals are his fills as they are
    return f
