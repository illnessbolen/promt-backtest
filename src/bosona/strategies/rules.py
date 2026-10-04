"""(c) Own strategy: the rules found in stage 4 as explicit entry, exit and size conditions.

Entry (maker): from `start_after_s` after the window opens until `stop_before_close_s` before it closes, keep
    one bid on each outcome at  min(best bid + improve_ticks * tick, fair - margin)  (anchor "touch"; anchor
    "fair": fair - margin, the book only keeps it below the ask), never crossing the ask,
    within [min_price, max_price], on a book no older than `max_book_age_s`. Re-quote when the target moves.
    He quotes at the best bid (87% of his maker fills, spread 1c) and his profit sits where the price is below
    the model (stage 4); `margin` is that gap.
Size: `size_shares` per quote (his tiers: BTC 5m 166 / 249 / 498, BTC 15m 100 / 150 / 300 shares), capped by
    the risk profile; no new bid on the side that would push |net| beyond `max_net_shares`.
Hedge (taker): when |net| >= hedge_min_net and the spot moved against the position by >= hedge_move_bps over
    the last 10 s, buy the other outcome for the whole net at up to ask + hedge_slip (his reducing taker fills:
    +3.0% after adverse moves, -6.1% after favourable ones, median size 93% of the net).
Directional (taker, off by default): after a 10 s spot move >= taker_move_bps, buy the favoured outcome when
    fair - (ask + fee) >= taker_edge (his taker opens towards the move: +6.6% ± 2.4%, 0.5% of his fills).
Exit: none before resolution. Pairs are merged (PnL-neutral, frees capital); the rest is held. All bids are
    cancelled `stop_before_close_s` before the close (his fills after the close lose 0.7%).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, fields
from typing import Any

from bosona.backtest.pricing import taker_fee
from bosona.strategies.base import DOWN, UP, Cancel, Intent, PlaceBid, State, TakerBuy
from bosona.strategies.profiles import cap_shares


@dataclass
class RulesParams:
    quote: bool = True
    anchor: str = "touch"          # touch: min(best bid + improve, fair - margin); fair: fair - margin (book only caps)
    margin: float = 0.02
    improve_ticks: int = 0
    size_shares: float = 150.0
    start_after_s: float = 5.0
    stop_before_close_s: float = 15.0
    min_price: float = 0.02
    max_price: float = 0.98
    max_net_shares: float = 600.0
    max_book_age_s: float = 5.0
    hedge: bool = True
    hedge_move_bps: float = 3.0
    hedge_min_net: float = 50.0
    hedge_slip: float = 0.02
    hedge_cooldown_s: float = 5.0
    taker: bool = False
    taker_move_bps: float = 5.0
    taker_edge: float = 0.03
    taker_shares: float = 50.0
    taker_cooldown_s: float = 10.0


def param_overrides(items: list[str]) -> dict[str, Any]:
    """`k=v` strings (CLI --param) -> RulesParams fields with the field's type."""
    types = {f.name: type(getattr(RulesParams(), f.name)) for f in fields(RulesParams)}
    out: dict[str, Any] = {}
    for item in items:
        k, sep, v = item.partition("=")
        if not sep or k not in types:
            raise ValueError(f"unknown RulesParams override {item!r}; fields: {', '.join(types)}")
        out[k] = v.lower() in ("1", "true", "yes") if types[k] is bool else types[k](v)
    return out


def floor_tick(p: float, tick: float) -> float:
    return round(math.floor(p / tick + 1e-9) * tick, 6)


class BosonaRules:
    name = "rules"

    def __init__(self, params: RulesParams | None = None) -> None:
        self.p = params or RulesParams()
        self._last_hedge = -math.inf
        self._last_taker = -math.inf

    def reset(self) -> None:
        self._last_hedge = self._last_taker = -math.inf

    def on_signal(self, s: State, signal) -> list[Intent]:
        return []

    # ------------------------------------------------------------------ decisions
    def on_state(self, s: State) -> list[Intent]:
        p, w = self.p, s.window
        active = s.now >= w.start + p.start_after_s and s.tau > p.stop_before_close_s and s.fair_up is not None
        if not active:
            return [Cancel(o.oid) for o in s.orders]
        out: list[Intent] = []
        out += self._hedge(s)
        out += self._directional(s)
        if p.quote:
            out += self._quotes(s)
        return out

    def _target(self, s: State, k: int) -> float | None:
        p, w, b = self.p, s.window, s.books[k]
        fair = s.fair(k)
        if b.bid is None or fair is None or b.age_s > p.max_book_age_s:
            return None
        toward = s.inventory.net if k == UP else -s.inventory.net    # buying k moves the net this way
        if toward >= p.max_net_shares:
            return None
        price = fair - p.margin if p.anchor == "fair" else min(b.bid + p.improve_ticks * w.tick, fair - p.margin)
        if b.ask is not None:
            price = min(price, b.ask - w.tick)
        price = floor_tick(price, w.tick)
        if not p.min_price <= price <= p.max_price:
            return None
        return price

    def _quotes(self, s: State) -> list[Intent]:
        out: list[Intent] = []
        reserved = sum(o.remaining * o.price for o in s.orders)
        room = s.max_window_usdc - s.inventory.at_risk - reserved
        for k in (UP, DOWN):
            target = self._target(s, k)
            mine = [o for o in s.orders if o.outcome == k]
            keep = [o for o in mine if target is not None and abs(o.price - target) < 1e-9 and o.remaining > 0]
            for o in mine:
                if o not in keep:
                    out.append(Cancel(o.oid))
                    room += o.remaining * o.price
            if target is not None and not keep:
                shares = cap_shares(self.p.size_shares, target, s.max_order_usdc, room, s.window.min_size)
                if shares > 0:
                    out.append(PlaceBid(k, target, shares, "quote"))
                    room -= shares * target
        return out

    def _hedge(self, s: State) -> list[Intent]:
        p, net, r = self.p, s.inventory.net, s.ret_10s_bps
        if not p.hedge or abs(net) < p.hedge_min_net or r is None or s.now - self._last_hedge < p.hedge_cooldown_s:
            return []
        against = (net > 0 and r <= -p.hedge_move_bps) or (net < 0 and r >= p.hedge_move_bps)
        k = DOWN if net > 0 else UP
        b = s.books[k]
        if not against or b.ask is None or b.age_s > p.max_book_age_s:
            return []
        shares = float(int(abs(net)))
        if shares < s.window.min_size:
            return []
        self._last_hedge = s.now
        return [TakerBuy(k, min(b.ask + p.hedge_slip, 0.99), shares, "hedge")]

    def _directional(self, s: State) -> list[Intent]:
        p, r = self.p, s.ret_10s_bps
        if not p.taker or r is None or abs(r) < p.taker_move_bps or s.now - self._last_taker < p.taker_cooldown_s:
            return []
        k = UP if r > 0 else DOWN
        b, fair = s.books[k], s.fair(k)
        if b.ask is None or fair is None or b.age_s > p.max_book_age_s:
            return []
        if fair - (b.ask + taker_fee(b.ask, s.window.fee_rate)) < p.taker_edge:
            return []
        room = s.max_window_usdc - s.inventory.at_risk - sum(o.remaining * o.price for o in s.orders)
        shares = cap_shares(p.taker_shares, b.ask, s.max_order_usdc, room, s.window.min_size)
        if shares <= 0:
            return []
        self._last_taker = s.now
        return [TakerBuy(k, min(b.ask + s.window.tick, 0.99), shares, "taker")]
