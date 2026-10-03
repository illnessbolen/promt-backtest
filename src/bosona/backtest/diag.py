"""Where the PnL of one window comes from; shared by the tape engine and the updown host (stage 5).

Pairs: the matched Up + Down shares at the average cost of each side (fees included); the rest of the PnL is the
unpaired (directional) part. Per role (maker / taker), the edge of the fills against the model, i.e. fair value of
the bought outcome - price - fee: at the fill, MARKOUT_S later and at resolution (the payout; these sum to the PnL).

The work is split in two so that a replay worker can summarize its fills before the outcomes are known:
fill_stats (no payout needed) and settle (adds the payout).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from typing import Any

MARKOUT_S = 10.0          # horizon of the post-fill markout
EPS = 1e-9
ROLES = ("maker", "taker")
_STATS = ("shares", "fair_shares", "edge", "mark", "q_up", "q_down", "c_up", "c_down")


def fill_stats(fills: Iterable[Any], fair_up_at: Callable[[float], float], horizon: float = MARKOUT_S) -> dict[str, float]:
    """Per role: shares, shares and cost (price x shares + fee) of each outcome, and the model edge at the fill and
    `horizon` seconds later over the fills whose fair value is known then. `fills` have t, outcome (0 Up / 1 Down),
    price, shares, fee and role; `fair_up_at(t)` is the model's P(Up) known at t (NaN when unknown)."""
    d = {f"{r}_{k}": 0.0 for r in ROLES for k in _STATS}
    for f in fills:
        r = "maker" if f.role == "maker" else "taker"
        side = "up" if f.outcome == 0 else "down"
        d[f"{r}_shares"] += f.shares
        d[f"{r}_q_{side}"] += f.shares
        d[f"{r}_c_{side}"] += f.shares * f.price + f.fee
        up = (fair_up_at(f.t), fair_up_at(f.t + horizon))
        fair = up if f.outcome == 0 else (1.0 - up[0], 1.0 - up[1])
        if math.isfinite(fair[0]) and math.isfinite(fair[1]):
            d[f"{r}_fair_shares"] += f.shares
            d[f"{r}_edge"] += f.shares * (fair[0] - f.price) - f.fee
            d[f"{r}_mark"] += f.shares * (fair[1] - f.price) - f.fee
    return d


def settle(stats: dict[str, float], shares: tuple[float, float] | list[float], cost: tuple[float, float] | list[float],
           payout: tuple[float, float]) -> dict[str, float]:
    """PnL at resolution and its split, from fill_stats and the position (shares and cost of each outcome)."""
    pnl = shares[0] * payout[0] + shares[1] * payout[1] - cost[0] - cost[1]
    paired = min(shares)
    pair_cost = paired * (cost[0] / shares[0] + cost[1] / shares[1]) if paired > EPS else 0.0
    d = {"pnl": pnl, "pair_cost_usdc": pair_cost, "pair_pnl": paired - pair_cost,
         "unpaired_pnl": pnl - (paired - pair_cost)}
    for r in ROLES:
        d.update({f"{r}_{k}": stats[f"{r}_{k}"] for k in ("shares", "fair_shares", "edge", "mark")})
        d[f"{r}_real"] = (payout[0] * stats[f"{r}_q_up"] + payout[1] * stats[f"{r}_q_down"]
                          - stats[f"{r}_c_up"] - stats[f"{r}_c_down"])
    return d


def pnl_split(fills: Iterable[Any], shares: tuple[float, float] | list[float], cost: tuple[float, float] | list[float],
              payout: tuple[float, float], fair_up_at: Callable[[float], float],
              horizon: float = MARKOUT_S) -> dict[str, float]:
    """fill_stats + settle in one go (the tape engine knows the outcome); without the PnL itself."""
    d = settle(fill_stats(fills, fair_up_at, horizon), shares, cost, payout)
    del d["pnl"]
    return d
