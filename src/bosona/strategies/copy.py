"""(a) / (b) Copying his fills.

(a) ideal copy is not a strategy: the engine books his fills as they happened (his price, size and fee).
(b) delayed copy reacts to each of his fills once we could have seen it (stage 3: on-chain logs 2.0 s after the
    CLOB match, plus our order latency):
      mode "taker": buy the same outcome and size at the ask then, up to his price + max_slip, taker fee;
      mode "maker": rest a bid at his price until the window closes (we queue behind whatever is left).
A signal is a dict with outcome (0 Up / 1 Down), price, size, role and match time (see engine.HisFill).
"""

from __future__ import annotations

from dataclasses import dataclass

from bosona.strategies.base import Cancel, Intent, PlaceBid, State, TakerBuy
from bosona.strategies.profiles import cap_shares


@dataclass
class CopyParams:
    mode: str = "taker"          # taker | maker
    max_slip: float = 0.05       # taker: highest price paid above his price
    scale: float = 1.0           # our size / his size, before the risk caps
    roles: tuple[str, ...] = ("maker", "taker")   # which of his fills to copy


class DelayedCopy:
    def __init__(self, params: CopyParams | None = None) -> None:
        self.p = params or CopyParams()
        self.name = f"copy_{self.p.mode}"

    def reset(self) -> None:
        pass

    def on_state(self, s: State) -> list[Intent]:
        if s.tau <= 0:
            return [Cancel(o.oid) for o in s.orders]
        return []

    def on_signal(self, s: State, fill) -> list[Intent]:
        if fill.role not in self.p.roles:
            return []
        room = s.max_window_usdc - s.inventory.at_risk - sum(o.remaining * o.price for o in s.orders)
        if self.p.mode == "taker":
            limit = min(fill.price + self.p.max_slip, 0.99)
            shares = cap_shares(fill.size * self.p.scale, limit, s.max_order_usdc, room, s.window.min_size)
            return [TakerBuy(fill.outcome, limit, shares, f"copy@{fill.price:.6f}")] if shares > 0 else []
        shares = cap_shares(fill.size * self.p.scale, fill.price, s.max_order_usdc, room, s.window.min_size)
        return [PlaceBid(fill.outcome, fill.price, shares, f"copy@{fill.price:.6f}")] if shares > 0 else []
