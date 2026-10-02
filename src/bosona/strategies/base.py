"""Strategy interface shared by the tape backtest (bosona.backtest.engine) and the updown adapter.

A strategy sees one Up/Down window at a time through a `State` and answers with intents. It never talks to a
venue: the host (the backtest engine, or the updown paper exchange) turns intents into orders, applies latency,
fees, queues and risk caps, and reports fills back through the next `State`. The same strategy object therefore
runs on history, on recorded ticks and live in paper (DRY_RUN) mode.

Outcomes are indexed 0 = Up, 1 = Down. Prices are per share in USDC; every order is a BUY (the wallet never
sells: pairs are merged, the rest is held to resolution).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

UP, DOWN = 0, 1


@dataclass
class Book:
    """Effective top of book of one outcome token (own book and the complement of the other token)."""

    bid: float | None
    ask: float | None
    bid_size: float | None = None      # unknown when the book is reconstructed from the tape
    ask_size: float | None = None
    age_s: float = 0.0                 # how old the information behind the prices is


@dataclass
class Window:
    key: str                           # condition id (or slug)
    asset: str
    timeframe: str
    start: float
    end: float
    strike: float | None               # price to beat, resolution-source units
    regime: str | None                 # chainlink_spot | chainlink_twap30 | chainlink_twap60 | binance_1h | ...
    tick: float = 0.01
    min_size: float = 5.0              # orderMinSize, shares
    fee_rate: float = 0.07


@dataclass
class Order:
    oid: str
    outcome: int
    price: float
    shares: float
    filled: float = 0.0
    posted: float = 0.0
    tag: str = ""

    @property
    def remaining(self) -> float:
        return max(0.0, self.shares - self.filled)


@dataclass
class Inventory:
    shares: list[float] = field(default_factory=lambda: [0.0, 0.0])
    cost: list[float] = field(default_factory=lambda: [0.0, 0.0])     # USDC paid, taker fees included
    fees: float = 0.0

    @property
    def net(self) -> float:
        """Up shares - Down shares (> 0: long Up)."""
        return self.shares[UP] - self.shares[DOWN]

    @property
    def at_risk(self) -> float:
        """Money that can still be lost: cost minus what merging the pairs returns ($1 per pair)."""
        return sum(self.cost) - min(self.shares)


@dataclass
class State:
    now: float
    window: Window
    books: tuple[Book, Book]
    spot: float | None                 # resolution-source units (Binance adjusted by the basis)
    ret_10s_bps: float | None          # spot move over the last 10 s
    sigma: float | None                # log-volatility per sqrt(second)
    fair_up: float | None              # host's model probability of Up (bosona.backtest.pricing.fair_up)
    inventory: Inventory
    orders: list[Order]
    max_order_usdc: float = float("inf")   # risk caps of the profile (see profiles.py)
    max_window_usdc: float = float("inf")

    @property
    def tau(self) -> float:
        return self.window.end - self.now

    def fair(self, k: int) -> float | None:
        if self.fair_up is None:
            return None
        return self.fair_up if k == UP else 1.0 - self.fair_up


@dataclass
class PlaceBid:
    outcome: int
    price: float
    shares: float
    tag: str = ""


@dataclass
class Cancel:
    oid: str


@dataclass
class TakerBuy:
    outcome: int
    limit: float
    shares: float
    tag: str = ""


Intent = PlaceBid | Cancel | TakerBuy


class Strategy(Protocol):
    name: str

    def on_state(self, s: State) -> list[Intent]: ...

    def on_signal(self, s: State, signal: Any) -> list[Intent]: ...
