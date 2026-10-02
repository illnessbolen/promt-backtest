"""Risk profiles conservative / moderate / aggressive.

The fractions are those of updown (latarb/risk/limits.py, `PROFILES`), so a strategy sized here is sized the same
way inside updown: every cap is a fraction of the CURRENT bankroll. Inside updown the adapter takes the limits
from updown itself (`resolve_limits`), with its hard bounds and RISK_* overrides; this copy is for standalone runs.

How they apply to a two-sided quoting strategy:
  * bet_pct       -> the most one order may cost (shares x price);
  * exposure_pct  -> the most money at risk across open windows; split evenly over `concurrent_windows`
                     (BTC 5m and 15m run at the same time) to give a per-window cap;
  * daily_stop_pct -> stop for the day after losing this share of the day-start bankroll (live / paper only:
                     the history sample is not a contiguous sequence of days).
"""

from __future__ import annotations

from dataclasses import dataclass

PROFILES: dict[str, dict[str, float]] = {
    "conservative": {"bet_pct": 0.01, "exposure_pct": 0.10, "daily_stop_pct": 0.03},
    "moderate": {"bet_pct": 0.02, "exposure_pct": 0.15, "daily_stop_pct": 0.05},
    "aggressive": {"bet_pct": 0.04, "exposure_pct": 0.25, "daily_stop_pct": 0.08},
}


@dataclass(frozen=True)
class RiskProfile:
    name: str
    bankroll: float
    bet_pct: float
    exposure_pct: float
    daily_stop_pct: float
    concurrent_windows: int = 2

    @classmethod
    def of(cls, name: str, bankroll: float = 10_000.0, concurrent_windows: int = 2) -> RiskProfile:
        if name not in PROFILES:
            raise ValueError(f"unknown risk profile {name!r}; expected one of {tuple(PROFILES)}")
        return cls(name, bankroll, concurrent_windows=concurrent_windows, **PROFILES[name])

    @property
    def max_order_usdc(self) -> float:
        return self.bankroll * self.bet_pct

    @property
    def max_window_usdc(self) -> float:
        return self.bankroll * self.exposure_pct / self.concurrent_windows

    @property
    def daily_stop_usdc(self) -> float:
        return self.bankroll * self.daily_stop_pct


def cap_shares(wanted: float, price: float, max_order_usdc: float, room_usdc: float, min_size: float) -> float:
    """Shares an order may have: the strategy's wish within the order cap and the room left in the window;
    0 when the result is below the venue minimum."""
    if price <= 0:
        return 0.0
    n = min(wanted, max_order_usdc / price, max(0.0, room_usdc) / price)
    n = float(int(n))                      # whole shares, like his orders (87% integer sizes)
    return n if n >= min_size else 0.0
