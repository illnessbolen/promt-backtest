"""Top of book of a market reconstructed from its trade tape (no historical order book exists).

A taker buy of a token lifts its effective ask: the token's own asks or, through the CTF mint, the bids of the
other token (ask_up = min(own ask, 1 - bid_down)). A taker sell hits the effective bid. So for token k:

    ask(k, t) ~ price of the last BUY print on k matched before t
    bid(k, t) ~ 1 - price of the last BUY print on the other token, or the last SELL print on k,
                whichever is more recent (88% of prints are buys, so the complement carries most of it)

Print times are block timestamps; the CLOB match happened ~2.4 s earlier (stage 3: median 2.44 s, sd 0.6),
so prints are placed on the match-time axis by subtracting MATCH_LAG_S. A print's price is the average of the
levels its taker order took, so multi-level prints overstate the touch slightly.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

MATCH_LAG_S = 2.4


@dataclass
class Prints:
    t: np.ndarray        # match-time estimate, s (sorted)
    price: np.ndarray
    size: np.ndarray
    seq: np.ndarray      # position in the market's tape (ties at one block keep the API order)

    @classmethod
    def of(cls, tape: pd.DataFrame, mask: np.ndarray, lag: float) -> Prints:
        x = tape[mask]
        return cls(x["ts"].to_numpy(dtype=float) - lag, x["price"].to_numpy(dtype=float),
                   x["size"].to_numpy(dtype=float), x["seq"].to_numpy(dtype=np.int64))

    def nearest(self, q: float) -> tuple[float, float]:
        """Price of the print closest in time to q (before or after) and the distance in s (inf when none).
        Used for execution prices: validated against live books it is the best estimate of the ask a copier
        faces (bias +0.2c, 72% within 1c, p90 3c), better than the last print before q (stale in fast moves)."""
        if not len(self.t):
            return float("nan"), float("inf")
        i = int(np.searchsorted(self.t, q, side="left"))
        best = None
        for j in (i - 1, i):
            if 0 <= j < len(self.t):
                d = abs(self.t[j] - q)
                if best is None or d < best[1]:
                    best = (float(self.price[j]), float(d))
        return best

    def around(self, q: float, half: float = 1.0, before: float = np.inf) -> tuple[float, float]:
        """Median price of the prints within +-half s of q (only prints before `before`), else the nearest one.
        Robust to a single odd print (a deep level hit by a small order, a multi-level average)."""
        m = (np.abs(self.t - q) <= half) & (self.t < before)
        if m.any():
            return float(np.median(self.price[m])), 0.0
        if before < np.inf:
            keep = self.t < before
            if not keep.any():
                return float("nan"), float("inf")
            return Prints(self.t[keep], self.price[keep], self.size[keep], self.seq[keep]).nearest(q)
        return self.nearest(q)

    def last(self, q: np.ndarray | float, exclude_seq: np.ndarray | None = None) -> tuple[np.ndarray, np.ndarray]:
        """Price and age of the last print strictly before each query time (NaN / inf when none)."""
        q = np.atleast_1d(np.asarray(q, dtype=float))
        i = np.searchsorted(self.t, q, side="left") - 1
        if exclude_seq is not None:                      # skip the query's own print (validation)
            same = (i >= 0) & (self.seq[np.clip(i, 0, None)] == exclude_seq)
            i = np.where(same, i - 1, i)
        ok = i >= 0
        j = np.clip(i, 0, None)
        price = np.where(ok, self.price[j] if len(self.price) else np.nan, np.nan)
        age = np.where(ok, q - (self.t[j] if len(self.t) else 0.0), np.inf)
        return price, age


class BookProxy:
    """Effective best bid / ask of both tokens (0 = Up, 1 = Down) of one market at any time."""

    def __init__(self, tape: pd.DataFrame, lag: float = MATCH_LAG_S) -> None:
        tape = tape.sort_values("seq")
        oi, side = tape["outcome_index"].to_numpy(), tape["side"].to_numpy()
        self.buys = [Prints.of(tape, (oi == k) & (side == "BUY"), lag) for k in (0, 1)]
        self.sells = [Prints.of(tape, (oi == k) & (side == "SELL"), lag) for k in (0, 1)]

    def ask(self, k: int, q, exclude_seq=None) -> tuple[np.ndarray, np.ndarray]:
        return self.buys[k].last(q, exclude_seq)

    def bid(self, k: int, q, exclude_seq=None) -> tuple[np.ndarray, np.ndarray]:
        p_c, a_c = self.buys[1 - k].last(q, exclude_seq)
        p_s, a_s = self.sells[k].last(q, exclude_seq)
        use_sell = a_s < a_c
        return np.where(use_sell, p_s, 1.0 - p_c), np.where(use_sell, a_s, a_c)

    def ask_nearest(self, k: int, q: float) -> tuple[float, float]:
        return self.buys[k].nearest(q)

    def bid_nearest(self, k: int, q: float) -> tuple[float, float]:
        p_c, d_c = self.buys[1 - k].nearest(q)
        p_s, d_s = self.sells[k].nearest(q)
        return (p_s, d_s) if d_s < d_c else (1.0 - p_c, d_c)

    def effective_sells(self, k: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Every print that sold token k to the bids, as (match time, effective price, size), time-sorted:
        taker sells of k at q, and taker buys of the other token at q (minted against k's bids at 1 - q)."""
        t = np.concatenate([self.sells[k].t, self.buys[1 - k].t])
        p = np.concatenate([self.sells[k].price, 1.0 - self.buys[1 - k].price])
        z = np.concatenate([self.sells[k].size, self.buys[1 - k].size])
        o = np.argsort(t, kind="stable")
        return t[o], p[o], z[o]

    def ask_exec(self, k: int, q: float, before: float = np.inf) -> tuple[float, float]:
        """The ask a taker order arriving at q pays: median of the buy prints within 1 s (validated on the
        stage 3 live books: bias -0.6c, 65% within 1c, p90 5c), only prints before the window end."""
        return self.buys[k].around(q, 1.0, before)
