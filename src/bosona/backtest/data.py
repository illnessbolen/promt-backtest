"""Inputs of the tape backtest for one window: metadata, tape, spot in resolution-source units, his fills.

The spot is Binance 1s closes (stage 2 cache) multiplied by (1 + basis), the basis being measured on the window
itself: strike / Binance equivalent of the strike (last close before the open for spot-settled windows, the
trailing mean over the TWAP lookback for TWAP-settled ones) — the stage 2 proxy rule (error 0.3-0.9 bps).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from bosona.backtest.pricing import TWAP_LOOKBACK_S
from bosona.backtest.proxy import MATCH_LAG_S, BookProxy
from bosona.spot import SYMBOLS
from bosona.strategies.base import Window

PRE_S = 900          # spot history kept before the open (volatility, 10 s moves)


@dataclass
class HisFill:
    outcome: int
    price: float
    size: float
    usdc: float          # what he paid, taker fee included (Data API usdc)
    fee: float
    role: str
    block_ts: int
    match_t: float       # block_ts - MATCH_LAG_S
    tx_hash: str


@dataclass
class WindowData:
    window: Window
    proxy: BookProxy
    tape: pd.DataFrame
    spot_t0: int                     # spot[i] = close of second spot_t0 + i (resolution-source units)
    spot: np.ndarray
    payout: tuple[float, float]
    winner: str | None
    sample: str = "random"
    his: list[HisFill] = field(default_factory=list)

    def spot_at(self, t: float) -> float:
        """Last full second's close at time t."""
        i = int(t) - 1 - self.spot_t0
        return float(self.spot[i]) if 0 <= i < len(self.spot) else float("nan")


def _basis(close: np.ndarray, t0: int, start: int, strike: float | None, regime: str | None) -> float:
    if strike is None or not np.isfinite(strike):
        return 0.0
    lb = TWAP_LOOKBACK_S.get(regime or "")
    if lb:
        seg = close[start - lb - t0 : start - t0]
        ref = float(np.nanmean(seg)) if len(seg) else np.nan
    else:
        i = start - 1 - t0
        ref = float(close[i]) if 0 <= i < len(close) else np.nan
    return strike / ref - 1.0 if np.isfinite(ref) and ref > 0 else 0.0


def window_list(tape_conn: sqlite3.Connection, timeframe: str | None = None, samples: tuple[str, ...] = ("random",)) -> pd.DataFrame:
    q = ",".join("?" * len(samples))
    sql = f"""SELECT w.slug, w.sample, m.condition_id, m.asset, m.timeframe, m.window_start_ts, m.window_end_ts,
                     m.resolution_regime, m.fee_rate, m.order_min_size, m.tick_size_last,
                     r.price_to_beat, r.payout_up, r.payout_down, r.winner
              FROM tape_windows w JOIN markets m USING (condition_id) JOIN resolutions r USING (condition_id)
              WHERE w.status = 'done' AND w.sample IN ({q}) AND r.winner IS NOT NULL"""
    params: list = list(samples)
    if timeframe:
        sql += " AND m.timeframe = ?"
        params.append(timeframe)
    w = pd.read_sql(sql + " ORDER BY m.window_start_ts", tape_conn, params=params)
    # strikes missing in Gamma: the previous window's final (priceToBeat(N) = finalPrice(N-1)) is not in this
    # sample, so such windows are skipped (stage 1: 0-4% of Chainlink windows).
    return w[w["price_to_beat"].notna()].reset_index(drop=True)


def load_window(row: pd.Series, tape_conn: sqlite3.Connection, bos_conn: sqlite3.Connection, cache,
                user: str) -> WindowData | None:
    start, end = int(row["window_start_ts"]), int(row["window_end_ts"])
    regime = row["resolution_regime"]
    strike = float(row["price_to_beat"])
    symbol = SYMBOLS.get(row["asset"])
    if symbol is None:
        return None
    t0 = start - PRE_S
    close = cache.closes(symbol, t0, end + 5)
    if np.isnan(close[PRE_S - 60 : end - t0]).mean() > 0.05:
        return None                                      # spot gap: no reliable fair value
    close = pd.Series(close).ffill().bfill().to_numpy()
    basis = _basis(close, t0, start, strike, regime)
    tape = pd.read_sql("SELECT seq, ts, outcome_index, side, price, size, taker, tx_hash FROM tape WHERE condition_id = ? "
                       "ORDER BY seq", tape_conn, params=(row["condition_id"],))
    window = Window(key=row["condition_id"], asset=row["asset"], timeframe=row["timeframe"], start=start, end=end,
                    strike=strike, regime=regime, tick=0.01,      # quotes on the 1c grid (the 0.001 tick only near 0/1)
                    min_size=float(row["order_min_size"] or 5.0), fee_rate=float(row["fee_rate"] or 0.0))
    his = []
    for r in bos_conn.execute("SELECT outcome, price, size, usdc, fee_usdc, role, ts, tx_hash FROM trades "
                              "WHERE condition_id = ? AND side = 'BUY' ORDER BY ts", (row["condition_id"],)):
        his.append(HisFill(outcome=0 if r[0] == "Up" else 1, price=float(r[1]), size=float(r[2]), usdc=float(r[3]),
                           fee=float(r[4] or 0.0), role=r[5], block_ts=int(r[6]), match_t=int(r[6]) - MATCH_LAG_S,
                           tx_hash=r[7]))
    return WindowData(window=window, proxy=BookProxy(tape), tape=tape, spot_t0=t0, spot=close * (1.0 + basis),
                      payout=(float(row["payout_up"]), float(row["payout_down"])), winner=row["winner"],
                      sample=row["sample"], his=his)


def iter_windows(tape_conn, bos_conn, cache, user: str, rows: pd.DataFrame) -> Iterator[WindowData]:
    for _, row in rows.iterrows():
        wd = load_window(row, tape_conn, bos_conn, cache, user)
        if wd is not None:
            yield wd
