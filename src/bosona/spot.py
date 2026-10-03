"""Binance 1-second closes: download, per-day cache and vectorized lookups.

Why Binance: the 1h and daily markets resolve on Binance {ASSET}USDT exactly; for the Chainlink-resolved
5m/15m/4h markets no public intra-window Chainlink history exists (CLAUDE.md, open question 2), so Binance
serves as a proxy anchored on each window's official strike (see context.py).

Cache layout: <cache_dir>/binance/<SYMBOL>/<YYYY-MM-DD>.npz with arrays
  close  float64[86400]  close of the 1s kline that starts at day_start + i (NaN if missing)
  volume float32[86400]  base volume of that second
  complete bool          the day had fully elapsed when it was downloaded (partial days are re-fetched)
"""

from __future__ import annotations

import asyncio
import logging
import time
from functools import lru_cache
from pathlib import Path

import numpy as np

from bosona.config import Config
from bosona.http import ApiClient

log = logging.getLogger(__name__)

DAY = 86_400
KLINES_PER_REQUEST = 1000

SYMBOLS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
    "doge": "DOGEUSDT",
    "bnb": "BNBUSDT",
    "hype": "HYPEUSDT",
    "zec": "ZECUSDT",
}


def day_start(ts: int) -> int:
    return ts - ts % DAY


class SpotCache:
    def __init__(self, root: Path) -> None:
        self.root = root / "binance"

    def path(self, symbol: str, day: int) -> Path:
        return self.root / symbol / f"{time.strftime('%Y-%m-%d', time.gmtime(day))}.npz"

    def is_complete(self, symbol: str, day: int) -> bool:
        p = self.path(symbol, day)
        if not p.is_file():
            return False
        with np.load(p) as z:
            return bool(z["complete"])

    def save(self, symbol: str, day: int, close: np.ndarray, volume: np.ndarray, complete: bool) -> None:
        p = self.path(symbol, day)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp.npz")
        np.savez_compressed(tmp, close=close, volume=volume, complete=np.array(complete))
        tmp.replace(p)

    def load_day(self, symbol: str, day: int) -> np.ndarray:
        """Closes for one UTC day (NaN where unknown / not downloaded)."""
        return _load_close(str(self.path(symbol, day)))

    def closes(self, symbol: str, t0: int, t1: int) -> np.ndarray:
        """Closes for seconds [t0, t1) as one array (index i -> second t0 + i)."""
        out = np.full(t1 - t0, np.nan)
        d = day_start(t0)
        while d < t1:
            arr = self.load_day(symbol, d)
            a, b = max(t0, d), min(t1, d + DAY)
            out[a - t0 : b - t0] = arr[a - d : b - d]
            d += DAY
        return out


@lru_cache(maxsize=64)
def _load_close(path: str) -> np.ndarray:
    p = Path(path)
    if not p.is_file():
        return np.full(DAY, np.nan)
    with np.load(p) as z:
        return z["close"].astype(np.float64)


async def fetch_day(cfg: Config, client: ApiClient, symbol: str, day: int) -> tuple[np.ndarray, np.ndarray]:
    close = np.full(DAY, np.nan)
    volume = np.zeros(DAY, dtype=np.float32)
    end = min(day + DAY, int(time.time()) - 5)
    url = f"{cfg.binance_api}/api/v3/klines"
    start = day
    while start < end:
        stop = min(start + KLINES_PER_REQUEST, end)
        rows = await client.get_json(
            url,
            {
                "symbol": symbol,
                "interval": "1s",
                "startTime": start * 1000,
                "endTime": stop * 1000 - 1,
                "limit": KLINES_PER_REQUEST,
            },
        )
        for k in rows:
            i = int(k[0]) // 1000 - day
            if 0 <= i < DAY:
                close[i] = float(k[4])
                volume[i] = float(k[5])
        start = stop
    return close, volume


def needed_days(conn, lookback_s: int) -> dict[str, set[int]]:
    """UTC days per asset that cover every traded market window (plus the lookback before it)."""
    need: dict[str, set[int]] = {}
    rows = conn.execute(
        "SELECT DISTINCT m.asset, m.window_start_ts, m.window_end_ts FROM markets m "
        "WHERE m.condition_id IN (SELECT DISTINCT condition_id FROM trades) AND m.window_start_ts IS NOT NULL"
    )
    for asset, ws, we in rows:
        days = need.setdefault(asset, set())
        # daily markets resolve on the 1m candle starting at the window end -> +60 s
        d = day_start(ws - lookback_s)
        while d <= we + 120:
            days.add(d)
            d += DAY
    return need


async def sync_spot(cfg: Config, conn, client: ApiClient) -> dict[str, int]:
    cache = SpotCache(cfg.spot_cache_dir)
    need = needed_days(conn, int(cfg.spot.get("lookback_s", 1200)))
    todo = [
        (SYMBOLS[asset], d)
        for asset, days in sorted(need.items())
        if asset in SYMBOLS
        for d in sorted(days)
        if not cache.is_complete(SYMBOLS[asset], d)
    ]
    log.info("spot sync: %d symbol-days to download (%d already cached)",
             len(todo), sum(len(v) for v in need.values()) - len(todo))
    sem = asyncio.Semaphore(int(cfg.spot.get("concurrency", 12)))
    done = 0

    async def run(symbol: str, d: int) -> None:
        nonlocal done
        async with sem:
            close, volume = await fetch_day(cfg, client, symbol, d)
        complete = d + DAY <= int(time.time()) - 60
        missing = int(np.isnan(close[: min(DAY, int(time.time()) - d)]).sum())
        cache.save(symbol, d, close, volume, complete)
        done += 1
        if missing:
            log.warning("spot %s %s: %d missing seconds", symbol, time.strftime("%Y-%m-%d", time.gmtime(d)), missing)
        if done % 25 == 0 or done == len(todo):
            log.info("spot sync: %d/%d symbol-days", done, len(todo))

    await asyncio.gather(*(run(s, d) for s, d in todo))
    return {"downloaded": len(todo)}
