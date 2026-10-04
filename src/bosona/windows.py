"""Window arithmetic of the recurring crypto Up/Down series and their Gamma slugs.

Inverse of `parse.parse_slug`: verified against every market slug in the stage 1 history
(5m/15m/4h are epoch-aligned, hourly windows are labelled by the ET start hour,
daily windows run noon ET -> noon ET and carry the end date in the slug).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
TIMEFRAMES = ("5m", "15m", "1h", "4h", "1d")
TF_SECONDS = {"5m": 300, "15m": 900, "1h": 3_600, "4h": 14_400, "1d": 86_400}
# hourly / daily slugs use long asset names (parse.ASSET_ALIASES maps them back)
LONG_NAMES = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp", "doge": "dogecoin", "bnb": "bnb"}
MONTHS = ("january", "february", "march", "april", "may", "june",
          "july", "august", "september", "october", "november", "december")  # locale-independent %B


def window_start(tf: str, t: int) -> int:
    """Start of the window of timeframe `tf` that contains unix second `t`."""
    if tf != "1d":
        return t - t % TF_SECONDS[tf]
    d = datetime.fromtimestamp(t, ET)
    if d.hour < 12:
        d -= timedelta(days=1)
    return int(d.replace(hour=12, minute=0, second=0, microsecond=0).timestamp())


def window_end(tf: str, ws: int) -> int:
    if tf != "1d":
        return ws + TF_SECONDS[tf]
    d = datetime.fromtimestamp(ws, ET) + timedelta(days=1)  # wall-clock day: 23 h / 25 h across DST changes
    return int(d.replace(hour=12, minute=0, second=0, microsecond=0).timestamp())


def next_window_start(tf: str, ws: int) -> int:
    return window_end(tf, ws)


def slug_for(asset: str, tf: str, ws: int) -> str:
    """Gamma slug of the window of `asset`/`tf` starting at `ws`."""
    if tf in ("5m", "15m", "4h"):
        return f"{asset}-updown-{tf}-{ws}"
    name = LONG_NAMES.get(asset, asset)
    if tf == "1h":
        d = datetime.fromtimestamp(ws, ET)
        hour = d.hour % 12 or 12
        return f"{name}-up-or-down-{MONTHS[d.month - 1]}-{d.day}-{d.year}-{hour}{'am' if d.hour < 12 else 'pm'}-et"
    if tf == "1d":
        d = datetime.fromtimestamp(window_end(tf, ws), ET)
        return f"{name}-up-or-down-on-{MONTHS[d.month - 1]}-{d.day}-{d.year}"
    raise ValueError(f"unknown timeframe {tf!r}")
