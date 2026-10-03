"""Pure parsing / normalization helpers (no I/O) for Data API v2 rows and Gamma markets."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from typing import Any

SIZE_SCALE = 1_000_000          # shares and pUSD have 6 decimals on-chain
PRICE_SCALE = 100_000_000       # API prices are ratios like 0.7400000012; 8 dp is stable across endpoints

# Fields of v2 activity/trade rows that are profile decoration or constant for our single wallet.
_DROP_FIELDS = {"proxy_wallet", "title", "icon", "name", "pseudonym", "bio", "profile_image", "profile_image_optimized"}
# Gamma fields not worth storing per market: images, nested copies, the templated rules text (captured by
# resolution_regime/resolution_source) and point-in-time snapshots (volumes, liquidity, best bid/ask, ...).
_MARKET_RAW_DROP = {
    "icon", "image", "events", "markets", "series", "tags", "description",
    "oneDayPriceChange", "oneHourPriceChange", "oneWeekPriceChange", "oneMonthPriceChange",
    "competitive", "spread", "bestBid", "bestAsk", "lastTradePrice", "openInterest", "commentCount",
}
_MARKET_RAW_DROP_PREFIXES = ("volume", "liquidity")
_EVENT_RAW_KEEP = {
    "id", "slug", "title", "seriesSlug", "startTime", "endDate", "closedTime",
    "eventMetadata", "resolutionSource", "automaticallyResolved",
}


def _slim_market(market: dict) -> dict:
    return {
        k: v for k, v in market.items()
        if k not in _MARKET_RAW_DROP and not k.startswith(_MARKET_RAW_DROP_PREFIXES)
    }

ASSET_ALIASES = {
    "bitcoin": "btc",
    "ethereum": "eth",
    "solana": "sol",
    "dogecoin": "doge",
}

_RE_UPDOWN = re.compile(r"^(?P<asset>[a-z0-9]+)-updown-(?P<tf>5m|15m|4h)-(?P<start>\d+)$")
_RE_HOURLY = re.compile(r"^(?P<asset>[a-z0-9]+)-up-or-down-[a-z]+-\d{1,2}(?:-\d{4})?-\d{1,2}(?:am|pm)-et$")
_RE_DAILY = re.compile(r"^(?P<asset>[a-z0-9]+)-up-or-down-on-[a-z]+-\d{1,2}(?:-\d{4})?$")


def to_raw(value: Any, scale: int = SIZE_SCALE) -> int:
    return int(round(float(value or 0) * scale))


def price_raw(price: Any) -> int:
    return to_raw(price, PRICE_SCALE)


def compact_json(row: dict) -> str:
    return json.dumps({k: v for k, v in row.items() if k not in _DROP_FIELDS}, separators=(",", ":"), sort_keys=True)


def parse_iso(value: str | None) -> int | None:
    """ISO-8601 / Gamma timestamps ('2026-09-28T17:45:00Z', '2026-09-28 17:46:27+00') -> unix seconds."""
    if not value:
        return None
    v = value.strip().replace(" ", "T")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    elif re.search(r"T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?[+-]\d{2}$", v):
        v += ":00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


# --------------------------------------------------------------------------- dedup keys


def trade_key(row: dict) -> str:
    """Natural key of a fill without its occurrence index (works for /v2/activity and /v2/trades rows)."""
    return f"{row['transaction_hash'].lower()}:{row['token_id']}:{row['side']}:{to_raw(row['size'])}:{price_raw(row['price'])}"


def activity_key(row: dict) -> str:
    return ":".join(
        [
            row["transaction_hash"].lower(),
            row["type"],
            row.get("condition_id") or "",
            row.get("token_id") or "",
            str(to_raw(row.get("size"))),
            str(to_raw(row.get("usdc_size"))),
        ]
    )


def with_seq(rows: Iterable[dict], keyfunc: Callable[[dict], str]) -> list[tuple[dict, str, int]]:
    """Attach an occurrence index to rows sharing the same natural key.

    Identical rows inside one transaction are real, separate fills (two orders of the same size at the
    same price). All rows of a transaction share one block timestamp, so any time window that contains
    the transaction contains all of its rows and the resulting (key, seq) pairs are stable across runs.
    """
    counts: dict[str, int] = defaultdict(int)
    out = []
    for row in rows:
        key = keyfunc(row)
        out.append((row, key, counts[key]))
        counts[key] += 1
    return out


# --------------------------------------------------------------------------- Data API v2 rows


def split_activity(rows: Iterable[dict], ingested_at: int) -> tuple[list[dict], list[dict]]:
    """Normalize /v2/activity rows into (trades, other_activity) table rows."""
    rows = list(rows)
    trade_rows = [r for r in rows if r.get("type") == "TRADE"]
    other_rows = [r for r in rows if r.get("type") != "TRADE"]
    trades = []
    for row, key, seq in with_seq(trade_rows, trade_key):
        trades.append(
            {
                "trade_uid": f"{key}:{seq}",
                "tx_hash": row["transaction_hash"].lower(),
                "seq": seq,
                "ts": int(row["timestamp"]),
                "condition_id": row["condition_id"].lower(),
                "slug": row.get("slug") or None,
                "token_id": str(row["token_id"]),
                "outcome": row.get("outcome") or None,
                "outcome_index": row.get("outcome_index"),
                "side": row["side"],
                "price": float(row["price"]),
                "size_raw": to_raw(row["size"]),
                "usdc_raw": to_raw(row.get("usdc_size")),
                "size": float(row["size"]),
                "usdc": float(row.get("usdc_size") or 0),
                "event_slug": row.get("event_slug") or None,
                "source": "data_api_v2",
                "ingested_at": ingested_at,
            }
        )
    activity = []
    for row, key, seq in with_seq(other_rows, activity_key):
        activity.append(
            {
                "activity_uid": f"{key}:{seq}",
                "tx_hash": row["transaction_hash"].lower(),
                "seq": seq,
                "ts": int(row["timestamp"]),
                "type": row["type"],
                "condition_id": (row.get("condition_id") or "").lower() or None,
                "slug": row.get("slug") or None,
                "token_id": row.get("token_id") or None,
                "outcome": row.get("outcome") or None,
                "outcome_index": row.get("outcome_index"),
                "size": float(row.get("size") or 0),
                "usdc_size": float(row.get("usdc_size") or 0),
                "raw_json": compact_json(row),
                "ingested_at": ingested_at,
            }
        )
    return trades, activity


def taker_fill_rows(rows: Iterable[dict], ingested_at: int) -> list[dict]:
    """Normalize /v2/trades?taker_only=true rows; uid is built exactly like trades.trade_uid."""
    return [
        {
            "trade_uid": f"{key}:{seq}",
            "tx_hash": row["transaction_hash"].lower(),
            "ts": int(row["timestamp"]),
            "ingested_at": ingested_at,
        }
        for row, key, seq in with_seq(rows, trade_key)
    ]


def taker_fee(size: float, price: float, rate: float) -> float:
    """Polymarket taker fee in USDC: C * feeRate * p * (1 - p), rounded to 5 dp (docs: trading/fees)."""
    return round(size * rate * price * (1.0 - price), 5)


# --------------------------------------------------------------------------- Gamma markets


def parse_slug(slug: str | None) -> tuple[str | None, str | None]:
    """Asset and timeframe of a recurring Up/Down market from its slug."""
    if not slug:
        return None, None
    m = _RE_UPDOWN.match(slug)
    if m:
        return ASSET_ALIASES.get(m["asset"], m["asset"]), m["tf"]
    m = _RE_HOURLY.match(slug)
    if m:
        return ASSET_ALIASES.get(m["asset"], m["asset"]), "1h"
    m = _RE_DAILY.match(slug)
    if m:
        return ASSET_ALIASES.get(m["asset"], m["asset"]), "1d"
    return None, None


def resolution_regime(timeframe: str | None, resolution_source: str | None, crypto_config: dict | None) -> str:
    """How the market resolves (see docs/stage0-recon.md §4)."""
    src = (resolution_source or "").lower()
    if timeframe in ("5m", "15m", "4h"):
        if crypto_config:
            if crypto_config.get("twapEnabled"):
                return f"chainlink_twap{int(crypto_config.get('twapLookbackSeconds') or 0)}"
            return "chainlink_spot"
        if "twap-30s" in src:
            return "chainlink_twap30"
        if "twap-60s" in src:
            return "chainlink_twap60"
        if "chain.link" in src:
            return "chainlink_spot"
    if timeframe == "1h" and "binance" in src:
        return "binance_1h"
    if timeframe == "1d" and "binance" in src:
        return "binance_noon_1m"
    return "unknown"


def _json_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if not value:
        return []
    return json.loads(value)


def parse_market(market: dict, event: dict | None, fetched_at: int) -> tuple[dict, dict | None]:
    """Gamma market (+ parent event) -> (markets row, resolutions row or None while open)."""
    event = event or {}
    outcomes = [str(o) for o in _json_list(market.get("outcomes"))]
    tokens = [str(t) for t in _json_list(market.get("clobTokenIds"))]
    prices = [float(p) for p in _json_list(market.get("outcomePrices"))]
    token_of = dict(zip(outcomes, tokens))
    price_of = dict(zip(outcomes, prices))

    slug = market.get("slug")
    asset, timeframe = parse_slug(slug)
    cfg = market.get("cryptoMarketConfig") or None
    if cfg:
        asset = asset or cfg.get("asset")
        timeframe = timeframe or cfg.get("duration")
    fee_schedule = market.get("feeSchedule") or {}
    fees_enabled = bool(market.get("feesEnabled"))
    closed = bool(market.get("closed"))

    row = {
        "condition_id": market["conditionId"].lower(),
        "market_id": str(market.get("id") or ""),
        "question_id": market.get("questionID"),
        "slug": slug,
        "event_slug": event.get("slug"),
        "series_slug": event.get("seriesSlug"),
        "title": market.get("question"),
        "asset": asset,
        "timeframe": timeframe,
        "window_start_ts": parse_iso(market.get("eventStartTime") or event.get("startTime")),
        "window_end_ts": parse_iso(market.get("endDate")),
        "accepting_orders_ts": parse_iso(market.get("acceptingOrdersTimestamp")),
        "up_token_id": token_of.get("Up"),
        "down_token_id": token_of.get("Down"),
        "resolution_regime": resolution_regime(timeframe, market.get("resolutionSource"), cfg),
        "resolution_source": market.get("resolutionSource"),
        "crypto_config_id": market.get("cryptoMarketConfigId"),
        "twap_lookback_s": (cfg or {}).get("twapLookbackSeconds") if (cfg or {}).get("twapEnabled") else None,
        "fee_type": market.get("feeType"),
        "fee_rate": float(fee_schedule.get("rate", 0)) if fees_enabled else 0.0,
        "fee_exponent": fee_schedule.get("exponent"),
        "fee_taker_only": int(bool(fee_schedule.get("takerOnly"))) if fee_schedule else None,
        "fee_rebate_rate": fee_schedule.get("rebateRate"),
        "order_min_size": market.get("orderMinSize"),
        "tick_size_last": market.get("orderPriceMinTickSize"),
        "neg_risk": int(bool(market.get("negRisk"))),
        "closed": int(closed),
        "raw_json": json.dumps(
            {"market": _slim_market(market), "event": {k: v for k, v in event.items() if k in _EVENT_RAW_KEEP}},
            separators=(",", ":"),
        ),
        "fetched_at": fetched_at,
    }

    resolution = None
    if closed and prices:
        up, down = price_of.get("Up"), price_of.get("Down")
        if up == 1.0 and down == 0.0:
            winner = "Up"
        elif down == 1.0 and up == 0.0:
            winner = "Down"
        elif up == 0.5 and down == 0.5:
            winner = "50-50"
        else:
            winner = None  # closed but payouts not final yet
        meta = event.get("eventMetadata") or {}
        resolution = {
            "condition_id": row["condition_id"],
            "winner": winner,
            "payout_up": up,
            "payout_down": down,
            "price_to_beat": meta.get("priceToBeat"),
            "final_price": meta.get("finalPrice"),
            "strike_source": "gamma_meta" if meta.get("priceToBeat") is not None else None,
            "closed_ts": parse_iso(market.get("closedTime")),
            "uma_status": market.get("umaResolutionStatus"),
            "fetched_at": fetched_at,
        }
    return row, resolution
