"""Reconcile the local history with the Data API's own aggregates and print a completeness report."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from typing import Any

from bosona import db
from bosona.config import Config
from bosona.http import ApiClient

log = logging.getLogger(__name__)
DAY = 86_400


async def store_pnl_daily(cfg: Config, conn: sqlite3.Connection, client: ApiClient) -> list[dict]:
    payload = await client.get_json(f"{cfg.data_api}/v2/user-pnl", {"user": cfg.user, "interval": "all", "fidelity": "1d"})
    points = (payload.get("data") or {}).get("points") or []
    rows = [
        {
            "ts": int(p["timestamp"]),
            "source_block": p.get("source_block"),
            "trade_pnl": p.get("trade_pnl"),
            "realized_market_pnl": p.get("realized_market_pnl"),
            "unrealized_pnl": p.get("unrealized_pnl"),
            "fees_paid": p.get("fees_paid"),
            "maker_rebate": p.get("maker_rebate"),
            "taker_rebate": p.get("taker_rebate"),
            "reward_income": p.get("reward_income"),
            "economic_pnl": p.get("economic_pnl"),
            "volume": p.get("volume"),
            "volume_usdc": p.get("volume_usdc"),
            "trade_count": p.get("trade_count"),
            "raw_json": json.dumps(p, separators=(",", ":")),
        }
        for p in points
    ]
    db.upsert(conn, "pnl_daily", rows)
    conn.commit()
    return rows


async def daily_api_volume(cfg: Config, client: ApiClient, days: list[int]) -> dict[int, dict]:
    sem = asyncio.Semaphore(int(cfg.sync.get("concurrency", 4)))

    async def one(day: int) -> tuple[int, dict]:
        async with sem:
            payload = await client.get_json(f"{cfg.data_api}/v2/user-volume", {"user": cfg.user, "start": day, "end": day})
        return day, payload.get("data") or {}

    return dict(await asyncio.gather(*(one(d) for d in days)))


async def verify(cfg: Config, conn: sqlite3.Connection, client: ApiClient) -> dict[str, Any]:
    q = lambda sql, *p: db.scalar(conn, sql, p)  # noqa: E731
    report: dict[str, Any] = {}

    first, last = conn.execute("SELECT MIN(ts), MAX(ts) FROM trades").fetchone()
    if first is None:
        return {"error": "no trades in the database"}
    report["trades"] = {
        "count": q("SELECT COUNT(*) FROM trades"),
        "first": _fmt(first),
        "last": _fmt(last),
        "distinct_markets": q("SELECT COUNT(DISTINCT condition_id) FROM trades"),
        "buy": q("SELECT COUNT(*) FROM trades WHERE side = 'BUY'"),
        "sell": q("SELECT COUNT(*) FROM trades WHERE side = 'SELL'"),
        "taker": q("SELECT COUNT(*) FROM trades WHERE role = 'taker'"),
        "maker": q("SELECT COUNT(*) FROM trades WHERE role = 'maker'"),
        "role_null": q("SELECT COUNT(*) FROM trades WHERE role IS NULL"),
        "shares": round(q("SELECT SUM(size) FROM trades") or 0, 2),
        "usdc": round(q("SELECT SUM(usdc) FROM trades") or 0, 2),
        "fees_formula": round(q("SELECT SUM(fee_usdc) FROM trades") or 0, 2),
    }
    report["taker_fills_unmatched"] = q(
        "SELECT COUNT(*) FROM taker_fills t WHERE NOT EXISTS (SELECT 1 FROM trades x WHERE x.trade_uid = t.trade_uid)"
    )
    report["activity_by_type"] = {r[0]: r[1] for r in conn.execute("SELECT type, COUNT(*) FROM activity GROUP BY type ORDER BY 2 DESC")}
    report["markets"] = {
        "rows": q("SELECT COUNT(*) FROM markets"),
        "trade_markets_missing": q(
            "SELECT COUNT(DISTINCT condition_id) FROM trades WHERE condition_id NOT IN (SELECT condition_id FROM markets)"
        ),
        "by_regime": {r[0]: r[1] for r in conn.execute("SELECT resolution_regime, COUNT(*) FROM markets GROUP BY 1 ORDER BY 2 DESC")},
    }
    report["resolutions"] = {
        "rows": q("SELECT COUNT(*) FROM resolutions"),
        "winner_known": q("SELECT COUNT(*) FROM resolutions WHERE winner IS NOT NULL"),
        "chainlink_markets_with_strike": q(
            "SELECT COUNT(*) FROM markets m JOIN resolutions r USING (condition_id) "
            "WHERE m.resolution_regime LIKE 'chainlink%' AND r.price_to_beat IS NOT NULL"
        ),
        "chainlink_markets_closed": q(
            "SELECT COUNT(*) FROM markets m JOIN resolutions r USING (condition_id) WHERE m.resolution_regime LIKE 'chainlink%'"
        ),
    }

    # --- the Data API's own aggregates
    stats = (await client.get_json(f"{cfg.data_api}/v2/user-stats", {"user": cfg.user})).get("data") or {}
    all_time = stats.get("all_time_pnl") or {}
    report["api_user_stats"] = {
        "distinct_markets(trades)": stats.get("trades"),
        "trade_count": all_time.get("trade_count"),
        "volume_shares": all_time.get("volume"),
        "volume_usdc": all_time.get("volume_usdc"),
        "fees_paid": all_time.get("fees_paid"),
        "trade_pnl": all_time.get("trade_pnl"),
        "as_of": all_time.get("timestamp"),
    }

    # --- per-day comparison with /v2/user-volume (whole UTC days fully covered by the local history)
    day0 = first - first % DAY
    day_last = (last - last % DAY) - DAY  # last complete day
    days = list(range(day0, day_last + 1, DAY))
    api_days = await daily_api_volume(cfg, client, days)
    local = {
        r["d"]: r
        for r in conn.execute(
            "SELECT (ts / 86400) * 86400 AS d, COUNT(*) AS n, SUM(size) AS shares, SUM(usdc) AS usdc "
            "FROM trades GROUP BY d"
        )
    }
    mismatches = []
    tot_api = tot_local = 0
    for d in days:
        a = api_days.get(d) or {}
        loc = local.get(d)
        n_api = int(a.get("trade_count") or 0)
        n_loc = int(loc["n"]) if loc else 0
        tot_api += n_api
        tot_local += n_loc
        if n_api != n_loc:
            mismatches.append({"day": _fmt(d)[:10], "api": n_api, "local": n_loc,
                               "api_usdc": round(a.get("volume_usdc") or 0, 2),
                               "local_usdc": round(loc["usdc"], 2) if loc else 0})
    report["daily_vs_api"] = {
        "days": len(days),
        "api_trade_count": tot_api,
        "local_trade_count": tot_local,
        "days_mismatched": len(mismatches),
        "mismatches": mismatches[:20],
    }

    # --- fees and PnL series
    pnl = await store_pnl_daily(cfg, conn, client)
    if pnl:
        last_point = pnl[-1]
        report["fees_vs_api"] = {
            "api_fees_paid": last_point["fees_paid"],
            "local_formula_fees": report["trades"]["fees_formula"],
            "api_point": _fmt(last_point["ts"]),
        }
    return report


def _fmt(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
