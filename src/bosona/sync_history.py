"""Wallet history from Data API v2: fills, other activity and maker/taker role.

Sources (verified in stage 0, docs/stage0-recon.md):
  * /v2/activity?user=…            every row of the wallet (TRADE, MERGE, REDEEM, rebates, ...), cursor-paged
  * /v2/trades?user=…&taker_only=true  exactly the fills where the wallet was the taker
Role = taker if the fill's uid is in the taker feed, otherwise maker.

Idempotency: rows are keyed by natural keys (see parse.with_seq) and inserted with INSERT OR IGNORE;
incremental runs re-read `overlap_s` seconds before the last synced second and never read the newest
`safety_lag_s` seconds, so partially ingested transactions heal on the next run.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Any

from bosona import db, parse
from bosona.config import Config
from bosona.http import ApiClient

log = logging.getLogger(__name__)

DAY = 86_400
STATE_SOURCE = "history"


async def walk(client: ApiClient, url: str, params: dict[str, Any]) -> list[dict]:
    """Follow pagination.next_cursor until it is null. Filters are re-sent on every page (docs requirement)."""
    rows: list[dict] = []
    cursor = None
    while True:
        query = dict(params)
        if cursor:
            query["cursor"] = cursor
        payload = await client.get_json(url, query)
        rows.extend(payload.get("data") or [])
        cursor = (payload.get("pagination") or {}).get("next_cursor")
        if not cursor:
            return rows


async def earliest_activity_ts(cfg: Config, client: ApiClient) -> int | None:
    payload = await client.get_json(
        f"{cfg.data_api}/v2/activity",
        {"user": cfg.user, "start": cfg.sync.get("history_start", 1), "sort_direction": "ASC", "limit": 1},
    )
    data = payload.get("data") or []
    return int(data[0]["timestamp"]) if data else None


def split_windows(start: int, end: int, size: int) -> list[tuple[int, int]]:
    """Inclusive, non-overlapping [a, b] windows covering [start, end]."""
    out = []
    a = start
    while a <= end:
        b = min(end, a + size - 1)
        out.append((a, b))
        a = b + 1
    return out


async def sync_window(cfg: Config, conn: sqlite3.Connection, client: ApiClient, start: int, end: int) -> dict[str, int]:
    base = {"user": cfg.user, "start": start, "end": end, "limit": int(cfg.sync.get("page_limit", 1000))}
    activity_rows = await walk(client, f"{cfg.data_api}/v2/activity", base)
    taker_rows = await walk(client, f"{cfg.data_api}/v2/trades", {**base, "taker_only": "true"})
    now = int(time.time())
    trades, other = parse.split_activity(activity_rows, now)
    takers = parse.taker_fill_rows(taker_rows, now)
    result = {
        "rows": len(activity_rows),
        "fills": len(trades),
        "taker_fills": len(takers),
        "new_trades": db.insert_ignore(conn, "trades", trades),
        "new_activity": db.insert_ignore(conn, "activity", other),
        "new_taker_fills": db.insert_ignore(conn, "taker_fills", takers),
    }
    conn.commit()
    return result


def finalize_trades(conn: sqlite3.Connection, start: int, end: int, default_fee_rate: float) -> None:
    """(Re)compute role and formula fee for fills in [start, end]; on-chain fees are never overwritten."""
    conn.execute(
        """
        UPDATE trades SET role = CASE
            WHEN EXISTS (SELECT 1 FROM taker_fills t WHERE t.trade_uid = trades.trade_uid) THEN 'taker'
            ELSE 'maker' END
        WHERE ts BETWEEN ? AND ?
        """,
        (start, end),
    )
    conn.execute(
        """
        UPDATE trades SET
            fee_usdc = CASE WHEN role = 'taker' THEN ROUND(
                size * COALESCE((SELECT m.fee_rate FROM markets m WHERE m.condition_id = trades.condition_id), ?)
                     * price * (1 - price), 5)
                ELSE 0 END,
            fee_source = 'formula'
        WHERE ts BETWEEN ? AND ? AND (fee_source IS NULL OR fee_source = 'formula')
        """,
        (default_fee_rate, start, end),
    )
    conn.commit()


async def sync_history(cfg: Config, conn: sqlite3.Connection, client: ApiClient, full: bool = False) -> dict[str, Any]:
    scope = cfg.user
    last = None if full else db.get_state(conn, STATE_SOURCE, scope)
    end = int(time.time()) - int(cfg.sync.get("safety_lag_s", 120))
    if last is None:
        first = await earliest_activity_ts(cfg, client)
        if first is None:
            log.warning("no activity for %s", cfg.user)
            return {"windows": 0}
        start = first - first % DAY
    else:
        start = max(1, last - int(cfg.sync.get("overlap_s", 3600)))

    windows = split_windows(start, end, int(cfg.sync.get("window_days", 7)) * DAY)
    log.info("history sync %s: %s .. %s in %d window(s)", scope, _fmt(start), _fmt(end), len(windows))
    sem = asyncio.Semaphore(int(cfg.sync.get("concurrency", 4)))
    totals: dict[str, int] = {}

    async def run(window: tuple[int, int]) -> None:
        async with sem:
            t0 = time.monotonic()
            res = await sync_window(cfg, conn, client, *window)
            for k, v in res.items():
                totals[k] = totals.get(k, 0) + v
            log.info(
                "window %s..%s: %d rows, %d fills (%d taker), new trades %d, new activity %d  [%.1fs]",
                _fmt(window[0]), _fmt(window[1]), res["rows"], res["fills"], res["taker_fills"],
                res["new_trades"], res["new_activity"], time.monotonic() - t0,
            )

    await asyncio.gather(*(run(w) for w in windows))
    finalize_trades(conn, start, end, cfg.crypto_fee_rate)
    db.set_state(conn, STATE_SOURCE, scope, end, note=f"windows={len(windows)}")
    conn.commit()
    return {"start": start, "end": end, "windows": len(windows), **totals}


def _fmt(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
