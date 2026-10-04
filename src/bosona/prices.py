"""Token price history (CLOB /prices-history, ~60 s points) for both outcomes of every traded market.

Historical order books are not available (/book returns 404 once a market resolves), so the minute
series is the only public record of the market's own prices around each fill. `p` of the Up and the
Down token are fetched separately: they are close to complementary but not exactly (sum 0.985-1.005 seen).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Any

from bosona import db
from bosona.config import Config
from bosona.http import ApiClient

log = logging.getLogger(__name__)

# markets not fetched yet, or fetched before their window was over (history still growing)
PENDING_SQL = """
SELECT m.condition_id, m.market_id, m.up_token_id, m.down_token_id, m.window_start_ts, m.window_end_ts
FROM markets m
LEFT JOIN price_sync s ON s.condition_id = m.condition_id
WHERE m.condition_id IN (SELECT DISTINCT condition_id FROM trades)
  AND m.up_token_id IS NOT NULL AND m.window_start_ts IS NOT NULL
  AND (s.condition_id IS NULL OR s.fetched_at < m.window_end_ts + :post)
"""


async def fetch_history(cfg: Config, client: ApiClient, token_id: str, start: int, end: int) -> list[dict]:
    payload = await client.get_json(
        f"{cfg.clob_api}/prices-history",
        {"market": token_id, "startTs": start, "endTs": end, "fidelity": 1},
    )
    return payload.get("history") or []


async def sync_token_prices(cfg: Config, conn: sqlite3.Connection, client: ApiClient) -> dict[str, Any]:
    pre = int(cfg.prices.get("pre_window_s", 300))
    post = int(cfg.prices.get("post_window_s", 120))
    markets = conn.execute(PENDING_SQL, {"post": post}).fetchall()
    log.info("token prices: %d market(s) to fetch", len(markets))
    sem = asyncio.Semaphore(int(cfg.prices.get("concurrency", 16)))
    stats = {"markets": 0, "points": 0, "empty": 0}

    async def run(m: sqlite3.Row) -> None:
        start, end = m["window_start_ts"] - pre, m["window_end_ts"] + post
        async with sem:
            up = await fetch_history(cfg, client, m["up_token_id"], start, end)
            down = await fetch_history(cfg, client, m["down_token_id"], start, end)
        mid = int(m["market_id"])
        rows = [{"market_id": mid, "outcome": 0, "t": int(p["t"]), "p": float(p["p"])} for p in up]
        rows += [{"market_id": mid, "outcome": 1, "t": int(p["t"]), "p": float(p["p"])} for p in down]
        db.upsert(conn, "token_prices", rows)
        db.upsert(conn, "price_sync", [{
            "condition_id": m["condition_id"], "fetched_at": int(time.time()),
            "points_up": len(up), "points_down": len(down),
        }])
        stats["markets"] += 1
        stats["points"] += len(rows)
        stats["empty"] += int(not rows)
        if stats["markets"] % 200 == 0:
            conn.commit()  # keep write transactions short: other commands may use the DB meanwhile
        if stats["markets"] % 2000 == 0:
            log.info("token prices: %d/%d markets, %d points", stats["markets"], len(markets), stats["points"])

    await asyncio.gather(*(run(m) for m in markets))
    conn.commit()
    return stats
