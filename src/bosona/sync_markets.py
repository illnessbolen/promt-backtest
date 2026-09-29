"""Market metadata and resolutions from Gamma for every market the wallet touched.

GET /markets?condition_ids=…&closed=true returns market objects with the parent event embedded
(`events[0]`: slug, seriesSlug, startTime, eventMetadata.priceToBeat/finalPrice). Note that Gamma
silently drops closed markets unless `closed=true` is passed, so open markets are fetched separately.
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

# Markets that may still change: open, unresolved, or recently closed with missing strike/final metadata
# (Gamma fills finalPrice lazily). Older markets with missing metadata are treated as final.
PENDING_SQL = """
SELECT c.condition_id FROM (
    SELECT condition_id FROM trades
    UNION
    SELECT condition_id FROM activity WHERE condition_id IS NOT NULL
) c
LEFT JOIN markets m ON m.condition_id = c.condition_id
LEFT JOIN resolutions r ON r.condition_id = c.condition_id
WHERE m.condition_id IS NULL
   OR m.closed = 0
   OR r.condition_id IS NULL
   OR r.winner IS NULL
   OR (m.resolution_regime LIKE 'chainlink%'
       AND (r.price_to_beat IS NULL OR r.final_price IS NULL)
       AND m.window_end_ts > :recent
       AND m.fetched_at < :stale)
"""


def pending_condition_ids(conn: sqlite3.Connection, refresh_all: bool = False) -> list[str]:
    if refresh_all:
        sql = "SELECT condition_id FROM trades UNION SELECT condition_id FROM activity WHERE condition_id IS NOT NULL"
        return [r[0] for r in conn.execute(sql)]
    now = int(time.time())
    rows = conn.execute(PENDING_SQL, {"recent": now - 7 * 86_400, "stale": now - 6 * 3_600})
    return [r[0] for r in rows]


async def fetch_markets(cfg: Config, client: ApiClient, condition_ids: list[str]) -> list[dict]:
    url = f"{cfg.gamma_api}/markets"
    found = await client.get_json(url, {"condition_ids": condition_ids, "closed": "true", "limit": 100})
    seen = {m["conditionId"].lower() for m in found}
    missing = [c for c in condition_ids if c not in seen]
    if missing:
        found += await client.get_json(url, {"condition_ids": missing, "closed": "false", "limit": 100})
    return found


async def sync_markets(cfg: Config, conn: sqlite3.Connection, client: ApiClient, refresh_all: bool = False) -> dict[str, Any]:
    ids = pending_condition_ids(conn, refresh_all)
    batch = int(cfg.sync.get("gamma_batch", 50))
    batches = [ids[i : i + batch] for i in range(0, len(ids), batch)]
    log.info("market sync: %d condition ids in %d batch(es)", len(ids), len(batches))
    sem = asyncio.Semaphore(int(cfg.sync.get("concurrency", 4)))
    stats = {"requested": len(ids), "markets": 0, "resolutions": 0, "not_found": 0}
    done = 0

    async def run(chunk: list[str]) -> None:
        nonlocal done
        async with sem:
            markets = await fetch_markets(cfg, client, chunk)
        now = int(time.time())
        rows, resolutions = [], []
        for m in markets:
            events = m.get("events") or []
            row, res = parse.parse_market(m, events[0] if events else None, now)
            rows.append(row)
            if res:
                resolutions.append(res)
        db.upsert(conn, "markets", rows)
        db.upsert(conn, "resolutions", resolutions)
        conn.commit()
        stats["markets"] += len(rows)
        stats["resolutions"] += len(resolutions)
        stats["not_found"] += len(set(chunk) - {r["condition_id"] for r in rows})
        done += 1
        if done % 100 == 0 or done == len(batches):
            log.info("market sync: %d/%d batches", done, len(batches))

    await asyncio.gather(*(run(c) for c in batches))
    return stats
