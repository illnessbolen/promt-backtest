"""Resolution of the markets traded live (Gamma, closed markets), stored in live.db `resolutions`.

Used by the tracker once a minute and by `live-report`, to put a PnL on every live fill and on copying it.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from bosona import parse
from bosona.http import ApiClient

log = logging.getLogger(__name__)

PENDING_SQL = """
SELECT DISTINCT f.condition_id FROM live_fills f
LEFT JOIN resolutions r ON r.condition_id = f.condition_id
WHERE f.condition_id IS NOT NULL AND f.window_end_ts < ? AND (r.winner IS NULL)
"""


def pending_markets(conn: sqlite3.Connection, now: float, grace_s: float = 90) -> list[str]:
    return [r[0] for r in conn.execute(PENDING_SQL, (now - grace_s,))]


async def fetch_resolutions(client: ApiClient, gamma_url: str, condition_ids: list[str]) -> tuple[list[dict], list[dict]]:
    """(markets rows, resolutions rows with a winner) for closed markets among condition_ids."""
    markets, resolutions = [], []
    now = int(time.time())
    for i in range(0, len(condition_ids), 50):
        chunk = condition_ids[i : i + 50]
        found = await client.get_json(f"{gamma_url}/markets",
                                      {"condition_ids": chunk, "closed": "true", "limit": len(chunk)})
        for m in found:
            events = m.get("events") or []
            row, res = parse.parse_market(m, events[0] if events else None, now)
            markets.append(row)
            if res and res.get("winner"):
                resolutions.append(res)
    return markets, resolutions


async def resolve_pending(client: ApiClient, gamma_url: str, conn: sqlite3.Connection) -> int:
    ids = pending_markets(conn, time.time())
    if not ids:
        return 0
    markets, resolutions = await fetch_resolutions(client, gamma_url, ids)
    _upsert(conn, "markets", markets)
    _upsert(conn, "resolutions", resolutions)
    conn.commit()
    return len(resolutions)


def _upsert(conn: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    conn.executemany(f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                     [tuple(r[c] for c in cols) for r in rows])
