"""Stage 5: market trade tapes — every taker fill of a market, all users — from the Data API.

    GET /v2/trades?condition=<conditionId>&limit=1000[&cursor=...]

`taker_only` defaults to true: one row per taker order and transaction (price = its average over the levels it
took), newest first, cursor pagination. A BTC 5m window has ~1 700 such rows, a 15m window ~700. Profile fields
(name, bio, images) are dropped; the taker wallet and the transaction hash are kept.

Windows are sampled at random from every slot of a series, traded by him or not, so that a strategy is not
tested only on the windows he chose. Their Gamma metadata (tokens, strike, outcome) is fetched by slug into the
same database (tables `markets` / `resolutions`, the layout of bosona.db). Everything lives in data/tape.db.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import time
from pathlib import Path
from typing import Any

from bosona import db, parse
from bosona.config import Config
from bosona.http import ApiClient
from bosona.windows import TF_SECONDS, slug_for

log = logging.getLogger(__name__)

TAPE_SCHEMA = """
PRAGMA journal_mode = WAL;
""" + db.MARKETS_DDL + db.RESOLUTIONS_DDL + """
-- Sampled windows and the state of their tape.
CREATE TABLE IF NOT EXISTS tape_windows (
  slug            TEXT PRIMARY KEY,
  asset           TEXT NOT NULL,
  timeframe       TEXT NOT NULL,
  window_start_ts INTEGER NOT NULL,
  sample          TEXT NOT NULL,       -- random | live_run | recording: why the window is in the set
  condition_id    TEXT,                -- NULL until the Gamma metadata is known
  status          TEXT NOT NULL,       -- pending | done | no_market
  rows            INTEGER,
  pages           INTEGER,
  fetched_at      INTEGER
);

-- Taker fills of a market in time order (seq 0 = oldest; the API returns newest first).
CREATE TABLE IF NOT EXISTS tape (
  condition_id   TEXT NOT NULL,
  seq            INTEGER NOT NULL,
  ts             INTEGER NOT NULL,      -- block timestamp, s
  outcome_index  INTEGER NOT NULL,      -- 0 = Up, 1 = Down
  side           TEXT NOT NULL,         -- side of the taker: BUY | SELL
  price          REAL NOT NULL,         -- average price of the taker order in this transaction
  size           REAL NOT NULL,         -- shares
  taker          TEXT,                  -- proxy wallet of the taker
  tx_hash        TEXT NOT NULL,
  PRIMARY KEY (condition_id, seq)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS tape_tx ON tape (tx_hash);
"""


def connect(path: Path) -> sqlite3.Connection:
    conn = db.connect(path)
    conn.executescript(TAPE_SCHEMA)
    conn.commit()
    return conn


def sample_slots(first: int, last: int, timeframe: str, n: int, seed: int) -> list[int]:
    """`n` distinct window starts drawn uniformly from the slots first..last (inclusive, aligned)."""
    step = TF_SECONDS[timeframe]
    slots = list(range(first - first % step, last + 1, step))
    return sorted(random.Random(seed).sample(slots, min(n, len(slots))))


def add_windows(conn: sqlite3.Connection, asset: str, timeframe: str, starts: list[int], sample: str) -> int:
    rows = [{"slug": slug_for(asset, timeframe, s), "asset": asset, "timeframe": timeframe, "window_start_ts": s,
             "sample": sample, "condition_id": None, "status": "pending", "rows": None, "pages": None,
             "fetched_at": None} for s in starts]
    n = db.insert_ignore(conn, "tape_windows", rows)
    conn.commit()
    return n


def tape_rows(condition_id: str, api_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """API rows (newest first) -> `tape` rows in time order."""
    out = []
    for seq, r in enumerate(reversed(api_rows)):
        out.append({
            "condition_id": condition_id, "seq": seq, "ts": int(r["timestamp"]),
            "outcome_index": int(r["outcome_index"]), "side": str(r["side"]), "price": float(r["price"]),
            "size": float(r["size"]), "taker": (r.get("proxy_wallet") or "").lower() or None,
            "tx_hash": str(r["transaction_hash"]).lower(),
        })
    return out


async def fetch_meta(cfg: Config, client: ApiClient, conn: sqlite3.Connection, slugs: list[str]) -> int:
    """Gamma metadata of windows by slug (closed markets) -> markets / resolutions; returns markets found."""
    found = 0
    url = f"{cfg.gamma_api}/markets"
    for i in range(0, len(slugs), 50):
        chunk = slugs[i : i + 50]
        markets = await client.get_json(url, [("slug", s) for s in chunk] + [("closed", "true"), ("limit", "100")])
        now = int(time.time())
        rows, res = [], []
        for m in markets:
            events = m.get("events") or []
            row, r = parse.parse_market(m, events[0] if events else None, now)
            rows.append(row)
            if r:
                res.append(r)
        db.upsert(conn, "markets", rows)
        db.upsert(conn, "resolutions", res)
        by_slug = {r["slug"]: r["condition_id"] for r in rows}
        for s in chunk:
            if s in by_slug:
                conn.execute("UPDATE tape_windows SET condition_id = ? WHERE slug = ?", (by_slug[s], s))
            else:
                conn.execute("UPDATE tape_windows SET status = 'no_market', fetched_at = ? WHERE slug = ?", (now, s))
        conn.commit()
        found += len(rows)
    return found


async def fetch_tape(cfg: Config, client: ApiClient, condition_id: str, max_pages: int = 50) -> tuple[list[dict], int]:
    rows: list[dict] = []
    cursor, pages = None, 0
    while True:
        params: list[tuple[str, Any]] = [("condition", condition_id), ("limit", 1000)]
        if cursor:
            params.append(("cursor", cursor))
        d = await client.get_json(f"{cfg.data_api}/v2/trades", params)
        rows += d.get("data") or []
        pages += 1
        pag = d.get("pagination") or {}
        cursor = pag.get("next_cursor") if pag.get("has_more") else None
        if not cursor or pages >= max_pages:
            return rows, pages


async def sync_tape(cfg: Config, conn: sqlite3.Connection, concurrency: int = 4) -> dict[str, int]:
    """Metadata for pending windows, then the tape of every window not fetched yet (idempotent)."""
    stats = {"meta": 0, "tapes": 0, "rows": 0, "requests": 0}
    async with ApiClient(cfg) as client:
        need_meta = [r[0] for r in conn.execute("SELECT slug FROM tape_windows WHERE status = 'pending' AND condition_id IS NULL")]
        if need_meta:
            stats["meta"] = await fetch_meta(cfg, client, conn, need_meta)
            log.info("tape: metadata for %d of %d windows", stats["meta"], len(need_meta))
        todo = [(r[0], r[1]) for r in conn.execute(
            "SELECT slug, condition_id FROM tape_windows WHERE status = 'pending' AND condition_id IS NOT NULL "
            "ORDER BY window_start_ts")]
        log.info("tape: %d windows to fetch", len(todo))
        sem = asyncio.Semaphore(concurrency)
        done = 0

        async def one(slug: str, cid: str) -> None:
            nonlocal done
            async with sem:
                api_rows, pages = await fetch_tape(cfg, client, cid)
            rows = tape_rows(cid, api_rows)
            conn.execute("DELETE FROM tape WHERE condition_id = ?", (cid,))
            db.insert_ignore(conn, "tape", rows)
            conn.execute("UPDATE tape_windows SET status = 'done', rows = ?, pages = ?, fetched_at = ? WHERE slug = ?",
                         (len(rows), pages, int(time.time()), slug))
            conn.commit()
            stats["tapes"] += 1
            stats["rows"] += len(rows)
            done += 1
            if done % 200 == 0:
                log.info("tape: %d/%d windows, %d rows", done, len(todo), stats["rows"])

        await asyncio.gather(*(one(s, c) for s, c in todo))
        stats["requests"] = client.requests
    return stats


def plan_sample(cfg: Config, conn: sqlite3.Connection, bos: sqlite3.Connection, n_5m: int, n_15m: int,
                seed: int = 5) -> dict[str, int]:
    """Random BTC 5m / 15m windows over the span of his history, plus every window of the stage 3 live run."""
    added = {}
    for tf, n in (("5m", n_5m), ("15m", n_15m)):
        first, last = bos.execute("SELECT MIN(window_start_ts), MAX(window_start_ts) FROM markets "
                                  "WHERE asset = 'btc' AND timeframe = ? AND closed = 1", (tf,)).fetchone()
        added[f"random_{tf}"] = add_windows(conn, "btc", tf, sample_slots(first, last, tf, n, seed), "random")
    live = cfg.live_db_path
    if live.exists():
        with sqlite3.connect(live) as lc:
            rows = lc.execute("SELECT DISTINCT m.asset, m.timeframe, m.window_start_ts FROM live_fills f "
                              "JOIN markets m USING (condition_id) WHERE m.asset IS NOT NULL").fetchall()
        n = 0
        for asset, tf, ws in rows:
            if tf in TF_SECONDS and ws:
                n += add_windows(conn, asset, tf, [int(ws)], "live_run")
        added["live_run"] = n
    return added
