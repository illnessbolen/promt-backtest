"""Live tracker storage: a separate SQLite file (default data/live.db).

Separate from bosona.db on purpose: the tracker writes small transactions every half second and must
never wait on a long batch job (`stage2` rewrites whole tables); later stages ATTACH it.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from bosona.db import MARKETS_DDL, RESOLUTIONS_DDL

log = logging.getLogger(__name__)

LIVE_SCHEMA = """
PRAGMA journal_mode = WAL;
""" + MARKETS_DDL + RESOLUTIONS_DDL + """
-- One row per fill of the wallet seen live. fill_key = '{tx}:{token_id}:{side}:{size_raw}:{seq}'
-- (the same fill reported by several channels collapses into one row).
CREATE TABLE IF NOT EXISTS live_fills (
  fill_key        TEXT PRIMARY KEY,
  tx_hash         TEXT NOT NULL,
  log_index       INTEGER,
  block_number    INTEGER,
  block_ts        INTEGER,             -- block timestamp, s (chain; else the Data API timestamp, which equals it)
  match_ms        REAL,                -- CLOB match time: market-channel last_trade_price of the same tx, ms
  condition_id    TEXT,
  slug            TEXT,
  asset           TEXT,
  timeframe       TEXT,
  window_start_ts INTEGER,
  window_end_ts   INTEGER,
  token_id        TEXT NOT NULL,
  outcome         TEXT,
  side            TEXT NOT NULL,       -- BUY|SELL of the wallet
  size            REAL NOT NULL,
  price           REAL,
  usdc            REAL,
  role            TEXT,                -- maker|taker (chain: taker when OrderFilled.taker is the exchange)
  fee_usdc        REAL,
  order_hash      TEXT,
  first_channel   TEXT NOT NULL,       -- rtds_activity|chain_logs|data_api
  first_seen_ms   REAL NOT NULL,       -- local clock, ms
  channels        TEXT,                -- every channel that reported it, first first
  lat_block_ms    REAL,                -- first_seen_ms - block_ts * 1000 (block ts has 1 s resolution)
  lat_match_ms    REAL,                -- first_seen_ms - match_ms
  secs_to_close   REAL,                -- window_end_ts - trade reference time
  backfill        INTEGER NOT NULL DEFAULT 0, -- 1: first seen > 60 s after the block (reconnect gap), excluded from latency stats
  ref_ms          REAL,                -- reference time of the trade (see ref_kind)
  ref_kind        TEXT,                -- match: CLOB match time | block_est: block ts - median(block - match) of this run
                                       -- | rtds_ts: RTDS timestamp (s resolution) when nothing else is known
  spot_source     TEXT,                -- source/kind of the headline spot below (resolution source when live, else binance)
  spot_ref        REAL,                -- at ref_ms
  spot_detect     REAL,                -- at first_seen_ms
  spot_shift_bps  REAL,                -- (spot_detect / spot_ref - 1) * 1e4
  bid_ref         REAL,                -- his token's top of book just before ref_ms (market channel history)
  ask_ref         REAL,
  bid_detect      REAL,                -- ... at detection
  ask_detect      REAL,
  copy_px         REAL,                -- best price a copier gets at detection: ask for BUY, bid for SELL
  copy_vwap       REAL,                -- VWAP to copy his full size from the detection book (NULL: not enough depth)
  copy_slip       REAL,                -- copy_vwap - price for BUY, price - copy_vwap for SELL (> 0: worse than him)
  size_at_px      REAL,                -- shares still offered at his price or better at detection
  finalized_ms    REAL,
  updated_ms      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_live_fills_seen ON live_fills(first_seen_ms);

-- Every channel's sighting of a fill (latency comparison between channels).
CREATE TABLE IF NOT EXISTS live_detections (
  fill_key     TEXT NOT NULL,
  channel      TEXT NOT NULL,
  recv_ms      REAL NOT NULL,
  src_ts_ms    REAL,                   -- timestamp attached by the channel (RTDS payload, block, API row)
  raw_json     TEXT,
  PRIMARY KEY (fill_key, channel)
);

-- Spot / reference prices around each fill, one row per source and kind.
CREATE TABLE IF NOT EXISTS live_spot_snaps (
  fill_key   TEXT NOT NULL,
  source     TEXT NOT NULL,            -- binance | chainlink | chainlink_ds | ...
  kind       TEXT NOT NULL,            -- spot | twap60
  ref_ts_ms  REAL,                     -- last tick at or before the trade reference time
  ref_price  REAL,
  det_ts_ms  REAL,                     -- last tick at detection
  det_price  REAL,
  det_age_ms REAL,                     -- detection time - det_ts_ms (how stale the source was)
  shift_bps  REAL,
  PRIMARY KEY (fill_key, source, kind)
);

-- Order books of both tokens at detection.
CREATE TABLE IF NOT EXISTS live_books (
  fill_key     TEXT NOT NULL,
  token_id     TEXT NOT NULL,
  src          TEXT NOT NULL,          -- ws: local book of the market channel | rest: GET /book at detection
  outcome      TEXT,
  taken_ms     REAL NOT NULL,          -- local time of the snapshot
  server_ts_ms REAL,                   -- timestamp of the book on the server
  best_bid     REAL,
  best_ask     REAL,
  bid_size     REAL,
  ask_size     REAL,
  bids_json    TEXT,                   -- top levels [[price, size], ...], best first
  asks_json    TEXT,
  PRIMARY KEY (fill_key, token_id, src)
);

-- Per-second prices of every source (last value in the second), kept for the whole run.
CREATE TABLE IF NOT EXISTS live_spot (
  source  TEXT NOT NULL,
  kind    TEXT NOT NULL,
  asset   TEXT NOT NULL,
  t       INTEGER NOT NULL,            -- unix second of the source timestamp
  price   REAL NOT NULL,
  ts_ms   REAL,
  recv_ms REAL,
  PRIMARY KEY (source, kind, asset, t)
) WITHOUT ROWID;

-- Binance vs Chainlink at every close of the Chainlink-resolved windows (5m/15m/4h) of the tracked assets.
CREATE TABLE IF NOT EXISTS live_window_close (
  asset           TEXT NOT NULL,
  timeframe       TEXT NOT NULL,
  window_end_ts   INTEGER NOT NULL,
  window_start_ts INTEGER,
  slug            TEXT,
  cl_start        REAL,                -- Chainlink TWAP-60 (RTDS) at the window start / end: the resolution inputs
  cl_end          REAL,
  cl_spot_end     REAL,                -- Chainlink spot at the end
  bn_start        REAL,                -- Binance 60 s TWAP of 1 s closes at start / end (same rule, own computation)
  bn_end          REAL,
  bn_spot_end     REAL,                -- last Binance trade at the end
  div_spot_bps    REAL,                -- (bn_spot_end / cl_spot_end - 1) * 1e4
  div_twap_bps    REAL,                -- (bn_end / cl_end - 1) * 1e4
  basis_bps       REAL,                -- median spot divergence over the previous 10 min (USDT/USD basis)
  move_cl_bps     REAL,                -- (cl_end / cl_start - 1) * 1e4
  winner_cl       TEXT,                -- Up if cl_end >= cl_start
  winner_bn       TEXT,                -- same rule on the Binance TWAP
  official_strike REAL,                -- Gamma eventMetadata.priceToBeat / finalPrice, filled after resolution
  official_final  REAL,
  official_winner TEXT,
  recorded_ms     REAL NOT NULL,
  resolved_ms     REAL,
  PRIMARY KEY (asset, timeframe, window_end_ts)
);

-- Once a minute: state of every feed (connected, message counts, staleness).
CREATE TABLE IF NOT EXISTS live_health (
  ts        INTEGER NOT NULL,
  component TEXT NOT NULL,
  json      TEXT NOT NULL,
  PRIMARY KEY (ts, component)
);
"""

FILL_COLUMNS = [
    "fill_key", "tx_hash", "log_index", "block_number", "block_ts", "match_ms", "condition_id", "slug", "asset",
    "timeframe", "window_start_ts", "window_end_ts", "token_id", "outcome", "side", "size", "price", "usdc", "role",
    "fee_usdc", "order_hash", "first_channel", "first_seen_ms", "channels", "lat_block_ms", "lat_match_ms",
    "secs_to_close", "backfill", "ref_ms", "ref_kind", "spot_source", "spot_ref", "spot_detect", "spot_shift_bps", "bid_ref",
    "ask_ref", "bid_detect", "ask_detect", "copy_px", "copy_vwap", "copy_slip", "size_at_px", "finalized_ms",
    "updated_ms",
]


class LiveStore:
    """Buffered writer: callers enqueue rows, `flush()` writes them in one transaction (called every ~0.5 s)."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.executescript(LIVE_SCHEMA)
        self.conn.commit()
        self._pending: list[tuple[str, Sequence[tuple]]] = []
        self._spot_rows: list[tuple] = []
        self.rows_written = 0

    def _enqueue(self, verb: str, table: str, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        cols = list(rows[0].keys())
        sql = f"{verb} INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
        self._pending.append((sql, [tuple(r.get(c) for c in cols) for r in rows]))

    def upsert(self, table: str, rows: Sequence[dict[str, Any]]) -> None:
        self._enqueue("INSERT OR REPLACE", table, rows)

    def insert_ignore(self, table: str, rows: Sequence[dict[str, Any]]) -> None:
        self._enqueue("INSERT OR IGNORE", table, rows)

    def add_spot(self, tick: Any) -> None:
        """One closed second of a price source (PriceBook.on_second)."""
        self._spot_rows.append((tick.source, tick.kind, tick.asset, int(tick.ts_ms // 1000), tick.price, tick.ts_ms, tick.recv_ms))

    def flush(self) -> int:
        if self._spot_rows:
            rows, self._spot_rows = self._spot_rows, []
            self._pending.append(("INSERT OR REPLACE INTO live_spot (source, kind, asset, t, price, ts_ms, recv_ms) "
                                  "VALUES (?, ?, ?, ?, ?, ?, ?)", rows))
        if not self._pending:
            return 0
        pending, self._pending = self._pending, []
        n = 0
        try:
            with self.conn:
                for sql, params in pending:
                    self.conn.executemany(sql, params)
                    n += len(params)
        except sqlite3.Error:
            log.exception("live store flush failed (%d statements dropped)", len(pending))
            return 0
        self.rows_written += n
        return n

    def spot_series(self, source: str, kind: str, asset: str, t0: int, t1: int) -> list[tuple[int, float]]:
        """Per-second values with t0 <= t <= t1 (ascending)."""
        return [(r[0], r[1]) for r in self.conn.execute(
            "SELECT t, price FROM live_spot WHERE source = ? AND kind = ? AND asset = ? AND t BETWEEN ? AND ? ORDER BY t",
            (source, kind, asset, t0, t1))]

    def spot_at(self, source: str, kind: str, asset: str, t: int, max_lag_s: int = 5) -> float | None:
        """Last per-second value at or before second t (None if older than max_lag_s)."""
        row = self.conn.execute(
            "SELECT price FROM live_spot WHERE source = ? AND kind = ? AND asset = ? AND t BETWEEN ? AND ? "
            "ORDER BY t DESC LIMIT 1", (source, kind, asset, t - max_lag_s, t)).fetchone()
        return None if row is None else row[0]

    def markets(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM markets")]

    def health(self, ts: int, component: str, state: dict[str, Any]) -> None:
        self.upsert("live_health", [{"ts": ts, "component": component, "json": json.dumps(state, default=str)}])

    def close(self) -> None:
        self.flush()
        self.conn.close()
