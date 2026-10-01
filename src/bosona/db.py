"""SQLite schema and small persistence helpers."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np

sqlite3.register_adapter(np.int64, int)
sqlite3.register_adapter(np.int32, int)
sqlite3.register_adapter(np.float64, float)
sqlite3.register_adapter(np.float32, float)
sqlite3.register_adapter(np.bool_, bool)

MARKETS_DDL = """
-- One row per market (conditionId). Metadata from Gamma; re-fetched until the market is final.
CREATE TABLE IF NOT EXISTS markets (
  condition_id        TEXT PRIMARY KEY,
  market_id           TEXT,
  question_id         TEXT,
  slug                TEXT,
  event_slug          TEXT,
  series_slug         TEXT,
  title               TEXT,
  asset               TEXT,              -- btc|eth|sol|xrp|doge|bnb|hype|zec|...
  timeframe           TEXT,              -- 5m|15m|1h|4h|1d
  window_start_ts     INTEGER,           -- eventStartTime (unix s)
  window_end_ts       INTEGER,           -- endDate (unix s)
  accepting_orders_ts INTEGER,           -- trading opens ~24 h before the window
  up_token_id         TEXT,
  down_token_id       TEXT,
  resolution_regime   TEXT,              -- chainlink_spot|chainlink_twap30|chainlink_twap60|binance_1h|binance_noon_1m|unknown
  resolution_source   TEXT,
  crypto_config_id    TEXT,
  twap_lookback_s     INTEGER,
  fee_type            TEXT,
  fee_rate            REAL,              -- 0 when fees are disabled
  fee_exponent        REAL,
  fee_taker_only      INTEGER,
  fee_rebate_rate     REAL,
  order_min_size      REAL,
  tick_size_last      REAL,              -- tick is dynamic (0.01 <-> 0.001); this is the last seen value
  neg_risk            INTEGER,
  closed              INTEGER,
  raw_json            TEXT NOT NULL,     -- Gamma market + parent event, without images/descriptions/volume snapshots
  fetched_at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_markets_asset_tf_start ON markets(asset, timeframe, window_start_ts);
"""

SCHEMA = """
PRAGMA journal_mode = WAL;

""" + MARKETS_DDL + """
CREATE TABLE IF NOT EXISTS resolutions (
  condition_id   TEXT PRIMARY KEY,
  winner         TEXT,                   -- Up|Down|50-50
  payout_up      REAL,
  payout_down    REAL,
  price_to_beat  REAL,                   -- strike (Gamma eventMetadata)
  final_price    REAL,
  strike_source  TEXT,                   -- gamma_meta|NULL
  closed_ts      INTEGER,
  uma_status     TEXT,
  fetched_at     INTEGER NOT NULL
);

-- One row per fill of the tracked wallet (Data API v2 /v2/activity type=TRADE).
-- trade_uid = '{tx}:{token_id}:{side}:{size_raw}:{price_raw}:{seq}', seq = index among identical rows of one tx
-- (two identical maker fills in one tx are real, separate fills - verified on-chain).
CREATE TABLE IF NOT EXISTS trades (
  trade_uid         TEXT PRIMARY KEY,
  tx_hash           TEXT NOT NULL,
  seq               INTEGER NOT NULL,
  ts                INTEGER NOT NULL,    -- block timestamp, seconds
  condition_id      TEXT NOT NULL,
  slug              TEXT,
  token_id          TEXT NOT NULL,
  outcome           TEXT,
  outcome_index     INTEGER,
  side              TEXT NOT NULL,       -- BUY|SELL
  price             REAL NOT NULL,       -- usdc / size as reported by the API
  size_raw          INTEGER NOT NULL,    -- shares * 1e6
  usdc_raw          INTEGER NOT NULL,    -- usdc * 1e6
  size              REAL NOT NULL,
  usdc              REAL NOT NULL,
  role              TEXT,                -- taker|maker (taker = present in /v2/trades?taker_only=true)
  fee_usdc          REAL,                -- taker: size*rate*p*(1-p) rounded to 5 dp; maker: 0
  fee_source        TEXT,                -- formula|chain
  log_index         INTEGER,             -- optional on-chain enrichment
  block_number      INTEGER,
  order_hash        TEXT,
  order_size        REAL,
  order_limit_price REAL,
  event_slug        TEXT,
  source            TEXT NOT NULL,       -- data_api_v2|chain|both
  ingested_at       INTEGER NOT NULL     -- every field of the API row is kept in the columns above
);
CREATE INDEX IF NOT EXISTS ix_trades_cond_ts ON trades(condition_id, ts);
CREATE INDEX IF NOT EXISTS ix_trades_ts ON trades(ts);
CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_chain ON trades(tx_hash, log_index) WHERE log_index IS NOT NULL;

-- Fills where the wallet was the taker (/v2/trades?taker_only=true), keyed exactly like trades.
CREATE TABLE IF NOT EXISTS taker_fills (
  trade_uid   TEXT PRIMARY KEY,
  tx_hash     TEXT NOT NULL,
  ts          INTEGER NOT NULL,
  ingested_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_taker_fills_ts ON taker_fills(ts);

-- Everything that is not a fill: MERGE, REDEEM, SPLIT, REWARD, MAKER_REBATE, TAKER_REBATE, CONVERSION, ...
-- activity_uid = '{tx}:{type}:{condition_id}:{token_id}:{size_raw}:{usdc_raw}:{seq}'
CREATE TABLE IF NOT EXISTS activity (
  activity_uid  TEXT PRIMARY KEY,
  tx_hash       TEXT NOT NULL,
  seq           INTEGER NOT NULL,
  ts            INTEGER NOT NULL,
  type          TEXT NOT NULL,
  condition_id  TEXT,
  slug          TEXT,
  token_id      TEXT,
  outcome       TEXT,
  outcome_index INTEGER,
  size          REAL,
  usdc_size     REAL,
  raw_json      TEXT NOT NULL,
  ingested_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_activity_cond_ts ON activity(condition_id, ts);
CREATE INDEX IF NOT EXISTS ix_activity_type_ts ON activity(type, ts);

-- ---------------------------------------------------------------- stage 2: context
-- Token price history (CLOB /prices-history, ~60 s points). outcome: 0 = Up, 1 = Down.
CREATE TABLE IF NOT EXISTS token_prices (
  market_id INTEGER NOT NULL,          -- markets.market_id (Gamma id)
  outcome   INTEGER NOT NULL,
  t         INTEGER NOT NULL,
  p         REAL NOT NULL,
  PRIMARY KEY (market_id, outcome, t)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS price_sync (
  condition_id TEXT PRIMARY KEY,
  fetched_at   INTEGER NOT NULL,
  points_up    INTEGER,
  points_down  INTEGER
);

-- Gamma eventMetadata of neighbouring windows, used to chain missing strikes / finals of traded windows.
CREATE TABLE IF NOT EXISTS window_meta (
  slug          TEXT PRIMARY KEY,
  price_to_beat REAL,
  final_price   REAL,
  found         INTEGER,
  fetched_at    INTEGER NOT NULL
);

-- Per traded market: official strike/final, how they were obtained, and the Binance proxy anchored on them.
CREATE TABLE IF NOT EXISTS window_refs (
  condition_id     TEXT PRIMARY KEY,
  asset            TEXT,
  timeframe        TEXT,
  regime           TEXT,
  window_start_ts  INTEGER,
  window_end_ts    INTEGER,
  strike           REAL,
  strike_source    TEXT,               -- gamma_meta | gamma_chain_prev | binance_kline | proxy_basis
  final            REAL,
  final_source     TEXT,               -- gamma_meta | gamma_chain_next | binance_kline | proxy_basis
  proxy_strike     REAL,               -- Binance equivalent of the strike (same rule: last close / trailing TWAP / candle)
  proxy_final      REAL,
  basis_bps        REAL,               -- (strike / proxy_strike - 1) * 1e4 when the strike is official
  basis_bps_used   REAL,               -- basis applied to this window (own, else rolling median of neighbours)
  end_residual_bps REAL,               -- (final / (proxy_final * (1 + basis)) - 1) * 1e4: proxy error at window end
  proxy_winner     TEXT,               -- winner implied by the proxy (validation only)
  winner           TEXT,
  computed_at      INTEGER NOT NULL
);

-- Per fill: where the market stood when he traded.
CREATE TABLE IF NOT EXISTS market_context (
  trade_uid      TEXT PRIMARY KEY,
  condition_id   TEXT NOT NULL,
  asset          TEXT,
  timeframe      TEXT,
  regime         TEXT,
  secs_from_open INTEGER,
  secs_to_close  INTEGER,
  strike         REAL,
  strike_source  TEXT,
  spot           REAL,                 -- Binance 1s close of the second before the trade's block second
  spot_adj       REAL,                 -- spot in the resolution source's terms (Chainlink regimes: * (1 + basis))
  spot_source    TEXT,                 -- binance_1s | binance_1s_basis_adj
  dist_bps       REAL,                 -- (spot_adj / strike - 1) * 1e4
  twap           REAL,                 -- trailing TWAP (regime lookback), TWAP-resolved markets only, source terms
  dist_twap_bps  REAL,
  ret_10s_bps    REAL,                 -- spot move over the last 10 s / 60 s before the trade
  ret_60s_bps    REAL,
  vol_1m_bps     REAL,                 -- realized: sqrt(sum of squared 1s log returns) * 1e4 over 1/5/15 min
  vol_5m_bps     REAL,
  vol_15m_bps    REAL,
  up_px          REAL,                 -- last CLOB price-history points at or before the trade
  down_px        REAL,
  px_age_s       INTEGER,
  winner         TEXT,
  payout         REAL,                 -- payout per share of the traded outcome (1, 0 or 0.5)
  pnl_if_held    REAL,                 -- size * payout - usdc - fee; merging before resolution does not change it
  computed_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_context_cond ON market_context(condition_id);

-- Per market position ("episode"): what he bought on each side and how it ended.
CREATE TABLE IF NOT EXISTS episodes (
  condition_id    TEXT PRIMARY KEY,
  asset           TEXT,
  timeframe       TEXT,
  regime          TEXT,
  window_start_ts INTEGER,
  window_end_ts   INTEGER,
  n_fills         INTEGER,
  n_taker         INTEGER,
  first_ts        INTEGER,
  last_ts         INTEGER,
  up_shares       REAL,
  up_cost         REAL,
  down_shares     REAL,
  down_cost       REAL,
  fees            REAL,
  avg_up_px       REAL,
  avg_down_px     REAL,
  paired_shares   REAL,                -- min(up_shares, down_shares)
  pair_cost       REAL,                -- avg_up_px + avg_down_px (both sides bought)
  net_exposure    REAL,                -- up_shares - down_shares (> 0: long Up)
  merged_shares   REAL,
  merge_usdc      REAL,
  redeem_usdc     REAL,
  winner          TEXT,
  payout_up       REAL,
  payout_down     REAL,
  pnl             REAL,                -- up*payout_up + down*payout_down - costs - fees
  computed_at     INTEGER NOT NULL
);

-- Daily PnL series of the wallet (/v2/user-pnl), used to reconcile the local history.
CREATE TABLE IF NOT EXISTS pnl_daily (
  ts                  INTEGER PRIMARY KEY,
  source_block        INTEGER,
  trade_pnl           REAL,
  realized_market_pnl REAL,
  unrealized_pnl      REAL,
  fees_paid           REAL,
  maker_rebate        REAL,
  taker_rebate        REAL,
  reward_income       REAL,
  economic_pnl        REAL,
  volume              REAL,
  volume_usdc         REAL,
  trade_count         INTEGER,
  raw_json            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
  source     TEXT NOT NULL,
  scope      TEXT NOT NULL,
  last_ts    INTEGER,
  updated_at INTEGER NOT NULL,
  note       TEXT,
  PRIMARY KEY (source, scope)
);
"""


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(market_context)")}
    if cols and "spot_adj" not in cols:  # stage-1 placeholder layout; derived data, safe to rebuild
        conn.execute("DROP TABLE market_context")
    conn.executescript(SCHEMA)
    conn.commit()


def insert_ignore(conn: sqlite3.Connection, table: str, rows: Sequence[dict]) -> int:
    """INSERT OR IGNORE rows (dicts with identical keys). Returns the number of new rows."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    sql = f"INSERT OR IGNORE INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
    before = conn.total_changes
    conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
    return conn.total_changes - before


def upsert(conn: sqlite3.Connection, table: str, rows: Sequence[dict]) -> int:
    """INSERT OR REPLACE rows (dicts with identical keys)."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    sql = f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
    conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
    return len(rows)


def replace_frame(conn: sqlite3.Connection, table: str, df, chunk: int = 50_000) -> int:
    """Replace the whole content of a derived table with a DataFrame (NaN -> NULL), in chunks."""
    import pandas as pd

    conn.execute(f"DELETE FROM {table}")
    cols = list(df.columns)
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
    for start in range(0, len(df), chunk):
        part = df.iloc[start : start + chunk].astype(object)
        part = part.where(pd.notna(part), None)
        conn.executemany(sql, part.itertuples(index=False, name=None))
    conn.commit()
    return len(df)


def get_state(conn: sqlite3.Connection, source: str, scope: str) -> int | None:
    row = conn.execute("SELECT last_ts FROM sync_state WHERE source = ? AND scope = ?", (source, scope)).fetchone()
    return None if row is None else row["last_ts"]


def set_state(conn: sqlite3.Connection, source: str, scope: str, last_ts: int, note: str = "") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO sync_state (source, scope, last_ts, updated_at, note) VALUES (?, ?, ?, ?, ?)",
        (source, scope, last_ts, int(time.time()), note),
    )


def scalar(conn: sqlite3.Connection, sql: str, params: Iterable = ()) -> object:
    row = conn.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]
