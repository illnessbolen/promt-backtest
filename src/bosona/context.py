"""Stage 2: market context of every fill.

Price sources (see CLAUDE.md, open question 2):
  * binance_1h / binance_noon_1m markets resolve on Binance {ASSET}USDT: spot, strike and final come from
    the same source, so distances are exact (up to the 1-second grid).
  * chainlink_* markets resolve on Chainlink Data Streams, which has no public intra-window history.
    The official strike/final are known (Gamma eventMetadata, gaps chained from neighbour windows);
    the intra-window price is Binance 1s anchored on the window: spot_adj = binance * (1 + basis), where
    basis = strike / proxy_strike - 1 is measured at the window start with the same rule the market uses
    (last price before the start, or the trailing TWAP for TWAP markets). The anchoring error is reported
    per window at the window end (window_refs.end_residual_bps).
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

import numpy as np
import pandas as pd

from bosona import db
from bosona.config import Config
from bosona.http import ApiClient
from bosona.spot import DAY, SYMBOLS, SpotCache, day_start

log = logging.getLogger(__name__)

DURATION = {"5m": 300, "15m": 900, "1h": 3_600, "4h": 14_400, "1d": 86_400}
TWAP_LOOKBACK = {"chainlink_twap30": 30, "chainlink_twap60": 60}
ROLLING_BASIS_WINDOW_S = 3 * 3_600


# --------------------------------------------------------------------------- series helpers


class Series:
    """1s closes of one symbol on a contiguous grid with O(1) window statistics."""

    def __init__(self, base: int, close: np.ndarray) -> None:
        self.base = base
        self.close = close
        valid = ~np.isnan(close)
        self._cs = np.concatenate([[0.0], np.cumsum(np.where(valid, close, 0.0))])
        self._cn = np.concatenate([[0], np.cumsum(valid)])
        logp = np.log(close)
        r = np.diff(logp, prepend=np.nan)            # r[i] = log(close[i] / close[i-1])
        rv = ~np.isnan(r)
        self._cr2 = np.concatenate([[0.0], np.cumsum(np.where(rv, r * r, 0.0))])
        self._crn = np.concatenate([[0], np.cumsum(rv)])

    def _idx(self, t: np.ndarray) -> np.ndarray:
        return np.asarray(t, dtype=np.int64) - self.base

    def at(self, t: np.ndarray) -> np.ndarray:
        """Close of the 1s kline that starts at second t."""
        i = self._idx(t)
        ok = (i >= 0) & (i < len(self.close))
        out = np.full(i.shape, np.nan)
        out[ok] = self.close[i[ok]]
        return out

    def mean(self, t0: np.ndarray, t1: np.ndarray) -> np.ndarray:
        """Mean close over seconds [t0, t1); NaN unless every second is known."""
        a, b = self._idx(t0), self._idx(t1)
        ok = (a >= 0) & (b <= len(self.close)) & (b > a)
        out = np.full(a.shape, np.nan)
        a, b = a[ok], b[ok]
        n = self._cn[b] - self._cn[a]
        full = n == (b - a)
        vals = np.full(a.shape, np.nan)
        vals[full] = (self._cs[b] - self._cs[a])[full] / n[full]
        out[ok] = vals
        return out

    def realized_vol_bps(self, t_end: np.ndarray, seconds: int) -> np.ndarray:
        """sqrt(sum of squared 1s log returns) over the `seconds` returns ending at close[t_end]; in bps."""
        b = self._idx(t_end) + 1
        a = b - seconds
        ok = (a >= 1) & (b <= len(self.close))
        out = np.full(b.shape, np.nan)
        a, b = a[ok], b[ok]
        n = self._crn[b] - self._crn[a]
        full = n == seconds
        vals = np.full(a.shape, np.nan)
        vals[full] = np.sqrt(self._cr2[b] - self._cr2[a])[full] * 1e4
        out[ok] = vals
        return out


def load_series(cache: SpotCache, symbol: str, t0: int, t1: int) -> Series:
    base = day_start(t0)
    end = day_start(t1 - 1) + DAY
    return Series(base, cache.closes(symbol, base, end))


def proxy_refs(s: Series, regime: np.ndarray, ws: np.ndarray, we: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Binance equivalents of the strike and the final price, using each market's own rule."""
    strike = s.at(ws - 1)            # last 1s close before the window start
    final = s.at(we - 1)             # last 1s close before the window end (= 1h candle close)
    for name, lb in TWAP_LOOKBACK.items():
        m = regime == name
        if m.any():
            strike[m] = s.mean(ws[m] - lb, ws[m])
            final[m] = s.mean(we[m] - lb, we[m])
    noon = regime == "binance_noon_1m"   # close of the 1m candle that starts at 12:00 ET
    if noon.any():
        strike[noon] = s.at(ws[noon] + 59)
        final[noon] = s.at(we[noon] + 59)
    return strike, final


# --------------------------------------------------------------------------- strike gaps via neighbours


def neighbour_slugs(conn: sqlite3.Connection) -> list[str]:
    """Adjacent windows needed to chain missing strikes/finals of Chainlink markets."""
    rows = conn.execute(
        """
        SELECT m.asset, m.timeframe, m.window_start_ts, r.price_to_beat, r.final_price
        FROM markets m JOIN resolutions r USING (condition_id)
        WHERE m.resolution_regime LIKE 'chainlink%' AND (r.price_to_beat IS NULL OR r.final_price IS NULL)
          AND m.condition_id IN (SELECT DISTINCT condition_id FROM trades)
        """
    ).fetchall()
    slugs = set()
    for asset, tf, ws, ptb, fin in rows:
        d = DURATION[tf]
        if ptb is None:
            slugs.add(f"{asset}-updown-{tf}-{ws - d}")
        if fin is None:
            slugs.add(f"{asset}-updown-{tf}-{ws + d}")
    known = {r[0] for r in conn.execute("SELECT slug FROM window_meta")}
    return sorted(slugs - known)


async def sync_neighbour_meta(cfg: Config, conn: sqlite3.Connection, client: ApiClient) -> dict[str, int]:
    slugs = neighbour_slugs(conn)
    batch = int(cfg.sync.get("gamma_batch", 50))
    got = 0
    for i in range(0, len(slugs), batch):
        chunk = slugs[i : i + batch]
        events = await client.get_json(f"{cfg.gamma_api}/events", {"slug": chunk, "closed": "true", "limit": 100})
        found = {e["slug"]: e for e in events}
        now = int(time.time())
        rows = []
        for slug in chunk:
            meta = (found.get(slug) or {}).get("eventMetadata") or {}
            rows.append({"slug": slug, "price_to_beat": meta.get("priceToBeat"),
                         "final_price": meta.get("finalPrice"), "found": int(slug in found), "fetched_at": now})
            got += int(slug in found)
        db.upsert(conn, "window_meta", rows)
        conn.commit()
    log.info("neighbour windows: %d requested, %d found", len(slugs), got)
    return {"requested": len(slugs), "found": got}


# --------------------------------------------------------------------------- window refs


def build_window_refs(conn: sqlite3.Connection, cache: SpotCache) -> pd.DataFrame:
    w = pd.read_sql(
        """
        SELECT m.condition_id, m.asset, m.timeframe, m.resolution_regime AS regime,
               m.window_start_ts AS ws, m.window_end_ts AS we,
               r.price_to_beat, r.final_price, r.winner, r.payout_up, r.payout_down
        FROM markets m LEFT JOIN resolutions r USING (condition_id)
        WHERE m.condition_id IN (SELECT DISTINCT condition_id FROM trades) AND m.window_start_ts IS NOT NULL
        """,
        conn,
    )
    meta = pd.read_sql("SELECT slug, price_to_beat, final_price FROM window_meta", conn).set_index("slug")

    strike = w["price_to_beat"].astype(float).copy()
    final = w["final_price"].astype(float).copy()
    s_src = np.where(strike.notna(), "gamma_meta", None).astype(object)
    f_src = np.where(final.notna(), "gamma_meta", None).astype(object)
    chain = w["regime"].str.startswith("chainlink")
    dur = w["timeframe"].map(DURATION)
    for i in np.flatnonzero(chain & strike.isna()):
        prev = f"{w.at[i, 'asset']}-updown-{w.at[i, 'timeframe']}-{int(w.at[i, 'ws'] - dur[i])}"
        v = meta["final_price"].get(prev)
        if v is not None and not pd.isna(v):
            strike[i], s_src[i] = v, "gamma_chain_prev"
    for i in np.flatnonzero(chain & final.isna()):
        nxt = f"{w.at[i, 'asset']}-updown-{w.at[i, 'timeframe']}-{int(w.at[i, 'ws'] + dur[i])}"
        v = meta["price_to_beat"].get(nxt)
        if v is not None and not pd.isna(v):
            final[i], f_src[i] = v, "gamma_chain_next"

    proxy_strike = np.full(len(w), np.nan)
    proxy_final = np.full(len(w), np.nan)
    for asset, idx in w.groupby("asset").groups.items():
        symbol = SYMBOLS.get(asset)
        if symbol is None:
            continue
        sub = w.loc[idx]
        s = load_series(cache, symbol, int(sub["ws"].min()) - 3_600, int(sub["we"].max()) + 3_600)
        ps, pf = proxy_refs(s, sub["regime"].to_numpy(), sub["ws"].to_numpy(), sub["we"].to_numpy())
        proxy_strike[np.asarray(idx)] = ps
        proxy_final[np.asarray(idx)] = pf

    binance = w["regime"].str.startswith("binance")
    # Binance markets: the proxy *is* the source; fill gaps from it
    strike = strike.where(~(binance & strike.isna()), pd.Series(proxy_strike, index=w.index))
    s_src = np.where(binance & pd.isna(s_src) & ~np.isnan(proxy_strike), "binance_1s", s_src)
    final = final.where(~(binance & final.isna()), pd.Series(proxy_final, index=w.index))
    f_src = np.where(binance & pd.isna(f_src) & ~np.isnan(proxy_final), "binance_1s", f_src)

    basis = np.where(chain & pd.Series(s_src).isin(["gamma_meta", "gamma_chain_prev"]),
                     (strike.to_numpy() / proxy_strike - 1) * 1e4, np.nan)
    basis_used = basis.copy()
    # windows without an official strike: rolling median basis of the same asset (+-3 h)
    for asset, idx in w.groupby("asset").groups.items():
        idx = np.asarray(idx)
        order = idx[np.argsort(w.loc[idx, "ws"].to_numpy())]
        t = w.loc[order, "ws"].to_numpy()
        b = basis[order]
        have = ~np.isnan(b)
        tk, bk = t[have], b[have]
        for j in np.flatnonzero(~have & chain.to_numpy()[order]):
            lo, hi = np.searchsorted(tk, [t[j] - ROLLING_BASIS_WINDOW_S, t[j] + ROLLING_BASIS_WINDOW_S])
            if hi > lo:
                basis_used[order[j]] = float(np.median(bk[lo:hi]))
    basis_used = np.where(binance, 0.0, basis_used)

    adj = 1 + basis_used / 1e4                      # NaN when no basis is known
    missing_strike = strike.isna().to_numpy() & ~np.isnan(proxy_strike * adj)
    strike = np.where(missing_strike, proxy_strike * adj, strike.to_numpy())
    s_src = np.where(missing_strike, "proxy_basis", s_src)
    missing_final = final.isna().to_numpy() & ~np.isnan(proxy_final * adj)
    final = np.where(missing_final, proxy_final * adj, final.to_numpy())
    f_src = np.where(missing_final, "proxy_basis", f_src)

    official_final = pd.Series(f_src).isin(["gamma_meta", "gamma_chain_next"]).to_numpy()
    end_resid = np.where(chain.to_numpy() & official_final, (final / (proxy_final * adj) - 1) * 1e4, np.nan)
    pf_adj = proxy_final * adj
    proxy_winner = np.where(np.isnan(pf_adj) | np.isnan(strike), None,
                            np.where(pf_adj >= strike, "Up", "Down"))

    out = pd.DataFrame({
        "condition_id": w["condition_id"], "asset": w["asset"], "timeframe": w["timeframe"], "regime": w["regime"],
        "window_start_ts": w["ws"], "window_end_ts": w["we"],
        "strike": strike, "strike_source": s_src, "final": final, "final_source": f_src,
        "proxy_strike": proxy_strike, "proxy_final": proxy_final,
        "basis_bps": basis, "basis_bps_used": basis_used, "end_residual_bps": end_resid,
        "proxy_winner": proxy_winner, "winner": w["winner"], "computed_at": int(time.time()),
    })
    return out


# --------------------------------------------------------------------------- per-trade context


def load_token_prices(conn: sqlite3.Connection) -> dict[int, tuple[np.ndarray, ...]]:
    tp = pd.read_sql("SELECT market_id, outcome, t, p FROM token_prices ORDER BY market_id, outcome, t", conn)
    out: dict[int, tuple[np.ndarray, ...]] = {}
    for (mid, outcome), g in tp.groupby(["market_id", "outcome"], sort=False):
        prev = out.get(mid, (np.empty(0), np.empty(0), np.empty(0), np.empty(0)))
        arrs = (g["t"].to_numpy(), g["p"].to_numpy())
        out[mid] = arrs + prev[2:] if outcome == 0 else prev[:2] + arrs
    return out


def last_at_or_before(t: np.ndarray, p: np.ndarray, ts: int) -> tuple[float, int | None]:
    if len(t) == 0:
        return np.nan, None
    k = np.searchsorted(t, ts, side="right") - 1
    return (float(p[k]), int(ts - t[k])) if k >= 0 else (np.nan, None)


def fill_pnl(side: np.ndarray, size: np.ndarray, usdc: np.ndarray, payout: np.ndarray) -> np.ndarray:
    """PnL of a fill held to resolution, after fees.

    `usdc` is the Data API `usdcSize`, which for a taker BUY already includes the fee: on-chain the wallet sends
    size * price + fee in pUSD, and usdc - size * price = fee_usdc for every taker fill of the history. So the fee
    is not subtracted again. A taker SELL is assumed symmetric (usdc = proceeds after the fee); the history has no
    SELL fills to check it on."""
    sign = np.where(side == "BUY", 1.0, -1.0)
    return sign * (size * payout - usdc)


def build_context(conn: sqlite3.Connection, cache: SpotCache, refs: pd.DataFrame) -> pd.DataFrame:
    tr = pd.read_sql(
        """
        SELECT t.trade_uid, t.condition_id, t.ts, t.outcome, t.side, t.size, t.usdc, t.fee_usdc,
               CAST(m.market_id AS INTEGER) AS market_id, r.payout_up, r.payout_down
        FROM trades t JOIN markets m USING (condition_id) LEFT JOIN resolutions r USING (condition_id)
        """,
        conn,
    )
    tr = tr.merge(refs, on="condition_id", how="left")
    n = len(tr)
    cols: dict[str, Any] = {k: np.full(n, np.nan) for k in (
        "spot", "spot_adj", "dist_bps", "twap", "dist_twap_bps", "ret_10s_bps", "ret_60s_bps",
        "vol_1m_bps", "vol_5m_bps", "vol_15m_bps")}
    for asset, idx in tr.groupby("asset").groups.items():
        symbol = SYMBOLS.get(asset)
        if symbol is None:
            continue
        idx = np.asarray(idx)
        sub = tr.loc[idx]
        ts = sub["ts"].to_numpy()
        s = load_series(cache, symbol, int(ts.min()) - 1_200, int(ts.max()) + 60)
        t_prev = ts - 1                                   # last full second before the block second
        spot = s.at(t_prev)
        adj = 1 + sub["basis_bps_used"].fillna(0).to_numpy() / 1e4
        strike = sub["strike"].to_numpy()
        cols["spot"][idx] = spot
        cols["spot_adj"][idx] = spot * adj
        cols["dist_bps"][idx] = (spot * adj / strike - 1) * 1e4
        cols["ret_10s_bps"][idx] = (spot / s.at(t_prev - 10) - 1) * 1e4
        cols["ret_60s_bps"][idx] = (spot / s.at(t_prev - 60) - 1) * 1e4
        for name, secs in (("vol_1m_bps", 60), ("vol_5m_bps", 300), ("vol_15m_bps", 900)):
            cols[name][idx] = s.realized_vol_bps(t_prev, secs)
        regime = sub["regime"].to_numpy()
        for name, lb in TWAP_LOOKBACK.items():
            m = regime == name
            if m.any():
                twap = s.mean(ts[m] - lb, ts[m]) * adj[m]
                cols["twap"][idx[m]] = twap
                cols["dist_twap_bps"][idx[m]] = (twap / strike[m] - 1) * 1e4

    prices = load_token_prices(conn)
    up_px, down_px, age = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    empty = (np.empty(0), np.empty(0), np.empty(0), np.empty(0))
    for i, (mid, ts) in enumerate(zip(tr["market_id"].to_numpy(), tr["ts"].to_numpy())):
        tu, pu, td, pdn = prices.get(int(mid), empty)
        up_px[i], a1 = last_at_or_before(tu, pu, int(ts))
        down_px[i], a2 = last_at_or_before(td, pdn, int(ts))
        ages = [a for a in (a1, a2) if a is not None]
        age[i] = max(ages) if ages else np.nan

    payout = np.where(tr["outcome"] == "Up", tr["payout_up"], tr["payout_down"]).astype(float)
    pnl = fill_pnl(tr["side"].to_numpy(), tr["size"].to_numpy(), tr["usdc"].to_numpy(), payout)

    return pd.DataFrame({
        "trade_uid": tr["trade_uid"], "condition_id": tr["condition_id"], "asset": tr["asset"],
        "timeframe": tr["timeframe"], "regime": tr["regime"],
        "secs_from_open": tr["ts"] - tr["window_start_ts"], "secs_to_close": tr["window_end_ts"] - tr["ts"],
        "strike": tr["strike"], "strike_source": tr["strike_source"],
        "spot": cols["spot"], "spot_adj": cols["spot_adj"],
        "spot_source": np.where(tr["regime"].str.startswith("chainlink"), "binance_1s_basis_adj", "binance_1s"),
        "dist_bps": cols["dist_bps"], "twap": cols["twap"], "dist_twap_bps": cols["dist_twap_bps"],
        "ret_10s_bps": cols["ret_10s_bps"], "ret_60s_bps": cols["ret_60s_bps"],
        "vol_1m_bps": cols["vol_1m_bps"], "vol_5m_bps": cols["vol_5m_bps"], "vol_15m_bps": cols["vol_15m_bps"],
        "up_px": up_px, "down_px": down_px, "px_age_s": age,
        "winner": tr["winner"], "payout": payout, "pnl_if_held": pnl, "computed_at": int(time.time()),
    })


def build_episodes(conn: sqlite3.Connection) -> pd.DataFrame:
    tr = pd.read_sql(
        """
        SELECT t.condition_id, t.ts, t.outcome, t.side, t.size, t.usdc, t.fee_usdc, t.role,
               m.asset, m.timeframe, m.resolution_regime AS regime, m.window_start_ts, m.window_end_ts,
               r.winner, r.payout_up, r.payout_down
        FROM trades t JOIN markets m USING (condition_id) LEFT JOIN resolutions r USING (condition_id)
        """,
        conn,
    )
    sign = np.where(tr["side"] == "BUY", 1.0, -1.0)
    up = tr["outcome"] == "Up"
    tr["up_sh"] = np.where(up, sign * tr["size"], 0.0)
    tr["up_c"] = np.where(up, sign * tr["usdc"], 0.0)
    tr["dn_sh"] = np.where(~up, sign * tr["size"], 0.0)
    tr["dn_c"] = np.where(~up, sign * tr["usdc"], 0.0)
    tr["is_taker"] = (tr["role"] == "taker").astype(int)
    g = tr.groupby("condition_id")
    ep = g.agg(
        asset=("asset", "first"), timeframe=("timeframe", "first"), regime=("regime", "first"),
        window_start_ts=("window_start_ts", "first"), window_end_ts=("window_end_ts", "first"),
        n_fills=("ts", "size"), n_taker=("is_taker", "sum"), first_ts=("ts", "min"), last_ts=("ts", "max"),
        up_shares=("up_sh", "sum"), up_cost=("up_c", "sum"), down_shares=("dn_sh", "sum"), down_cost=("dn_c", "sum"),
        fees=("fee_usdc", "sum"), winner=("winner", "first"),
        payout_up=("payout_up", "first"), payout_down=("payout_down", "first"),
    ).reset_index()
    act = pd.read_sql(
        """
        SELECT condition_id,
               SUM(CASE WHEN type = 'MERGE' THEN size ELSE 0 END) AS merged_shares,
               SUM(CASE WHEN type = 'MERGE' THEN usdc_size ELSE 0 END) AS merge_usdc,
               SUM(CASE WHEN type = 'REDEEM' THEN usdc_size ELSE 0 END) AS redeem_usdc
        FROM activity WHERE condition_id IS NOT NULL GROUP BY condition_id
        """,
        conn,
    )
    ep = ep.merge(act, on="condition_id", how="left")
    for c in ("merged_shares", "merge_usdc", "redeem_usdc"):
        ep[c] = ep[c].fillna(0.0)
    ep["avg_up_px"] = np.where(ep["up_shares"] > 0, ep["up_cost"] / ep["up_shares"], np.nan)
    ep["avg_down_px"] = np.where(ep["down_shares"] > 0, ep["down_cost"] / ep["down_shares"], np.nan)
    ep["paired_shares"] = np.minimum(ep["up_shares"], ep["down_shares"]).clip(lower=0)
    ep["pair_cost"] = ep["avg_up_px"] + ep["avg_down_px"]
    ep["net_exposure"] = ep["up_shares"] - ep["down_shares"]
    # costs are Data API usdc, which already include taker fees (see fill_pnl); `fees` is kept for information only
    ep["pnl"] = (ep["up_shares"] * ep["payout_up"] + ep["down_shares"] * ep["payout_down"]
                 - ep["up_cost"] - ep["down_cost"])
    ep["computed_at"] = int(time.time())
    cols = [c for c in (
        "condition_id asset timeframe regime window_start_ts window_end_ts n_fills n_taker first_ts last_ts "
        "up_shares up_cost down_shares down_cost fees avg_up_px avg_down_px paired_shares pair_cost "
        "net_exposure merged_shares merge_usdc redeem_usdc winner payout_up payout_down pnl computed_at"
    ).split()]
    return ep[cols]


def enrich(cfg: Config, conn: sqlite3.Connection) -> dict[str, int]:
    """Recompute window_refs, market_context and episodes from local data (idempotent full rebuild)."""
    cache = SpotCache(cfg.spot_cache_dir)
    t0 = time.monotonic()
    refs = build_window_refs(conn, cache)
    n_refs = db.replace_frame(conn, "window_refs", refs)
    log.info("window_refs: %d rows [%.0fs]", n_refs, time.monotonic() - t0)
    ctx = build_context(conn, cache, refs)
    n_ctx = db.replace_frame(conn, "market_context", ctx)
    log.info("market_context: %d rows [%.0fs]", n_ctx, time.monotonic() - t0)
    ep = build_episodes(conn)
    n_ep = db.replace_frame(conn, "episodes", ep)
    log.info("episodes: %d rows [%.0fs]", n_ep, time.monotonic() - t0)
    return {"window_refs": n_refs, "market_context": n_ctx, "episodes": n_ep}
