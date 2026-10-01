"""`python -m bosona live-report`: what the live run measured (stage 3 questions).

  * detection latency per channel and for the first sighting (vs block and vs CLOB match time);
  * spot shift and his token's top-of-book shift between the trade and our detection;
  * cost of copying at detection (best price / VWAP of his size vs his price);
  * Binance vs Chainlink at window closes and whether the two would pick different winners;
  * PnL of his live fills vs copying each of them at detection (taker at the detection book, taker fee);
  * coverage of the feeds.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

import numpy as np
import pandas as pd

from bosona.config import Config
from bosona.http import ApiClient
from bosona.live.outcomes import resolve_pending
from bosona.live.store import LiveStore

log = logging.getLogger(__name__)


TEXT_COLUMNS = {"fill_key", "tx_hash", "condition_id", "slug", "asset", "timeframe", "token_id", "outcome", "side", "role",
                "order_hash", "first_channel", "channels", "ref_kind", "spot_source", "channel", "source", "kind", "winner",
                "winner_cl", "winner_bn", "official_winner"}


def _read(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    """read_sql with numeric columns forced to float (an all-NULL column would otherwise be 'object')."""
    df = pd.read_sql(sql, conn, params=params)
    for col in df.columns:
        if col not in TEXT_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _q(s: pd.Series, qs: tuple[float, ...] = (0.1, 0.5, 0.9)) -> dict[str, float] | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return {f"p{int(q * 100)}": round(float(s.quantile(q)), 3) for q in qs} | {"n": int(len(s))}


def latency(conn: sqlite3.Connection, since_ms: float) -> dict[str, Any]:
    fills = _read(conn, "SELECT * FROM live_fills WHERE first_seen_ms >= ?", (since_ms,))
    det = _read(
        conn,
        "SELECT d.fill_key, d.channel, d.recv_ms, f.block_ts, f.match_ms, f.backfill FROM live_detections d "
        "JOIN live_fills f USING (fill_key) WHERE f.first_seen_ms >= ?", (since_ms,))
    live = fills[fills["backfill"] == 0]
    out: dict[str, Any] = {
        "fills": int(len(fills)),
        "backfilled": int((fills["backfill"] == 1).sum()),
        "first_channel": live["first_channel"].value_counts().to_dict(),
        "roles": live["role"].value_counts(dropna=False).to_dict(),
        "with_match_time": int(live["match_ms"].notna().sum()),
        "first_seen_minus_block_ms": _q(live["lat_block_ms"]),
        "first_seen_minus_match_ms": _q(live["lat_match_ms"]),
        "block_minus_match_ms": _q(live["block_ts"] * 1000 - live["match_ms"]),
    }
    det = det[det["backfill"] == 0]
    per = {}
    for ch, g in det.groupby("channel"):
        per[ch] = {
            "seen": int(len(g)),
            "share_of_fills": round(len(g) / max(1, len(live)), 3),
            "minus_block_ms": _q(g["recv_ms"] - g["block_ts"] * 1000),
            "minus_match_ms": _q(g["recv_ms"] - g["match_ms"]),
        }
    out["per_channel"] = per
    if not det.empty:
        first = det.loc[det.groupby("fill_key")["recv_ms"].idxmin()]
        out["won_race"] = first["channel"].value_counts().to_dict()
    return out


def price_shift(conn: sqlite3.Connection, since_ms: float) -> dict[str, Any]:
    f = _read(conn, "SELECT * FROM live_fills WHERE first_seen_ms >= ? AND backfill = 0", (since_ms,))
    snaps = _read(
        conn,
        "SELECT s.* FROM live_spot_snaps s JOIN live_fills f USING (fill_key) WHERE f.first_seen_ms >= ? AND f.backfill = 0", (since_ms,))
    out: dict[str, Any] = {"headline_spot_source": f["spot_source"].value_counts(dropna=False).to_dict(),
                           "spot_shift_bps": _q(f["spot_shift_bps"]),
                           "abs_spot_shift_bps": _q(f["spot_shift_bps"].abs())}
    by_src = {}
    for (src, kind), g in snaps.groupby(["source", "kind"]):
        by_src[f"{src}:{kind}"] = {"shift_bps": _q(g["shift_bps"]), "abs_shift_bps": _q(g["shift_bps"].abs()),
                                   "detect_tick_age_ms": _q(g["det_age_ms"])}
    out["by_source"] = by_src
    mid_ref = (f["bid_ref"] + f["ask_ref"]) / 2
    mid_det = (f["bid_detect"] + f["ask_detect"]) / 2
    buy = f["side"] == "BUY"
    out["token"] = {
        "mid_shift_c": _q((mid_det - mid_ref) * 100),
        "ask_shift_c_buys": _q((f.loc[buy, "ask_detect"] - f.loc[buy, "ask_ref"]) * 100),
        "copy_best_minus_his_price_c_buys": _q((f.loc[buy, "copy_px"] - f.loc[buy, "price"]) * 100),
        "copy_vwap_slip_c": _q(f["copy_slip"] * 100),
        "share_copy_possible_at_his_price_or_better": None if f["copy_slip"].dropna().empty
        else round(float((f["copy_slip"].dropna() <= 1e-9).mean()), 3),
        "share_with_full_depth": round(float(f["copy_vwap"].notna().mean()), 3) if len(f) else None,
    }
    by_role = {}
    for role, g in f.groupby("role"):
        by_role[role] = {"fills": int(len(g)), "copy_vwap_slip_c": _q(g["copy_slip"] * 100),
                         "spot_shift_abs_bps": _q(g["spot_shift_bps"].abs())}
    out["by_role"] = by_role
    return out


def closes(conn: sqlite3.Connection, since_ts: float) -> dict[str, Any]:
    c = _read(conn, "SELECT * FROM live_window_close WHERE window_end_ts >= ?", (since_ts,))
    if c.empty:
        return {"closes": 0}
    c["div_twap_demeaned"] = c["div_twap_bps"] - c["basis_bps"]
    # the stage 2 proxy replayed on live data: Binance TWAP anchored on the Chainlink value at the window start
    c["proxy_err_bps"] = (c["bn_end"] * c["cl_start"] / c["bn_start"] / c["cl_end"] - 1) * 1e4
    both = c.dropna(subset=["winner_cl", "winner_bn"])
    official = c.dropna(subset=["official_final", "cl_end"])
    out: dict[str, Any] = {"closes": int(len(c)), "with_both_sources": int(len(both))}
    per = {}
    for (asset, tf), g in c.groupby(["asset", "timeframe"]):
        b = g.dropna(subset=["winner_cl", "winner_bn"])
        per[f"{asset} {tf}"] = {
            "closes": int(len(g)),
            "div_spot_bps": _q(g["div_spot_bps"]),
            "div_twap_bps": _q(g["div_twap_bps"]),
            "div_twap_minus_basis_abs_bps": _q(g["div_twap_demeaned"].abs()),
            "abs_move_bps": _q(g["move_cl_bps"].abs()),
            "anchored_binance_proxy_abs_err_bps": _q(g["proxy_err_bps"].abs()),
            "winner_disagree": int((b["winner_cl"] != b["winner_bn"]).sum()),
        }
    out["per_series"] = per
    out["winner_disagree_total"] = int((both["winner_cl"] != both["winner_bn"]).sum())
    if not official.empty:
        out["rtds_twap_vs_official_final_bps"] = _q((official["cl_end"] / official["official_final"] - 1) * 1e4)
        o = official.dropna(subset=["official_winner", "winner_cl"])
        out["rtds_winner_matches_official"] = None if o.empty else round(float((o["winner_cl"] == o["official_winner"]).mean()), 4)
        ob = official.dropna(subset=["official_winner", "winner_bn"])
        out["binance_winner_matches_official"] = None if ob.empty else round(float((ob["winner_bn"] == ob["official_winner"]).mean()), 4)
    return out


def copy_pnl(conn: sqlite3.Connection, since_ms: float, default_fee_rate: float = 0.07) -> dict[str, Any]:
    """Resolved live fills: his PnL vs a copier who buys the same size at detection from the book then
    (VWAP over the asks, taker fee size * rate * p * (1 - p)). Fills without enough depth are left out."""
    f = _read(
        conn,
        "SELECT f.*, r.winner, m.fee_rate FROM live_fills f JOIN resolutions r USING (condition_id) "
        "LEFT JOIN markets m USING (condition_id) WHERE f.first_seen_ms >= ? AND f.backfill = 0 AND f.side = 'BUY'", (since_ms,))
    if f.empty:
        return {"resolved_fills": 0}
    rate = f["fee_rate"].fillna(default_fee_rate)
    payout = np.where(f["winner"] == f["outcome"], 1.0, np.where(f["winner"] == "50-50", 0.5, 0.0))
    his_cost = f["usdc"] + f["fee_usdc"].fillna(0)
    f["his_pnl"] = f["size"] * payout - his_cost
    copy_fee = f["size"] * rate * f["copy_vwap"] * (1 - f["copy_vwap"])
    f["copy_cost"] = f["size"] * f["copy_vwap"] + copy_fee
    f["copy_pnl"] = f["size"] * payout - f["copy_cost"]
    c = f.dropna(subset=["copy_vwap"])

    def agg(g: pd.DataFrame) -> dict[str, Any]:
        gc = g.dropna(subset=["copy_vwap"])
        his_c = (gc["usdc"] + gc["fee_usdc"].fillna(0)).sum()
        return {"fills": int(len(g)), "copyable": int(len(gc)),
                "his_pnl": round(float(gc["his_pnl"].sum()), 2), "his_pnl_per_usdc": round(float(gc["his_pnl"].sum() / his_c), 4) if his_c else None,
                "copy_pnl": round(float(gc["copy_pnl"].sum()), 2),
                "copy_pnl_per_usdc": round(float(gc["copy_pnl"].sum() / gc["copy_cost"].sum()), 4) if len(gc) else None,
                "avg_extra_cost_c_per_share": round(float(((gc["copy_cost"] - gc["usdc"] - gc["fee_usdc"].fillna(0)) / gc["size"]).mean() * 100), 2) if len(gc) else None}

    out = {"resolved_fills": int(len(f)), "copyable_fills": int(len(c)), "all": agg(f)}
    out["by_role"] = {k: agg(g) for k, g in f.groupby("role")}
    out["by_timeframe"] = {k: agg(g) for k, g in f.groupby("timeframe")}
    return out


def coverage(conn: sqlite3.Connection, since_ts: float) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT source, kind, asset, COUNT(*), MIN(t), MAX(t) FROM live_spot WHERE t >= ? GROUP BY 1, 2, 3", (since_ts,)).fetchall()
    out = {}
    for src, kind, asset, n, t0, t1 in rows:
        span = max(1, t1 - t0 + 1)
        out[f"{src}:{kind}:{asset}"] = {"seconds": n, "share_of_span": round(n / span, 3),
                                        "from": time.strftime("%Y-%m-%d %H:%M", time.gmtime(t0)),
                                        "to": time.strftime("%Y-%m-%d %H:%M", time.gmtime(t1))}
    return out


async def _resolve(cfg: Config, conn: sqlite3.Connection) -> int:
    async with ApiClient(cfg) as client:
        return await resolve_pending(client, cfg.gamma_api, conn)


async def live_report(cfg: Config, since_h: float | None = None, resolve: bool = True) -> dict[str, Any]:
    store = LiveStore(cfg.live_db_path)  # creates tables a tracker of an older version did not have
    conn = store.conn
    if resolve:
        try:
            await _resolve(cfg, conn)
        except Exception as exc:  # noqa: BLE001 - the report still works offline
            log.warning("could not fetch resolutions: %s", exc)
    since_ts = time.time() - since_h * 3600 if since_h else 0
    out = {
        "db": str(cfg.live_db_path),
        "latency": latency(conn, since_ts * 1000),
        "price_shift": price_shift(conn, since_ts * 1000),
        "copy_pnl": copy_pnl(conn, since_ts * 1000, cfg.crypto_fee_rate),
        "window_closes": closes(conn, since_ts),
        "spot_coverage": coverage(conn, since_ts),
    }
    conn.close()
    return out
