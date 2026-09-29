"""Stage 2 quality report: coverage of the context, proxy accuracy, PnL reconciliation."""

from __future__ import annotations

import sqlite3
from typing import Any

import numpy as np
import pandas as pd


def _q(values: pd.Series, q: float) -> float | None:
    v = values.dropna()
    return None if v.empty else round(float(v.quantile(q)), 3)


def proxy_quality(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    w = pd.read_sql("SELECT * FROM window_refs", conn)
    out = []
    for (asset, regime), g in w.groupby(["asset", "regime"]):
        decided = g[g["winner"].isin(["Up", "Down"]) & g["proxy_winner"].notna()]
        move = (decided["final"] / decided["strike"] - 1).abs() * 1e4
        clear = decided[move > 2]
        out.append({
            "asset": asset,
            "regime": regime,
            "windows": int(len(g)),
            "strike_sources": {k: int(v) for k, v in g["strike_source"].value_counts(dropna=False).items()},
            "basis_bps_median": _q(g["basis_bps"], 0.5),
            "basis_bps_iqr": None if g["basis_bps"].dropna().empty
            else round(float(g["basis_bps"].quantile(0.75) - g["basis_bps"].quantile(0.25)), 3),
            "end_residual_abs_bps_median": _q(g["end_residual_bps"].abs(), 0.5),
            "end_residual_abs_bps_p95": _q(g["end_residual_bps"].abs(), 0.95),
            "winner_agreement": None if decided.empty else round(float((decided["proxy_winner"] == decided["winner"]).mean()), 4),
            "winner_agreement_move_gt_2bps": None if clear.empty
            else round(float((clear["proxy_winner"] == clear["winner"]).mean()), 4),
        })
    return out


def rolling_basis_error(conn: sqlite3.Connection, window_s: int = 3 * 3_600) -> list[dict[str, Any]]:
    """Leave-one-out check of the strike estimate used when a window has no official strike:
    |own basis - median basis of the other windows of the asset within +-window_s|, in bps."""
    w = pd.read_sql(
        "SELECT asset, regime, window_start_ts AS ws, basis_bps FROM window_refs "
        "WHERE regime LIKE 'chainlink%' AND basis_bps IS NOT NULL ORDER BY asset, ws",
        conn,
    )
    out = []
    for (asset, regime), g in w.groupby(["asset", "regime"]):
        all_asset = w[w["asset"] == asset]
        t, b = all_asset["ws"].to_numpy(), all_asset["basis_bps"].to_numpy()
        errs = []
        for ws, own in zip(g["ws"].to_numpy(), g["basis_bps"].to_numpy()):
            lo, hi = np.searchsorted(t, [ws - window_s, ws + window_s])
            others = np.concatenate([b[lo:hi][t[lo:hi] != ws]])
            if len(others):
                errs.append(abs(own - float(np.median(others))))
        e = pd.Series(errs, dtype=float)
        out.append({"asset": asset, "regime": regime, "n": int(len(e)),
                    "abs_err_bps_median": _q(e, 0.5), "abs_err_bps_p95": _q(e, 0.95)})
    return out


def context_coverage(conn: sqlite3.Connection) -> dict[str, Any]:
    c = pd.read_sql(
        "SELECT regime, spot, dist_bps, dist_twap_bps, vol_15m_bps, up_px, down_px, px_age_s, pnl_if_held FROM market_context",
        conn,
    )
    n = len(c)
    twap = c["regime"].str.startswith("chainlink_twap")
    return {
        "trades": n,
        "spot": round(float(c["spot"].notna().mean()), 4),
        "dist_bps": round(float(c["dist_bps"].notna().mean()), 4),
        "dist_twap_bps_on_twap_markets": round(float(c.loc[twap, "dist_twap_bps"].notna().mean()), 4) if twap.any() else None,
        "vol_15m": round(float(c["vol_15m_bps"].notna().mean()), 4),
        "token_prices": round(float((c["up_px"].notna() & c["down_px"].notna()).mean()), 4),
        "token_price_age_s_median": _q(c["px_age_s"], 0.5),
        "token_price_age_s_p95": _q(c["px_age_s"], 0.95),
        "pnl_if_held": round(float(c["pnl_if_held"].notna().mean()), 4),
    }


def pnl_reconciliation(conn: sqlite3.Connection) -> dict[str, Any]:
    ctx_pnl = conn.execute("SELECT SUM(pnl_if_held) FROM market_context").fetchone()[0] or 0.0
    fees = conn.execute("SELECT SUM(fee_usdc) FROM trades").fetchone()[0] or 0.0
    ep = pd.read_sql("SELECT * FROM episodes", conn)
    last = conn.execute(
        "SELECT ts, trade_pnl, realized_market_pnl, unrealized_pnl, fees_paid FROM pnl_daily ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    resolved = ep["winner"].notna()
    merged_excess = (ep["merged_shares"] - ep["paired_shares"]).clip(lower=0)
    held_after_merge = (ep["up_shares"] - ep["merged_shares"]) * ep["payout_up"] + \
                       (ep["down_shares"] - ep["merged_shares"]) * ep["payout_down"]
    redeem_gap = (held_after_merge - ep["redeem_usdc"])[resolved]
    return {
        "sum_pnl_if_held_after_fees": round(ctx_pnl, 2),
        "sum_pnl_before_fees": round(ctx_pnl + fees, 2),
        "episodes_pnl": round(float(ep.loc[resolved, "pnl"].sum()), 2),
        "api_latest_point": None if last is None else {
            "ts": last[0], "trade_pnl": last[1], "realized_market_pnl": last[2],
            "unrealized_pnl": last[3], "fees_paid": last[4]},
        "episodes_resolved": int(resolved.sum()),
        "episodes_merged_more_than_paired": int((merged_excess > 1e-6).sum()),
        "redeem_vs_expected_abs_gap_usdc": round(float(redeem_gap.abs().sum()), 2),
        "episodes_with_unredeemed_payout_gt_1usd": int((redeem_gap > 1).sum()),
    }


def validate(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "coverage": context_coverage(conn),
        "proxy_quality": proxy_quality(conn),
        "rolling_basis_error": rolling_basis_error(conn),
        "pnl": pnl_reconciliation(conn),
    }
