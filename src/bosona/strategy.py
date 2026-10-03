"""Stage 4: what @bosona does, tested on the history of stages 1-2.

Every function takes the fills frame (`load_fills`) and/or the database and returns plain tables
(DataFrames / dicts) that `render` turns into docs/stage4-data.md. Numbers are after fees.

Conventions:
  * EV per $1 = sum(pnl_if_held) / sum(usdc): what a dollar put into such fills returned at resolution
    (usdc includes the taker fee, as the Data API reports it);
  * ev_se: standard error of that ratio with fills clustered by market (all fills of a market share one outcome);
  * win = the bought outcome won (payout 1); 50-50 resolutions count as half a win;
  * `ahead` = the bought outcome is currently winning on the resolution-source spot (TWAP for TWAP markets);
  * time left is measured as the share of the window still to run (0.05 = last 5%);
  * max drawdown is taken on the cumulative PnL in fill order (PnL booked at entry; windows are short).
"""

from __future__ import annotations

import logging
import math
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

TF_SECONDS = {"5m": 300, "15m": 900, "1h": 3_600, "4h": 14_400, "1d": 86_400}
PRICE_EDGES = [0.0, 0.10, 0.30, 0.50, 0.70, 0.90, 0.97, 1.0001]
PRICE_LABELS = ["<0.10", "0.10-0.30", "0.30-0.50", "0.50-0.70", "0.70-0.90", "0.90-0.97", ">=0.97"]
LEFT_EDGES = [-1.0, 0.05, 0.20, 0.50, 1.0001]
LEFT_LABELS = ["last 5%", "5-20% left", "20-50% left", ">50% left"]
BLOCK_AFTER_MATCH_S = 2.4  # stage 3: block timestamp - CLOB match time, median


# ------------------------------------------------------------------------------------------- loading
def load_fills(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT t.trade_uid, t.tx_hash, t.ts, t.condition_id, t.outcome, t.side, t.price, t.size, t.usdc, t.role,
                  t.fee_usdc, x.asset, x.timeframe, x.regime, x.secs_from_open, x.secs_to_close, x.dist_bps,
                  x.dist_twap_bps, x.ret_10s_bps, x.ret_60s_bps, x.vol_1m_bps, x.vol_5m_bps, x.vol_15m_bps, x.up_px,
                  x.down_px, x.winner, x.payout, x.pnl_if_held
           FROM trades t JOIN market_context x USING (trade_uid)
           WHERE t.side = 'BUY'""",
        conn,
    )
    df["fee_usdc"] = df["fee_usdc"].fillna(0.0)
    df["dur"] = df["timeframe"].map(TF_SECONDS)
    df["left"] = (df["secs_to_close"] / df["dur"]).clip(lower=-1, upper=1)
    df["resolved"] = df["payout"].notna()
    df["win"] = df["payout"]  # 1 / 0 / 0.5
    sign = np.where(df["outcome"] == "Up", 1.0, -1.0)
    twap = df["regime"].str.startswith("chainlink_twap") & df["dist_twap_bps"].notna()
    df["dist_src"] = np.where(twap, df["dist_twap_bps"], df["dist_bps"])
    df["signed_dist"] = sign * df["dist_src"]                      # > 0: the bought side is ahead
    df["ahead"] = np.sign(df["signed_dist"])
    df["signed_ret10"] = sign * df["ret_10s_bps"]                  # > 0: spot moved towards the bought side
    df["signed_ret60"] = sign * df["ret_60s_bps"]
    # driftless random-walk probability that the bought side finishes ahead (model fair value)
    sigma = df["vol_5m_bps"] / math.sqrt(300.0)                     # bps per sqrt(second), realized over 5 min
    horizon = df["secs_to_close"].clip(lower=1)
    df["z"] = df["signed_dist"] / (sigma * np.sqrt(horizon))
    df["p_model"] = _norm_cdf(df["z"].to_numpy())
    df["price_bucket"] = pd.cut(df["price"], PRICE_EDGES, labels=PRICE_LABELS, right=False)
    df["left_bucket"] = pd.cut(df["left"], LEFT_EDGES, labels=LEFT_LABELS)
    return df


def _norm_cdf(x: np.ndarray) -> np.ndarray:
    from math import erf

    v = np.vectorize(lambda t: 0.5 * (1 + erf(t / math.sqrt(2))) if np.isfinite(t) else np.nan)
    return v(x)


# ----------------------------------------------------------------------------------------- metrics
def max_drawdown(pnl: pd.Series) -> float:
    """Largest peak-to-trough fall of the cumulative PnL (input ordered by time)."""
    if pnl.empty:
        return 0.0
    cum = pnl.cumsum().to_numpy()
    peak = np.maximum.accumulate(np.concatenate([[0.0], cum]))[1:]
    return float((peak - cum).max())


def ev_se(r: pd.DataFrame) -> float | None:
    """Standard error of EV = sum(pnl) / sum(usdc), fills clustered by market (linearized ratio estimator)."""
    g = r.groupby("condition_id")[["pnl_if_held", "usdc"]].sum()
    n = len(g)
    total = float(g["usdc"].sum())
    if n < 2 or total <= 0:
        return None
    resid = g["pnl_if_held"] - g["pnl_if_held"].sum() / total * g["usdc"]
    return float(math.sqrt(n / (n - 1) * float((resid**2).sum())) / total)


def summarize(g: pd.DataFrame) -> dict[str, Any]:
    """The per-segment metrics of the stage 4 spec (resolved fills only)."""
    r = g[g["resolved"]]
    usdc = float(r["usdc"].sum())
    pnl = float(r["pnl_if_held"].sum())
    shares = float(r["size"].sum())
    se = ev_se(r)
    return {
        "fills": len(r),
        "markets": int(r["condition_id"].nunique()),
        "usdc": round(usdc, 2),
        "winrate": round(float(r["win"].mean()), 4) if len(r) else None,
        # per share: directly comparable with avg_entry (EV per share = winrate_shares - avg_entry - fee per share)
        "winrate_shares": round(float((r["win"] * r["size"]).sum() / shares), 4) if shares else None,
        "avg_entry": round(float((r["price"] * r["size"]).sum() / shares), 4) if shares else None,
        "ev_per_usd": round(pnl / usdc, 4) if usdc else None,
        "ev_se": round(se, 4) if se is not None else None,
        "pnl": round(pnl, 2),
        "max_drawdown": round(max_drawdown(r.sort_values("ts")["pnl_if_held"]), 2),
        "maker_share": round(float((r["role"] == "maker").mean()), 3) if len(r) else None,
    }


def group_summary(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    rows = []
    for key, g in df.groupby(by, observed=True):
        key = key if isinstance(key, tuple) else (key,)
        rows.append({**dict(zip(by, key, strict=True)), **summarize(g)})
    return pd.DataFrame(rows)


def segments(df: pd.DataFrame) -> pd.DataFrame:
    """asset x timeframe x price bucket x time-left bucket (the stage 4 segment table)."""
    out = group_summary(df, ["asset", "timeframe", "price_bucket", "left_bucket"])
    return out.sort_values(["asset", "timeframe", "price_bucket", "left_bucket"]).reset_index(drop=True)


def _q(s: pd.Series, qs: tuple[int, ...] = (10, 25, 50, 75, 90), nd: int = 3) -> dict[str, float | None]:
    s = s.dropna()
    return {f"p{q}": round(float(s.quantile(q / 100)), nd) if len(s) else None for q in qs}


# ------------------------------------------------------------------------------ H1: pair trading
def side_costs(df: pd.DataFrame) -> pd.DataFrame:
    """Per market and side: shares, cost with fees (usdc) and fees."""
    up = df["outcome"] == "Up"
    t = pd.DataFrame({
        "condition_id": df["condition_id"],
        "up_sh": np.where(up, df["size"], 0.0), "up_usdc": np.where(up, df["usdc"], 0.0),
        "up_fee": np.where(up, df["fee_usdc"], 0.0),
        "dn_sh": np.where(~up, df["size"], 0.0), "dn_usdc": np.where(~up, df["usdc"], 0.0),
        "dn_fee": np.where(~up, df["fee_usdc"], 0.0),
    })
    return t.groupby("condition_id").sum()


def pnl_split(s: pd.DataFrame) -> pd.DataFrame:
    """Exact split of a market's PnL: pairs + unpaired remainder - fees.

    `s` has per-side shares, usdc (with fees), fees and payouts. With gross (fee-free) average prices a_up, a_dn:
      pairs       = paired x (1 - a_up - a_dn)              what both sides together lock in
      directional = rest_up x (payout_up - a_up) + rest_dn x (payout_dn - a_dn)
      total       = pairs + directional - fees = sum(shares x payout) - sum(usdc)"""
    a_up = ((s["up_usdc"] - s["up_fee"]) / s["up_sh"]).where(s["up_sh"] > 0, 0.0)
    a_dn = ((s["dn_usdc"] - s["dn_fee"]) / s["dn_sh"]).where(s["dn_sh"] > 0, 0.0)
    paired = np.minimum(s["up_sh"], s["dn_sh"])
    out = pd.DataFrame(index=s.index)
    out["pairs"] = paired * (1 - a_up - a_dn)
    out["directional"] = (s["up_sh"] - paired) * (s["payout_up"] - a_up) + (s["dn_sh"] - paired) * (s["payout_down"] - a_dn)
    out["fees"] = -(s["up_fee"] + s["dn_fee"])
    out["total"] = out["pairs"] + out["directional"] + out["fees"]
    return out


def pair_trading(conn: sqlite3.Connection, df: pd.DataFrame) -> dict[str, Any]:
    """Does he buy Up and Down for less than $1 a pair, and merge or hold the pairs?"""
    ep = pd.read_sql("SELECT * FROM episodes WHERE winner IS NOT NULL", conn).set_index("condition_id")
    s = side_costs(df).join(ep[["timeframe", "payout_up", "payout_down", "merged_shares"]], how="inner")
    split = pnl_split(s)
    both = ep[(ep["up_shares"] > 0) & (ep["down_shares"] > 0)]
    hedge = both["paired_shares"] / both[["up_shares", "down_shares"]].max(axis=1)
    shares = ep["up_shares"] + ep["down_shares"]
    out: dict[str, Any] = {
        "episodes": len(ep),
        "both_sides": len(both),
        "both_sides_share": round(len(both) / len(ep), 3),
        "paired_share_of_shares": round(float(2 * ep["paired_shares"].sum() / shares.sum()), 3),
        # pair cost with fees: what a pair really cost him
        "pair_cost_quantiles": _q(both["pair_cost"]),
        "pair_cost_below_1": round(float((both["pair_cost"] < 1).mean()), 3),
        "hedge_ratio_quantiles": _q(hedge),
        "fully_hedged_share": round(float((hedge >= 0.95).mean()), 3),
        "pnl_split": {k: round(float(split[k].sum()), 0) for k in ("pairs", "directional", "fees", "total")},
        "merged_episodes": int((ep["merged_shares"] > 0).sum()),
        "merged_share_of_paired": round(float(ep["merged_shares"].sum() / max(1.0, ep["paired_shares"].sum())), 3),
    }
    by_tf = split.join(s["timeframe"]).groupby("timeframe")[["pairs", "directional", "fees", "total"]].sum().round(0)
    out["pnl_split_by_timeframe"] = by_tf.reset_index()
    out["pair_cost_by_timeframe"] = (both.groupby("timeframe")["pair_cost"].describe(percentiles=[.1, .5, .9])
                                     [["count", "10%", "50%", "90%"]].round(3).reset_index())
    out["near_simultaneous_pairs"] = near_pairs(df)
    out["first_fill_gap"] = first_fill_gap(df)
    return out


def near_pairs(df: pd.DataFrame, within_s: int = 5) -> dict[str, Any]:
    """Opposite-side fills of one market within `within_s` seconds: the sum of their prices is the spread
    he captures (< 1) or gives away (> 1) on a two-sided quote."""
    d = df[["condition_id", "ts", "outcome", "price", "size", "role"]].sort_values(["condition_id", "ts"])
    nxt = d.groupby("condition_id").shift(-1)
    pair = (nxt["outcome"].notna()) & (nxt["outcome"] != d["outcome"]) & ((nxt["ts"] - d["ts"]) <= within_s)
    s = (d.loc[pair, "price"] + nxt.loc[pair, "price"])
    return {
        "pairs": int(pair.sum()),
        "share_of_fills": round(float(pair.mean()), 3),
        "sum_quantiles": _q(s),
        "sum_below_1": round(float((s < 1).mean()), 3),
        "both_maker": round(float(((d.loc[pair, "role"] == "maker") & (nxt.loc[pair, "role"] == "maker")).mean()), 3),
    }


def first_fill_gap(df: pd.DataFrame) -> dict[str, Any]:
    """In markets where he bought both sides: time between the first Up and the first Down fill, as a share of the
    window, and the sum of those two first prices."""
    f = df.sort_values("ts").groupby(["condition_id", "outcome"]).agg(ts=("ts", "first"), price=("price", "first"),
                                                                      dur=("dur", "first"))
    w = f.unstack("outcome").dropna()
    gap = (w[("ts", "Up")] - w[("ts", "Down")]).abs() / w[("dur", "Up")]
    return {"markets": len(w), "gap_share_of_window": _q(gap, (25, 50, 75)),
            "first_prices_sum": _q(w[("price", "Up")] + w[("price", "Down")], (25, 50, 75))}


# ------------------------------------------------------------------------ inventory: open / add / reduce
def net_before(df: pd.DataFrame) -> pd.Series:
    """His net exposure in the market (Up shares - Down shares) just before each fill, fills in time order.
    Merges remove pairs and leave the net exposure unchanged."""
    d = df.sort_values(["condition_id", "ts", "trade_uid"])
    signed = pd.Series(np.where(d["outcome"] == "Up", d["size"], -d["size"]), index=d.index)
    return (signed.groupby(d["condition_id"]).cumsum() - signed).reindex(df.index)


def inventory_actions(df: pd.DataFrame) -> pd.Series:
    """For each fill: does it open a position, add to his net exposure in the market, or reduce it (buying the
    other side)?"""
    before = net_before(df)
    signed = pd.Series(np.where(df["outcome"] == "Up", df["size"], -df["size"]), index=df.index)
    tol = 1e-6
    act = np.select(
        [before.abs() <= tol, np.sign(before) == np.sign(signed), signed.abs() > before.abs() + tol],
        ["open", "add", "reduce+flip"], default="reduce")
    return pd.Series(act, index=df.index)


def inventory(df: pd.DataFrame) -> dict[str, Any]:
    """Open / add / reduce by role; for taker fills also against the last 10 s of spot, and how much of the net
    exposure a reducing taker fill takes off."""
    d = df.assign(action=inventory_actions(df), net=net_before(df).abs())
    out: dict[str, Any] = {"by_action": group_summary(d, ["role", "action"])}
    t = d[(d["role"] == "taker") & (d["signed_ret10"].abs() >= 0.5)]
    t = t.assign(kind=np.where(t["action"].str.startswith("reduce"), "reduce", "open/add"),
                 spot_10s=np.where(t["signed_ret10"] > 0, "towards the bought side", "against the bought side"))
    out["taker_vs_spot"] = group_summary(t, ["kind", "spot_10s"])
    red = d[(d["role"] == "taker") & d["action"].str.startswith("reduce")]
    ratio = red["size"] / red["net"]
    out["taker_reduce_size_vs_net"] = {**_q(ratio, (25, 50, 75)),
                                       "flattens": round(float(((ratio > 0.95) & (ratio < 1.05)).mean()), 3)}
    return out


# ---------------------------------------------------------------------- H2: late high-probability entries
def late_entries(df: pd.DataFrame) -> dict[str, Any]:
    r = df[df["resolved"]]
    hi = r[r["price"] >= 0.90].copy()
    hi["px"] = pd.cut(hi["price"], [0.90, 0.95, 0.97, 0.99, 1.0001], labels=["0.90-0.95", "0.95-0.97", "0.97-0.99", ">=0.99"],
                      right=False)
    hi["secs_left"] = pd.cut(hi["secs_to_close"], [-1e9, 0, 30, 60, 120, 300, 1e9],
                             labels=["after close", "last 30 s", "30-60 s", "1-2 min", "2-5 min", ">5 min"])
    out = {
        "share_of_fills": round(len(hi) / len(r), 4),
        "share_of_usdc": round(float(hi["usdc"].sum() / r["usdc"].sum()), 4),
        "overall": summarize(hi),
        "by_price": group_summary(hi, ["px"]),
        "by_time_left": group_summary(hi, ["secs_left"]),
        "by_timeframe": group_summary(hi, ["timeframe"]),
        "by_role": group_summary(hi, ["role"]),
        "loss_rate": round(float((hi["win"] == 0).mean()), 4),
        # win rate per share that makes EV zero: cost per share with fees
        "breakeven_winrate": round(float(hi["usdc"].sum() / hi["size"].sum()), 4),
    }
    hi["zb"] = pd.cut(hi["z"], [-np.inf, 0, 1, 2, 3, np.inf], labels=["behind", "0-1σ", "1-2σ", "2-3σ", ">3σ"])
    out["by_z"] = group_summary(hi, ["zb"])
    return out


# ------------------------------------------------------------------------ H4: market making / pricing
def market_making(conn: sqlite3.Connection, df: pd.DataFrame) -> dict[str, Any]:
    r = df[df["resolved"]]
    out: dict[str, Any] = {"by_role": group_summary(r, ["role"])}
    out["fill_size_quantiles"] = pd.DataFrame(
        [{"role": role, **_q(g["usdc"], (10, 50, 90, 99), 2)} for role, g in r.groupby("role")])
    per_ep = df.groupby("condition_id").agg(fills=("ts", "size"))
    out["fills_per_market_quantiles"] = {f"p{q}": int(per_ep["fills"].quantile(q / 100)) for q in (10, 50, 90, 99)}
    d = df.sort_values(["condition_id", "ts"])
    prev = d.groupby("condition_id")["outcome"].shift()
    out["side_switch_share"] = round(float((prev.notna() & (prev != d["outcome"])).sum() / prev.notna().sum()), 3)
    # his price against the last CLOB price-history point (~60 s resolution, a coarse mid)
    mid = np.where(d["outcome"] == "Up", d["up_px"], d["down_px"])
    diff = pd.Series((d["price"].to_numpy() - mid) * 100, index=d.index)
    out["price_minus_token_px_c"] = pd.DataFrame([{"role": role, **_q(g, nd=2)} for role, g in diff.groupby(d["role"])])
    out["model_calibration"] = calibration(r)
    out["edge_vs_model"] = edge_vs_model(r)
    reb = dict(conn.execute("SELECT type, SUM(usdc_size) FROM activity WHERE type IN ('MAKER_REBATE','TAKER_REBATE','REWARD') "
                            "GROUP BY type").fetchall())
    out["rebates"] = {k: round(v, 0) for k, v in reb.items()}
    return out


def calibration(r: pd.DataFrame) -> pd.DataFrame:
    """Is the model probability right? Realized win rate by model-probability bucket."""
    b = pd.cut(r["p_model"], [0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0001], right=False).rename("model_bucket")
    g = r.groupby(b, observed=True).agg(fills=("win", "size"), p_model_mean=("p_model", "mean"), winrate=("win", "mean"),
                                        price=("price", "mean"))
    g = g.round(3).reset_index()
    g["model_bucket"] = g["model_bucket"].astype(str)
    return g


def edge_vs_model(r: pd.DataFrame) -> pd.DataFrame:
    """Where does he buy relative to the model fair value, and does that pay?"""
    e = (r["p_model"] - r["price"]) * 100
    b = pd.cut(e, [-100, -10, -5, -2, 2, 5, 10, 100], labels=["<-10c", "-10..-5c", "-5..-2c", "±2c", "2..5c", "5..10c", ">10c"])
    rows = []
    for (role, eb), g in r.groupby([r["role"], b], observed=True):
        rows.append({"role": role, "model_minus_price": eb, **summarize(g)})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------ H3: leading the spot
def spot_lead(cache: Any, df: pd.DataFrame, rng_seed: int = 7) -> dict[str, Any]:
    """Does he trade right after sharp spot moves, in their direction, before the book reprices?

    1. Alignment: share of fills whose side the last 10 s / 60 s of spot favoured (50% if unrelated).
    2. Matched baseline: |10 s move| at his fills vs at a random second of the same window.
    3. Event study (per asset, pooled): his fills per second around sharp 3 s moves (top 0.1% |move|), aligned vs
       opposite side. Fill times are block timestamps, ~2.4 s after the actual match (stage 3).
    """
    from bosona.spot import SYMBOLS

    r = df[df["resolved"] & df["signed_ret10"].notna()]
    out: dict[str, Any] = {}
    rows = []
    for role, g in r.groupby("role"):
        for name, col in (("10s", "signed_ret10"), ("60s", "signed_ret60")):
            moved = g[g[col].abs() >= 0.5]                          # ignore flat seconds
            al, op = summarize(moved[moved[col] > 0]), summarize(moved[moved[col] < 0])
            rows.append({"role": role, "window": name, "fills_with_move": len(moved),
                         "share_aligned": round(float((moved[col] > 0).mean()), 4),
                         "ev_aligned": al["ev_per_usd"], "ev_aligned_se": al["ev_se"],
                         "ev_opposite": op["ev_per_usd"], "ev_opposite_se": op["ev_se"]})
    out["alignment"] = pd.DataFrame(rows)
    rng = np.random.default_rng(rng_seed)
    base_rows, events_rows, tags = [], [], []
    for asset, g in df.groupby("asset"):
        symbol = SYMBOLS.get(asset)
        if symbol is None:
            continue
        t0, t1 = int(g["ts"].min()) - 3_600, int(g["ts"].max()) + 120
        px = pd.Series(cache.closes(symbol, t0, t1)).ffill().to_numpy()
        logp = np.log(px)
        # matched baseline: a random second of the same window for every fill
        ws = (g["ts"] - g["secs_from_open"]).to_numpy(dtype=np.int64)
        we = ws + g["dur"].to_numpy(dtype=np.int64)
        rand_t = ws + (rng.random(len(g)) * (we - ws)).astype(np.int64)
        idx_f, idx_r = g["ts"].to_numpy(dtype=np.int64) - 1 - t0, rand_t - 1 - t0
        ok = (idx_r - 10 >= 0) & (idx_r < len(px)) & (idx_f - 10 >= 0) & (idx_f < len(px))
        mf = np.abs(logp[idx_f[ok]] - logp[idx_f[ok] - 10]) * 1e4
        mr = np.abs(logp[idx_r[ok]] - logp[idx_r[ok] - 10]) * 1e4
        base_rows.append({"asset": asset, "fills": int(ok.sum()),
                          **{f"fills_p{q}": round(float(np.nanpercentile(mf, q)), 2) for q in (50, 90, 99)},
                          **{f"random_p{q}": round(float(np.nanpercentile(mr, q)), 2) for q in (50, 90, 99)}})
        events_rows.extend(_event_study(asset, g, logp, t0))
        tags.append(after_move(g, *sharp_moves(logp, t0)[:2]))
    out["move_vs_random_second"] = pd.DataFrame(base_rows)
    out["event_study"] = pd.DataFrame(events_rows)
    out["lag"] = lead_lag(out["event_study"])
    # what the fills of the first 10 s after a sharp move earn: picked-off quotes vs reacting orders
    tagged = df.assign(after_move=pd.concat(tags).reindex(df.index).fillna("no sharp move in 10 s"))
    out["after_move"] = group_summary(tagged, ["role", "after_move"])
    return out


def sharp_moves(logp: np.ndarray, t0: int, quantile: float = 0.999, gap_s: int = 60) -> tuple[np.ndarray, np.ndarray, float]:
    """Seconds at which a sharp 3 s move (top `quantile` of |move|) completed, its sign, and the threshold in bps.
    `logp[i]` is the log price of second t0 + i. One event per burst: the next one counts after `gap_s`."""
    r3 = np.full(len(logp), np.nan)
    r3[3:] = (logp[3:] - logp[:-3]) * 1e4
    thr = float(np.nanquantile(np.abs(r3), quantile))
    cand = np.flatnonzero(np.abs(np.nan_to_num(r3)) >= thr)
    events, last = [], -10**9
    for i in cand:
        if i - last >= gap_s:
            events.append(i)
            last = i
    idx = np.array(events, dtype=np.int64)
    return idx + t0, np.sign(r3[idx]) if len(idx) else np.empty(0), thr


def after_move(g: pd.DataFrame, ev_t: np.ndarray, ev_dir: np.ndarray, within_s: int = 10) -> pd.Series:
    """Tag fills made 0..`within_s` s (block time) after a sharp move: on the side the move favoured or against it
    ("aligned" / "opposite"; missing for other fills)."""
    tag = pd.Series(None, index=g.index, dtype=object)
    if not len(ev_t):
        return tag
    t = g["ts"].to_numpy(dtype=np.int64)
    k = np.searchsorted(ev_t, t, side="right") - 1               # the latest move at or before the fill
    ok = k >= 0
    off = np.where(ok, t - ev_t[np.clip(k, 0, None)], -1)
    near = ok & (off >= 0) & (off <= within_s)
    sign = np.where(g["outcome"].to_numpy() == "Up", 1.0, -1.0)
    aligned = sign * ev_dir[np.clip(k, 0, None)] > 0
    tag[near] = np.where(aligned[near], "aligned", "opposite")
    return tag


def _event_study(asset: str, g: pd.DataFrame, logp: np.ndarray, t0: int, horizon: int = 30,
                 quantile: float = 0.999, gap_s: int = 60) -> list[dict[str, Any]]:
    """Fills at each offset (s) from sharp 3 s moves of one asset; rows carry fill counts and the event count."""
    ev_t, ev_dir, thr = sharp_moves(logp, t0, quantile, gap_s)
    if not len(ev_t):
        return []
    fills_t = g["ts"].to_numpy(dtype=np.int64)
    sign = np.where(g["outcome"].to_numpy() == "Up", 1.0, -1.0)
    role = g["role"].to_numpy()
    order = np.argsort(fills_t)
    fills_t, sign, role = fills_t[order], sign[order], role[order]
    counts: dict[tuple[str, str, int], int] = {}
    for t, d in zip(ev_t, ev_dir, strict=True):
        a, b = np.searchsorted(fills_t, [t - horizon, t + horizon + 1])
        for j in range(a, b):
            key = (role[j], "aligned" if sign[j] * d > 0 else "opposite", int(fills_t[j] - t))
            counts[key] = counts.get(key, 0) + 1
    return [{"asset": asset, "events": len(ev_t), "threshold_bps": round(thr, 2), "role": rl, "side": side,
             "offset_s": off, "fills": cnt} for (rl, side, off), cnt in counts.items()]


def _pooled_rates(ev: pd.DataFrame) -> pd.DataFrame:
    """Fills per event per second at each offset, pooled over assets (sum of fills / sum of events)."""
    n_events = ev.groupby("asset")["events"].first().sum()
    rates = ev.groupby(["role", "side", "offset_s"])["fills"].sum() / n_events
    full = pd.MultiIndex.from_product([sorted(ev["role"].unique()), ["aligned", "opposite"], range(-30, 31)],
                                      names=["role", "side", "offset_s"])
    return rates.reindex(full, fill_value=0.0).rename("rate").reset_index()


def event_profile(ev: pd.DataFrame) -> pd.DataFrame:
    """Fills per event per second in bins around the move, aligned vs opposite, by role."""
    if ev.empty:
        return ev
    p = _pooled_rates(ev)
    p["bin"] = pd.cut(p["offset_s"], [-31, -20, -10, -5, -1, 0, 4, 9, 19, 30],
                      labels=["-30..-21", "-20..-11", "-10..-6", "-5..-1", "0", "+1..+4", "+5..+9", "+10..+19", "+20..+30"])
    piv = p.pivot_table(index="bin", columns=["role", "side"], values="rate", aggfunc="mean", observed=True).round(4)
    piv.columns = [f"{a} {b}" for a, b in piv.columns]
    return piv.reset_index()


def lead_lag(ev: pd.DataFrame, pre: tuple[int, int] = (-30, -6), post: tuple[int, int] = (0, 15)) -> pd.DataFrame:
    """Reaction to a sharp move: excess fills over the pre-move rate in the `post` seconds, and the median offset of
    that excess (block time; minus ~2.4 s for the match time)."""
    if ev.empty:
        return ev
    p = _pooled_rates(ev)
    rows = []
    for (role, side), g in p.groupby(["role", "side"]):
        g = g.set_index("offset_s")["rate"]
        base = float(g.loc[pre[0]:pre[1]].mean())
        exc = (g.loc[post[0]:post[1]] - base).clip(lower=0)
        tot = float(exc.sum())
        med = float(exc.index[np.searchsorted(exc.cumsum().to_numpy(), tot / 2)]) if tot > 0 else None
        rows.append({"role": role, "side": side, "base_per_s": round(base, 4),
                     "peak_per_s": round(float(g.loc[post[0]:post[1]].max()), 4),
                     "excess_fills_per_event": round(tot, 3),
                     "excess_vs_base_x": round(tot / (base * (post[1] - post[0] + 1)), 2) if base else None,
                     "median_offset_block_s": med,
                     "median_offset_match_s": round(med - BLOCK_AFTER_MATCH_S, 1) if med is not None else None})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------- rules: where, when, how much
def participation(conn: sqlite3.Connection) -> pd.DataFrame:
    """Share of the windows of each series he traded, within the span he was active in that series."""
    ep = pd.read_sql("SELECT asset, timeframe, window_start_ts FROM episodes", conn)
    rows = []
    for (a, tf), g in ep.groupby(["asset", "timeframe"]):
        possible = (g["window_start_ts"].max() - g["window_start_ts"].min()) / TF_SECONDS[tf] + 1
        rows.append({"asset": a, "timeframe": tf, "markets": len(g), "windows_in_span": int(possible),
                     "share_traded": round(len(g) / possible, 3)})
    return pd.DataFrame(rows).sort_values("markets", ascending=False).reset_index(drop=True)


def timing(df: pd.DataFrame) -> pd.DataFrame:
    """When inside the window: first and last fill per market, and fills after the close."""
    g = df.groupby("condition_id").agg(tf=("timeframe", "first"), dur=("dur", "first"),
                                       first=("secs_from_open", "min"), last_left=("secs_to_close", "min"))
    rows = []
    for tf, x in g.groupby("tf"):
        f = df[df["timeframe"] == tf]
        rows.append({"timeframe": tf, "markets": len(x),
                     "first_fill_s_p25": round(float(x["first"].quantile(.25)), 0),
                     "first_fill_s_p50": round(float(x["first"].quantile(.5)), 0),
                     "first_fill_share_p50": round(float((x["first"] / x["dur"]).median()), 3),
                     "last_fill_left_s_p50": round(float(x["last_left"].median()), 0),
                     "fills_after_close": round(float((f["secs_to_close"] < 0).mean()), 4)})
    return pd.DataFrame(rows)


def sizing(df: pd.DataFrame, conn: sqlite3.Connection) -> dict[str, Any]:
    """Fill sizes by price and role, and money per market by timeframe."""
    rows = []
    for (role, b), g in df.groupby(["role", "price_bucket"], observed=True):
        rows.append({"role": role, "price_bucket": b, "fills": len(g), "usdc_p50": round(float(g["usdc"].median()), 2),
                     "shares_p50": round(float(g["size"].median()), 1), "usdc_p90": round(float(g["usdc"].quantile(.9)), 2)})
    ep = pd.read_sql("SELECT timeframe, up_cost + down_cost AS cost, ABS(net_exposure) AS net FROM episodes", conn)
    per_market = pd.DataFrame([{"timeframe": tf, "markets": len(g), **{f"usdc_p{q}": round(float(g["cost"].quantile(q / 100)), 0)
                                                                      for q in (25, 50, 75, 90)},
                                "net_shares_p50": round(float(g["net"].median()), 0)} for tf, g in ep.groupby("timeframe")])
    e = df[df["resolved"] & df["p_model"].notna()]
    corr = {role: g["usdc"].rank().corr((g["p_model"] - g["price"]).abs().rank()) for role, g in e.groupby("role")}
    return {"by_price": pd.DataFrame(rows), "per_market": per_market,
            "rank_corr_size_vs_model_edge": {k: round(float(v), 3) for k, v in corr.items()}}


def stability(df: pd.DataFrame) -> dict[str, Any]:
    """Daily PnL distribution (UTC days) and monthly drift of the main numbers."""
    r = df[df["resolved"]].assign(day=lambda x: pd.to_datetime(x["ts"], unit="s").dt.floor("D"))
    daily = r.groupby("day")["pnl_if_held"].sum()
    by_role = r.groupby(["role", "day"])["pnl_if_held"].sum()
    out: dict[str, Any] = {"daily": {
        "days": len(daily), "profitable_share": round(float((daily > 0).mean()), 3),
        "mean": round(float(daily.mean()), 0), "median": round(float(daily.median()), 0),
        "std": round(float(daily.std()), 0), "worst": round(float(daily.min()), 0), "best": round(float(daily.max()), 0),
        "mean_over_std": round(float(daily.mean() / daily.std()), 2),
        "t_stat": round(float(daily.mean() / daily.std() * math.sqrt(len(daily))), 1),
        "maker_days_profitable": round(float((by_role.loc["maker"] > 0).mean()), 3),
        "taker_days_profitable": round(float((by_role.loc["taker"] > 0).mean()), 3),
    }}
    r = r.assign(month=pd.to_datetime(r["ts"], unit="s").dt.strftime("%Y-%m"))
    rows = []
    for m, g in r.groupby("month"):
        rows.append({"month": m, "days": int(g["day"].nunique()), **summarize(g),
                     "taker_ev": summarize(g[g["role"] == "taker"])["ev_per_usd"],
                     "usdc_per_day": round(float(g["usdc"].sum() / g["day"].nunique()), 0)})
    out["monthly"] = pd.DataFrame(rows)
    return out


# ---------------------------------------------------------------------- his orders (calldata sample)
def order_sample(conn: sqlite3.Connection) -> dict[str, Any] | None:
    """Sizes and limit prices of his orders from `order_samples` (python -m bosona sample-orders)."""
    if not _has_table(conn, "order_samples"):
        return None
    o = pd.read_sql("SELECT * FROM order_samples WHERE side = 'BUY'", conn)
    if o.empty:
        return None
    t = pd.read_sql("""SELECT tx_hash, token_id, SUM(size) AS shares, SUM(size * price) AS gross FROM trades
                       WHERE tx_hash IN (SELECT tx_hash FROM order_samples) GROUP BY tx_hash, token_id""", conn)
    o = o.merge(t, on=["tx_hash", "token_id"], how="left")
    # maker orders execute at their limit; a taker order's shares come from his fills in that tx
    o["filled"] = np.where(o["role"] == "maker", o["fill_amount"] / o["limit_price"], o["shares"])
    o["fill_share"] = o["filled"] / o["order_shares"]
    o["exec_price"] = o["gross"] / o["shares"]
    rows = []
    for role, g in o.groupby("role"):
        rows.append({"role": role, "orders": len(g), **{f"shares_p{q}": round(float(g["order_shares"].quantile(q / 100)), 1)
                                                       for q in (10, 25, 50, 75, 90)},
                     "usdc_p50": round(float(g["order_usdc"].median()), 2), "usdc_p90": round(float(g["order_usdc"].quantile(.9)), 2),
                     "filled_in_tx_p50": round(float(g["fill_share"].median()), 3),
                     "fully_filled_in_tx": round(float((g["fill_share"] >= 0.999).mean()), 3),
                     "integer_size": round(float((np.abs(g["order_shares"] - g["order_shares"].round()) < 1e-6).mean()), 3),
                     "order_ts_zero": round(float((g["order_ts"] == 0).mean()), 3)})
    out: dict[str, Any] = {"by_role": pd.DataFrame(rows),
                           "tx": pd.read_sql("SELECT stratum, status, COUNT(*) AS tx FROM order_tx GROUP BY 1, 2", conn)}
    common = []
    for role, g in o.groupby("role"):
        vc = g["order_shares"].round(2).value_counts()
        for size, n in vc.head(10).items():
            common.append({"role": role, "order_shares": size, "orders": int(n), "share": round(n / len(g), 3)})
    out["common_sizes"] = pd.DataFrame(common)
    o["px_bucket"] = pd.cut(o["limit_price"], PRICE_EDGES, labels=PRICE_LABELS, right=False)
    out["size_by_price"] = (o.groupby(["role", "px_bucket"], observed=True)
                            .agg(orders=("order_shares", "size"), shares_p50=("order_shares", "median"),
                                 usdc_p50=("order_usdc", "median")).round(2).reset_index())
    lx = o[o["role"] == "maker"]
    out["log_size_vs_log_price_slope"] = (round(float(np.polyfit(np.log(lx["limit_price"]), np.log(lx["order_shares"]), 1)[0]), 2)
                                          if len(lx) > 10 else None)
    tk = o[(o["role"] == "taker") & o["exec_price"].notna()]
    out["taker_limit_minus_exec_c"] = _q((tk["limit_price"] - tk["exec_price"]) * 100, (25, 50, 75, 90, 99), 2)
    out["taker_limit_above_exec_1c"] = round(float(((tk["limit_price"] - tk["exec_price"]) >= 0.01).mean()), 3)
    return out


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


# ------------------------------------------------------------------- live books of stage 3 (top of book)
def live_quotes(live_db: Path | None) -> pd.DataFrame | None:
    """His fill price against the best bid / ask of his token just before the match (stage 3 market channel)."""
    if live_db is None or not Path(live_db).exists():
        return None
    with sqlite3.connect(live_db) as c:
        f = pd.read_sql("SELECT role, price, bid_ref, ask_ref FROM live_fills WHERE side = 'BUY' AND bid_ref IS NOT NULL "
                        "AND ask_ref IS NOT NULL", c)
    if f.empty:
        return None
    f["spread_c"] = (f["ask_ref"] - f["bid_ref"]) * 100
    f["vs_bid_c"] = (f["price"] - f["bid_ref"]) * 100
    f["vs_mid_c"] = (f["price"] - (f["bid_ref"] + f["ask_ref"]) / 2) * 100
    rows = []
    for role, g in f.groupby("role"):
        rows.append({"role": role, "fills": len(g), "spread_c_p50": round(float(g["spread_c"].median()), 2),
                     "at_best_bid": round(float((g["vs_bid_c"].abs() < 0.05).mean()), 3),
                     "below_best_bid": round(float((g["vs_bid_c"] <= -0.05).mean()), 3),
                     "at_or_above_ask": round(float((g["price"] >= g["ask_ref"] - 0.0005).mean()), 3),
                     "vs_mid_c_p50": round(float(g["vs_mid_c"].median()), 2)})
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------------- report
def run_all(conn: sqlite3.Connection, cache: Any, live_db: Path | None = None) -> dict[str, Any]:
    df = load_fills(conn)
    log.info("stage 4: %d buy fills loaded", len(df))
    res: dict[str, Any] = {"fills": len(df), "resolved": int(df["resolved"].sum()),
                           "from": int(df["ts"].min()), "to": int(df["ts"].max())}
    res["overall"] = summarize(df)
    res["by_asset"] = group_summary(df, ["asset"])
    res["by_timeframe"] = group_summary(df, ["timeframe"])
    res["by_price"] = group_summary(df, ["price_bucket"])
    res["by_left"] = group_summary(df, ["left_bucket"])
    res["by_ahead"] = group_summary(df.assign(side_state=np.select([df["ahead"] > 0, df["ahead"] < 0], ["ahead", "behind"],
                                                                     default="level")), ["role", "side_state"])
    res["inventory"] = inventory(df)
    res["participation"] = participation(conn)
    res["timing"] = timing(df)
    res["sizing"] = sizing(df, conn)
    res["stability"] = stability(df)
    res["pairs"] = pair_trading(conn, df)
    res["late"] = late_entries(df)
    res["lead"] = spot_lead(cache, df)
    res["mm"] = market_making(conn, df)
    res["orders"] = order_sample(conn)
    res["live_quotes"] = live_quotes(live_db)
    res["segments"] = segments(df)
    return res


MONEY_COLS = {"usdc", "pnl", "max_drawdown", "pairs", "directional", "fees", "total", "lock", "count", "fills", "events",
              "markets", "mean", "median", "std", "worst", "best", "usdc_per_day", "windows_in_span", "orders", "tx"}
PCT_COLS = {"ev_per_usd", "ev_aligned", "ev_opposite", "taker_ev"}
SE_COLS = {"ev_se", "ev_aligned_se", "ev_opposite_se"}
SHARE_COLS = {"maker_share", "share_aligned", "share_of_fills", "share_of_usdc", "loss_rate", "both_sides_share",
              "paired_share_of_shares", "pair_cost_below_1", "fully_hedged_share", "merged_share_of_paired", "sum_below_1",
              "both_maker", "share_traded", "fills_after_close", "profitable_share", "maker_days_profitable",
              "taker_days_profitable", "fully_filled_in_tx", "integer_size", "order_ts_zero", "share", "at_best_bid",
              "below_best_bid", "at_or_above_ask", "taker_limit_above_exec_1c"}


def md_table(df: pd.DataFrame | None, floatfmt: int = 3) -> str:
    """Plain markdown table (no extra dependency). Money columns without decimals, EV in %, shares in %."""
    if df is None or len(df) == 0:
        return "_нет данных_\n"
    cols = list(df.columns)

    def cell(col: str, v: Any) -> str:
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            return "—"
        if isinstance(v, (float, np.floating)):
            if col in PCT_COLS:
                return f"{v * 100:+.2f}%".replace("-", "−")
            if col in SE_COLS:
                return f"±{v * 100:.2f}%"
            if col in SHARE_COLS:
                return f"{v * 100:.1f}%"
            if col in MONEY_COLS:
                return f"{v:,.0f}".replace(",", " ").replace("-", "−")
            return f"{v:.{floatfmt}f}".replace("-", "−")
        if isinstance(v, (int, np.integer)) and col in MONEY_COLS:
            return f"{v:,}".replace(",", " ")
        return str(v)

    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(cell(c, v) for c, v in zip(cols, row, strict=True)) + " |"
              for row in df.itertuples(index=False, name=None)]
    return "\n".join(lines) + "\n"


def _row(d: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame([{k: v for k, v in d.items() if not isinstance(v, (dict, pd.DataFrame))}])


def render(res: dict[str, Any]) -> str:
    """docs/stage4-data.md: every table behind the stage 4 report (regenerated by `python -m bosona stage4`)."""
    import time as _t

    fmt = lambda ts: _t.strftime("%Y-%m-%d %H:%M", _t.gmtime(ts))
    p, late, lead, mm, st, sz = res["pairs"], res["late"], res["lead"], res["mm"], res["stability"], res["sizing"]
    parts = [
        "# Этап 4: таблицы (генерируются `python -m bosona stage4`)\n",
        f"Покупки (BUY) с {fmt(res['from'])} по {fmt(res['to'])} UTC: {res['fills']} fills, из них с известным исходом {res['resolved']}.",
        "EV на $1 = PnL после комиссий / вложенные USDC (с комиссией). `ev_se` — стандартная ошибка EV с кластеризацией по рынку.",
        "Винрейт: исход 50-50 считается за половину выигрыша. Просадка — по накопленному PnL в порядке сделок.\n",
        "## Итого\n", md_table(_row(res["overall"])),
        "## По активу\n", md_table(res["by_asset"]),
        "## По таймфрейму\n", md_table(res["by_timeframe"]),
        "## По цене входа\n", md_table(res["by_price"]),
        "## По остатку окна\n", md_table(res["by_left"]),
        "## Сторона впереди / позади по споту источника резолва\n", md_table(res["by_ahead"]),
        "## Позиция: открывает, добавляет или сокращает (покупка другой стороны)\n",
        "`reduce+flip` — покупка другой стороны больше текущего нетто: позиция переворачивается.\n",
        md_table(res["inventory"]["by_action"]),
        "Taker-сделки против движения спота за 10 с до сделки (движение < 0.5 bps не считается):\n",
        md_table(res["inventory"]["taker_vs_spot"]),
        (f"Размер сокращающей taker-сделки / нетто-позиция до неё: {res['inventory']['taker_reduce_size_vs_net']} "
         "(`flattens` — доля сделок, обнуляющих нетто ±5%).\n"),
        "## Где и когда он торгует\n",
        "Доля окон серии, в которых была хотя бы одна сделка (в пределах периода его активности в серии):\n",
        md_table(res["participation"]),
        "Первая и последняя сделка в окне (секунды от открытия / до закрытия), сделки после закрытия:\n",
        md_table(res["timing"]),
        "## Размер\n",
        "Размер сделки (fill) по цене и роли:\n", md_table(sz["by_price"]),
        "Вложено в рынок (обе стороны, USDC) и нетто-экспозиция в акциях:\n", md_table(sz["per_market"]),
        f"Ранговая корреляция размера сделки с |модель − цена|: {sz['rank_corr_size_vs_model_edge']}\n",
        "## Стабильность\n",
        "PnL по дням (UTC):\n", md_table(pd.DataFrame([st["daily"]])),
        "По месяцам:\n", md_table(st["monthly"]),
        "## Гипотеза 1: парная торговля\n", md_table(_row(p)),
        "Цена пары с комиссией (средняя Up + средняя Down) в рынках, где куплены обе стороны:\n",
        md_table(pd.DataFrame([p["pair_cost_quantiles"]])),
        "Доля захеджированного объёма (парные акции / большая сторона):\n",
        md_table(pd.DataFrame([p["hedge_ratio_quantiles"]])),
        "Разложение PnL: пары и непарный остаток по ценам без комиссии, комиссии отдельно:\n",
        md_table(pd.DataFrame([p["pnl_split"]])), md_table(p["pnl_split_by_timeframe"]),
        "Цена пары по таймфреймам:\n", md_table(p["pair_cost_by_timeframe"]),
        "Сделки на противоположных сторонах одного рынка в пределах 5 с:\n",
        md_table(_row(p["near_simultaneous_pairs"])), md_table(pd.DataFrame([p["near_simultaneous_pairs"]["sum_quantiles"]])),
        f"Первая покупка Up и первая покупка Down: {p['first_fill_gap']}\n",
        "## Гипотеза 2: поздние входы по 90¢+\n",
        md_table(pd.DataFrame([{**late["overall"], "share_of_fills": late["share_of_fills"], "share_of_usdc": late["share_of_usdc"],
                                "loss_rate": late["loss_rate"], "breakeven_winrate": late["breakeven_winrate"]}])),
        "По цене:\n", md_table(late["by_price"]),
        "По времени до закрытия:\n", md_table(late["by_time_left"]),
        "По таймфрейму:\n", md_table(late["by_timeframe"]),
        "По роли:\n", md_table(late["by_role"]),
        "По запасу до strike в сигмах оставшегося движения:\n", md_table(late["by_z"]),
        "## Гипотеза 3: опережение по споту\n",
        "Доля сделок, чью сторону поддержало движение спота за 10 / 60 с до сделки (без движения < 0.5 bps не считаются):\n",
        md_table(lead["alignment"]),
        "|Движение за 10 с| в момент его сделок и в случайную секунду того же окна, bps:\n",
        md_table(lead["move_vs_random_second"]),
        "Событийный анализ: его сделки на одно событие в секунду вокруг резких 3-секундных движений (top 0.1%, все активы):\n",
        md_table(event_profile(lead["event_study"])),
        "Реакция: избыток сделок над фоном за 0..+15 с и медианная задержка этого избытка (время блока; матч ≈ на 2.4 с раньше):\n",
        md_table(lead["lag"]),
        "Сделки в первые 10 с (время блока) после резкого движения: на стороне движения и против него:\n",
        md_table(lead["after_move"]),
        "## Гипотеза 4: маркет-мейкинг\n", md_table(mm["by_role"]),
        "Размер сделки, USDC:\n", md_table(mm["fill_size_quantiles"]),
        f"Сделок на рынок: {mm['fills_per_market_quantiles']}; доля смен стороны между соседними сделками: {mm['side_switch_share']}\n",
        "Его цена минус последняя цена токена из prices-history (~60 с), центы:\n", md_table(mm["price_minus_token_px_c"]),
        "Цена его сделки относительно лучших bid/ask своего токена прямо перед матчем (живой стакан этапа 3):\n",
        md_table(res["live_quotes"]),
        "Калибровка модели (случайное блуждание по источнику резолва): вероятность модели и фактический винрейт:\n",
        md_table(mm["model_calibration"]),
        "Где он покупает относительно модели и что это даёт:\n", md_table(mm["edge_vs_model"]),
        f"Ребейты и награды: {mm['rebates']}\n",
    ]
    o = res.get("orders")
    if o:
        parts += [
            "## Его ордера (calldata `matchOrders`, выборка последних 30 дней)\n",
            md_table(o["tx"]), md_table(o["by_role"]),
            "Самые частые размеры ордеров, акции:\n", md_table(o["common_sizes"]),
            "Размер ордера по лимит-цене:\n", md_table(o["size_by_price"]),
            (f"Наклон log(акции) по log(цена) у maker-ордеров: {o['log_size_vs_log_price_slope']} "
             "(0 — размер в акциях не зависит от цены, −1 — постоянная сумма в USDC).\n"),
            (f"Taker-ордера: лимит минус средняя цена исполнения, центы: {o['taker_limit_minus_exec_c']}; "
             f"лимит выше исполнения на 1¢+: {o['taker_limit_above_exec_1c']}\n"),
        ]
    parts += [
        "## Сегменты: актив × таймфрейм × цена × остаток окна\n",
        "Полная таблица — `docs/stage4/segments.csv`. Здесь — сегменты с оборотом от $20K, по EV на $1.\n",
        md_table(res["segments"][res["segments"]["usdc"] >= 20_000].sort_values("ev_per_usd", ascending=False)),
    ]
    return "\n".join(parts)
