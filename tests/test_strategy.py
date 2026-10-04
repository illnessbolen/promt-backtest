import math

import numpy as np
import pandas as pd
import pytest

from bosona.strategy import (
    _event_study,
    after_move,
    ev_se,
    inventory_actions,
    lead_lag,
    max_drawdown,
    md_table,
    near_pairs,
    pnl_split,
    sharp_moves,
    summarize,
)


def fills(rows):
    """Minimal resolved BUY fills: (condition_id, ts, outcome, price, size, payout, role, fee)."""
    df = pd.DataFrame(rows, columns=["condition_id", "ts", "outcome", "price", "size", "payout", "role", "fee_usdc"])
    df["usdc"] = df["price"] * df["size"] + df["fee_usdc"]            # like the Data API: the fee is inside usdc
    df["pnl_if_held"] = df["size"] * df["payout"] - df["usdc"]
    df["win"] = df["payout"]
    df["resolved"] = True
    df["trade_uid"] = [f"u{i}" for i in range(len(df))]
    return df


def test_max_drawdown():
    assert max_drawdown(pd.Series([1.0, -2.0, 0.5, -1.0, 3.0])) == pytest.approx(2.5)  # peak 1 -> trough -1.5
    assert max_drawdown(pd.Series([-1.0, 2.0])) == pytest.approx(1.0)                # a loss from the start counts
    assert max_drawdown(pd.Series([], dtype=float)) == 0.0


def test_summarize_ev_and_share_weighted_winrate():
    df = fills([
        ("a", 1, "Up", 0.90, 100, 1, "maker", 0.0),    # +10
        ("b", 2, "Up", 0.90, 10, 0, "taker", 0.063),   # -9.063
        ("c", 3, "Down", 0.20, 50, 1, "maker", 0.0),   # +40
    ])
    s = summarize(df)
    usdc = 90 + 9.063 + 10
    assert s["fills"] == 3 and s["markets"] == 3
    assert s["pnl"] == pytest.approx(10 - 9.063 + 40, abs=0.005)   # rounded to cents
    assert s["ev_per_usd"] == pytest.approx(round((10 - 9.063 + 40) / usdc, 4))
    assert s["winrate"] == pytest.approx(2 / 3, abs=1e-4)
    assert s["winrate_shares"] == pytest.approx(150 / 160, abs=1e-4)
    assert s["avg_entry"] == pytest.approx((90 + 9 + 10) / 160, abs=1e-4)
    assert s["maker_share"] == pytest.approx(0.667, abs=1e-3)


def test_ev_se_clusters_fills_of_a_market():
    one = fills([("a", 1, "Up", 0.5, 10, 1, "maker", 0.0), ("b", 2, "Up", 0.5, 10, 0, "maker", 0.0)])
    split = fills([("a", 1, "Up", 0.5, 5, 1, "maker", 0.0), ("a", 1, "Up", 0.5, 5, 1, "maker", 0.0),
                   ("b", 2, "Up", 0.5, 10, 0, "maker", 0.0)])
    # splitting a market's fill in two changes nothing: the market is the unit
    assert ev_se(one) == pytest.approx(ev_se(split))
    # two markets of $5 each, EV 0: residuals +5 and -5 -> sqrt(2 / (2 - 1) * 50) / $10
    assert ev_se(one) == pytest.approx(math.sqrt(2 / (2 - 1) * 50) / 10)
    assert ev_se(one.iloc[:1]) is None


def test_pnl_split_is_exact():
    s = pd.DataFrame({"up_sh": [10.0, 5.0, 0.0], "up_usdc": [4.0, 4.5, 0.0], "up_fee": [0.0, 0.5, 0.0],
                      "dn_sh": [6.0, 0.0, 8.0], "dn_usdc": [3.2, 0.0, 2.0], "dn_fee": [0.2, 0.0, 0.0],
                      "payout_up": [1.0, 0.0, 1.0], "payout_down": [0.0, 1.0, 0.0]}, index=["m1", "m2", "m3"])
    out = pnl_split(s)
    direct = s["up_sh"] * s["payout_up"] + s["dn_sh"] * s["payout_down"] - s["up_usdc"] - s["dn_usdc"]
    assert out["total"].to_numpy() == pytest.approx(direct.to_numpy())
    # m1: 6 pairs at 0.40 + 0.50 lock 0.60; the 4 unpaired Up won at 0.40 -> +2.40; fee 0.20
    assert out.loc["m1", "pairs"] == pytest.approx(0.6)
    assert out.loc["m1", "directional"] == pytest.approx(2.4)
    assert out.loc["m1", "fees"] == pytest.approx(-0.2)


def test_inventory_actions():
    df = fills([
        ("a", 1, "Up", 0.5, 10, 1, "maker", 0.0),     # open (net +10)
        ("a", 2, "Up", 0.5, 5, 1, "maker", 0.0),      # add (+15)
        ("a", 3, "Down", 0.5, 5, 0, "taker", 0.0),    # reduce (+10)
        ("a", 4, "Down", 0.5, 25, 0, "taker", 0.0),   # reduce+flip (-15)
        ("a", 5, "Up", 0.5, 15, 1, "maker", 0.0),     # reduce to flat (0)
        ("a", 6, "Down", 0.5, 1, 0, "maker", 0.0),    # open again from flat
        ("b", 1, "Down", 0.5, 3, 0, "maker", 0.0),    # other market: open
    ])
    assert list(inventory_actions(df)) == ["open", "add", "reduce", "reduce+flip", "reduce", "open", "open"]


def test_near_pairs():
    df = fills([
        ("a", 10, "Up", 0.45, 10, 1, "maker", 0.0),
        ("a", 12, "Down", 0.50, 10, 0, "maker", 0.0),   # 2 s later, other side: a pair at 0.95
        ("a", 30, "Down", 0.60, 10, 0, "taker", 0.0),   # same side as the previous fill
        ("b", 10, "Up", 0.30, 10, 1, "maker", 0.0),
        ("b", 20, "Down", 0.60, 10, 0, "maker", 0.0),   # 10 s later: outside the 5 s window
    ])
    res = near_pairs(df, within_s=5)
    assert res["pairs"] == 1
    assert res["sum_quantiles"]["p50"] == pytest.approx(0.95)
    assert res["sum_below_1"] == 1.0 and res["both_maker"] == 1.0


def test_event_study_and_lag_on_synthetic_moves():
    t0, n = 1_000, 4_000
    logp = np.zeros(n)
    events = [500, 1_500, 2_500, 3_500]                          # +20 bps jumps completing at these indices
    for e in events:
        logp[e - 2:] += 20e-4 / 3
        logp[e - 1:] += 20e-4 / 3
        logp[e:] += 20e-4 / 3
    rows = []
    for e in events:
        rows.append(("x", t0 + e + 3, "Up", 0.5, 1, 1, "taker", 0.0))    # aligned, 3 s after the move
        rows.append(("x", t0 + e + 8, "Down", 0.5, 1, 0, "maker", 0.0))  # opposite (picked off), 8 s after
    g = fills(rows)
    ev = pd.DataFrame(_event_study("btc", g, logp, t0))
    assert set(ev["events"]) == {4}
    taker = ev[(ev["role"] == "taker")]
    assert list(taker["side"]) == ["aligned"] and list(taker["offset_s"]) == [3] and list(taker["fills"]) == [4]
    lag = lead_lag(ev).set_index(["role", "side"])
    assert lag.loc[("taker", "aligned"), "median_offset_block_s"] == 3
    assert lag.loc[("maker", "opposite"), "median_offset_block_s"] == 8
    assert lag.loc[("taker", "aligned"), "excess_fills_per_event"] == pytest.approx(1.0)


def test_md_table_formats():
    df = pd.DataFrame([{"timeframe": "5m", "fills": 1234, "usdc": 12345.6, "ev_per_usd": -0.0123, "ev_se": 0.004,
                        "maker_share": 0.9, "avg_entry": 0.5}])
    out = md_table(df)
    assert "| 5m | 1 234 | 12 346 | −1.23% | ±0.40% | 90.0% | 0.500 |" in out
    assert md_table(pd.DataFrame()) == "_нет данных_\n"


def test_sharp_moves_and_after_move_tags():
    t0 = 1_000
    logp = np.zeros(400)
    logp[100:] += 30e-4                                     # +30 bps jump completing at t0 + 100
    logp[300:] -= 30e-4                                     # -30 bps at t0 + 300
    ev_t, ev_dir, _ = sharp_moves(logp, t0, quantile=0.99, gap_s=60)
    assert list(ev_t) == [t0 + 100, t0 + 300] and list(ev_dir) == [1.0, -1.0]
    g = fills([
        ("x", t0 + 95, "Up", 0.5, 1, 1, "maker", 0.0),      # before the first move
        ("x", t0 + 104, "Up", 0.5, 1, 1, "taker", 0.0),     # 4 s after the up move, Up: aligned
        ("x", t0 + 106, "Down", 0.5, 1, 0, "maker", 0.0),   # 6 s after, Down: opposite (picked off)
        ("x", t0 + 115, "Up", 0.5, 1, 1, "maker", 0.0),     # 15 s after: outside the 10 s window
        ("x", t0 + 302, "Down", 0.5, 1, 1, "taker", 0.0),   # 2 s after the down move, Down: aligned
    ])
    tags = after_move(g, ev_t, ev_dir, within_s=10).fillna("-")    # not right after a move: missing
    assert list(tags) == ["-", "aligned", "opposite", "-", "aligned"]
