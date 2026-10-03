"""updown-grid: grid parsing, settling host snapshots, his row and the summary table (no updown checkout needed)."""

import math
import sqlite3

import pandas as pd
import pytest

from bosona.backtest.diag import fill_stats, settle
from bosona.backtest.proxy import MATCH_LAG_S
from bosona.updown import WinCtx, _Fill, his_rows, window_rows
from bosona.updown_grid import (
    DEFAULT_GRID,
    GridPoint,
    parse_grid,
    render_grid,
    summarize_grid,
)


def test_parse_grid():
    pts = parse_grid(["default", "  # a comment", "", "margin=0.05   anchor=fair  # quote deeper", "react_ms=-1",
                      "hedge=false cancel_ms=300"])
    assert pts[0] == GridPoint("default")
    assert pts[1].label == "margin=0.05 anchor=fair" and pts[1].rules == {"margin": 0.05, "anchor": "fair"}
    assert pts[2].react_ms == -1 and pts[2].rules == {}
    assert pts[3].rules == {"hedge": False} and pts[3].cancel_ms == 300
    assert len(parse_grid(DEFAULT_GRID.splitlines())) == 16
    with pytest.raises(ValueError):
        parse_grid(["no_such_param=1"])
    with pytest.raises(ValueError):
        parse_grid(["default", "default"])


def _snapshot(slug, fills, fair=lambda t: 0.5, start=0.0, end=300.0):
    shares, cost = [0.0, 0.0], [0.0, 0.0]
    for f in fills:
        shares[f.outcome] += f.shares
        cost[f.outcome] += f.shares * f.price + f.fee
    return {"slug": slug, "label": "5m", "start": start, "end": end, "fills": len(fills),
            "maker_fills": sum(f.role == "maker" for f in fills), "taker_fills": sum(f.role != "maker" for f in fills),
            "usdc": sum(cost), "fees": sum(f.fee for f in fills), "shares_up": shares[0], "shares_down": shares[1],
            "cost_up": cost[0], "cost_down": cost[1], "paired": min(shares), **fill_stats(fills, fair)}


def test_window_rows_settle_pnl_and_split():
    fills = [_Fill(10, 0, 0.40, 100, 0.0, "maker"), _Fill(20, 1, 0.55, 60, 0.6, "taker")]
    snap = [_snapshot("w1", fills), _snapshot("w2", fills)]
    rows = window_rows(snap, {"w1": (1.0, 0.0)})
    r1, r2 = rows
    assert r1["resolved"] and r1["pnl"] == pytest.approx(100 - 40 - 33.6)
    assert r1["maker_real"] + r1["taker_real"] == pytest.approx(r1["pnl"])          # resolution edge sums to PnL
    assert r1["pair_pnl"] + r1["unpaired_pnl"] == pytest.approx(r1["pnl"])
    assert r1["maker_edge"] == pytest.approx(100 * (0.5 - 0.40))
    assert not r2["resolved"] and r2["pnl"] is None and "maker_real" not in r2
    assert settle(snap[0], (100, 60), (40, 33.6), (1.0, 0.0))["pnl"] == pytest.approx(r1["pnl"])


def test_fair_series_lookup():
    c = WinCtx(w=None, strategy=None, window=None)
    c.fair_t, c.fair_v = [100.0, 101.0, 102.5], [0.5, 0.6, 0.7]
    assert math.isnan(c.fair_up_at(99.9))
    assert c.fair_up_at(100.0) == 0.5 and c.fair_up_at(102.4) == 0.6 and c.fair_up_at(1e9) == 0.7


def test_his_rows_use_match_time_and_his_cost():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE trades (slug, side, outcome, price, size, usdc, fee_usdc, role, ts)")
    conn.executemany("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?)", [
        ("w1", "BUY", "Up", 0.40, 10, 4.0, 0.0, "maker", 1000),
        ("w1", "BUY", "Down", 0.55, 10, 5.67, 0.17, "taker", 1010),
        ("w2", "BUY", "Up", 0.50, 5, 2.5, 0.0, "maker", 2000)])
    # P(Up) 0.5 until 1005, then 0.9: his Up fill at 1000 - 2.4 sees 0.5, 10 s later 0.9
    fair = {"w1": ([990.0, 1005.0], [0.5, 0.9])}
    pay = {"w1": (1.0, 0.0), "w2": (0.0, 1.0), "w3": (1.0, 0.0)}
    rows = {r["slug"]: r for r in his_rows(conn, ["w1", "w2", "w3"], pay, fair)}
    w1 = rows["w1"]
    assert w1["pnl"] == pytest.approx(10 - 4.0 - 5.67) and w1["fills"] == 2 and w1["maker_fills"] == 1
    assert w1["maker_edge"] == pytest.approx(10 * (0.5 - 0.40)) and w1["maker_mark"] == pytest.approx(10 * (0.9 - 0.40))
    assert 1000 - MATCH_LAG_S + 10 > 1005
    assert rows["w2"]["pnl"] == pytest.approx(-2.5) and rows["w2"]["maker_fair_shares"] == 0     # no fair series
    assert rows["w3"]["fills"] == 0 and rows["w3"]["pnl"] == 0


def test_summary_and_table():
    rows = []
    for i, (cfg, react, pnl) in enumerate([("default", 50.0, 3.0), ("react_ms=-1", -1.0, -2.0)]):
        for slug, full in (("w1", True), ("w2", True), ("w3", False)):
            rows.append({"config": cfg, "order": i, "react_ms": react, "slug": slug, "full": full, "resolved": True,
                         "fills": 4, "maker_fills": 3, "usdc": 100.0, "pnl": pnl, "pair_cost_usdc": 99.0,
                         "paired": 100.0, "maker_edge": 5.0, "maker_mark": 2.0, "maker_fair_shares": 100.0,
                         "maker_real": pnl, "maker_shares": 100.0, "taker_real": 0.0, "taker_shares": 0.0})
    df = pd.DataFrame(rows)
    his = df[df.config == "default"].assign(pnl=1.0)
    s = summarize_grid(df, his)
    assert list(s["config"]) == ["он (a)", "default", "react_ms=-1"]
    assert list(s["windows"]) == [2, 2, 2]                                      # w3 is not covered in full
    assert s.loc[1, "ev_per_usd"] == pytest.approx(0.03) and s.loc[2, "pnl"] == -4
    assert summarize_grid(df, None, all_windows=True)["windows"].tolist() == [3, 3]
    one = summarize_grid(df.assign(usdc=[100.0, 0.0, 0.0] * 2), None)
    assert one["ev_se"].isna().all()                                         # a single traded window: no SE
    md = render_grid(s, {"span": "x", "files": 1, "windows": "2", "profile": "moderate", "react_ms": 50.0})
    assert "| react_ms=-1 | таймер 1 с |" in md and "| −4 |" in md and "| +3.00% ± 0.00% |" in md
    assert "| он (a) | — |" in md and "taker на резолве" in md
