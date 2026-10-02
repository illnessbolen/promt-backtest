import math

import numpy as np
import pandas as pd
import pytest

from bosona.backtest.data import HisFill, WindowData
from bosona.backtest.engine import ExecParams, WindowSim, simulate
from bosona.backtest.pricing import (
    fair_up,
    fair_up_array,
    settle_moments,
    t5_cdf,
    taker_fee,
    unit_t5_cdf,
)
from bosona.backtest.proxy import BookProxy
from bosona.backtest.report import ratio_se
from bosona.strategies.base import (
    DOWN,
    UP,
    Book,
    Cancel,
    Inventory,
    PlaceBid,
    State,
    TakerBuy,
    Window,
)
from bosona.strategies.copy import CopyParams, DelayedCopy
from bosona.strategies.profiles import RiskProfile, cap_shares
from bosona.strategies.rules import BosonaRules, RulesParams
from bosona.tape import sample_slots, tape_rows

START, END = 1_000_000, 1_000_300


def tape(rows):
    """(block ts, outcome_index, side, price, size) -> tape frame in time order."""
    df = pd.DataFrame(rows, columns=["ts", "outcome_index", "side", "price", "size"])
    df["seq"] = range(len(df))
    df["taker"], df["tx_hash"] = "0xt", [f"0x{i:x}" for i in range(len(df))]
    return df


def window_data(rows, his=(), payout=(1.0, 0.0), strike=100_000.0, spot=100_000.0):
    t0 = START - 900
    w = Window(key="0xc", asset="btc", timeframe="5m", start=START, end=END, strike=strike, regime="chainlink_spot")
    tp = tape(rows)
    return WindowData(window=w, proxy=BookProxy(tp), tape=tp, spot_t0=t0, spot=np.full(END - t0 + 5, spot),
                      payout=payout, winner="Up" if payout[0] else "Down", his=list(his))


# ------------------------------------------------------------------------------------------- pricing
def test_t5_cdf_closed_form():
    assert t5_cdf(2.5706) == pytest.approx(0.975, abs=1e-5)       # t(5) quantile
    assert unit_t5_cdf(0.0) == 0.5
    assert unit_t5_cdf(1.0) == pytest.approx(0.873415, abs=1e-6)  # = updown's std_t_cdf(1, 5)


def test_fair_up_limits_and_twap():
    assert fair_up(100_000, 100_000, 300, 1e-4) == pytest.approx(0.5, abs=0.01)
    assert fair_up(100_200, 100_000, 1, 1e-4) > 0.99
    assert fair_up(99_800, 100_000, 1, 1e-4) < 0.01
    # TWAP: inside the averaging interval the known part pins the mean and the variance shrinks as tau^3
    mean, v = settle_moments(100_100, 30, 1e-4, "chainlink_twap60", twap_avg=100_000)
    assert mean == pytest.approx(100_050)
    assert v == pytest.approx(1e-8 * 30**3 / (3 * 3600))
    _, v_before = settle_moments(100_000, 120, 1e-4, "chainlink_twap60")
    assert v_before == pytest.approx(1e-8 * (120 - 60 + 20))
    arr = fair_up_array(np.array([100_100.0]), 100_000, np.array([30.0]), np.array([1e-4]), "chainlink_twap60",
                        np.array([100_000.0]))
    assert arr[0] == pytest.approx(fair_up(100_100, 100_000, 30, 1e-4, "chainlink_twap60", 100_000), abs=1e-12)


def test_taker_fee():
    assert taker_fee(0.46, 0.07) == pytest.approx(0.07 * 0.46 * 0.54)


# ------------------------------------------------------------------------------------------- proxy
def test_book_proxy_uses_buys_of_both_tokens():
    bp = BookProxy(tape([(START + 12, 0, "BUY", 0.60, 10), (START + 13, 1, "BUY", 0.42, 10)]), lag=2.0)
    ask, age = bp.ask(UP, START + 12)            # Up bought at 0.60, matched at START + 10
    assert ask[0] == pytest.approx(0.60) and age[0] == pytest.approx(2.0)
    bid, _ = bp.bid(UP, START + 12)              # a Down buyer at 0.42 minted against Up bids at 0.58
    assert bid[0] == pytest.approx(0.58)
    _t, e, z = bp.effective_sells(UP)
    assert list(e) == [pytest.approx(0.58)] and list(z) == [10]
    assert bp.ask_exec(UP, START + 10.4)[0] == pytest.approx(0.60)


def test_proxy_median_resists_an_odd_print():
    rows = [(START + 10, 0, "BUY", 0.60, 5), (START + 10, 0, "BUY", 0.61, 5), (START + 10, 0, "BUY", 0.30, 1)]
    assert BookProxy(tape(rows), lag=0.0).ask_exec(UP, START + 10)[0] == pytest.approx(0.60)


# ------------------------------------------------------------------------------------------- engine
class Script:
    """Strategy that sends fixed intents at given times."""

    name = "script"

    def __init__(self, plan):
        self.plan = dict(plan)
        self.seen = []

    def on_state(self, s):
        self.seen.append(s)
        return self.plan.pop(s.now, [])

    def on_signal(self, s, sig):
        return []


def test_taker_walks_levels_with_fee_and_limit():
    wd = window_data([(START + 20, 0, "BUY", 0.50, 10)])
    ep = ExecParams(order_latency_s=0.0, depth_per_tick=100, max_quote_age_s=60)
    r = WindowSim(wd, Script({START + 20.0: [TakerBuy(UP, 0.515, 250)]}), ep, None, "t").run()
    # 100 @ 0.50 and 100 @ 0.51; 0.52 is above the limit
    assert [(f.price, f.shares) for f in r.fills] == [(0.50, 100), (0.51, 100)]
    assert r.fees == pytest.approx(100 * taker_fee(0.50, 0.07) + 100 * taker_fee(0.51, 0.07))   # window fee rate
    assert r.pnl == pytest.approx(200 * 1.0 - (50 + 51) - r.fees)


def test_maker_fills_from_the_other_tokens_buyers_and_queue_modes():
    # our Up bid at 0.55 is live from START+5. A Down buyer at 0.45 sold Up at 0.55 (our level); a Down buyer at
    # 0.47 sold Up at 0.53, below our bid: a better bid is hit first, so that one fills us whatever the queue.
    rows = [(START + 30, 1, "BUY", 0.45, 100), (START + 40, 1, "BUY", 0.47, 10)]
    fills = {}
    for mode in ("front", "touch", "through"):
        wd = window_data(rows)
        ep = ExecParams(order_latency_s=0.0, queue_mode=mode, queue_shares=80)
        r = WindowSim(wd, Script({START + 5.0: [PlaceBid(UP, 0.55, 60)]}), ep, None, mode).run()
        fills[mode] = sum(f.shares for f in r.fills)
    assert fills["front"] == 60                    # the first print at our price fills us fully
    assert fills["touch"] == 20 + 10               # 80 ahead: 20 of the 100 reach us, then the print below our bid
    assert fills["through"] == 10                  # only the print below our bid


def test_maker_trade_through_and_cancel():
    rows = [(START + 30, 1, "BUY", 0.50, 30), (START + 60, 1, "BUY", 0.50, 30)]   # effective 0.50 < our 0.55
    wd = window_data(rows)
    ep = ExecParams(order_latency_s=0.0, cancel_latency_s=0.0, queue_mode="through")
    s = Script({START + 5.0: [PlaceBid(UP, 0.55, 100)]})
    sim = WindowSim(wd, s, ep, None, "x")
    sim.s.plan[START + 40.0] = [Cancel("o1")]
    r = sim.run()
    assert sum(f.shares for f in r.fills) == 30    # the second print comes after the cancel
    assert r.fills[0].price == 0.55 and r.fills[0].fee == 0.0


def test_ideal_mode_books_his_fills():
    his = [HisFill(UP, 0.40, 10, 4.0, 0.0, "maker", START + 50, START + 47.6, "0x1"),
           HisFill(DOWN, 0.55, 10, 5.5 + 0.17, 0.17, "taker", START + 60, START + 57.6, "0x2")]
    r = simulate(window_data([], his=his), None, ExecParams(), None, "a", "ideal")
    assert r.pnl == pytest.approx(10 * 1.0 - 4.0 - 5.67)


def test_copy_signal_is_seen_after_detection_delay():
    his = [HisFill(UP, 0.50, 20, 10.0, 0.0, "maker", START + 50, START + 47.6, "0x1")]
    rows = [(START + 52, 0, "BUY", 0.52, 50)]                       # matched at 49.6 (lag 2.4)
    wd = window_data(rows, his=his)
    r = simulate(wd, DelayedCopy(CopyParams("taker", max_slip=0.05)), ExecParams(max_quote_age_s=60), None, "b")
    assert len(r.fills) == 1 and r.fills[0].t == pytest.approx(START + 47.6 + 2.0 + 0.3)
    assert r.fills[0].price == pytest.approx(0.52)
    assert r.summary()["copy_premium_usdc"] == pytest.approx(20 * 0.02 + r.fills[0].fee)


# ------------------------------------------------------------------------------------------- strategies
def state(**kw):
    w = Window(key="k", asset="btc", timeframe="5m", start=START, end=END, strike=100_000.0, regime="chainlink_spot")
    base = {"now": START + 60.0, "window": w, "books": (Book(0.55, 0.56), Book(0.44, 0.45)), "spot": 100_000.0,
            "ret_10s_bps": 0.0, "sigma": 1e-4, "fair_up": 0.60, "inventory": Inventory(), "orders": []}
    base.update(kw)
    return State(**base)


def test_rules_quote_at_touch_below_fair():
    out = BosonaRules(RulesParams(margin=0.02, size_shares=100)).on_state(state())
    bids = {i.outcome: i.price for i in out if isinstance(i, PlaceBid)}
    assert bids == {UP: 0.55, DOWN: 0.38}           # Up: touch 0.55 < fair - 2c; Down: fair 0.40 - 2c
    fair_anchor = BosonaRules(RulesParams(anchor="fair", margin=0.02)).on_state(state())
    assert {i.outcome: i.price for i in fair_anchor if isinstance(i, PlaceBid)}[UP] == 0.55   # capped below the ask


def test_rules_hedge_after_adverse_move_and_stop_before_close():
    inv = Inventory(shares=[300.0, 0.0], cost=[160.0, 0.0])
    out = BosonaRules(RulesParams(hedge_move_bps=3)).on_state(state(inventory=inv, ret_10s_bps=-4.0))
    hedge = [i for i in out if isinstance(i, TakerBuy)]
    assert len(hedge) == 1 and hedge[0].outcome == DOWN and hedge[0].shares == 300
    assert hedge[0].limit == pytest.approx(0.47)
    late = BosonaRules().on_state(state(now=END - 5.0, orders=[]))
    assert late == []


def test_cap_shares_and_profiles():
    p = RiskProfile.of("moderate", 10_000)
    assert p.max_order_usdc == 200 and p.max_window_usdc == 750
    assert cap_shares(1_000, 0.5, p.max_order_usdc, 750, 5) == 400
    assert cap_shares(1_000, 0.5, 200, 2.0, 5) == 0                 # below the venue minimum
    with pytest.raises(ValueError):
        RiskProfile.of("yolo")


# ------------------------------------------------------------------------------------------- tape / report
def test_tape_rows_reverse_api_order():
    api = [{"timestamp": 20, "outcome_index": 1, "side": "BUY", "price": 0.4, "size": 5, "proxy_wallet": "0xA",
            "transaction_hash": "0xB"},
           {"timestamp": 10, "outcome_index": 0, "side": "SELL", "price": 0.6, "size": 3, "proxy_wallet": "0xC",
            "transaction_hash": "0xD"}]
    rows = tape_rows("0xc", api)
    assert [r["ts"] for r in rows] == [10, 20] and [r["seq"] for r in rows] == [0, 1]
    assert rows[0]["taker"] == "0xc" and rows[1]["tx_hash"] == "0xb"


def test_sample_slots_aligned_and_reproducible():
    a = sample_slots(1_000_050, 1_003_000, "5m", 4, seed=1)
    assert a == sample_slots(1_000_050, 1_003_000, "5m", 4, seed=1)
    assert all(s % 300 == 0 for s in a) and len(set(a)) == 4


def test_ratio_se():
    se = ratio_se(pd.Series([5.0, -5.0]), pd.Series([5.0, 5.0]))
    assert se == pytest.approx(math.sqrt(2 / 1 * 50) / 10)
