import numpy as np
import pytest

from bosona.context import Series, last_at_or_before, proxy_refs


def make_series(base=1_000, n=4_000, start=100.0, step=0.0):
    close = start + step * np.arange(n, dtype=float)
    return Series(base, close)


def test_at_and_out_of_range():
    s = make_series(step=1.0)
    assert s.at(np.array([1_000]))[0] == 100.0
    assert s.at(np.array([1_010]))[0] == 110.0
    assert np.isnan(s.at(np.array([999, 1_000 + 4_000]))).all()


def test_mean_requires_complete_window():
    close = np.arange(100.0, 110.0)
    close[5] = np.nan
    s = Series(0, close)
    assert s.mean(np.array([0]), np.array([4]))[0] == pytest.approx(101.5)   # 100..103
    assert np.isnan(s.mean(np.array([3]), np.array([7]))[0])                 # contains the gap


def test_realized_vol_matches_direct_computation():
    rng = np.random.default_rng(0)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 1e-4, 2_000)))
    s = Series(0, close)
    t_end = 1_500
    r = np.diff(np.log(close[t_end - 60 : t_end + 1]))
    assert s.realized_vol_bps(np.array([t_end]), 60)[0] == pytest.approx(np.sqrt((r * r).sum()) * 1e4)
    assert np.isnan(s.realized_vol_bps(np.array([30]), 60)[0])                # not enough history


def test_proxy_refs_follow_market_rules():
    s = make_series(base=0, n=200_000, step=0.01)                             # close[i] = 100 + 0.01 i
    ws = np.array([1_000, 1_000, 1_000, 90_000])
    we = np.array([1_300, 1_900, 4_600, 176_400])
    regime = np.array(["chainlink_spot", "chainlink_twap60", "binance_1h", "binance_noon_1m"])
    strike, final = proxy_refs(s, regime, ws, we)
    assert strike[0] == pytest.approx(100 + 0.01 * 999)                       # last close before start
    assert strike[1] == pytest.approx(100 + 0.01 * np.mean(np.arange(940, 1_000)))  # 60 s TWAP before start
    assert final[1] == pytest.approx(100 + 0.01 * np.mean(np.arange(1_840, 1_900)))
    assert final[2] == pytest.approx(100 + 0.01 * 4_599)                      # 1h candle close
    assert strike[3] == pytest.approx(100 + 0.01 * 90_059)                    # close of the 12:00 ET 1m candle
    assert final[3] == pytest.approx(100 + 0.01 * 176_459)


def test_last_at_or_before():
    t = np.array([10, 70, 130])
    p = np.array([0.5, 0.6, 0.7])
    assert last_at_or_before(t, p, 69) == (0.5, 59)
    assert last_at_or_before(t, p, 70) == (0.6, 0)
    v, age = last_at_or_before(t, p, 5)
    assert np.isnan(v) and age is None
