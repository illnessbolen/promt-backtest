"""The updown adapter: run only when an updown checkout is available (config updown.path / UPDOWN_PATH)."""

import dataclasses

import pytest

from bosona.config import load_config
from bosona.updown import regime_for


def test_regime_for_dates():
    assert regime_for("5m", "chainlink", 1786665600) == "chainlink_twap60"     # from 2026-08-14
    assert regime_for("5m", "chainlink", 1786060800) == "chainlink_twap30"     # 2026-08-07 .. 08-14
    assert regime_for("15m", "chainlink", 1786060800) == "chainlink_twap60"
    assert regime_for("1h", "binance", 1790000000) == "binance"
    assert regime_for("5m", "chainlink", 1780000000) == "chainlink_spot"


@pytest.fixture(scope="module")
def U():
    from bosona.updown import import_updown

    try:
        return import_updown(load_config().updown_path)
    except (FileNotFoundError, ImportError) as exc:
        pytest.skip(f"updown not available: {exc}")


def test_bid_is_filled_by_a_buyer_of_the_other_outcome(U):
    from bosona.updown import make_exchange_class

    cfg = U.config.load_settings(dotenv=False, ASSETS=("btc",))
    clock = U.clock.ReplayClock(100.0)
    sched = U.scheduler.ReplayScheduler(clock)
    hub = U.data_hub.MarketDataHub(cfg, clock)
    w = U.data_markets.MarketWindow(slug="btc-updown-5m-0", asset="btc", label="5m", duration_s=300, start_ts=0,
                                    end_ts=1_000, up_token="U", down_token="D", resolution="chainlink")
    hub.set_markets([w])
    ev = U.data_events
    hub.apply_many([ev.BookSnapshot("U", [(0.54, 100.0)], [(0.56, 100.0)], 100.0),
                    ev.BookSnapshot("D", [(0.44, 100.0)], [(0.46, 100.0)], 100.0)])
    fills = []
    ex = make_exchange_class(U)(cfg, hub, clock, sched, on_fill=lambda o, q, p, m: fills.append((q, p, m)))
    order = U.execution_paper.PaperOrder(window=w, side="up", token="U", other_token="D", kind="maker", limit=0.55,
                                         shares=50, t_send=100.0, fee_rate=0.07, fee_exp=1.0, on_done=lambda o: None)
    ex.submit(order)
    sched.run_until(101.0)
    assert order.status == "resting"
    # updown ignores this print; a Down buyer at 0.44 sold Up at 0.56 > our 0.55: no fill
    hub.apply_many([ev.LastTrade("D", 0.44, 30.0, "BUY", 102.0)])
    assert fills == []
    # a Down buyer at 0.46 took Up bids at 0.54 < 0.55: our better bid would have been hit first
    hub.apply_many([ev.LastTrade("D", 0.46, 30.0, "BUY", 103.0)])
    assert fills == [(30.0, 0.55, True)]
    # the quote stays past updown's MAKER_TIMEOUT_MS (<= 10 s) until cancelled
    sched.run_until(150.0)
    clock.advance_to(150.0)
    assert order.status == "resting"
    ex.cancel(order.order_id, "test")
    assert order.status == "done"


def test_profiles_match_updown_and_overrides_apply(U, monkeypatch):
    from bosona.strategies.profiles import PROFILES, RiskProfile
    from bosona.updown import updown_profile

    for env in ("RISK_PROFILE", "RISK_BET_PCT", "RISK_EXPOSURE_PCT", "RISK_DAILY_STOP_PCT"):
        monkeypatch.delenv(env, raising=False)
    for name, fractions in PROFILES.items():
        assert {k: U.risk_limits.PROFILES[name][k] for k in fractions} == fractions
        assert updown_profile(U, {}, name, 10_000) == RiskProfile.of(name, 10_000)
    p = updown_profile(U, {"RISK_BET_PCT": 0.005}, None, 10_000)    # updown's default profile + an override
    assert p.name == "conservative" and p.max_order_usdc == pytest.approx(50.0)


def test_strike_is_the_oracle_twap_before_the_open(U):
    from bosona.strategies.profiles import RiskProfile
    from bosona.updown import StrategyHost, WinCtx

    start = 1_790_000_100                                   # TWAP-60 regime

    def host_with_ticks(first: int, last: int):
        cfg = U.config.load_settings(dotenv=False, ASSETS=("btc",))
        clock = U.clock.ReplayClock(0.0)
        hub = U.data_hub.MarketDataHub(cfg, clock)
        for t in range(first, last + 1):                     # price = 100 + seconds since start - 70
            hub.assets["btc"].oracle_hist.append(float(t), 100.0 + t - (start - 70))
        return StrategyHost(U, cfg, hub, clock, U.scheduler.ReplayScheduler(clock),
                            U.data_reference.ReferenceResolver(cfg, hub), lambda: None, RiskProfile.of("moderate"))

    w = U.data_markets.MarketWindow(slug="btc-updown-5m-1790000100", asset="btc", label="5m", duration_s=300,
                                    start_ts=start, end_ts=start + 300, up_token="U", down_token="D",
                                    resolution="chainlink")
    host = host_with_ticks(start - 70, start - 2)
    c = WinCtx(w=w, strategy=None, window=host._window(w))
    assert host._strike(c, start + 0.5) is None             # the last second before the open is not in yet
    host = host_with_ticks(start - 70, start)
    c = WinCtx(w=w, strategy=None, window=host._window(w))
    assert host._strike(c, start + 1.0) == pytest.approx(100.0 + (10 + 69) / 2)   # not the tick at the open (170)
    host = host_with_ticks(start - 30, start)               # joined after the averaging began: unpriced
    assert host._strike(WinCtx(w=w, strategy=None, window=host._window(w)), start + 1.0) is None
    known = dataclasses.replace(w, price_to_beat=123.0)     # Gamma's priceToBeat wins once published
    assert host._strike(WinCtx(w=known, strategy=None, window=host._window(known)), start + 1.0) == 123.0
