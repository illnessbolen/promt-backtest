import json
from pathlib import Path

import pytest

from bosona import parse

SAMPLES = Path(__file__).resolve().parents[1] / "docs" / "samples" / "stage0"


def load(name: str):
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))["response"]


@pytest.mark.parametrize(
    "slug,expected",
    [
        ("btc-updown-5m-1790617500", ("btc", "5m")),
        ("eth-updown-15m-1790617500", ("eth", "15m")),
        ("hype-updown-4h-1790611200", ("hype", "4h")),
        ("bitcoin-up-or-down-september-28-2026-12pm-et", ("btc", "1h")),
        ("dogecoin-up-or-down-may-21-2026-10am-et", ("doge", "1h")),
        ("bitcoin-up-or-down-on-september-28-2026", ("btc", "1d")),
        ("solana-up-or-down-on-september-29-2026", ("sol", "1d")),
        ("bitcoin-up-or-down-on-may-21", ("btc", "1d")),  # 2025 slugs had no year
        ("will-it-rain-tomorrow", (None, None)),
        (None, (None, None)),
    ],
)
def test_parse_slug(slug, expected):
    assert parse.parse_slug(slug) == expected


def test_parse_iso():
    assert parse.parse_iso("2026-09-28T17:45:00Z") == 1790617500
    assert parse.parse_iso("2026-09-28 17:46:27+00") == 1790617587
    assert parse.parse_iso("2026-09-27T17:39:03.844892Z") == 1790530743
    assert parse.parse_iso(None) is None


@pytest.mark.parametrize(
    "fixture,regime,timeframe",
    [
        ("gamma_event_btc_15m_resolved.json", "chainlink_twap60", "15m"),
        ("gamma_event_btc_5m_resolved.json", "chainlink_twap60", "5m"),
        ("gamma_event_btc_15m_pre_twap.json", "chainlink_spot", "15m"),
        ("gamma_event_btc_hourly_resolved.json", "binance_1h", "1h"),
        ("gamma_event_btc_daily_resolved.json", "binance_noon_1m", "1d"),
    ],
)
def test_parse_market_regimes(fixture, regime, timeframe):
    event = load(fixture)
    row, res = parse.parse_market(event["markets"][0], event, fetched_at=0)
    assert row["resolution_regime"] == regime
    assert row["timeframe"] == timeframe
    assert row["asset"] == "btc"
    assert row["up_token_id"] and row["down_token_id"] and row["up_token_id"] != row["down_token_id"]
    assert row["window_end_ts"] > row["window_start_ts"]
    assert res is not None and res["winner"] in ("Up", "Down")


def test_parse_market_15m_details():
    event = load("gamma_event_btc_15m_resolved.json")
    row, res = parse.parse_market(event["markets"][0], event, fetched_at=0)
    assert row["condition_id"] == "0xf6a64d2de1345a777be97e4540110a7db36a081ed4576e5182e7ccae5f3db41e"
    assert (row["window_start_ts"], row["window_end_ts"]) == (1790616600, 1790617500)
    assert row["up_token_id"].startswith("72701660")
    assert row["fee_rate"] == 0.07 and row["fee_taker_only"] == 1
    assert row["twap_lookback_s"] == 60
    assert row["series_slug"] == "btc-up-or-down-15m"
    assert res["winner"] == "Up"
    assert res["price_to_beat"] == pytest.approx(83884.3164, abs=1e-3)
    assert res["closed_ts"] == 1790617587


def test_split_activity_on_real_v2_rows():
    rows = load("data_v2_activity.json")["data"]
    trades, other = parse.split_activity(rows, ingested_at=1)
    assert len(trades) + len(other) == len(rows)
    t = trades[0]
    assert t["trade_uid"] == f"{parse.trade_key(rows[0])}:0"
    assert t["size_raw"] == round(rows[0]["size"] * 1_000_000)
    assert t["usdc_raw"] == round(rows[0]["usdc_size"] * 1_000_000)
    assert t["event_slug"] == rows[0]["event_slug"]


def test_compact_json_drops_profile_decoration():
    raw = parse.compact_json({"type": "MERGE", "size": 1, "pseudonym": "Impolite-Sister", "profile_image": "x", "title": "t"})
    assert "pseudonym" not in raw and "profile_image" not in raw and "title" not in raw and '"type":"MERGE"' in raw


def test_market_raw_json_is_slim():
    event = load("gamma_event_btc_15m_resolved.json")
    row, _ = parse.parse_market(event["markets"][0], event, fetched_at=0)
    raw = json.loads(row["raw_json"])
    assert "description" not in raw["market"] and "volume24hr" not in raw["market"]
    assert raw["market"]["conditionId"] == event["markets"][0]["conditionId"]
    assert raw["event"]["eventMetadata"]["priceToBeat"] == pytest.approx(83884.3164, abs=1e-3)


def test_trade_key_is_stable_across_endpoints():
    # /v2/activity and /v2/trades report the same fill; price has float noise in one of them
    activity_row = {"transaction_hash": "0xAB", "token_id": "1", "side": "BUY", "size": 84.115385, "price": 0.7400000012}
    trades_row = {"transaction_hash": "0xab", "token_id": "1", "side": "BUY", "size": 84.115385, "price": 0.74}
    assert parse.trade_key(activity_row) == parse.trade_key(trades_row)


def test_taker_fee_matches_onchain_fee():
    # tx 0x63326de7…: bosona took 108 shares at 0.36; on-chain OrderFilled.fee = 1.741820
    assert parse.taker_fee(108, 0.36, 0.07) == pytest.approx(1.74182, abs=1e-12)
    assert parse.taker_fee(100, 0.50, 0.07) == pytest.approx(1.75)
