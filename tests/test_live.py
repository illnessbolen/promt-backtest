"""Stage 3: parsers of the live feeds, keys shared across channels, price book, books, Data Streams helpers."""

import json
from pathlib import Path

import pytest

from bosona.live.books import BookFeed, copy_cost, parse_rest_book, summarize
from bosona.live.detect import FillEvent, Occurrences, data_api_events, parse_order_filled, parse_rtds_trade
from bosona.live.prices import (PriceBook, PriceTick, build_providers, decode_ds_report, ds_auth_headers,
                                parse_feed_map, parse_rtds_price)
from bosona.live.store import LiveStore
from bosona.live.tracker import LiveFill, Tracker
from bosona.windows import slug_for, window_end, window_start

FIX = Path(__file__).parent / "fixtures"
SAMPLES = Path(__file__).parent.parent / "docs" / "samples"
BOS = "0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed"


# ------------------------------------------------------------------------------------------- windows
@pytest.mark.parametrize("asset,tf,ws,slug", [
    ("btc", "5m", 1790701200, "btc-updown-5m-1790701200"),
    ("eth", "15m", 1790695800, "eth-updown-15m-1790695800"),
    ("btc", "4h", 1790683200, "btc-updown-4h-1790683200"),
    ("btc", "1h", 1790701200, "bitcoin-up-or-down-september-29-2026-1pm-et"),
    ("bnb", "1h", 1790697600, "bnb-up-or-down-september-29-2026-12pm-et"),
    ("doge", "1d", 1790611200, "dogecoin-up-or-down-on-september-29-2026"),
])
def test_slug_for_matches_gamma(asset, tf, ws, slug):
    assert slug_for(asset, tf, ws) == slug
    assert window_start(tf, ws) == ws
    assert window_start(tf, window_end(tf, ws) - 1) == ws


def test_daily_window_across_dst_end():
    # 2026-10-31 12:00 EDT -> 2026-11-01 12:00 EST is 25 hours
    ws = window_start("1d", 1793455200)  # 2026-10-31 14:00 UTC = 10:00 EDT -> window opened 2026-10-30 noon EDT
    assert ws == 1793376000
    nxt = window_end("1d", ws)
    assert nxt == 1793462400 and window_end("1d", nxt) - nxt == 25 * 3600
    assert slug_for("btc", "1d", nxt) == "bitcoin-up-or-down-on-november-1-2026"


# ------------------------------------------------------------------------------------------ RTDS prices
def test_parse_rtds_chainlink_and_twap():
    spot = {"topic": "crypto_prices_chainlink", "type": "update", "timestamp": 1790877851082,
            "payload": {"full_accuracy_value": "118181903510394990000", "symbol": "sol/usd", "timestamp": 1790877850000,
                        "value": 118.18190351039499}}
    t = parse_rtds_price(spot, 1.0, {"sol"})
    assert (t.source, t.kind, t.asset, t.ts_ms) == ("chainlink", "spot", "sol", 1790877850000)
    assert t.price == pytest.approx(118.18190351039499, rel=1e-12)
    twap = {"topic": "crypto_prices_twap_sixty", "type": "update", "timestamp": 1790877851173,
            "payload": {"full_accuracy_value": "2695888685950540709888", "symbol": "eth/usd", "timestamp": 1790877850000,
                        "value": 2695.8886859505405, "window_s": 60}}
    t = parse_rtds_price(twap, 1.0, {"eth"})
    assert (t.kind, t.asset) == ("twap60", "eth") and t.price == pytest.approx(2695.88868595054, rel=1e-12)
    assert parse_rtds_price(twap, 1.0, {"btc"}) is None  # asset not tracked
    assert parse_rtds_price({"topic": "crypto_prices", "type": "update", "payload": {"symbol": "btcusdt"}}, 1.0, {"btc"}) is None


def test_build_providers_skips_data_streams_without_keys(monkeypatch):
    monkeypatch.delenv("CHAINLINK_DS_API_KEY", raising=False)
    names = [p.name for p in build_providers(["binance_ws", "chainlink_rtds", "chainlink_data_streams"], {}, ["btc"])]
    assert names == ["binance_ws", "chainlink_rtds"]
    monkeypatch.setenv("CHAINLINK_DS_API_KEY", "k")
    monkeypatch.setenv("CHAINLINK_DS_USER_SECRET", "s")
    monkeypatch.setenv("CHAINLINK_DS_FEEDS", "btc:spot=0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8")
    names = [p.name for p in build_providers(["binance_ws", "chainlink_data_streams"], {}, ["btc"])]
    assert names == ["binance_ws", "chainlink_data_streams"]
    with pytest.raises(ValueError):
        build_providers(["nope"], {}, ["btc"])


# ------------------------------------------------------------------------------------ Data Streams
def test_ds_auth_matches_official_sdk():
    ref = json.loads((FIX / "ds_v3_report.json").read_text())["auth"]
    h = ds_auth_headers("test-key", "test-secret", "GET", ref["rest_url"], ts_ms=ref["ts_ms"])
    assert h == {"Authorization": "test-key", "X-Authorization-Timestamp": str(ref["ts_ms"]),
                 "X-Authorization-Signature-SHA256": ref["rest_signature"]}
    h = ds_auth_headers("test-key", "test-secret", "GET", ref["ws_url"], ts_ms=ref["ts_ms"])
    assert h["X-Authorization-Signature-SHA256"] == ref["ws_signature"]


def test_decode_ds_report_v3():
    fx = json.loads((FIX / "ds_v3_report.json").read_text())
    d = decode_ds_report(fx["fullReport"])
    assert d["feed_id"] == fx["feed_id"] and d["version"] == 3
    assert (d["valid_from_ts"], d["observations_ts"]) == (fx["valid_from_ts"], fx["observations_ts"])
    assert d["price"] == pytest.approx(int(fx["sdk_decoded"]["price"]) / 1e18)


def test_parse_feed_map():
    assert parse_feed_map("btc:spot=0xAA, eth:twap60=0xbb") == {"0xaa": ("btc", "spot"), "0xbb": ("eth", "twap60")}
    assert parse_feed_map({"btc": "0xcc"}) == {"0xcc": ("btc", "spot")}
    assert parse_feed_map(None) == {}


# ---------------------------------------------------------------------------------------- price book
def test_price_book_at_and_seconds():
    seconds = []
    pb = PriceBook(keep_s=60, on_second=seconds.append)
    for ts, p in [(1000.0, 1.0), (1500.0, 2.0), (2100.0, 3.0), (1900.0, 9.0), (3050.0, 4.0)]:
        pb.update(PriceTick("binance", "spot", "btc", p, ts, ts + 30))
    assert pb.last("binance", "spot", "btc").price == 4.0
    assert pb.at("binance", "spot", "btc", 2000.0).price == 2.0      # out-of-order 9.0 was dropped
    assert pb.at("binance", "spot", "btc", 999.0) is None
    assert [t.price for t in seconds] == [2.0, 3.0]                  # last tick of second 1 and of second 2
    pb.flush_seconds()
    assert seconds[-1].price == 4.0


# ----------------------------------------------------------------------------------- fill detection
def _chain_log() -> dict:
    sample = json.loads((SAMPLES / "stage0" / "chain_orderfilled_samples.json").read_text())
    tx = "0x63326de78ac4221ae367baa510c24c1a2dcb6ac2b376f1a7fea259ffed609c2a"
    return {**sample["response"][tx]["raw_log_example"], "transactionHash": tx}


def test_parse_order_filled_taker_fill():
    lg = _chain_log()
    ev = parse_order_filled(lg, 123.0)
    # API row: BUY 108 @ 0.36, taker, fee 1.74182 (verified in stage 0)
    assert (ev.side, ev.size, ev.price, ev.role) == ("BUY", 108.0, pytest.approx(0.36), "taker")
    assert ev.fee_usdc == pytest.approx(1.74182) and ev.usdc == pytest.approx(38.88)
    assert ev.token_id == "87378234650055540935470495291045092567587444793277575994368171700863579455312"


def test_same_fill_has_same_key_on_every_channel():
    fx = json.loads((FIX / "stage3_same_fill.json").read_text())
    rt = parse_rtds_trade(fx["rtds"], BOS, 1.0)
    ch = parse_order_filled(fx["chain_log"], 2.0)
    da = data_api_events([fx["data_api_row"]], 3.0)[0]
    assert rt.fill_key == ch.fill_key == da.fill_key
    assert ch.role == fx["expected_role"]
    assert rt.price == pytest.approx(ch.price, abs=1e-6) and da.price == pytest.approx(ch.price, abs=1e-6)


def test_rtds_trade_filter_by_wallet():
    fx = json.loads((FIX / "stage3_same_fill.json").read_text())
    other = json.loads(json.dumps(fx["rtds"]))
    other["payload"]["proxyWallet"] = "0x8d32B6C89B3efDB581dE4F0f7451955862A825A8"
    assert parse_rtds_trade(other, BOS, 1.0) is None
    assert parse_rtds_trade({"topic": "crypto_prices"}, BOS, 1.0) is None


def test_data_api_events_seq_and_types():
    row = {"type": "TRADE", "transaction_hash": "0xAB", "token_id": "1", "side": "BUY", "size": 5, "price": 0.5,
           "usdc_size": 2.5, "timestamp": 100}
    evs = data_api_events([row, dict(row), {**row, "type": "MERGE"}], 1.0)
    assert [e.fill_key for e in evs] == ["0xab:1:BUY:5000000:0", "0xab:1:BUY:5000000:1"]
    occ = Occurrences()
    assert [occ.next("k"), occ.next("k"), occ.next("j")] == [0, 1, 0]


def test_live_fill_merge_prefers_chain_amounts():
    rt = FillEvent("rtds_activity", 10.0, "0xab", "1", "BUY", 10.0, price=0.5, usdc=5.0, src_ts_ms=1_000.0, fee_usdc=0.0)
    lf = LiveFill(rt)
    ch = FillEvent("chain_logs", 20.0, "0xab", "1", "BUY", 10.0, price=0.4999, usdc=4.999, role="maker", fee_usdc=0.0,
                   block_ts=2, block_number=7, log_index=3)
    assert lf.merge(ch) is True and lf.merge(ch) is False
    assert lf.f["price"] == 0.4999 and lf.f["role"] == "maker" and lf.f["block_number"] == 7
    assert lf.f["first_channel"] == "rtds_activity" and lf.f["channels"] == "rtds_activity,chain_logs"


def test_headline_spot_prefers_resolution_source():
    snaps = [{"source": "binance", "kind": "spot", "det_price": 1.0, "det_age_ms": 50, "ref_price": 1.0, "shift_bps": 0.0},
             {"source": "chainlink", "kind": "spot", "det_price": 2.0, "det_age_ms": 900, "ref_price": 2.0, "shift_bps": 0.0}]
    assert Tracker._headline(snaps, "chainlink_twap60")["source"] == "chainlink"
    assert Tracker._headline(snaps, "binance_1h")["source"] == "binance"
    snaps[1]["det_age_ms"] = 60_000  # Chainlink feed dead at detection -> Binance
    assert Tracker._headline(snaps, "chainlink_twap60")["source"] == "binance"


# -------------------------------------------------------------------------------------------- books
def test_book_feed_snapshot_updates_and_match_time():
    feed = BookFeed()
    feed.wanted = {"A", "B"}
    feed.ws.connected = True
    feed.on_message(json.dumps([{"event_type": "book", "asset_id": "A", "timestamp": "1000",
                                 "bids": [{"price": "0.40", "size": "10"}, {"price": "0.45", "size": "5"}],
                                 "asks": [{"price": "0.55", "size": "7"}, {"price": "0.50", "size": "3"}]}]), 1001.0)
    feed.on_message(json.dumps({"event_type": "price_change", "timestamp": "2000", "price_changes": [
        {"asset_id": "A", "price": "0.50", "size": "0", "side": "SELL", "best_bid": "0.45", "best_ask": "0.55"},
        {"asset_id": "A", "price": "0.46", "size": "4", "side": "BUY", "best_bid": "0.46", "best_ask": "0.55"}]}), 2001.0)
    feed.on_message(json.dumps({"event_type": "last_trade_price", "asset_id": "A", "price": "0.5", "size": "3",
                                "side": "BUY", "timestamp": "1999", "transaction_hash": "0xABC"}), 2002.0)
    snap = feed.snapshot("A", 5)
    assert (snap["best_bid"], snap["best_ask"], snap["ask_size"]) == (0.46, 0.55, 7.0)
    assert json.loads(snap["bids_json"]) == [[0.46, 4.0], [0.45, 5.0], [0.4, 10.0]]
    assert feed.tob_before("A", 1999.0) == (0.45, 0.5) and feed.tob_before("A", 2001.0) == (0.46, 0.55)
    assert feed.match_ms("0xabc") == 1999.0
    assert feed.snapshot("B", 5) is None  # no snapshot received yet -> REST fallback


def test_copy_cost():
    asks = {0.50: 10.0, 0.51: 20.0}
    bids = {0.48: 5.0}
    c = copy_cost("BUY", 20.0, 0.50, bids, asks)
    assert c["copy_px"] == 0.50 and c["size_at_px"] == 10.0
    assert c["copy_vwap"] == pytest.approx(0.505) and c["copy_slip"] == pytest.approx(0.005)
    assert copy_cost("BUY", 100.0, 0.50, bids, asks)["copy_vwap"] is None  # not enough depth
    s = copy_cost("SELL", 5.0, 0.49, bids, asks)
    assert s["copy_px"] == 0.48 and s["copy_slip"] == pytest.approx(0.01) and s["size_at_px"] == 0


def test_rest_book_parse():
    sample = json.loads((SAMPLES / "stage0" / "clob_book_open_trimmed.json").read_text())["response"]
    bids, asks, ts = parse_rest_book(sample)
    s = summarize(bids, asks, 2)
    assert (s["best_bid"], s["best_ask"], ts) == (0.5, 0.51, 1790619014171.0)


# -------------------------------------------------------------------------------------------- store
def test_live_store_roundtrip(tmp_path):
    st = LiveStore(tmp_path / "live.db")
    for t in (100, 101, 103):
        st.add_spot(PriceTick("chainlink", "spot", "btc", float(t), t * 1000.0 + 5, t * 1000.0 + 1500))
    st.upsert("live_detections", [{"fill_key": "k", "channel": "chain_logs", "recv_ms": 1.0, "src_ts_ms": None, "raw_json": "{}"}])
    st.insert_ignore("live_detections", [{"fill_key": "k", "channel": "chain_logs", "recv_ms": 9.0, "src_ts_ms": None, "raw_json": "{}"}])
    assert st.flush() == 5
    assert st.spot_at("chainlink", "spot", "btc", 102) == 101.0
    assert st.spot_at("chainlink", "spot", "btc", 120) is None  # older than max lag
    assert st.spot_series("chainlink", "spot", "btc", 100, 103) == [(100, 100.0), (101, 101.0), (103, 103.0)]
    assert st.conn.execute("SELECT recv_ms FROM live_detections").fetchone()[0] == 1.0
    st.close()


# ------------------------------------------------------------------------------------------- report
def test_report_copy_pnl_and_latency(tmp_path):
    from bosona.live.report import copy_pnl, latency
    from bosona.live.store import FILL_COLUMNS

    st = LiveStore(tmp_path / "live.db")
    base = {c: None for c in FILL_COLUMNS}
    fills = [
        # maker buy of Up at 0.53; copier pays 0.58 at detection; Up wins
        {**base, "fill_key": "a", "tx_hash": "0xa", "condition_id": "c1", "token_id": "t1", "outcome": "Up", "side": "BUY",
         "size": 100.0, "price": 0.53, "usdc": 53.0, "fee_usdc": 0.0, "role": "maker", "copy_vwap": 0.58,
         "first_channel": "chain_logs", "first_seen_ms": 10_000.0, "block_ts": 11, "match_ms": 9_000.0,
         "lat_block_ms": -1_000.0, "lat_match_ms": 1_000.0, "backfill": 0, "timeframe": "5m", "updated_ms": 1.0},
        # taker buy of Down at 0.40 (fee paid); Up wins -> both lose
        {**base, "fill_key": "b", "tx_hash": "0xb", "condition_id": "c1", "token_id": "t2", "outcome": "Down", "side": "BUY",
         "size": 10.0, "price": 0.40, "usdc": 4.0, "fee_usdc": 0.168, "role": "taker", "copy_vwap": 0.41,
         "first_channel": "rtds_activity", "first_seen_ms": 20_000.0, "block_ts": 21, "match_ms": 18_000.0,
         "lat_block_ms": -1_000.0, "lat_match_ms": 2_000.0, "backfill": 0, "timeframe": "5m", "updated_ms": 1.0},
    ]
    st.upsert("live_fills", fills)
    st.upsert("live_detections", [
        {"fill_key": "a", "channel": "chain_logs", "recv_ms": 10_000.0, "src_ts_ms": None, "raw_json": "{}"},
        {"fill_key": "a", "channel": "rtds_activity", "recv_ms": 10_800.0, "src_ts_ms": None, "raw_json": "{}"},
        {"fill_key": "b", "channel": "rtds_activity", "recv_ms": 20_000.0, "src_ts_ms": None, "raw_json": "{}"}])
    st.upsert("resolutions", [{"condition_id": "c1", "winner": "Up", "payout_up": 1.0, "payout_down": 0.0, "price_to_beat": None,
                               "final_price": None, "strike_source": None, "closed_ts": None, "uma_status": None, "fetched_at": 1}])
    st.flush()
    res = copy_pnl(st.conn, 0)
    assert res["resolved_fills"] == 2 and res["copyable_fills"] == 2
    # his: 100*1 - 53 + (0 - 4 - 0.168) = 42.832 ; copier: 100*(1 - 0.58) - fee 100*.07*.58*.42 + (0 - 10*.41 - 10*.07*.41*.59)
    assert res["all"]["his_pnl"] == pytest.approx(42.83, abs=0.01)
    exp_copy = 100 * 0.42 - 100 * 0.07 * 0.58 * 0.42 - 10 * 0.41 - 10 * 0.07 * 0.41 * 0.59
    assert res["all"]["copy_pnl"] == pytest.approx(exp_copy, abs=0.01)
    assert res["by_role"]["maker"]["fills"] == 1
    lat = latency(st.conn, 0)
    assert lat["won_race"] == {"chain_logs": 1, "rtds_activity": 1}
    assert lat["per_channel"]["rtds_activity"]["seen"] == 2
    assert lat["first_seen_minus_match_ms"]["p50"] == pytest.approx(1_500.0)
    st.close()
