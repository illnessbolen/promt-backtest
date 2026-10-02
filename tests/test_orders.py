import json
from pathlib import Path

import pytest

from bosona import db
from bosona.orders import decode_match_orders, his_orders, pick_sample

FIX = Path(__file__).parent / "fixtures"
BOS = "0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed"
MAKER_TX = "0x77baeee51c31fd0bc98a661c28ab0e62373c468d7a9cbf9b7b6cad5fc0dbf485"
TAKER_TX = "0x376a64fdd9a7cb8d87304bc9d696051db936005cb618c159b9f4233d6908e8e5"
DEEP_TX = "0x3de15f3dc34ee1fa87bd43590fd651d5c912271cc3f1923291c8caafd6a398bf"


@pytest.fixture(scope="module")
def txs():
    # calldata of real matchOrders transactions (public Polygon node); decoding cross-checked with eth_abi
    return json.loads((FIX / "match_orders_txs.json").read_text())


def test_decode_his_maker_order(txs):
    dec = decode_match_orders(txs[MAKER_TX]["input"])
    assert dec["condition_id"].startswith("0xedfba0b625")
    rows = his_orders(MAKER_TX, dec, BOS)
    assert len(rows) == 1
    r = rows[0]
    # BUY 996 shares @ 0.01 (9.96 USDC), 5.39 USDC of it filled here = his 539-share fill in the Data API
    assert (r["k"], r["role"], r["side"]) == (1, "maker", "BUY")
    assert r["order_shares"] == pytest.approx(996) and r["order_usdc"] == pytest.approx(9.96)
    assert r["limit_price"] == pytest.approx(0.01)
    assert r["fill_amount"] == pytest.approx(5.39) and r["fee_amount"] == 0
    assert r["order_ts"] == 0 and r["signature_type"] == 3


def test_decode_his_taker_order(txs):
    rows = his_orders(TAKER_TX, decode_match_orders(txs[TAKER_TX]["input"]), BOS)
    assert len(rows) == 1
    r = rows[0]
    # BUY 79.4 @ 0.46; 4.6 USDC filled (10 shares) and a 0.17388 fee on top: the wallet sent 4.77388
    assert (r["k"], r["role"]) == (0, "taker")
    assert r["order_shares"] == pytest.approx(79.4) and r["limit_price"] == pytest.approx(0.46)
    assert r["fill_amount"] == pytest.approx(4.6) and r["fee_amount"] == pytest.approx(0.17388)


def test_decode_order_deep_in_the_maker_list(txs):
    dec = decode_match_orders(txs[DEEP_TX]["input"])
    assert len(dec["makers"]) == 3
    assert [m["maker"] == BOS for m in dec["makers"]] == [False, False, True]
    (r,) = his_orders(DEEP_TX, dec, BOS)
    assert r["k"] == 3 and r["order_shares"] == pytest.approx(297) and r["limit_price"] == pytest.approx(0.11)
    assert r["fill_amount"] == pytest.approx(9.39615)


def test_decode_rejects_other_calls():
    with pytest.raises(ValueError):
        decode_match_orders("0xdeadbeef" + "00" * 64)


def test_pick_sample_stratified_and_tops_up(tmp_path):
    conn = db.connect(tmp_path / "o.db")
    db.init_schema(conn)
    now = 1_000_000.0
    rows = []
    for i in range(10):
        for role in ("maker", "taker"):
            rows.append({"trade_uid": f"u{role}{i}", "tx_hash": f"0x{role}{i}", "seq": 0, "ts": int(now) - 100,
                         "condition_id": "0xc", "token_id": "1", "side": "BUY", "price": 0.5, "size_raw": 1, "usdc_raw": 1,
                         "size": 1.0, "usdc": 0.5, "role": role, "source": "data_api_v2", "ingested_at": 0})
    rows.append({**rows[0], "trade_uid": "old", "tx_hash": "0xold", "ts": int(now) - 40 * 86_400})
    db.insert_ignore(conn, "trades", rows)
    first = pick_sample(conn, 3, 2, days=30, seed=1, now=now)
    assert sorted(s for _, s in first) == ["maker"] * 3 + ["taker"] * 2
    assert all(tx.startswith(f"0x{s}") for tx, s in first) and "0xold" not in {tx for tx, _ in first}
    db.insert_ignore(conn, "order_tx", [{"tx_hash": tx, "stratum": s, "status": "ok", "block_number": None, "n_orders": 2,
                                         "fetched_at": 0} for tx, s in first])
    more = pick_sample(conn, 5, 2, days=30, seed=1, now=now)   # tops the maker stratum up to 5, taker is full
    assert sorted(s for _, s in more) == ["maker", "maker"]
    assert not {tx for tx, _ in more} & {tx for tx, _ in first}
