import pytest

from bosona import db, parse
from bosona.context import build_episodes


@pytest.fixture
def conn(tmp_path):
    c = db.connect(tmp_path / "ep.db")
    db.init_schema(c)
    yield c
    c.close()


def fill(tx, token, outcome, size, price, ts=1_000):
    return {
        "transaction_hash": tx, "token_id": token, "side": "BUY", "size": size, "price": price,
        "usdc_size": round(size * price, 6), "timestamp": ts, "type": "TRADE", "condition_id": "0xc1",
        "slug": "btc-updown-5m-900", "outcome": outcome, "outcome_index": 0 if outcome == "Up" else 1,
    }


def test_pair_bought_and_merged(conn):
    conn.execute(
        "INSERT INTO markets (condition_id, market_id, asset, timeframe, resolution_regime, window_start_ts, "
        "window_end_ts, up_token_id, down_token_id, fee_rate, raw_json, fetched_at) "
        "VALUES ('0xc1', '7', 'btc', '5m', 'chainlink_twap60', 900, 1200, 'U', 'D', 0.07, '{}', 0)"
    )
    conn.execute(
        "INSERT INTO resolutions (condition_id, winner, payout_up, payout_down, fetched_at) VALUES ('0xc1', 'Up', 1, 0, 0)"
    )
    trades, _ = parse.split_activity([fill("0xa", "U", "Up", 10, 0.40), fill("0xb", "D", "Down", 10, 0.50)], 0)
    db.insert_ignore(conn, "trades", trades)
    db.insert_ignore(conn, "taker_fills", parse.taker_fill_rows([fill("0xb", "D", "Down", 10, 0.50)], 0))
    _, merge = parse.split_activity([{
        "transaction_hash": "0xm", "type": "MERGE", "condition_id": "0xc1", "token_id": "", "size": 10,
        "usdc_size": 10, "timestamp": 1_100, "price": 0, "side": "",
    }], 0)
    db.insert_ignore(conn, "activity", merge)
    from bosona.sync_history import finalize_trades
    finalize_trades(conn, 0, 2**62, 0.07)

    ep = build_episodes(conn).iloc[0]
    fee = parse.taker_fee(10, 0.50, 0.07)                     # only the Down fill was a taker fill
    assert ep["n_fills"] == 2 and ep["n_taker"] == 1
    assert ep["paired_shares"] == pytest.approx(10)
    assert ep["pair_cost"] == pytest.approx(0.90)
    assert ep["merged_shares"] == pytest.approx(10)
    assert ep["net_exposure"] == pytest.approx(0)
    # merging the pair returns $1 per pair, exactly what holding both sides to resolution pays
    assert ep["pnl"] == pytest.approx(10 * 1 + 10 * 0 - 4.0 - 5.0 - fee)
