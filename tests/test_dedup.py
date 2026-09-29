import pytest

from bosona import db, parse
from bosona.sync_history import finalize_trades, split_windows


def row(tx="0xabc", token="111", side="BUY", size=249.0, price=0.97, usdc=241.53, ts=1790518751,
        typ="TRADE", cond="0xcond", outcome="Down"):
    return {
        "transaction_hash": tx, "token_id": token, "side": side, "size": size, "price": price,
        "usdc_size": usdc, "timestamp": ts, "type": typ, "condition_id": cond,
        "slug": "btc-updown-5m-1790518500", "outcome": outcome, "outcome_index": 1,
        "name": "bosona", "pseudonym": "Impolite-Sister",
    }


@pytest.fixture
def conn(tmp_path):
    c = db.connect(tmp_path / "test.db")
    db.init_schema(c)
    yield c
    c.close()


def count(conn, table):
    return db.scalar(conn, f"SELECT COUNT(*) FROM {table}")


def test_identical_rows_in_one_tx_are_separate_fills():
    # tx 0xc12bff78…: two identical maker fills BUY 249 @ 0.97 (two OrderFilled logs on-chain)
    trades, _ = parse.split_activity([row(), row()], 0)
    assert [t["seq"] for t in trades] == [0, 1]
    assert trades[0]["trade_uid"] != trades[1]["trade_uid"]


def test_reinsert_is_idempotent(conn):
    trades, _ = parse.split_activity([row(), row(), row(tx="0xdef", size=10, usdc=5, price=0.5)], 0)
    assert db.insert_ignore(conn, "trades", trades) == 3
    assert db.insert_ignore(conn, "trades", trades) == 0
    assert count(conn, "trades") == 3


def test_overlapping_windows_do_not_duplicate(conn):
    a = [row(tx="0x1", ts=100), row(tx="0x2", ts=200)]
    b = [row(tx="0x2", ts=200), row(tx="0x3", ts=300)]  # re-read overlap
    for batch in (a, b):
        trades, _ = parse.split_activity(batch, 0)
        db.insert_ignore(conn, "trades", trades)
    assert count(conn, "trades") == 3


def test_partially_ingested_tx_heals_on_next_run(conn):
    first, _ = parse.split_activity([row()], 0)  # API had indexed only one of two identical fills
    db.insert_ignore(conn, "trades", first)
    second, _ = parse.split_activity([row(), row()], 0)
    assert db.insert_ignore(conn, "trades", second) == 1
    assert count(conn, "trades") == 2


def test_role_and_fee_from_taker_feed(conn):
    fills = [row(), row(), row(tx="0xdef", size=108, price=0.36, usdc=38.88)]
    trades, _ = parse.split_activity(fills, 0)
    db.insert_ignore(conn, "trades", trades)
    # taker feed contains one of the two identical fills and the 0xdef fill
    takers = parse.taker_fill_rows([row(), row(tx="0xdef", size=108, price=0.36, usdc=38.88)], 0)
    db.insert_ignore(conn, "taker_fills", takers)
    finalize_trades(conn, 0, 2**62, 0.07)
    got = sorted((r["tx_hash"], r["seq"], r["role"], r["fee_usdc"]) for r in conn.execute("SELECT * FROM trades"))
    assert got == [
        ("0xabc", 0, "taker", parse.taker_fee(249, 0.97, 0.07)),
        ("0xabc", 1, "maker", 0.0),
        ("0xdef", 0, "taker", 1.74182),
    ]


def test_market_fee_rate_is_used_when_known(conn):
    trades, _ = parse.split_activity([row(tx="0xdef", size=100, price=0.5, usdc=50)], 0)
    db.insert_ignore(conn, "trades", trades)
    db.insert_ignore(conn, "taker_fills", parse.taker_fill_rows([row(tx="0xdef", size=100, price=0.5, usdc=50)], 0))
    conn.execute("INSERT INTO markets (condition_id, fee_rate, raw_json, fetched_at) VALUES ('0xcond', 0, '{}', 0)")
    finalize_trades(conn, 0, 2**62, 0.07)
    assert db.scalar(conn, "SELECT fee_usdc FROM trades") == 0.0


def test_activity_rows_keep_per_outcome_redeems_and_rebates():
    rows = [
        row(typ="REDEEM", token="up", size=150, usdc=150, price=0, side="", outcome="Up"),
        row(typ="REDEEM", token="down", size=50, usdc=0, price=0, side="", outcome="Down"),
        row(typ="MAKER_REBATE", token="", cond="", size=357.349, usdc=357.349, price=0, side="", outcome=""),
        row(typ="TAKER_REBATE", token="", cond="", size=92.3183, usdc=92.3183, price=0, side="", outcome=""),
    ]
    trades, other = parse.split_activity(rows, 0)
    assert trades == [] and len(other) == 4
    assert len({o["activity_uid"] for o in other}) == 4
    assert other[2]["condition_id"] is None and other[2]["type"] == "MAKER_REBATE"


def test_split_windows_cover_range_without_overlap():
    assert split_windows(100, 350, 100) == [(100, 199), (200, 299), (300, 350)]
    assert split_windows(5, 5, 100) == [(5, 5)]
