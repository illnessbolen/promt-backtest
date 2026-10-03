"""Stage 4: the orders behind his fills, read from the calldata of a sample of recent transactions.

The Data API has fills only. How big the order behind a fill was, and at what limit price, is in the calldata of the
exchange's `matchOrders` call (CTF Exchange V2, selector 0x3c2b4399, stage 0 §2.4):

    matchOrders(bytes32 conditionId, Order takerOrder, Order[] makerOrders, uint256 takerFillAmount,
                uint256[] makerFillAmounts, uint256 takerFeeAmount, uint256[] makerFeeAmounts)
    Order = (uint256 salt, address maker, address signer, uint256 tokenId, uint256 makerAmount, uint256 takerAmount,
             uint8 side, uint8 signatureType, uint256 timestamp, bytes32 metadata, bytes32 builder, bytes signature)

Amounts have 6 decimals. A BUY order gives makerAmount USDC for takerAmount tokens (limit price = makerAmount /
takerAmount); fill and fee amounts are in the order's maker asset (USDC for a BUY). The public node serves
transactions by hash for ~40 days only, so the sample is drawn from the recent weeks.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import time
from typing import Any

from bosona import db
from bosona.config import Config
from bosona.http import ApiClient, ApiError

log = logging.getLogger(__name__)

MATCH_ORDERS = "0x3c2b4399"
DECIMALS = 1e6


def _word(buf: bytes, pos: int) -> int:
    return int.from_bytes(buf[pos : pos + 32], "big")


def _order(buf: bytes, pos: int) -> dict[str, Any]:
    v = [_word(buf, pos + 32 * i) for i in range(11)]  # the 12th head word is the offset of the signature
    return {
        "salt": str(v[0]), "maker": f"0x{v[1]:040x}", "signer": f"0x{v[2]:040x}", "token_id": str(v[3]),
        "maker_amount": v[4], "taker_amount": v[5], "side": "BUY" if v[6] == 0 else "SELL", "signature_type": v[7],
        "timestamp": v[8], "metadata": f"0x{v[9]:064x}", "builder": f"0x{v[10]:064x}",
    }


def _uints(buf: bytes, pos: int) -> list[int]:
    return [_word(buf, pos + 32 * (1 + i)) for i in range(_word(buf, pos))]


def decode_match_orders(input_hex: str) -> dict[str, Any]:
    """ABI-decode `matchOrders` calldata. Offsets of dynamic parts are relative to the start of their enclosing block."""
    if not input_hex.startswith(MATCH_ORDERS):
        raise ValueError(f"not matchOrders: {input_hex[:10]}")
    buf = bytes.fromhex(input_hex[10:])
    arr = _word(buf, 64)                               # makerOrders: length, then offsets relative to arr + 32
    makers = [_order(buf, arr + 32 + _word(buf, arr + 32 + 32 * i)) for i in range(_word(buf, arr))]
    maker_fills, maker_fees = _uints(buf, _word(buf, 128)), _uints(buf, _word(buf, 192))
    return {
        "condition_id": "0x" + buf[:32].hex(),
        "taker": {**_order(buf, _word(buf, 32)), "fill": _word(buf, 96), "fee": _word(buf, 160)},
        "makers": [{**m, "fill": f, "fee": fe} for m, f, fe in zip(makers, maker_fills, maker_fees, strict=True)],
    }


def his_orders(tx_hash: str, decoded: dict[str, Any], user: str) -> list[dict[str, Any]]:
    """Rows for `order_samples`: the orders of `user` in one match (taker order first, k = 0)."""
    rows = []
    for k, o in enumerate([decoded["taker"], *decoded["makers"]]):
        if o["maker"] != user.lower():
            continue
        buy = o["side"] == "BUY"
        usdc, shares = (o["maker_amount"], o["taker_amount"]) if buy else (o["taker_amount"], o["maker_amount"])
        rows.append({
            "tx_hash": tx_hash.lower(), "k": k, "role": "taker" if k == 0 else "maker",
            "condition_id": decoded["condition_id"], "token_id": o["token_id"], "side": o["side"], "salt": o["salt"],
            "order_shares": shares / DECIMALS, "order_usdc": usdc / DECIMALS,
            "limit_price": usdc / shares if shares else None,
            "fill_amount": o["fill"] / DECIMALS, "fee_amount": o["fee"] / DECIMALS,
            "signature_type": o["signature_type"], "order_ts": o["timestamp"],
        })
    return rows


def pick_sample(conn: sqlite3.Connection, n_maker: int, n_taker: int, days: float, seed: int,
                now: float | None = None) -> list[tuple[str, str]]:
    """Random transactions of his recent fills, stratified by the role of the fill, skipping ones already read."""
    since = int((now or time.time()) - days * 86_400)
    done = {r[0] for r in conn.execute("SELECT tx_hash FROM order_tx")}
    rng = random.Random(seed)
    out = []
    for role, n in (("maker", n_maker), ("taker", n_taker)):
        txs = sorted({r[0] for r in conn.execute("SELECT DISTINCT tx_hash FROM trades WHERE role = ? AND ts >= ?",
                                                 (role, since))})
        rng.shuffle(txs)
        have = conn.execute("SELECT COUNT(*) FROM order_tx WHERE stratum = ?", (role,)).fetchone()[0]
        out += [(tx, role) for tx in txs if tx not in done][: max(0, n - have)]
    return out


async def sample_orders(cfg: Config, conn: sqlite3.Connection, n_maker: int = 800, n_taker: int = 400,
                        days: float = 30, seed: int = 4, concurrency: int = 4) -> dict[str, int]:
    """Read the calldata of a sample of his recent transactions and store his orders (idempotent, tops the sample up)."""
    url = cfg.live.get("rpc_http", "https://polygon-bor-rpc.publicnode.com")
    todo = pick_sample(conn, n_maker, n_taker, days, seed)
    log.info("order sample: %d transactions to read", len(todo))
    sem = asyncio.Semaphore(concurrency)
    stats = {"ok": 0, "not_found": 0, "other_call": 0, "failed": 0, "orders": 0}

    async with ApiClient(cfg) as client:
        async def one(tx: str, stratum: str) -> None:
            async with sem:
                try:
                    t = await client.rpc(url, "eth_getTransactionByHash", [tx])
                except ApiError as exc:                   # not recorded: the next run retries it
                    log.warning("order sample: %s skipped (%s)", tx, exc)
                    stats["failed"] += 1
                    return
            row = {"tx_hash": tx, "stratum": stratum, "status": "not_found", "block_number": None, "n_orders": None,
                   "fetched_at": int(time.time())}
            orders: list[dict[str, Any]] = []
            if t is not None:
                row["block_number"] = int(t["blockNumber"], 16) if t.get("blockNumber") else None
                if t["input"].startswith(MATCH_ORDERS):
                    dec = decode_match_orders(t["input"])
                    orders = his_orders(tx, dec, cfg.user)
                    row.update(status="ok", n_orders=1 + len(dec["makers"]))
                else:
                    row["status"] = "other_call"
            db.insert_ignore(conn, "order_samples", orders)
            db.insert_ignore(conn, "order_tx", [row])
            stats[row["status"]] += 1
            stats["orders"] += len(orders)

        for i in range(0, len(todo), 200):                # commit in chunks: an interrupted run keeps its progress
            await asyncio.gather(*(one(tx, s) for tx, s in todo[i : i + 200]))
            conn.commit()
            log.info("order sample: %d/%d read", min(i + 200, len(todo)), len(todo))
    stats["requests"] = client.requests
    return stats
