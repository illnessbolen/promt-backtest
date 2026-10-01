"""Command line entry point: `python -m bosona <command>`."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import logging.handlers
import sys
import time

from bosona import db
from bosona.config import Config, load_config
from bosona.context import enrich, sync_neighbour_meta
from bosona.http import ApiClient
from bosona.prices import sync_token_prices
from bosona.spot import sync_spot
from bosona.sync_history import finalize_trades, sync_history
from bosona.sync_markets import sync_markets
from bosona.validate import validate
from bosona.verify import verify

log = logging.getLogger("bosona")


def setup_logging(cfg: Config, filename: str = "bosona.log") -> None:
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    fmt.converter = time.gmtime
    file_handler = logging.handlers.RotatingFileHandler(cfg.log_dir / filename, maxBytes=20_000_000, backupCount=10)
    file_handler.setFormatter(fmt)
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(fmt)
    root = logging.getLogger()
    root.handlers[:] = [file_handler, console]
    root.setLevel(cfg.log_level.upper())
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def _run(args: argparse.Namespace, cfg: Config) -> int:
    if args.command == "track":
        from bosona.live.tracker import run_tracker

        await run_tracker(cfg, duration_s=args.duration)
        return 0
    if args.command == "live-report":
        from bosona.live.report import live_report

        print(json.dumps(live_report(cfg, since_h=args.hours), indent=2, ensure_ascii=False, default=str))
        return 0
    conn = db.connect(cfg.db_path)
    db.init_schema(conn)
    if args.command == "init-db":
        log.info("schema ready at %s", cfg.db_path)
        return 0
    if args.command == "stats":
        print(json.dumps(table_counts(conn), indent=2))
        return 0
    if args.command == "validate":
        print(json.dumps(validate(conn), indent=2, ensure_ascii=False, default=str))
        return 0
    async with ApiClient(cfg) as client:
        t0 = time.monotonic()
        if args.command in ("sync", "sync-history"):
            res = await sync_history(cfg, conn, client, full=args.full)
            log.info("history: %s", res)
        if args.command in ("sync", "sync-markets"):
            res = await sync_markets(cfg, conn, client, refresh_all=getattr(args, "refresh_all", False))
            log.info("markets: %s", res)
            # fee rates are per market: recompute formula fees now that market metadata is known
            finalize_trades(conn, 0, 2**62, cfg.crypto_fee_rate)
        if args.command in ("sync-spot", "stage2"):
            log.info("spot: %s", await sync_spot(cfg, conn, client))
        if args.command in ("sync-prices", "stage2"):
            log.info("token prices: %s", await sync_token_prices(cfg, conn, client))
        if args.command in ("enrich", "stage2"):
            log.info("neighbour windows: %s", await sync_neighbour_meta(cfg, conn, client))
            log.info("enrich: %s", enrich(cfg, conn))
        if args.command == "verify":
            report = await verify(cfg, conn, client)
            print(json.dumps(report, indent=2, ensure_ascii=False))
        log.info("%s done in %.1fs, %d HTTP requests", args.command, time.monotonic() - t0, client.requests)
    return 0


def table_counts(conn) -> dict[str, int]:
    tables = ["trades", "taker_fills", "activity", "markets", "resolutions", "pnl_daily",
              "token_prices", "price_sync", "window_meta", "window_refs", "market_context", "episodes"]
    return {t: db.scalar(conn, f"SELECT COUNT(*) FROM {t}") for t in tables}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bosona", description="Read-only history tracker for Polymarket @bosona")
    parser.add_argument("--config", help="path to config.yaml (default: $BOSONA_CONFIG or ./config.yaml)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="create the SQLite schema")
    p = sub.add_parser("sync", help="sync history + market metadata (idempotent, incremental)")
    p.add_argument("--full", action="store_true", help="re-walk the whole history instead of the incremental window")
    p = sub.add_parser("sync-history", help="sync fills and activity from Data API v2")
    p.add_argument("--full", action="store_true", help="re-walk the whole history instead of the incremental window")
    p = sub.add_parser("sync-markets", help="sync Gamma metadata / resolutions for markets in the history")
    p.add_argument("--refresh-all", action="store_true", help="re-fetch every market, not only pending ones")
    sub.add_parser("verify", help="reconcile the local history with Data API aggregates")
    sub.add_parser("sync-spot", help="download Binance 1s closes for every traded market window (cached per day)")
    sub.add_parser("sync-prices", help="download CLOB price history of both tokens for every traded market")
    sub.add_parser("enrich", help="compute window refs, per-trade market context and episodes (stage 2)")
    sub.add_parser("stage2", help="sync-spot + sync-prices + enrich")
    sub.add_parser("validate", help="stage 2 quality report: coverage, proxy accuracy, PnL reconciliation")
    sub.add_parser("stats", help="print row counts")
    p = sub.add_parser("track", help="stage 3: live tracker of new fills (Ctrl+C / SIGTERM stops it cleanly)")
    p.add_argument("--duration", type=float, default=None, help="stop after this many seconds (default: run until stopped)")
    p = sub.add_parser("live-report", help="stage 3: detection latency, price shift and Binance vs Chainlink summary")
    p.add_argument("--hours", type=float, default=None, help="only the last N hours (default: everything recorded)")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    setup_logging(cfg, "live.log" if args.command == "track" else "bosona.log")
    try:
        return asyncio.run(_run(args, cfg))
    except KeyboardInterrupt:
        log.warning("interrupted; the next run resumes from the last committed window")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
