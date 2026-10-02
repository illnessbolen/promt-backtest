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
    if args.command == "stage4":
        from bosona.spot import SpotCache
        from bosona.strategy import render, run_all

        conn = db.connect(cfg.db_path)
        res = run_all(conn, SpotCache(cfg.spot_cache_dir), live_db=cfg.live_db_path)
        out_dir = cfg.root / "docs" / "stage4"
        out_dir.mkdir(parents=True, exist_ok=True)
        res["segments"].to_csv(out_dir / "segments.csv", index=False)
        (cfg.root / "docs" / "stage4-data.md").write_text(render(res), encoding="utf-8")
        log.info("stage 4: docs/stage4-data.md, docs/stage4/segments.csv (%d segments)", len(res["segments"]))
        return 0
    if args.command == "backtest":
        from dataclasses import asdict

        from bosona.backtest.engine import ExecParams
        from bosona.backtest.report import render as render_bt
        from bosona.backtest.run import QUEUE_MODES, run_backtest
        from bosona.strategies.rules import param_overrides

        ep = ExecParams(**cfg.backtest.get("exec", {}))
        rules = param_overrides(args.param)
        profile = args.profile or cfg.backtest.get("profile", "moderate")
        df = run_backtest(cfg, variants=args.variants.split(",") if args.variants else None,
                          queue_modes=tuple(args.queue.split(",")) if args.queue else QUEUE_MODES, ep=ep,
                          profile=profile, bankroll=float(args.bankroll or cfg.backtest.get("bankroll", 10_000)),
                          limit=args.limit, workers=args.workers, samples=tuple(args.samples.split(",")), rules=rules)
        out = cfg.root / "data" / "backtest"
        out.mkdir(parents=True, exist_ok=True)
        extra = [x.replace("=", "") for x in args.param] + ([f"n{args.limit}"] if args.limit else [])
        tag = "".join("-" + x for x in ([] if args.samples == "random" else [args.samples.replace(",", "-")]) + extra)
        df.to_csv(out / f"windows{tag}.csv.gz", index=False)
        meta = {"ep": asdict(ep), "profile": profile, "rules": rules or "по умолчанию"}
        md = (out if extra else cfg.root / "docs") / f"stage5-data{tag}.md"     # --param / --limit runs stay in data/
        md.write_text(render_bt(df, meta), encoding="utf-8")
        log.info("backtest: %d window results -> %s, %s", len(df), out / f"windows{tag}.csv.gz", md)
        return 0
    if args.command in ("updown-replay", "updown-paper"):
        from bosona.strategies.rules import BosonaRules, RulesParams, param_overrides
        from bosona.updown import import_updown, replay, run_paper, updown_profile

        U = import_updown(cfg.updown_path)
        params = RulesParams(**param_overrides(args.param))
        settings = dict(cfg.updown.get("settings", {}))
        profile = updown_profile(U, settings, args.profile, float(args.bankroll or cfg.backtest.get("bankroll", 10_000)))
        react = None if args.react_ms is None or args.react_ms < 0 else args.react_ms / 1000.0
        if args.command == "updown-paper":
            res = await run_paper(U, lambda: BosonaRules(params), profile, cfg.root / "data" / "paper",
                                  settings=settings, duration_s=args.duration, react_s=react, record=args.record)
        else:
            host, _winners = replay(U, args.paths, lambda: BosonaRules(params), profile, settings=settings, react_s=react)
            res = host.results(await updown_payouts(cfg, list(host.ctx)))
        print(json.dumps(summarize_windows(res), indent=2, default=float))
        return 0
    if args.command == "live-report":
        from bosona.live.report import live_report

        print(json.dumps(await live_report(cfg, since_h=args.hours), indent=2, ensure_ascii=False, default=str))
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
    if args.command == "sync-tape":
        from bosona.tape import connect as tape_connect
        from bosona.tape import plan_sample, sync_tape

        tc = tape_connect(cfg.tape_db_path)
        log.info("tape sample: %s", plan_sample(cfg, tc, conn, args.n5m, args.n15m, args.seed))
        log.info("tape: %s", await sync_tape(cfg, tc))
        return 0
    if args.command == "sample-orders":
        from bosona.orders import sample_orders

        res = await sample_orders(cfg, conn, n_maker=args.maker, n_taker=args.taker, days=args.days)
        log.info("order sample: %s", res)
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
    p = sub.add_parser("sample-orders", help="stage 4: his order sizes / limit prices from matchOrders calldata (public RPC)")
    p.add_argument("--maker", type=int, default=800, help="transactions sampled from his maker fills (total, tops up)")
    p.add_argument("--taker", type=int, default=400, help="transactions sampled from his taker fills (total, tops up)")
    p.add_argument("--days", type=float, default=30, help="sample from the last N days (the public node keeps ~40)")
    sub.add_parser("stage4", help="stage 4: strategy tables -> docs/stage4-data.md, docs/stage4/segments.csv")
    p = sub.add_parser("backtest", help="stage 5: (a)/(b)/(c) on the sampled windows -> docs/stage5-data.md")
    p.add_argument("--variants", default=None, help="comma list (default: all; see bosona/backtest/run.py)")
    p.add_argument("--queue", default=None, help="queue assumptions for maker orders: front,touch,through")
    p.add_argument("--profile", default=None, help="risk profile of the own strategy (default: config backtest.profile)")
    p.add_argument("--bankroll", type=float, default=None)
    p.add_argument("--limit", type=int, default=None, help="only this many random windows (quick runs)")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--samples", default="random", help="window sets of tape.db: random (default), recording, live_run")
    p.add_argument("--param", action="append", default=[],
                   help="RulesParams override for the c_* variants, e.g. margin=0.05 (output gets a suffix)")
    for name, hlp in (("updown-replay", "stage 5: run the rules strategy over updown recordings (ticks-*.tsv.gz)"),
                      ("updown-paper", "stage 5: live paper trading (DRY_RUN) with updown feeds; Ctrl+C stops")):
        p = sub.add_parser(name, help=hlp)
        if name == "updown-replay":
            p.add_argument("paths", nargs="+", help="tick files or directories recorded by updown (shadow --record)")
        else:
            p.add_argument("--duration", type=float, default=None, help="stop after N seconds")
            p.add_argument("--record", action="store_true", help="also record raw frames (updown format)")
        p.add_argument("--profile", default=None,
                       help="conservative | moderate | aggressive (default: updown's RISK_PROFILE; RISK_* overrides apply)")
        p.add_argument("--bankroll", type=float, default=None)
        p.add_argument("--react-ms", type=float, default=50.0,
                       help="re-quote on Binance quotes at most every N ms (-1: only the 1 s timer)")
        p.add_argument("--param", action="append", default=[], help="RulesParams override, e.g. margin=0.05")
    p = sub.add_parser("sync-tape", help="stage 5: trade tapes of sampled BTC 5m/15m windows -> data/tape.db (tops up)")
    p.add_argument("--n5m", type=int, default=3000, help="random BTC 5m windows (total, tops up)")
    p.add_argument("--n15m", type=int, default=800, help="random BTC 15m windows (total, tops up)")
    p.add_argument("--seed", type=int, default=5)
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


async def updown_payouts(cfg: Config, slugs: list[str]) -> dict[str, tuple[float, float]]:
    """Payouts of windows seen in an updown replay: tape.db first, then Gamma by slug (stored in tape.db)."""
    from bosona.tape import add_windows, fetch_meta
    from bosona.tape import connect as tape_connect
    from bosona.windows import TF_SECONDS

    tc = tape_connect(cfg.tape_db_path)

    def known() -> dict[str, tuple[float, float]]:
        if not slugs:
            return {}
        q = ",".join("?" * len(slugs))
        rows = tc.execute(f"SELECT m.slug, r.payout_up, r.payout_down FROM markets m JOIN resolutions r USING (condition_id) "
                          f"WHERE m.slug IN ({q}) AND r.winner IS NOT NULL", slugs).fetchall()
        return {r[0]: (float(r[1]), float(r[2])) for r in rows}

    out = known()
    missing = [s for s in slugs if s not in out]
    for slug in missing:                              # <asset>-updown-<tf>-<start>
        parts = slug.split("-")
        if len(parts) == 4 and parts[2] in TF_SECONDS and parts[3].isdigit():
            add_windows(tc, parts[0], parts[2], [int(parts[3])], "recording")
    if missing:
        async with ApiClient(cfg) as client:
            await fetch_meta(cfg, client, tc, missing)
        out = known()
    return out


def summarize_windows(res: list[dict]) -> dict:
    done = [r for r in res if r.get("resolved")]
    usdc = sum(r["usdc"] for r in done)
    pnl = sum(r["pnl"] for r in done)
    return {"windows": len(res), "settled": len(done), "fills": sum(r["fills"] for r in res),
            "maker_fills": sum(r["maker_fills"] for r in res), "usdc_settled": round(usdc, 2), "pnl": round(pnl, 2),
            "ev_per_usd": round(pnl / usdc, 4) if usdc else None, "unsettled_usdc": round(sum(r["usdc"] for r in res if not r.get("resolved")), 2)}
