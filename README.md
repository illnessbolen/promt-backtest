# bosona-tracker

Read-only collection and analysis of Polymarket trader **@bosona**'s trades
(proxy wallet `0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed`).
It uses only public sources: Polymarket Data API v2, Gamma, the public CLOB (REST and market WebSocket), RTDS,
Binance market data and a public Polygon RPC. No orders, keys or wallets.

- Stage 0 reconnaissance report: [`docs/stage0-recon.md`](docs/stage0-recon.md)
- Stage 1 report: [`docs/stage1-history.md`](docs/stage1-history.md)
- Stage 2 report: [`docs/stage2-context.md`](docs/stage2-context.md)
- Stage 3 report: [`docs/stage3-live.md`](docs/stage3-live.md)
- Project plan and open questions: [`CLAUDE.md`](CLAUDE.md)

## Install

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
cp .env.example .env        # optional, only for overrides; no secrets are needed
```

## Commands

```bash
.venv/bin/python -m bosona init-db        # create data/bosona.db schema
.venv/bin/python -m bosona sync           # full history on the first run, then incremental; + market metadata
.venv/bin/python -m bosona sync --full    # re-walk the whole history (idempotent, no duplicates)
.venv/bin/python -m bosona sync-history   # fills + activity + maker/taker role only
.venv/bin/python -m bosona sync-markets   # Gamma metadata / resolutions for pending markets
.venv/bin/python -m bosona verify         # reconcile with /v2/user-stats, /v2/user-volume, /v2/user-pnl
.venv/bin/python -m bosona stats          # row counts
.venv/bin/pytest                          # parsing, dedup and context tests

# stage 2: market context of every fill
.venv/bin/python -m bosona sync-spot      # Binance 1s closes for every traded window -> data/spot (~35 min first time, cached)
.venv/bin/python -m bosona sync-prices    # CLOB price history of both tokens per market (~80 min first time, incremental)
.venv/bin/python -m bosona enrich         # window_refs, market_context, episodes (full rebuild, ~2 min)
.venv/bin/python -m bosona stage2         # the three steps above
.venv/bin/python -m bosona validate       # coverage, proxy accuracy vs Chainlink strikes, PnL reconciliation

# stage 3: live tracker
.venv/bin/python -m bosona track          # runs until Ctrl+C / SIGTERM (clean shutdown); data/live.db, logs/live.log
.venv/bin/python -m bosona track --duration 3600   # stop by itself after an hour
.venv/bin/python -m bosona live-report    # latency per channel, price shift, copy cost, Binance vs Chainlink at closes
.venv/bin/python -m bosona live-report --hours 24
```

Logs go to `logs/bosona.log` (`logs/live.log` for the tracker) and stderr. Settings live in `config.yaml`, and `BOSONA_*` environment variables override them.

## How the history is built

| Table | Source | Key |
|---|---|---|
| `trades` | `/v2/activity?user=…` rows with `type=TRADE` (one row per fill) | `tx:token:side:size_raw:price_raw:seq` |
| `taker_fills` | `/v2/trades?user=…&taker_only=true` (fills where he was the taker) | same key as `trades` |
| `activity` | every other `/v2/activity` row: MERGE, REDEEM (per outcome), REWARD, MAKER_REBATE, TAKER_REBATE, … | `tx:type:condition:token:size_raw:usdc_raw:seq` |
| `markets`, `resolutions` | Gamma `/markets?condition_ids=…&closed=true` with the parent event embedded | `condition_id` |
| `pnl_daily` | `/v2/user-pnl` (reconciliation only) | day |
| `token_prices` | CLOB `/prices-history` (~60 s points) of the Up and Down token | `market_id, outcome, t` |
| `window_meta` | Gamma `eventMetadata` of neighbouring windows (to chain missing strikes) | `slug` |
| `window_refs` | strike / final per traded market + Binance proxy anchored on them | `condition_id` |
| `market_context` | per fill: window timing, strike, spot, distance, TWAP, returns, volatility, token prices, PnL if held | `trade_uid` |
| `episodes` | per market position: shares and cost per side, pair cost, merges, redeems, PnL | `condition_id` |

How the columns are derived:

- **Role.** `taker` if the fill appears in the taker feed, otherwise `maker`. This was checked on-chain against `OrderFilled` (stage 0).
- **Fee** (`fee_usdc`). For taker fills it is `size × fee_rate × p × (1 − p)` rounded to 5 dp; maker fills pay 0 (docs.polymarket.com/trading/fees).
- **`seq`.** Two identical rows in one transaction are two real fills, and `seq` keeps them apart.
  All rows of a transaction share one block timestamp, so the keys are stable across runs.
- **Incremental runs.** They re-read `sync.overlap_s` before the last synced second and skip the newest `sync.safety_lag_s`.
  Inserts are `INSERT OR IGNORE`, so re-running never duplicates rows.

## Live tracker (stage 3)

`python -m bosona track` is one asyncio process. Everything is public and read-only.

| Component | Source | What it gives |
|---|---|---|
| detector `chain_logs` | `eth_subscribe` logs on `wss://polygon-bor-rpc.publicnode.com`: `OrderFilled` of both CLOB V2 exchanges with maker topic = wallet | every fill, exact amounts, role, fee, log index; gaps after a reconnect are back-filled with `eth_getLogs` |
| detector `rtds_activity` | RTDS `activity/trades` on `wss://ws-live-data.polymarket.com`, filtered by `proxyWallet` | every fill (maker fills included), independent second push channel |
| detector `data_api` | `/v2/activity?user=…` every 3 s | the canonical rows of the history; ~10 s behind, backstop only |
| books | CLOB market channel `wss://ws-subscriptions-clob.polymarket.com/ws/market` | local books of the open BTC 5m/15m/1h and ETH 5m windows (+ any market he just traded), match time of every trade by tx hash |
| REST books | `GET /book?token_id=` for both tokens at detection | snapshot for markets that are not streamed |
| price `binance_ws` | `wss://data-stream.binance.vision` aggTrade | Binance spot, always on |
| price `chainlink_rtds` | RTDS `crypto_prices_chainlink`, `crypto_prices_twap_sixty` | Chainlink spot and 60 s TWAP (the resolution input of 5m/15m/4h) while the legacy topics exist |
| price `chainlink_data_streams` | Chainlink Data Streams WebSocket | off by default; switches on when `CHAINLINK_DS_API_KEY` and `CHAINLINK_DS_USER_SECRET` are set in `.env` |

Every price provider is a `PriceProvider` subclass registered by name (`bosona/live/prices.py`); `live.price_providers`
in `config.yaml` lists the ones to start. A provider that cannot connect is retried with backoff and the others keep going.
Every stored price carries its `source` and `kind`.

Tables of `data/live.db` (kept apart from `bosona.db` so the tracker never waits on batch jobs):

| Table | Content |
|---|---|
| `live_fills` | one row per fill: trade fields, first channel and time, latency vs block and vs CLOB match, spot at the trade and at detection (shift in bps), his token's top of book then and at detection, cost of copying at detection |
| `live_detections` | every channel's sighting of every fill (raw payload included) |
| `live_spot_snaps` | per fill and price source: value at the trade, at detection, staleness, shift |
| `live_books` | both tokens' books at detection (`ws` local book and `rest` snapshot), top 10 levels |
| `live_spot` | per-second values of every price source for the whole run |
| `live_window_close` | Binance vs Chainlink at every close of the 5m/15m/4h windows, the winner each one implies, and the official strike/final/winner from Gamma |
| `live_health` | once a minute: connection state, message counts and staleness of every feed |
| `markets` | Gamma metadata of the windows seen live |
