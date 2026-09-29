# bosona-tracker

Read-only collection and analysis of Polymarket trader **@bosona**'s trades
(proxy wallet `0xc2ad03f79ca3f3c17d8c7de2612ce0c89b7d40ed`).
It uses only public sources: Polymarket Data API v2, Gamma, the public CLOB and Polygon RPC. No orders, keys or wallets.

- Stage 0 reconnaissance report: [`docs/stage0-recon.md`](docs/stage0-recon.md)
- Stage 1 report: [`docs/stage1-history.md`](docs/stage1-history.md)
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
.venv/bin/pytest                          # parsing and dedup tests
```

Logs go to `logs/bosona.log` and stderr. Settings live in `config.yaml`, and `BOSONA_*` environment variables override them.

## How the history is built

| Table | Source | Key |
|---|---|---|
| `trades` | `/v2/activity?user=…` rows with `type=TRADE` (one row per fill) | `tx:token:side:size_raw:price_raw:seq` |
| `taker_fills` | `/v2/trades?user=…&taker_only=true` (fills where he was the taker) | same key as `trades` |
| `activity` | every other `/v2/activity` row: MERGE, REDEEM (per outcome), REWARD, MAKER_REBATE, TAKER_REBATE, … | `tx:type:condition:token:size_raw:usdc_raw:seq` |
| `markets`, `resolutions` | Gamma `/markets?condition_ids=…&closed=true` with the parent event embedded | `condition_id` |
| `pnl_daily` | `/v2/user-pnl` (reconciliation only) | day |
| `market_context` | stage 2 | `trade_uid` |

How the columns are derived:

- **Role.** `taker` if the fill appears in the taker feed, otherwise `maker`. This was checked on-chain against `OrderFilled` (stage 0).
- **Fee** (`fee_usdc`). For taker fills it is `size × fee_rate × p × (1 − p)` rounded to 5 dp; maker fills pay 0 (docs.polymarket.com/trading/fees).
- **`seq`.** Two identical rows in one transaction are two real fills, and `seq` keeps them apart.
  All rows of a transaction share one block timestamp, so the keys are stable across runs.
- **Incremental runs.** They re-read `sync.overlap_s` before the last synced second and skip the newest `sync.safety_lag_s`.
  Inserts are `INSERT OR IGNORE`, so re-running never duplicates rows.
