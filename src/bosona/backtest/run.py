"""Run the variants of the stage 5 comparison over the sampled windows (parallel over windows).

Variants (one window is loaded once and every variant is simulated on it):
  a_ideal            his own fills, as they happened (upper bound of copying)
  b_copy_taker       buy what he bought once his fill is visible (+2.0 s) and our order lands (+0.3 s), at the
                     ask then, up to his price + 5c, taker fee
  b_copy_taker_any   the same without the price cap
  b_copy_maker       rest a bid at his price instead (queue behind what is left at that level)
  c_rules            own strategy: two-sided quotes at the touch below fair value + hedge (strategies/rules.py)
  c_rules_taker      c_rules + directional taker entries
  c_taker_only       directional taker entries alone (no quotes, no hedge)
Maker variants are run under each queue assumption (front / touch / through), taker ones once.
"""

from __future__ import annotations

import logging
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pandas as pd

from bosona.backtest.data import iter_windows, window_list
from bosona.backtest.engine import ExecParams, simulate
from bosona.config import Config
from bosona.spot import SpotCache
from bosona.strategies.copy import CopyParams, DelayedCopy
from bosona.strategies.profiles import RiskProfile
from bosona.strategies.rules import BosonaRules, RulesParams

log = logging.getLogger(__name__)

VARIANTS: dict[str, dict[str, Any]] = {
    "a_ideal": {"kind": "ideal", "maker": False},
    "b_copy_taker": {"kind": "copy", "params": CopyParams("taker", max_slip=0.05), "maker": False},
    "b_copy_taker_any": {"kind": "copy", "params": CopyParams("taker", max_slip=1.0), "maker": False},
    "b_copy_maker": {"kind": "copy", "params": CopyParams("maker"), "maker": True},
    "c_rules": {"kind": "rules", "params": RulesParams(), "maker": True, "profile": True},
    "c_rules_taker": {"kind": "rules", "params": RulesParams(taker=True), "maker": True, "profile": True},
    "c_taker_only": {"kind": "rules", "params": RulesParams(quote=False, hedge=False, taker=True), "maker": False,
                     "profile": True},
}
QUEUE_MODES = ("front", "touch", "through")


def _strategy(spec: dict[str, Any]):
    if spec["kind"] == "copy":
        return DelayedCopy(spec["params"])
    if spec["kind"] == "rules":
        return BosonaRules(spec["params"])
    return None


def _worker(job: dict[str, Any]) -> list[dict[str, Any]]:
    tape = sqlite3.connect(job["tape_db"])
    bos = sqlite3.connect(job["db"])
    cache = SpotCache(Path(job["spot_dir"]))
    rows = pd.DataFrame(job["rows"])
    base = ExecParams(**job["ep"])
    profile = RiskProfile.of(job["profile"], job["bankroll"]) if job["profile"] else None
    out: list[dict[str, Any]] = []
    strategies = {name: _strategy(spec) for name, spec in job["variants"].items()}
    for wd in iter_windows(tape, bos, cache, job["user"], rows):
        for name, spec in job["variants"].items():
            modes = job["queue_modes"] if spec.get("maker") else ("-",)
            for qm in modes:
                ep = base if qm == "-" else replace(base, queue_mode=qm)
                r = simulate(wd, strategies[name], ep, profile if spec.get("profile") else None, name,
                             "ideal" if spec["kind"] == "ideal" else "strategy")
                d = r.summary()
                d.update(queue=qm, his_fills=len(wd.his), his_usdc=sum(f.usdc for f in wd.his))
                out.append(d)
    return out


def run_backtest(cfg: Config, variants: list[str] | None = None, queue_modes: tuple[str, ...] = QUEUE_MODES,
                 ep: ExecParams | None = None, profile: str | None = "moderate", bankroll: float = 10_000.0,
                 samples: tuple[str, ...] = ("random",), timeframe: str | None = None, limit: int | None = None,
                 workers: int = 4, rules: dict[str, Any] | None = None) -> pd.DataFrame:
    """`rules`: RulesParams overrides for the own-strategy variants (c_*)."""
    tape = sqlite3.connect(cfg.tape_db_path)
    rows = window_list(tape, timeframe, samples)
    if limit:
        rows = rows.sample(min(limit, len(rows)), random_state=1).sort_values("window_start_ts")
    specs = {k: VARIANTS[k] for k in (variants or list(VARIANTS))}
    if rules:
        specs = {k: {**v, "params": replace(v["params"], **rules)} if v["kind"] == "rules" else v
                 for k, v in specs.items()}
    ep = ep or ExecParams()
    chunks = [rows.iloc[i::workers * 4] for i in range(workers * 4)]
    jobs = [{"tape_db": str(cfg.tape_db_path), "db": str(cfg.db_path), "spot_dir": str(cfg.spot_cache_dir),
             "user": cfg.user, "rows": c.to_dict("records"), "ep": asdict(ep), "variants": specs,
             "queue_modes": queue_modes, "profile": profile, "bankroll": bankroll} for c in chunks if len(c)]
    log.info("backtest: %d windows, variants %s, queue modes %s, rules %s, %d jobs", len(rows), list(specs),
             queue_modes, rules or "default", len(jobs))
    out: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for i, part in enumerate(pool.map(_worker, jobs)):
            out += part
            log.info("backtest: %d/%d jobs done", i + 1, len(jobs))
    return pd.DataFrame(out)
