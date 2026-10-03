"""Grid of the rules strategy over updown recordings (`python -m bosona updown-grid`).

A grid is a list of points, one per line: RulesParams overrides `k=v` separated by spaces, plus two host settings,
`react_ms` (re-quote on Binance moves at most every N ms; -1 = only the 1 s timer) and `cancel_ms` (a cancel
reaches the venue this much later). `default` is the strategy as it is; `#` starts a comment.

The recording is parsed once per worker process: every worker feeds the same ticks to several strategy hosts
(one per grid point, each with its own paper exchange; see updown.replay_many) and returns their per-window
summaries. The outcomes are settled afterwards (tape.db / Gamma, plus the @outcome frames of the recording), and
his own fills in the same windows (bosona.db, if synced over the recording period) make the "(a)" row.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import sqlite3
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from bosona.backtest.report import ratio_se
from bosona.strategies.profiles import RiskProfile
from bosona.strategies.rules import param_overrides

log = logging.getLogger(__name__)

# the stage 5 tape grid (docs/stage5-backtest.md) plus the reaction speed
DEFAULT_GRID = """
default
react_ms=-1
react_ms=250
margin=0
margin=0.05
margin=0.10
anchor=fair
anchor=fair margin=0.05
improve_ticks=1
hedge=false
hedge_move_bps=6
start_after_s=60
stop_before_close_s=60
max_net_shares=150
size_shares=50
max_book_age_s=2
"""
HOST_KEYS = ("react_ms", "cancel_ms")


@dataclass(frozen=True)
class GridPoint:
    label: str
    rules: dict[str, Any] = field(default_factory=dict)
    react_ms: float | None = None          # None: the command's --react-ms
    cancel_ms: float | None = None         # None: 100 ms


def parse_grid(lines: Iterable[str]) -> list[GridPoint]:
    out: list[GridPoint] = []
    for raw in lines:
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        host: dict[str, float] = {}
        rules: list[str] = []
        for item in ([] if line == "default" else line.split()):
            k, sep, v = item.partition("=")
            if k in HOST_KEYS:
                if not sep:
                    raise ValueError(f"grid point {line!r}: {k} needs a value")
                host[k] = float(v)
            else:
                rules.append(item)
        out.append(GridPoint(" ".join(line.split()), param_overrides(rules), host.get("react_ms"), host.get("cancel_ms")))
    labels = [p.label for p in out]
    if not out or len(set(labels)) != len(labels):
        raise ValueError(f"the grid needs at least one point and no duplicates: {labels}")
    return out


def _react_s(p: GridPoint, default_ms: float) -> float | None:
    ms = default_ms if p.react_ms is None else p.react_ms
    return None if ms < 0 else ms / 1000.0


def _worker(job: dict[str, Any]) -> dict[str, Any]:
    from bosona.strategies.rules import BosonaRules, RulesParams
    from bosona.updown import HostSpec, import_updown, replay_many

    U = import_updown(job["updown_path"])
    points: list[GridPoint] = job["points"]
    specs = [HostSpec(p.label, (lambda r=p.rules: BosonaRules(RulesParams(**r))), _react_s(p, job["react_ms"]),
                      0.1 if p.cancel_ms is None else p.cancel_ms / 1000.0) for p in points]
    hosts, winners, span = replay_many(U, job["paths"], specs, RiskProfile(**job["profile"]),
                                       settings=job["settings"])
    out = {"snapshots": {}, "evaluations": {}, "winners": winners, "span": span, "fair": {}}
    for i, (p, h) in enumerate(zip(points, hosts, strict=True)):
        snap = h.snapshot(keep_fair=job["keep_fair"] and i == 0)
        if job["keep_fair"] and i == 0:
            out["fair"] = {d["slug"]: (d.pop("fair_t"), d.pop("fair_v")) for d in snap}
        out["snapshots"][p.label] = snap
        out["evaluations"][p.label] = h.evaluations
    log.info("grid worker done: %s", [p.label for p in points])
    return out


def run_pass(updown_path: str | Path | None, paths: list[str], points: list[GridPoint], profile: RiskProfile,
             settings: dict[str, Any], react_ms: float = 50.0, workers: int = 4) -> dict[str, Any]:
    """Replay the recording with every grid point; points are spread over `workers` processes, each parsing the
    recording once. Returns per-point window snapshots, the fair value series of each window (first point), the
    recorded winners and the time span."""
    n = max(1, min(workers, len(points)))
    groups = [points[i::n] for i in range(n)]
    jobs = [{"updown_path": str(updown_path) if updown_path else None, "paths": list(paths), "points": g,
             "profile": dataclasses.asdict(profile), "settings": settings, "react_ms": react_ms, "keep_fair": i == 0}
            for i, g in enumerate(groups)]
    log.info("updown grid: %d points in %d processes over %d path(s)", len(points), n, len(paths))
    merged: dict[str, Any] = {"snapshots": {}, "evaluations": {}, "winners": {}, "fair": {}, "span": None}
    with ProcessPoolExecutor(max_workers=n) as pool:
        for part in pool.map(_worker, jobs):
            merged["snapshots"].update(part["snapshots"])
            merged["evaluations"].update(part["evaluations"])
            merged["winners"].update(part["winners"])
            merged["fair"].update(part["fair"])
            merged["span"] = merged["span"] or part["span"]
    return merged


def grid_frames(res: dict[str, Any], payouts: dict[str, tuple[float, float]], points: list[GridPoint],
                react_ms: float, bos_conn: sqlite3.Connection | None) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    """Settled window rows of every grid point and of his own fills (None without bosona.db)."""
    from bosona.updown import his_rows, window_rows

    first, last = res["span"] or (0.0, 0.0)
    frames = []
    for i, p in enumerate(points):
        rows = pd.DataFrame(window_rows(res["snapshots"][p.label], payouts))
        if rows.empty:
            continue
        ms = react_ms if p.react_ms is None else p.react_ms
        rows.insert(0, "config", p.label)
        rows.insert(1, "order", i)
        rows.insert(2, "react_ms", ms)
        frames.append(rows)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not df.empty:
        df["full"] = (df["start"] >= first) & (df["end"] <= last)
    his = None
    if bos_conn is not None and not df.empty:
        meta = df.drop_duplicates("slug").set_index("slug")[["label", "start", "end", "full"]]
        rows = his_rows(bos_conn, list(meta.index), payouts, res["fair"])
        if rows:
            his = pd.DataFrame(rows).join(meta, on="slug")
    return df, his


def summarize_grid(df: pd.DataFrame, his: pd.DataFrame | None, all_windows: bool = False) -> pd.DataFrame:
    """One row per grid point (and "он (a)" first): windows that resolved and, unless all_windows, that the
    recording covers from open to close."""

    def keep(x: pd.DataFrame) -> pd.DataFrame:
        x = x[x["resolved"]]
        return x if all_windows else x[x["full"]]

    def cents(num: float, den: float) -> float | None:
        return round(num / den * 100, 2) if den > 0 else None

    def row(name: str, react: Any, g: pd.DataFrame) -> dict[str, Any]:
        usdc, pnl, fills = float(g["usdc"].sum()), float(g["pnl"].sum()), int(g["fills"].sum())
        se = (ratio_se(g["pnl"].astype(float), g["usdc"].astype(float))
              if int((g["usdc"] > 0).sum()) >= 2 else None)                 # one traded window has no spread
        return {
            "config": name, "react_ms": react, "windows": len(g), "traded": int((g["fills"] > 0).sum()),
            "fills": fills, "maker_share": round(float(g["maker_fills"].sum()) / fills, 3) if fills else None,
            "usdc": round(usdc), "pnl": round(pnl), "ev_per_usd": pnl / usdc if usdc else None,
            "ev_se": se,
            "pair_cost_c": cents(float(g["pair_cost_usdc"].sum()), float(g["paired"].sum())),
            "maker_edge_c": cents(float(g["maker_edge"].sum()), float(g["maker_fair_shares"].sum())),
            "maker_10s_c": cents(float(g["maker_mark"].sum()), float(g["maker_fair_shares"].sum())),
            "maker_final_c": cents(float(g["maker_real"].sum()), float(g["maker_shares"].sum())),
            "taker_final_c": cents(float(g["taker_real"].sum()), float(g["taker_shares"].sum())),
        }

    out = []
    if his is not None:
        h = keep(his)
        if len(h):
            out.append(row("он (a)", "—", h))
    for (_, name), g in keep(df).groupby(["order", "config"], sort=True):
        out.append(row(name, g["react_ms"].iloc[0], g))
    return pd.DataFrame(out)


def _pct(v: Any) -> str:
    return "—" if v is None or (isinstance(v, float) and not math.isfinite(v)) else f"{v * 100:+.2f}%".replace("-", "−")


def _c(v: Any) -> str:
    return "—" if v is None or (isinstance(v, float) and not math.isfinite(v)) else f"{v:+.1f}".replace("-", "−")


def _react(v: Any) -> str:
    if isinstance(v, int | float):
        return "таймер 1 с" if v < 0 else f"{v:g}"
    return str(v)


def render_grid(summary: pd.DataFrame, meta: dict[str, Any]) -> str:
    lines = [
        "# Сетка правил на записи updown (генерирует `python -m bosona updown-grid`)\n",
        (f"Запись: {meta['span']}, файлов: {meta['files']}. Окна: {meta['windows']}. "
         f"Профиль: {meta['profile']}. Реакция по умолчанию: {_react(meta['react_ms'])} мс.\n"),
        ("EV на $1 = PnL на резолве / вложенные USDC с комиссией; ± — стандартная ошибка с кластеризацией по окну. "
         "Край maker-сделки — справедливая цена купленной стороны минус цена, ¢ на акцию: в момент сделки, "
         "через 10 с и на резолве. Paper-исполнение не забирает ликвидность: оборот и PnL — оценка сверху.\n"),
        ("| параметры | реакция, мс | окон | сделок | maker | оборот | PnL | EV на $1 | цена пары | "
         "maker: в момент / 10 с / резолв, ¢ | taker на резолве, ¢ |"),
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in summary.itertuples(index=False):
        ev = _pct(r.ev_per_usd) + (f" ± {r.ev_se * 100:.2f}%" if r.ev_se is not None and math.isfinite(r.ev_se) else "")
        pair = "—" if r.pair_cost_c is None else f"{r.pair_cost_c:.1f}¢"
        maker = f"{_c(r.maker_edge_c)} / {_c(r.maker_10s_c)} / {_c(r.maker_final_c)}"
        share = "—" if r.maker_share is None else f"{r.maker_share * 100:.0f}%"
        lines.append(f"| {r.config} | {_react(r.react_ms)} | {r.windows} | {_int(r.fills)} | {share} | "
                     f"${_int(r.usdc)} | {_int(r.pnl, sign=True)} | {ev} | {pair} | {maker} | {_c(r.taker_final_c)} |")
    return "\n".join(lines) + "\n"


def _int(v: Any, sign: bool = False) -> str:
    return format(int(v), "+," if sign else ",").replace(",", " ").replace("-", "−")
