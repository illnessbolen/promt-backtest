"""Tables of the stage 5 comparison (docs/stage5-data.md) from the window-level backtest results."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from bosona.strategy import md_table

WINDOWS_PER_DAY = {"5m": 288, "15m": 96}


def ratio_se(pnl: pd.Series, usdc: pd.Series) -> float | None:
    """Standard error of sum(pnl) / sum(usdc) with windows as clusters (linearized ratio estimator)."""
    n, total = len(pnl), float(usdc.sum())
    if n < 2 or total <= 0:
        return None
    resid = pnl - pnl.sum() / total * usdc
    return math.sqrt(n / (n - 1) * float((resid**2).sum())) / total


def summarize(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, g in df.groupby(by, sort=False):
        key = key if isinstance(key, tuple) else (key,)
        traded = g[g["fills"] > 0]
        usdc, pnl = float(g["usdc"].sum()), float(g["pnl"].sum())
        se = ratio_se(g["pnl"], g["usdc"])
        per_w = g["pnl"]
        rows.append({
            **dict(zip(by, key, strict=True)),
            "windows": len(g), "traded": len(traded), "fills": int(g["fills"].sum()),
            "maker_share": round(float(g["maker_fills"].sum() / max(1, g["fills"].sum())), 3),
            "usdc": round(usdc, 0), "fees": round(float(g["fees"].sum()), 0), "pnl": round(pnl, 0),
            "ev_per_usd": round(pnl / usdc, 4) if usdc else None, "ev_se": round(se, 4) if se is not None else None,
            "pnl_per_window": round(float(per_w.mean()), 2),
            "pnl_per_window_se": round(float(per_w.std(ddof=1) / math.sqrt(len(per_w))), 2) if len(per_w) > 1 else None,
            "pair_share": round(float(2 * g["paired"].sum() / max(1e-9, (g["shares_up"] + g["shares_down"]).sum())), 3),
            "copy_premium_c": (round(float(g["copy_premium_usdc"].sum() / g["copy_shares"].sum() * 100), 2)
                               if g["copy_shares"].sum() > 0 else None),
        })
    return pd.DataFrame(rows)


def per_day(df: pd.DataFrame) -> pd.DataFrame:
    """Expected PnL per day if the variant ran in every window of the series (sample mean x windows per day)."""
    rows = []
    for (variant, queue), g in df.groupby(["variant", "queue"], sort=False):
        total, var = 0.0, 0.0
        for tf, x in g.groupby("timeframe"):
            n = WINDOWS_PER_DAY.get(tf)
            if n is None or len(x) < 2:
                continue
            total += n * float(x["pnl"].mean())
            var += (n * float(x["pnl"].std(ddof=1)) / math.sqrt(len(x))) ** 2
        rows.append({"variant": variant, "queue": queue, "pnl_per_day": round(total, 0), "se": round(math.sqrt(var), 0)})
    return pd.DataFrame(rows)


def render(df: pd.DataFrame, meta: dict[str, Any]) -> str:
    his = df[df["his_fills"] > 0]
    parts = [
        "# Этап 5: таблицы бэктеста (генерируются `python -m bosona backtest`)\n",
        (f"Окон в выборке: {df['key'].nunique()} (5m: {df[df.timeframe == '5m']['key'].nunique()}, "
        f"15m: {df[df.timeframe == '15m']['key'].nunique()}), из них с его сделками: {his['key'].nunique()}."),
        f"Параметры исполнения: {meta.get('ep')}. Профиль: {meta.get('profile')}.\n",
        "EV на $1 = PnL / вложенные USDC (с taker-комиссией); ± — стандартная ошибка с кластеризацией по окну.",
        ("`queue`: допущение об очереди перед нашей заявкой (front — никого, touch — медианный объём у касания, "
        "through — исполнение только при торговле ниже нашей цены); `-` — у варианта нет maker-заявок.\n"),
        "## Все окна выборки\n", md_table(summarize(df, ["variant", "queue"])),
        "## Окна, где торговал он (сравнение (a) / (b) на одних и тех же окнах)\n",
        md_table(summarize(his, ["variant", "queue"])),
        "## По таймфрейму\n", md_table(summarize(df, ["variant", "queue", "timeframe"])),
        "## Ожидаемый PnL в день, если торговать каждое окно BTC 5m и 15m\n", md_table(per_day(df)),
    ]
    df = df.assign(month=pd.to_datetime(df["start"], unit="s").dt.strftime("%Y-%m"))
    parts += ["## По месяцам\n", md_table(summarize(df, ["variant", "queue", "month"]))]
    return "\n".join(parts)


def window_ev(df: pd.DataFrame) -> pd.Series:
    """EV per $1 of each window (diagnostics)."""
    return (df["pnl"] / df["usdc"].replace(0, np.nan)).rename("ev")
