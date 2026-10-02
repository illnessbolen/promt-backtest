"""Tables of the stage 5 comparison (docs/stage5-data.md) from the window-level backtest results."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from bosona.strategy import md_table

WINDOWS_PER_DAY = {"5m": 288, "15m": 96}


def _int(v: float) -> str:
    """Whole number with thin grouping and a real minus sign, as md_table prints money."""
    return f"{round(v):,}".replace(",", " ").replace("-", "−")


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
            "copy_premium_c": (round(float(g["copy_premium_usdc"].sum() / g["copy_shares"].sum() * 100), 2) + 0.0
                               if g["copy_shares"].sum() > 0 else None),
        })
    return pd.DataFrame(rows)


def decompose(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Where the PnL comes from (engine WindowSim._diag): pairs vs the unpaired rest, and per role the edge of the
    fills against the model in cents per share at the fill, MARKOUT_S later and at resolution."""
    rows: list[dict[str, Any]] = []

    def cents(num: float, den: float) -> float | None:
        return round(num / den * 100, 2) if den > 0 else None

    for key, g in df.groupby(by, sort=False):
        key = key if isinstance(key, tuple) else (key,)
        paired = float(g["paired"].sum())
        row: dict[str, Any] = {
            **dict(zip(by, key, strict=True)),
            "pnl": round(float(g["pnl"].sum()), 0),
            "pair_cost_c": cents(float(g["pair_cost_usdc"].sum()), paired),
            "pair_pnl": _int(g["pair_pnl"].sum()), "unpaired_pnl": _int(g["unpaired_pnl"].sum()),
        }
        for role in ("maker", "taker"):
            fs = float(g[f"{role}_fair_shares"].sum())
            row.update({
                f"{role}_shares": _int(g[f"{role}_shares"].sum()),
                f"{role}_edge_c": cents(float(g[f"{role}_edge"].sum()), fs),
                f"{role}_10s_c": cents(float(g[f"{role}_mark"].sum()), fs),
                f"{role}_final_c": cents(float(g[f"{role}_real"].sum()), float(g[f"{role}_shares"].sum())),
            })
        rows.append(row)
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
        rows.append({"variant": variant, "queue": queue, "pnl_per_day": _int(total), "se": _int(math.sqrt(var))})
    return pd.DataFrame(rows)


def render(df: pd.DataFrame, meta: dict[str, Any]) -> str:
    his = df[df["his_fills"] > 0]
    parts = [
        "# Этап 5: таблицы бэктеста (генерируются `python -m bosona backtest`)\n",
        (f"Окон в выборке: {df['key'].nunique()} (5m: {df[df.timeframe == '5m']['key'].nunique()}, "
        f"15m: {df[df.timeframe == '15m']['key'].nunique()}), из них с его сделками: {his['key'].nunique()}."),
        (f"Параметры исполнения: {meta.get('ep')}. Профиль: {meta.get('profile')}. "
         f"Параметры правил (c_*): {meta.get('rules', 'по умолчанию')}.\n"),
        "EV на $1 = PnL / вложенные USDC (с taker-комиссией); ± — стандартная ошибка с кластеризацией по окну.",
        ("`queue`: допущение об очереди перед нашей заявкой (front — никого, touch — медианный объём у касания, "
        "through — исполнение только при торговле ниже нашей цены); `-` — у варианта нет maker-заявок.\n"),
        "## Все окна выборки\n", md_table(summarize(df, ["variant", "queue"]), 2),
        "## Окна, где торговал он (сравнение (a) / (b) на одних и тех же окнах)\n",
        md_table(summarize(his, ["variant", "queue"]), 2),
        "## Откуда PnL (окна, где торговал он)\n",
        ("Пары — совпавшие акции Up и Down по средней цене каждой стороны (с комиссией), `pair_cost_c` — средняя цена "
         "пары в центах; `unpaired_pnl` — остальное (направленная часть). Край сделки — справедливая цена купленного "
         "исхода по модели минус цена и комиссия, ¢ на акцию: в момент сделки (`edge`), через 10 с (`10s`) и на "
         "резолве (`final`, выплата; в сумме даёт PnL).\n"),
        md_table(decompose(his, ["variant", "queue"]), 2),
        "## По таймфрейму\n", md_table(summarize(df, ["variant", "queue", "timeframe"]), 2),
        "## Ожидаемый PnL в день, если торговать каждое окно BTC 5m и 15m\n", md_table(per_day(df)),
    ]
    df = df.assign(month=pd.to_datetime(df["start"], unit="s").dt.strftime("%Y-%m"))
    parts += ["## По месяцам\n", md_table(summarize(df, ["variant", "queue", "month"]), 2)]
    return "\n".join(parts)


def window_ev(df: pd.DataFrame) -> pd.Series:
    """EV per $1 of each window (diagnostics)."""
    return (df["pnl"] / df["usdc"].replace(0, np.nan)).rename("ev")
