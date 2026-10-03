"""Fair probability that an Up/Down window resolves Up, from the spot in resolution-source units.

The market pays 1 if the resolution price at the close is >= the strike K. With a driftless log-price and
volatility sigma per sqrt(second), standing at tau seconds before the close:

  * spot-settled windows (chainlink_spot, Binance candles): the close is S_T, variance sigma^2 * tau;
  * TWAP-settled windows (chainlink_twap30 / twap60, lookback L): the close is the mean of S over the last L
    seconds. Before that interval starts (tau >= L) its variance is sigma^2 * (tau - L) + sigma^2 * L / 3;
    inside it the elapsed part is known (mean A over L - tau seconds) and only the rest is random:
        mean = ((L - tau) * A + tau * S_t) / L,   variance = sigma^2 * tau^3 / (3 L^2)

    P(Up) = F((ln(mean / K) - v / 2) / sqrt(v + eta^2))

F is a unit-variance Student-t (fat tails; the stage 4 Gaussian model was overconfident) or the normal CDF.
eta is the oracle noise: how far our estimate of the resolution price can be from the real one (stage 2 proxy
error ~0.3-0.9 bps). The same formula as updown's model/pricing.py, plus the TWAP settlement.
"""

from __future__ import annotations

import math

import numpy as np

TWAP_LOOKBACK_S = {"chainlink_twap30": 30, "chainlink_twap60": 60}


def t5_cdf(x: float) -> float:
    """CDF of Student-t with 5 degrees of freedom (closed form for odd dof)."""
    th = math.atan(x / math.sqrt(5.0))
    c = math.cos(th)
    return 0.5 + (th + math.sin(th) * c * (1.0 + 2.0 / 3.0 * c * c)) / math.pi


def unit_t5_cdf(d: float) -> float:
    """P(Z <= d) for a Student-t(5) scaled to unit variance."""
    return t5_cdf(d * math.sqrt(5.0 / 3.0))


def norm_cdf(d: float) -> float:
    return 0.5 * (1.0 + math.erf(d / math.sqrt(2.0)))


def settle_moments(spot: float, tau: float, sigma: float, regime: str | None,
                   twap_avg: float | None = None) -> tuple[float, float]:
    """(expected settlement price, variance of its log) given the spot and, inside a TWAP interval, the mean of
    the interval so far."""
    tau = max(0.0, tau)
    lookback = TWAP_LOOKBACK_S.get(regime or "")
    if not lookback:
        return spot, sigma * sigma * tau
    if tau >= lookback:
        return spot, sigma * sigma * (tau - lookback + lookback / 3.0)
    known = twap_avg if twap_avg is not None else spot
    mean = ((lookback - tau) * known + tau * spot) / lookback
    return mean, sigma * sigma * tau ** 3 / (3.0 * lookback * lookback)


def fair_up(spot: float, strike: float, tau: float, sigma: float, regime: str | None = None,
            twap_avg: float | None = None, oracle_noise: float = 1e-4, fat_tails: bool = True) -> float:
    """P(close >= strike). spot and strike in resolution-source units; sigma in log units per sqrt(s)."""
    if not (spot > 0 and strike > 0):
        return float("nan")
    mean, v = settle_moments(spot, tau, sigma, regime, twap_avg)
    sd = math.sqrt(v + oracle_noise * oracle_noise)
    if sd <= 0:
        return 1.0 if mean >= strike else 0.0
    d = (math.log(mean / strike) - v / 2.0) / sd
    return unit_t5_cdf(d) if fat_tails else norm_cdf(d)


def taker_fee(price: float, rate: float) -> float:
    """Polymarket taker fee per share: rate * p * (1 - p) (stage 0; makers pay nothing)."""
    return rate * price * (1.0 - price)


def fair_up_array(spot: np.ndarray, strike: float, tau: np.ndarray, sigma: np.ndarray, regime: str | None = None,
                  twap_avg: np.ndarray | None = None, oracle_noise: float = 1e-4) -> np.ndarray:
    """Vectorized fair_up (fat tails) over a grid of times; same formulas."""
    spot, tau, sigma = (np.asarray(x, dtype=float) for x in (spot, tau, sigma))
    tau = np.clip(tau, 0.0, None)
    lookback = TWAP_LOOKBACK_S.get(regime or "")
    s2 = sigma * sigma
    if not lookback:
        mean, v = spot, s2 * tau
    else:
        known = spot if twap_avg is None else np.where(np.isnan(twap_avg), spot, twap_avg)
        inside = tau < lookback
        mean = np.where(inside, ((lookback - tau) * known + tau * spot) / lookback, spot)
        v = np.where(inside, s2 * tau ** 3 / (3.0 * lookback * lookback), s2 * (tau - lookback + lookback / 3.0))
    sd = np.sqrt(v + oracle_noise * oracle_noise)
    with np.errstate(divide="ignore", invalid="ignore"):
        d = (np.log(mean / strike) - v / 2.0) / sd
    th = np.arctan(d * np.sqrt(5.0 / 3.0) / np.sqrt(5.0))
    c = np.cos(th)
    return 0.5 + (th + np.sin(th) * c * (1.0 + 2.0 / 3.0 * c * c)) / np.pi
