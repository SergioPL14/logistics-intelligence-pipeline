"""Pure delivery-risk-score computation.

The MVP uses a weighted sum of normalized 0-1 sub-scores. See the README
"Risk Score" section for the rationale and tuning notes.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.models import DeliveryRiskScore, RiskBand

WEIGHT_CAP_G = 30_000.0
DIESEL_BASE_USD = 3.0
DIESEL_RANGE_USD = 3.0
WIND_CAP_MS = 20.0
PRECIP_CAP_MM = 10.0

CONDITION_RISK: dict[str, float] = {
    "Clear": 0.0,
    "Clouds": 0.0,
    "Mist": 0.5,
    "Fog": 0.5,
    "Haze": 0.5,
    "Drizzle": 0.5,
    "Rain": 0.5,
    "Snow": 1.0,
    "Thunderstorm": 1.0,
}
DEFAULT_CONDITION_RISK = 0.3

SUBSCORE_WEIGHTS: dict[str, float] = {
    "weight": 0.30,
    "diesel": 0.25,
    "condition": 0.25,
    "wind": 0.10,
    "precip": 0.10,
}

BAND_LOW_MAX = 0.33
BAND_MEDIUM_MAX = 0.66


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _condition_score(condition: str) -> float:
    return CONDITION_RISK.get(condition, DEFAULT_CONDITION_RISK)


def _band(score: float) -> RiskBand:
    if score < BAND_LOW_MAX:
        return "low"
    if score < BAND_MEDIUM_MAX:
        return "medium"
    return "high"


def compute_risk(
    *,
    order_id: str,
    order_weight_g: float | None,
    diesel_price_usd_per_gallon: float,
    weather_condition: str,
    wind_speed_ms: float,
    precipitation_mm: float,
    computed_at: datetime | None = None,
) -> DeliveryRiskScore:
    """Compute a 0.0-1.0 delivery risk score from order/diesel/weather inputs.

    Treats a missing ``order_weight_g`` as zero (no contribution to the
    weight sub-score) — keeps orders without product weights scoreable
    rather than dropping them.
    """
    weight_score = _clip01((order_weight_g or 0.0) / WEIGHT_CAP_G)
    diesel_score = _clip01((diesel_price_usd_per_gallon - DIESEL_BASE_USD) / DIESEL_RANGE_USD)
    cond_score = _condition_score(weather_condition)
    wind_score = _clip01(wind_speed_ms / WIND_CAP_MS)
    precip_score = _clip01(precipitation_mm / PRECIP_CAP_MM)

    score = (
        SUBSCORE_WEIGHTS["weight"] * weight_score
        + SUBSCORE_WEIGHTS["diesel"] * diesel_score
        + SUBSCORE_WEIGHTS["condition"] * cond_score
        + SUBSCORE_WEIGHTS["wind"] * wind_score
        + SUBSCORE_WEIGHTS["precip"] * precip_score
    )
    score = round(_clip01(score), 4)

    return DeliveryRiskScore(
        order_id=order_id,
        score=score,
        risk_band=_band(score),
        diesel_price_usd_per_gallon=diesel_price_usd_per_gallon,
        weather_condition=weather_condition,
        computed_at=computed_at or datetime.now(UTC),
    )
