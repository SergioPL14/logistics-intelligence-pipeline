"""Delivery risk score contract — gold-layer output of the risk model."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from src.models._base import LogisticsModel

RiskBand = Literal["low", "medium", "high"]


class DeliveryRiskScore(LogisticsModel):
    """Computed delivery risk for a single order.

    The score blends order weight, diesel cost at purchase time, and weather
    conditions at the destination zip into a 0.0-1.0 risk indicator.

    Attributes:
        order_id: Olist order UUID this score belongs to.
        score: Risk score in [0.0, 1.0]; higher means riskier delivery.
        risk_band: Discretized score bucket (``low`` / ``medium`` / ``high``).
        diesel_price_usd_per_gallon: Diesel price snapshot used for the score.
        weather_condition: OpenWeather condition string at destination.
        computed_at: When the score was generated (UTC).
    """

    order_id: str = Field(min_length=1)
    score: float = Field(ge=0.0, le=1.0)
    risk_band: RiskBand
    diesel_price_usd_per_gallon: float = Field(gt=0)
    weather_condition: str = Field(min_length=1)
    computed_at: datetime
