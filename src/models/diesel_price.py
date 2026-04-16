"""Diesel price contract — bronze-layer output of the EIA extractor."""
from __future__ import annotations

from datetime import date

from pydantic import Field

from src.models._base import LogisticsModel


class DieselPrice(LogisticsModel):
    """A single weekly retail diesel price observation from the EIA API.

    Attributes:
        period: Week-ending date the price applies to.
        region: EIA region code (e.g. ``"NUS"`` for U.S. average,
            or PADD region codes such as ``"R10"``).
        price_usd_per_gallon: Retail diesel price, USD per gallon (>0).
        series_id: EIA series identifier (e.g. ``"PET.EMD_EPD2D_PTE_NUS_DPG.W"``).
    """

    period: date
    region: str = Field(min_length=1)
    price_usd_per_gallon: float = Field(gt=0)
    series_id: str = Field(min_length=1)
