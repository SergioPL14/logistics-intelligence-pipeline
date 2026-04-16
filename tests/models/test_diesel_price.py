"""Unit tests for the DieselPrice contract."""
from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from src.models import DieselPrice


def test_diesel_price_happy_path() -> None:
    price = DieselPrice(
        period=date(2026, 4, 13),
        region="NUS",
        price_usd_per_gallon=3.85,
        series_id="PET.EMD_EPD2D_PTE_NUS_DPG.W",
    )
    assert price.region == "NUS"
    assert price.price_usd_per_gallon == 3.85


@pytest.mark.parametrize("bad_price", [0.0, -1.0])
def test_diesel_price_rejects_non_positive(bad_price: float) -> None:
    with pytest.raises(ValidationError):
        DieselPrice(
            period=date(2026, 4, 13),
            region="NUS",
            price_usd_per_gallon=bad_price,
            series_id="PET.X.W",
        )


def test_diesel_price_rejects_empty_strings() -> None:
    with pytest.raises(ValidationError):
        DieselPrice(
            period=date(2026, 4, 13),
            region="",
            price_usd_per_gallon=3.0,
            series_id="PET.X.W",
        )


def test_diesel_price_is_frozen() -> None:
    price = DieselPrice(
        period=date(2026, 4, 13),
        region="NUS",
        price_usd_per_gallon=3.0,
        series_id="PET.X.W",
    )
    with pytest.raises(ValidationError):
        price.region = "R10"  # type: ignore[misc]
