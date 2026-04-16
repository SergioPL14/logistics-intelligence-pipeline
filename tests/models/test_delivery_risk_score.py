"""Unit tests for the DeliveryRiskScore contract."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.models import DeliveryRiskScore


def _base() -> dict[str, object]:
    return {
        "order_id": "abc",
        "score": 0.5,
        "risk_band": "medium",
        "diesel_price_usd_per_gallon": 3.85,
        "weather_condition": "Clear",
        "computed_at": datetime(2026, 4, 16, tzinfo=UTC),
    }


@pytest.mark.parametrize("band", ["low", "medium", "high"])
def test_risk_band_accepts_known_values(band: str) -> None:
    payload = _base() | {"risk_band": band}
    score = DeliveryRiskScore(**payload)  # type: ignore[arg-type]
    assert score.risk_band == band


def test_risk_band_rejects_unknown_value() -> None:
    payload = _base() | {"risk_band": "extreme"}
    with pytest.raises(ValidationError):
        DeliveryRiskScore(**payload)  # type: ignore[arg-type]


@pytest.mark.parametrize("score", [-0.01, 1.01])
def test_score_must_be_in_unit_interval(score: float) -> None:
    payload = _base() | {"score": score}
    with pytest.raises(ValidationError):
        DeliveryRiskScore(**payload)  # type: ignore[arg-type]


@pytest.mark.parametrize("price", [0.0, -1.0])
def test_diesel_price_must_be_positive(price: float) -> None:
    payload = _base() | {"diesel_price_usd_per_gallon": price}
    with pytest.raises(ValidationError):
        DeliveryRiskScore(**payload)  # type: ignore[arg-type]


def test_score_is_frozen() -> None:
    score = DeliveryRiskScore(**_base())  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        score.score = 0.9  # type: ignore[misc]
