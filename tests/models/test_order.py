"""Unit tests for the Order contract."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from src.models import Order


def _ts() -> datetime:
    return datetime(2018, 1, 15, 10, 30, 0)


def _kwargs(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "order_id": "abc",
        "customer_zip_code": "12345",
        "customer_city": "sao paulo",
        "customer_state": "SP",
        "order_purchase_timestamp": _ts(),
    }
    base.update(overrides)
    return base


def test_order_happy_path() -> None:
    order = Order(**_kwargs(order_id="abc-123", customer_zip_code="01310", order_weight_g=750.5))
    assert order.order_id == "abc-123"
    assert order.customer_zip_code == "01310"
    assert order.customer_city == "sao paulo"
    assert order.customer_state == "SP"
    assert order.order_weight_g == 750.5


def test_order_normalizes_zip_with_missing_leading_zero() -> None:
    order = Order(**_kwargs(customer_zip_code="1310"))
    assert order.customer_zip_code == "01310"


def test_order_weight_is_optional() -> None:
    order = Order(**_kwargs())
    assert order.order_weight_g is None


def test_order_rejects_empty_id() -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(order_id=""))


def test_order_rejects_negative_weight() -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(order_weight_g=-1.0))


def test_order_rejects_invalid_zip() -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(customer_zip_code="abcde"))


def test_order_rejects_none_zip() -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(customer_zip_code=None))


def test_order_rejects_empty_city() -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(customer_city=""))


@pytest.mark.parametrize("bad_state", ["S", "SAO", ""])
def test_order_rejects_bad_state_length(bad_state: str) -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(customer_state=bad_state))


def test_order_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        Order(**_kwargs(unexpected_field="boom"))


def test_order_is_frozen() -> None:
    order = Order(**_kwargs())
    with pytest.raises(ValidationError):
        order.order_id = "mutated"  # type: ignore[misc]
