"""Unit tests for src.extract.extract_diesel (EIA API mocked)."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from pydantic import ValidationError

from src.extract.extract_diesel import OUTPUT_COLUMNS, extract_diesel


def _fake_response(payload: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = payload
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"{status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


SAMPLE_PAYLOAD = {
    "response": {
        "data": [
            {"period": "2026-04-13", "value": 3.85, "series": "EMD_EPD2D_PTE_NUS_DPG"},
            {"period": "2026-04-06", "value": 3.80, "series": "EMD_EPD2D_PTE_NUS_DPG"},
            {"period": "2026-03-30", "value": 3.78, "series": "EMD_EPD2D_PTE_NUS_DPG"},
        ]
    }
}


@pytest.fixture
def eia_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EIA_API_KEY", "test-key")


def test_extract_diesel_happy_path(eia_key: None) -> None:
    with patch(
        "src.extract.extract_diesel.requests.get",
        return_value=_fake_response(SAMPLE_PAYLOAD),
    ):
        df = extract_diesel(output_dir=None)

    assert list(df.columns) == OUTPUT_COLUMNS
    assert len(df) == 3
    # Sorted ascending by period
    assert df["period"].tolist() == sorted(df["period"].tolist())
    assert df["price_usd_per_gallon"].tolist() == [3.78, 3.80, 3.85]
    assert (df["region"] == "NUS").all()
    assert (df["series_id"] == "EMD_EPD2D_PTE_NUS_DPG").all()


def test_extract_diesel_writes_parquet_and_csv(eia_key: None, tmp_path: Path) -> None:
    out = tmp_path / "bronze"
    with patch(
        "src.extract.extract_diesel.requests.get",
        return_value=_fake_response(SAMPLE_PAYLOAD),
    ):
        extract_diesel(output_dir=out, output_stem="diesel")

    assert (out / "diesel.parquet").exists()
    assert (out / "diesel.csv").exists()
    from_parquet = pd.read_parquet(out / "diesel.parquet")
    assert len(from_parquet) == 3


def test_extract_diesel_passes_start_date(eia_key: None) -> None:
    with patch(
        "src.extract.extract_diesel.requests.get",
        return_value=_fake_response(SAMPLE_PAYLOAD),
    ) as mock_get:
        extract_diesel(start_date="2024-01-01", output_dir=None)

    params = dict(mock_get.call_args.kwargs["params"])
    assert params["start"] == "2024-01-01"


def test_extract_diesel_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    # Ensure load_dotenv inside the function can't repopulate from a real .env
    monkeypatch.setattr("src.extract.extract_diesel.load_dotenv", lambda: None)
    with pytest.raises(RuntimeError, match="EIA_API_KEY"):
        extract_diesel(output_dir=None)


def test_extract_diesel_http_error(eia_key: None) -> None:
    with patch(
        "src.extract.extract_diesel.requests.get",
        return_value=_fake_response({}, status_code=500),
    ), pytest.raises(requests.HTTPError):
        extract_diesel(output_dir=None)


def test_extract_diesel_rejects_invalid_row(eia_key: None) -> None:
    bad = {
        "response": {
            "data": [
                {"period": "2026-04-13", "value": -1.0, "series": "X"},
            ]
        }
    }
    with patch(
        "src.extract.extract_diesel.requests.get",
        return_value=_fake_response(bad),
    ), pytest.raises(ValidationError):
        extract_diesel(output_dir=None)


def test_extract_diesel_empty_response(eia_key: None) -> None:
    with patch(
        "src.extract.extract_diesel.requests.get",
        return_value=_fake_response({"response": {"data": []}}),
    ):
        df = extract_diesel(output_dir=None)
    assert df.empty
    assert list(df.columns) == OUTPUT_COLUMNS
