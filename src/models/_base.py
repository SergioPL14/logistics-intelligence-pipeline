"""Shared base class for all Logistics Intelligence Pipeline models."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class LogisticsModel(BaseModel):
    """Base model enforcing strict, immutable contracts.

    All pipeline data contracts inherit from this class so that:
      * Unknown fields are rejected (`extra="forbid"`) — bronze layer schemas
        must match the source exactly.
      * Instances are immutable (`frozen=True`) — prevents accidental mutation
        between transform stages.
      * String fields are whitespace-stripped on input.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
    )
