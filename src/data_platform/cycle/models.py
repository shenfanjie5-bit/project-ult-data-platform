"""Types for PostgreSQL-backed cycle control metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final, Literal, TypeAlias

CycleStatus: TypeAlias = Literal[
    "pending",
    "phase0",
    "phase1",
    "phase2",
    "phase3",
    "published",
    "failed",
]

CYCLE_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^CYCLE_[0-9]{8}$")
CYCLE_METADATA_TABLE: Final[str] = "data_platform.cycle_metadata"

_CYCLE_STATUSES: Final[frozenset[str]] = frozenset(
    ("pending", "phase0", "phase1", "phase2", "phase3", "published", "failed")
)


class CycleError(RuntimeError):
    """Base class for cycle metadata errors."""


class CycleAlreadyExists(CycleError):
    """Raised when a cycle already exists for the requested cycle date."""


class CycleNotFound(CycleError):
    """Raised when a cycle id does not exist."""


class InvalidCycleId(CycleError):
    """Raised when a cycle id does not match CYCLE_YYYYMMDD."""


class InvalidCycleTransition(CycleError):
    """Raised when a requested cycle status transition is not allowed."""


@dataclass(frozen=True, slots=True)
class CycleMetadata:
    """A row from data_platform.cycle_metadata."""

    cycle_id: str
    cycle_date: date
    status: CycleStatus
    cutoff_submitted_at: datetime | None
    cutoff_ingest_seq: int | None
    candidate_count: int
    selection_frozen_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        validate_cycle_id(self.cycle_id)
        validate_cycle_status(self.status)
        if self.candidate_count < 0:
            msg = "candidate_count must be greater than or equal to 0"
            raise ValueError(msg)


def validate_cycle_id(cycle_id: str) -> None:
    """Validate the public cycle id contract."""

    if CYCLE_ID_PATTERN.fullmatch(cycle_id) is None:
        msg = f"cycle_id must match CYCLE_YYYYMMDD: {cycle_id!r}"
        raise InvalidCycleId(msg)


def validate_cycle_status(status: str) -> None:
    """Validate the cycle status enum contract."""

    if status not in _CYCLE_STATUSES:
        msg = f"cycle status must be one of {sorted(_CYCLE_STATUSES)}: {status!r}"
        raise InvalidCycleTransition(msg)
