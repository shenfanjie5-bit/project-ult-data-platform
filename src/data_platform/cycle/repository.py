"""Repository helpers for PostgreSQL-backed cycle metadata."""

from __future__ import annotations

from datetime import date
from typing import cast

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, RowMapping
from sqlalchemy.exc import IntegrityError

from data_platform.config import get_settings
from data_platform.cycle.models import (
    CYCLE_METADATA_TABLE,
    CycleAlreadyExists,
    CycleMetadata,
    CycleNotFound,
    CycleStatus,
    InvalidCycleTransition,
    validate_cycle_id,
    validate_cycle_status,
)
from data_platform.ddl.runner import _sqlalchemy_postgres_uri

_ORDERED_TRANSITIONS: dict[str, str] = {
    "pending": "phase0",
    "phase0": "phase1",
    "phase1": "phase2",
    "phase2": "phase3",
    "phase3": "published",
}


def create_cycle(cycle_date: date) -> CycleMetadata:
    """Create a pending cycle for one cycle date."""

    cycle_id = f"CYCLE_{cycle_date:%Y%m%d}"
    engine = _create_engine()
    try:
        try:
            with engine.begin() as connection:
                row = connection.execute(
                    text(
                        f"""
                        INSERT INTO {CYCLE_METADATA_TABLE} (cycle_id, cycle_date)
                        VALUES (:cycle_id, :cycle_date)
                        RETURNING
                            cycle_id,
                            cycle_date,
                            status,
                            cutoff_submitted_at,
                            cutoff_ingest_seq,
                            candidate_count,
                            selection_frozen_at,
                            created_at,
                            updated_at
                        """
                    ),
                    {"cycle_id": cycle_id, "cycle_date": cycle_date},
                ).mappings().one()
        except IntegrityError as exc:
            msg = f"cycle already exists for cycle_date {cycle_date.isoformat()}"
            raise CycleAlreadyExists(msg) from exc
        return _row_to_cycle(row)
    finally:
        engine.dispose()


def get_cycle(cycle_id: str) -> CycleMetadata:
    """Read one cycle by id."""

    validate_cycle_id(cycle_id)
    engine = _create_engine()
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT
                        cycle_id,
                        cycle_date,
                        status,
                        cutoff_submitted_at,
                        cutoff_ingest_seq,
                        candidate_count,
                        selection_frozen_at,
                        created_at,
                        updated_at
                    FROM {CYCLE_METADATA_TABLE}
                    WHERE cycle_id = :cycle_id
                    """
                ),
                {"cycle_id": cycle_id},
            ).mappings().one_or_none()
        if row is None:
            msg = f"cycle not found: {cycle_id}"
            raise CycleNotFound(msg)
        return _row_to_cycle(row)
    finally:
        engine.dispose()


def transition_cycle_status(cycle_id: str, status: CycleStatus) -> CycleMetadata:
    """Move a cycle through the allowed status state machine."""

    validate_cycle_id(cycle_id)
    validate_cycle_status(status)
    engine = _create_engine()
    try:
        with engine.begin() as connection:
            current_row = connection.execute(
                text(
                    f"""
                    SELECT status
                    FROM {CYCLE_METADATA_TABLE}
                    WHERE cycle_id = :cycle_id
                    FOR UPDATE
                    """
                ),
                {"cycle_id": cycle_id},
            ).mappings().one_or_none()
            if current_row is None:
                msg = f"cycle not found: {cycle_id}"
                raise CycleNotFound(msg)

            current_status = str(current_row["status"])
            if not _is_transition_allowed(current_status, status):
                msg = f"cannot transition cycle {cycle_id} from {current_status!r} to {status!r}"
                raise InvalidCycleTransition(msg)

            updated_row = connection.execute(
                text(
                    f"""
                    UPDATE {CYCLE_METADATA_TABLE}
                    SET status = :status,
                        updated_at = now()
                    WHERE cycle_id = :cycle_id
                    RETURNING
                        cycle_id,
                        cycle_date,
                        status,
                        cutoff_submitted_at,
                        cutoff_ingest_seq,
                        candidate_count,
                        selection_frozen_at,
                        created_at,
                        updated_at
                    """
                ),
                {"cycle_id": cycle_id, "status": status},
            ).mappings().one()
        return _row_to_cycle(updated_row)
    finally:
        engine.dispose()


def _create_engine() -> Engine:
    return create_engine(_sqlalchemy_postgres_uri(str(get_settings().pg_dsn)))


def _is_transition_allowed(current_status: str, next_status: str) -> bool:
    if next_status == "failed":
        return current_status in _ORDERED_TRANSITIONS
    return _ORDERED_TRANSITIONS.get(current_status) == next_status


def _row_to_cycle(row: RowMapping) -> CycleMetadata:
    return CycleMetadata(
        cycle_id=str(row["cycle_id"]),
        cycle_date=cast(date, row["cycle_date"]),
        status=cast(CycleStatus, str(row["status"])),
        cutoff_submitted_at=row["cutoff_submitted_at"],
        cutoff_ingest_seq=(
            None if row["cutoff_ingest_seq"] is None else int(row["cutoff_ingest_seq"])
        ),
        candidate_count=int(row["candidate_count"]),
        selection_frozen_at=row["selection_frozen_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
