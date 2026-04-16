"""PostgreSQL repository for Lite candidate queue writes."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
import json
import os
from typing import Any, Final, cast

from data_platform.queue.models import (
    CANDIDATE_QUEUE_TABLE,
    CandidatePayloadType,
    CandidateQueueItem,
    ValidationStatus,
)
from data_platform.queue.validation import CandidateEnvelope

_RETURNING_COLUMNS: Final[tuple[str, ...]] = (
    "id",
    "payload_type",
    "payload",
    "submitted_by",
    "submitted_at",
    "ingest_seq",
    "validation_status",
    "rejection_reason",
)
_INSERT_CANDIDATE_SQL: Final[str] = f"""
INSERT INTO {CANDIDATE_QUEUE_TABLE} (
    payload_type,
    payload,
    submitted_by
)
VALUES (
    :payload_type,
    CAST(:payload AS jsonb),
    :submitted_by
)
RETURNING {", ".join(_RETURNING_COLUMNS)}
"""


class CandidateQueueWriteError(RuntimeError):
    """Raised when candidate_queue insertion or returned-row mapping fails."""

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        self.cause = cause
        detail = f": {cause}" if cause is not None else ""
        super().__init__(f"{message}{detail}")


class CandidateRepository:
    """Repository for synchronous candidate_queue inserts."""

    def __init__(self, dsn: str | None = None, *, engine: Any | None = None) -> None:
        if dsn is not None and engine is not None:
            msg = "provide either dsn or engine, not both"
            raise ValueError(msg)

        self._owns_engine = engine is None
        self._engine = engine if engine is not None else _create_engine(dsn or _resolve_dsn())

    def insert_candidate(self, envelope: CandidateEnvelope) -> CandidateQueueItem:
        """Insert one validated candidate envelope and return the PostgreSQL row."""

        try:
            text = _sqlalchemy_text()
            payload = json.dumps(dict(envelope.payload), allow_nan=False)
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        text(_INSERT_CANDIDATE_SQL),
                        {
                            "payload_type": envelope.payload_type,
                            "payload": payload,
                            "submitted_by": envelope.submitted_by,
                        },
                    )
                    .mappings()
                    .one()
                )
        except CandidateQueueWriteError:
            raise
        except Exception as exc:
            if _is_sqlalchemy_error(exc):
                raise CandidateQueueWriteError("candidate queue insert failed", exc) from exc
            raise

        return _row_to_candidate_queue_item(row)

    def close(self) -> None:
        """Dispose an owned SQLAlchemy engine."""

        if self._owns_engine:
            self._engine.dispose()


def _resolve_dsn() -> str:
    dsn = os.environ.get("DP_PG_DSN")
    if dsn:
        return dsn

    try:
        config_module = import_module("data_platform.config")
        get_settings = getattr(config_module, "get_settings")
        return str(get_settings().pg_dsn)
    except ModuleNotFoundError as exc:
        raise CandidateQueueWriteError("DP_PG_DSN is required for candidate queue writes", exc)
    except Exception as exc:
        raise CandidateQueueWriteError("DP_PG_DSN is required for candidate queue writes", exc)


def _create_engine(dsn: str) -> Any:
    try:
        sqlalchemy = import_module("sqlalchemy")
    except ModuleNotFoundError as exc:
        raise CandidateQueueWriteError("candidate queue writes require SQLAlchemy", exc)

    create_engine = getattr(sqlalchemy, "create_engine")
    return create_engine(_sqlalchemy_postgres_uri(dsn))


def _sqlalchemy_text() -> Any:
    try:
        sqlalchemy = import_module("sqlalchemy")
    except ModuleNotFoundError as exc:
        raise CandidateQueueWriteError("candidate queue writes require SQLAlchemy", exc)

    return getattr(sqlalchemy, "text")


def _is_sqlalchemy_error(exc: BaseException) -> bool:
    try:
        sqlalchemy_exc = import_module("sqlalchemy.exc")
    except ModuleNotFoundError:
        return False

    SQLAlchemyError = getattr(sqlalchemy_exc, "SQLAlchemyError")
    return isinstance(exc, SQLAlchemyError)


def _sqlalchemy_postgres_uri(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgresql://")
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgres://")
    return dsn


def _row_to_candidate_queue_item(row: Mapping[str, Any]) -> CandidateQueueItem:
    try:
        payload = _coerce_payload(row["payload"])
        return CandidateQueueItem(
            id=int(row["id"]),
            payload_type=cast(CandidatePayloadType, row["payload_type"]),
            payload=payload,
            submitted_by=str(row["submitted_by"]),
            submitted_at=row["submitted_at"],
            ingest_seq=int(row["ingest_seq"]),
            validation_status=cast(ValidationStatus, row["validation_status"]),
            rejection_reason=(
                None if row["rejection_reason"] is None else str(row["rejection_reason"])
            ),
        )
    except KeyError as exc:
        raise CandidateQueueWriteError(
            f"candidate queue returning row missing field {exc.args[0]!r}", exc
        ) from exc
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CandidateQueueWriteError("candidate queue returning row is invalid", exc) from exc


def _coerce_payload(payload: object) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        return cast(Mapping[str, Any], payload)
    if isinstance(payload, str):
        decoded = json.loads(payload)
        if isinstance(decoded, Mapping):
            return cast(Mapping[str, Any], decoded)

    msg = "candidate queue returning payload must be a JSON object mapping"
    raise TypeError(msg)


__all__ = [
    "CandidateQueueWriteError",
    "CandidateRepository",
]
