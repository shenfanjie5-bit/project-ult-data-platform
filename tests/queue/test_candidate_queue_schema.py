from __future__ import annotations

import json
import os
from collections.abc import Generator
from dataclasses import FrozenInstanceError, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, get_args
from uuid import uuid4

import pytest

from data_platform.queue import (
    CANDIDATE_QUEUE_TABLE,
    INGEST_METADATA_VIEW,
    CandidatePayloadType,
    CandidateQueueItem,
    IngestMetadataRecord,
    ValidationStatus,
)


def _sqlalchemy_postgres_uri(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgresql://")
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn.removeprefix("postgres://")
    return dsn


def _sqlalchemy_helpers() -> tuple[Any, Any, Any, type[Exception]]:
    pytest.importorskip("sqlalchemy", reason="candidate queue schema tests require SQLAlchemy")
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import SQLAlchemyError

    return create_engine, text, make_url, SQLAlchemyError


def _create_engine(dsn: str, *, autocommit: bool = False) -> Any:
    create_engine, _, _, _ = _sqlalchemy_helpers()
    isolation_level = "AUTOCOMMIT" if autocommit else None
    return create_engine(
        _sqlalchemy_postgres_uri(dsn),
        isolation_level=isolation_level,
        connect_args={"connect_timeout": 2},
    )


@pytest.fixture()
def postgres_dsn() -> Generator[str]:
    admin_dsn = os.environ.get("DATABASE_URL") or os.environ.get("DP_PG_DSN")
    if not admin_dsn:
        pytest.skip("candidate queue schema tests require DATABASE_URL or DP_PG_DSN")

    _, text, make_url, SQLAlchemyError = _sqlalchemy_helpers()
    admin_engine = _create_engine(admin_dsn, autocommit=True)
    database_name = f"dp_candidate_queue_test_{uuid4().hex}"
    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    except SQLAlchemyError as exc:
        admin_engine.dispose()
        pytest.skip(
            "candidate queue schema tests require a reachable PostgreSQL server "
            f"and permission to create test databases: {exc}"
        )

    test_dsn = str(make_url(admin_dsn).set(database=database_name))
    try:
        yield test_dsn
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database_name
                      AND pid <> pg_backend_pid()
                    """
                ),
                {"database_name": database_name},
            )
            connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        admin_engine.dispose()


@pytest.fixture()
def migrated_postgres_dsn(postgres_dsn: str) -> str:
    try:
        from data_platform.ddl.runner import MigrationRunner
    except ModuleNotFoundError as exc:
        if exc.name == "sqlalchemy":
            pytest.skip("candidate queue schema tests require SQLAlchemy")
        raise

    runner = MigrationRunner()

    assert runner.apply_pending(postgres_dsn) == ["0001", "0002"]
    assert runner.apply_pending(postgres_dsn) == []
    return postgres_dsn


def _fetch_all(dsn: str, sql: str, params: dict[str, object] | None = None) -> list[object]:
    _, text, _, _ = _sqlalchemy_helpers()
    engine = _create_engine(dsn)
    try:
        with engine.connect() as connection:
            return list(connection.execute(text(sql), params or {}).mappings())
    finally:
        engine.dispose()


def _execute(dsn: str, sql: str, params: dict[str, object] | None = None) -> None:
    _, text, _, _ = _sqlalchemy_helpers()
    engine = _create_engine(dsn)
    try:
        with engine.begin() as connection:
            connection.execute(text(sql), params or {})
    finally:
        engine.dispose()


def test_queue_model_exports_match_contract() -> None:
    assert CANDIDATE_QUEUE_TABLE == "data_platform.candidate_queue"
    assert INGEST_METADATA_VIEW == "data_platform.ingest_metadata"
    assert get_args(CandidatePayloadType) == ("Ex-0", "Ex-1", "Ex-2", "Ex-3")
    assert get_args(ValidationStatus) == ("pending", "accepted", "rejected")
    assert is_dataclass(CandidateQueueItem)
    assert is_dataclass(IngestMetadataRecord)
    assert CandidateQueueItem.__slots__ == (
        "id",
        "payload_type",
        "payload",
        "submitted_by",
        "submitted_at",
        "ingest_seq",
        "validation_status",
        "rejection_reason",
    )


def test_candidate_queue_item_rejects_pg_generated_payload_fields() -> None:
    submitted_at = datetime.now(UTC)
    item = CandidateQueueItem(
        id=1,
        payload_type="Ex-0",
        payload={"symbol": "000001.SZ"},
        submitted_by="pytest",
        submitted_at=submitted_at,
        ingest_seq=1,
        validation_status="pending",
        rejection_reason=None,
    )

    with pytest.raises(FrozenInstanceError):
        item.submitted_by = "other"  # type: ignore[misc]

    for field_name in ("submitted_at", "ingest_seq"):
        with pytest.raises(ValueError, match="PostgreSQL-generated fields"):
            CandidateQueueItem(
                id=1,
                payload_type="Ex-0",
                payload={field_name: "not allowed"},
                submitted_by="pytest",
                submitted_at=submitted_at,
                ingest_seq=1,
                validation_status="pending",
                rejection_reason=None,
            )


def test_migration_creates_candidate_queue_schema(migrated_postgres_dsn: str) -> None:
    enum_labels = _fetch_all(
        migrated_postgres_dsn,
        """
        SELECT typ.typname, enum.enumlabel
        FROM pg_type typ
        JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
        JOIN pg_enum enum ON enum.enumtypid = typ.oid
        WHERE nsp.nspname = 'data_platform'
          AND typ.typname IN ('candidate_payload_type', 'validation_status')
        ORDER BY typ.typname, enum.enumsortorder
        """,
    )
    labels_by_type: dict[str, list[str]] = {}
    for row in enum_labels:
        labels_by_type.setdefault(str(row["typname"]), []).append(str(row["enumlabel"]))

    assert labels_by_type == {
        "candidate_payload_type": ["Ex-0", "Ex-1", "Ex-2", "Ex-3"],
        "validation_status": ["pending", "accepted", "rejected"],
    }

    columns = _fetch_all(
        migrated_postgres_dsn,
        """
        SELECT column_name, data_type, udt_schema, udt_name, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'data_platform'
          AND table_name = 'candidate_queue'
        ORDER BY ordinal_position
        """,
    )
    column_names = [str(row["column_name"]) for row in columns]
    defaults = {str(row["column_name"]): row["column_default"] for row in columns}
    nullable = {str(row["column_name"]): row["is_nullable"] for row in columns}

    assert column_names == [
        "id",
        "payload_type",
        "payload",
        "submitted_by",
        "submitted_at",
        "ingest_seq",
        "validation_status",
        "rejection_reason",
    ]
    assert "nextval" in str(defaults["id"])
    assert defaults["submitted_at"] == "now()"
    assert "nextval" in str(defaults["ingest_seq"])
    assert defaults["validation_status"] == "'pending'::data_platform.validation_status"
    assert nullable == {
        "id": "NO",
        "payload_type": "NO",
        "payload": "NO",
        "submitted_by": "NO",
        "submitted_at": "NO",
        "ingest_seq": "NO",
        "validation_status": "NO",
        "rejection_reason": "YES",
    }

    constraints = _fetch_all(
        migrated_postgres_dsn,
        """
        SELECT conname, contype
        FROM pg_constraint
        WHERE connamespace = 'data_platform'::regnamespace
          AND conrelid = 'data_platform.candidate_queue'::regclass
        ORDER BY conname
        """,
    )
    assert {(str(row["conname"]), str(row["contype"])) for row in constraints} == {
        ("candidate_queue_ingest_seq_key", "u"),
        ("candidate_queue_payload_no_system_fields", "c"),
        ("candidate_queue_pkey", "p"),
    }

    indexes = _fetch_all(
        migrated_postgres_dsn,
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'data_platform'
          AND tablename = 'candidate_queue'
        ORDER BY indexname
        """,
    )
    assert {str(row["indexname"]) for row in indexes} == {
        "candidate_queue_ingest_seq_key",
        "candidate_queue_pkey",
    }

    view_columns = _fetch_all(
        migrated_postgres_dsn,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'data_platform'
          AND table_name = 'ingest_metadata'
        ORDER BY ordinal_position
        """,
    )
    assert [str(row["column_name"]) for row in view_columns] == [
        "candidate_id",
        "payload_type",
        "submitted_by",
        "submitted_at",
        "ingest_seq",
        "validation_status",
        "rejection_reason",
    ]


def test_candidate_queue_defaults_and_ingest_metadata_view(
    migrated_postgres_dsn: str,
) -> None:
    _, text, _, _ = _sqlalchemy_helpers()
    engine = _create_engine(migrated_postgres_dsn)
    try:
        with engine.begin() as connection:
            first = connection.execute(
                text(
                    """
                    INSERT INTO data_platform.candidate_queue (
                        payload_type,
                        payload,
                        submitted_by
                    )
                    VALUES ('Ex-0', CAST(:payload AS jsonb), 'pytest')
                    RETURNING
                        id,
                        payload_type,
                        payload,
                        submitted_by,
                        submitted_at,
                        ingest_seq,
                        validation_status,
                        rejection_reason
                    """
                ),
                {"payload": json.dumps({"symbol": "000001.SZ"})},
            ).mappings().one()
            second = connection.execute(
                text(
                    """
                    INSERT INTO data_platform.candidate_queue (
                        payload_type,
                        payload,
                        submitted_by
                    )
                    VALUES ('Ex-1', CAST(:payload AS jsonb), 'pytest')
                    RETURNING ingest_seq
                    """
                ),
                {"payload": json.dumps({"symbol": "000002.SZ"})},
            ).mappings().one()

            view_row = connection.execute(
                text(
                    """
                    SELECT
                        candidate_id,
                        payload_type,
                        submitted_by,
                        submitted_at,
                        ingest_seq,
                        validation_status,
                        rejection_reason
                    FROM data_platform.ingest_metadata
                    WHERE candidate_id = :candidate_id
                    """
                ),
                {"candidate_id": first["id"]},
            ).mappings().one()
    finally:
        engine.dispose()

    assert first["payload_type"] == "Ex-0"
    assert first["payload"] == {"symbol": "000001.SZ"}
    assert first["submitted_by"] == "pytest"
    assert first["submitted_at"] is not None
    assert second["ingest_seq"] == first["ingest_seq"] + 1
    assert first["validation_status"] == "pending"
    assert first["rejection_reason"] is None
    assert dict(view_row) == {
        "candidate_id": first["id"],
        "payload_type": first["payload_type"],
        "submitted_by": first["submitted_by"],
        "submitted_at": first["submitted_at"],
        "ingest_seq": first["ingest_seq"],
        "validation_status": first["validation_status"],
        "rejection_reason": first["rejection_reason"],
    }


@pytest.mark.parametrize(
    ("sql", "bad_value"),
    [
        (
            """
            INSERT INTO data_platform.candidate_queue (
                payload_type,
                payload,
                submitted_by
            )
            VALUES (:bad_value, CAST(:payload AS jsonb), 'pytest')
            """,
            "Ex-4",
        ),
        (
            """
            INSERT INTO data_platform.candidate_queue (
                payload_type,
                payload,
                submitted_by,
                validation_status
            )
            VALUES ('Ex-0', CAST(:payload AS jsonb), 'pytest', :bad_value)
            """,
            "done",
        ),
    ],
)
def test_candidate_queue_rejects_unknown_enum_values(
    migrated_postgres_dsn: str,
    sql: str,
    bad_value: str,
) -> None:
    _, _, _, SQLAlchemyError = _sqlalchemy_helpers()

    with pytest.raises(SQLAlchemyError):
        _execute(
            migrated_postgres_dsn,
            sql,
            {
                "payload": json.dumps({"symbol": "000001.SZ"}),
                "bad_value": bad_value,
            },
        )


@pytest.mark.parametrize("field_name", ["submitted_at", "ingest_seq"])
def test_candidate_queue_rejects_payload_system_fields(
    migrated_postgres_dsn: str,
    field_name: str,
) -> None:
    _, _, _, SQLAlchemyError = _sqlalchemy_helpers()

    with pytest.raises(SQLAlchemyError):
        _execute(
            migrated_postgres_dsn,
            """
            INSERT INTO data_platform.candidate_queue (
                payload_type,
                payload,
                submitted_by
            )
            VALUES ('Ex-0', CAST(:payload AS jsonb), 'pytest')
            """,
            {"payload": json.dumps({"symbol": "000001.SZ", field_name: "bad"})},
        )


def test_candidate_queue_migration_is_packaged() -> None:
    migration = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "data_platform"
        / "ddl"
        / "migrations"
        / "0002_candidate_queue.sql"
    )

    assert migration.exists()
