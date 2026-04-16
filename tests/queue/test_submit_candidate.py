from __future__ import annotations

import os
from collections.abc import Generator
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from data_platform.queue import (
    CandidateEnvelope,
    CandidateQueueItem,
    CandidateValidationError,
    ForbiddenIngestMetadataError,
    submit_candidate,
    validate_candidate_envelope,
)
from data_platform.queue import api as queue_api
from data_platform.queue.repository import (
    CandidateRepository,
    _sqlalchemy_postgres_uri,
)


def test_submit_candidate_validates_and_writes_with_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_envelopes: list[CandidateEnvelope] = []
    closed = False

    class FakeRepository:
        def insert_candidate(self, envelope: CandidateEnvelope) -> CandidateQueueItem:
            seen_envelopes.append(envelope)
            return CandidateQueueItem(
                id=10,
                payload_type=envelope.payload_type,
                payload=envelope.payload,
                submitted_by=envelope.submitted_by,
                submitted_at=datetime.now(UTC),
                ingest_seq=99,
                validation_status="pending",
                rejection_reason=None,
            )

        def close(self) -> None:
            nonlocal closed
            closed = True

    monkeypatch.setattr(queue_api, "CandidateRepository", FakeRepository)

    payload = {
        "payload_type": "Ex-1",
        "submitted_by": "test-subsystem",
        "candidate": {"id": "alpha"},
    }
    item = submit_candidate(payload)

    assert item.id == 10
    assert item.ingest_seq == 99
    assert item.submitted_at is not None
    assert item.validation_status == "pending"
    assert item.rejection_reason is None
    assert dict(item.payload) == payload
    assert seen_envelopes == [
        CandidateEnvelope(
            payload_type="Ex-1",
            submitted_by="test-subsystem",
            payload=payload,
        )
    ]
    assert closed is True


def test_validate_candidate_envelope_returns_immutable_payload_copy() -> None:
    payload = {
        "payload_type": "Ex-2",
        "submitted_by": "test-subsystem",
        "candidate": "alpha",
    }

    envelope = validate_candidate_envelope(payload)
    payload["candidate"] = "mutated"

    assert envelope.payload_type == "Ex-2"
    assert envelope.submitted_by == "test-subsystem"
    assert dict(envelope.payload) == {
        "payload_type": "Ex-2",
        "submitted_by": "test-subsystem",
        "candidate": "alpha",
    }
    with pytest.raises(TypeError):
        envelope.payload["ingest_seq"] = 1  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        envelope.submitted_by = "other-subsystem"


def test_submit_candidate_rejects_invalid_payload_type() -> None:
    with pytest.raises(CandidateValidationError, match="payload_type must be one of"):
        submit_candidate(
            {
                "payload_type": "Ex-4",
                "submitted_by": "test-subsystem",
                "candidate": "alpha",
            }
        )


@pytest.mark.parametrize("payload_type", [None, ["Ex-1"]])
def test_submit_candidate_rejects_non_string_payload_type(payload_type: object) -> None:
    with pytest.raises(CandidateValidationError, match="payload_type must be one of"):
        submit_candidate(
            {
                "payload_type": payload_type,
                "submitted_by": "test-subsystem",
                "candidate": "alpha",
            }
        )


def test_submit_candidate_rejects_missing_submitted_by() -> None:
    with pytest.raises(CandidateValidationError, match="submitted_by is required"):
        submit_candidate({"payload_type": "Ex-1", "candidate": "alpha"})


def test_submit_candidate_rejects_ingest_metadata_before_other_fields() -> None:
    with pytest.raises(ForbiddenIngestMetadataError, match="must not include"):
        submit_candidate({"payload_type": "Ex-1", "ingest_seq": 123})


@pytest.mark.parametrize("forbidden_key", ["submitted_at", "ingest_seq"])
def test_submit_candidate_rejects_top_level_ingest_metadata(
    monkeypatch: pytest.MonkeyPatch,
    forbidden_key: str,
) -> None:
    class UnexpectedRepository:
        def __init__(self) -> None:
            raise AssertionError("repository must not be created for invalid payloads")

    monkeypatch.setattr(queue_api, "CandidateRepository", UnexpectedRepository)

    with pytest.raises(ForbiddenIngestMetadataError, match="must not include"):
        submit_candidate(
            {
                "payload_type": "Ex-1",
                "submitted_by": "test-subsystem",
                forbidden_key: "not producer-owned",
            }
        )


def test_submit_candidate_rejects_non_json_payload() -> None:
    with pytest.raises(CandidateValidationError, match="JSON serializable"):
        submit_candidate(
            {
                "payload_type": "Ex-1",
                "submitted_by": "test-subsystem",
                "as_of": datetime.now(UTC),
            }
        )


def test_submit_candidate_rejects_nested_non_string_payload_keys() -> None:
    with pytest.raises(CandidateValidationError, match="JSON object keys must be strings"):
        submit_candidate(
            {
                "payload_type": "Ex-1",
                "submitted_by": "test-subsystem",
                "candidate": {"id": "alpha", "nested": {1: "not-json-object-key"}},
            }
        )


def test_submit_candidate_rejects_nested_non_string_payload_keys_in_lists() -> None:
    with pytest.raises(CandidateValidationError, match="JSON object keys must be strings"):
        submit_candidate(
            {
                "payload_type": "Ex-1",
                "submitted_by": "test-subsystem",
                "candidate": [{"id": "alpha"}, {1: "not-json-object-key"}],
            }
        )


def test_repository_rewrites_plain_postgres_dsn_to_psycopg() -> None:
    assert _sqlalchemy_postgres_uri("postgresql://dp:dp@localhost/db") == (
        "postgresql+psycopg://dp:dp@localhost/db"
    )
    assert _sqlalchemy_postgres_uri("postgres://dp:dp@localhost/db") == (
        "postgresql+psycopg://dp:dp@localhost/db"
    )
    assert _sqlalchemy_postgres_uri("postgresql+psycopg://dp:dp@localhost/db") == (
        "postgresql+psycopg://dp:dp@localhost/db"
    )


def test_submit_candidate_inserts_and_returns_pg_generated_fields(
    migrated_postgres_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_settings_env(monkeypatch, migrated_postgres_dsn)

    payload = {
        "payload_type": "Ex-1",
        "submitted_by": "test-subsystem",
        "candidate": {"id": "alpha"},
    }

    item = submit_candidate(payload)

    assert item.id is not None
    assert item.submitted_at is not None
    assert item.ingest_seq is not None
    assert item.payload_type == "Ex-1"
    assert item.submitted_by == "test-subsystem"
    assert dict(item.payload) == payload
    assert item.validation_status == "pending"
    assert item.rejection_reason is None


def test_forbidden_ingest_metadata_does_not_insert_row(
    migrated_postgres_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_settings_env(monkeypatch, migrated_postgres_dsn)
    engine = _create_engine(migrated_postgres_dsn)
    try:
        before_count = _candidate_count(engine)
        with pytest.raises(ForbiddenIngestMetadataError):
            submit_candidate(
                {
                    "payload_type": "Ex-1",
                    "submitted_by": "test-subsystem",
                    "ingest_seq": 123,
                }
            )
        assert _candidate_count(engine) == before_count
    finally:
        engine.dispose()


def test_repository_maps_pg_returning_fields(migrated_postgres_dsn: str) -> None:
    envelope = validate_candidate_envelope(
        {
            "payload_type": "Ex-3",
            "submitted_by": "test-subsystem",
            "candidate": "beta",
        }
    )
    repository = CandidateRepository(dsn=migrated_postgres_dsn)
    try:
        item = repository.insert_candidate(envelope)
    finally:
        repository.close()

    assert item.id > 0
    assert item.payload_type == "Ex-3"
    assert item.submitted_by == "test-subsystem"
    assert item.submitted_at is not None
    assert item.ingest_seq > 0
    assert item.validation_status == "pending"
    assert item.rejection_reason is None


@pytest.fixture()
def migrated_postgres_dsn() -> Generator[str]:
    admin_dsn = os.environ.get("DATABASE_URL") or os.environ.get("DP_PG_DSN")
    if not admin_dsn:
        pytest.skip("PostgreSQL submit_candidate tests require DATABASE_URL or DP_PG_DSN")

    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL submit_candidate tests require SQLAlchemy",
    )
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL submit_candidate tests require the migration runner",
    )
    make_url = pytest.importorskip(
        "sqlalchemy.engine",
        reason="PostgreSQL submit_candidate tests require SQLAlchemy",
    ).make_url
    sqlalchemy_error = pytest.importorskip(
        "sqlalchemy.exc",
        reason="PostgreSQL submit_candidate tests require SQLAlchemy",
    ).SQLAlchemyError

    admin_engine = sqlalchemy.create_engine(
        runner_module._sqlalchemy_postgres_uri(admin_dsn),
        isolation_level="AUTOCOMMIT",
    )
    database_name = f"dp_submit_candidate_test_{uuid4().hex}"
    try:
        with admin_engine.connect() as connection:
            connection.execute(sqlalchemy.text(f'CREATE DATABASE "{database_name}"'))
    except sqlalchemy_error as exc:
        admin_engine.dispose()
        pytest.skip(
            "PostgreSQL submit_candidate tests require permission to create "
            f"test databases: {exc}"
        )

    test_dsn = str(
        make_url(runner_module._sqlalchemy_postgres_uri(admin_dsn)).set(database=database_name)
    )
    try:
        runner_module.MigrationRunner().apply_pending(test_dsn)
        yield test_dsn
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                sqlalchemy.text(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database_name
                      AND pid <> pg_backend_pid()
                    """
                ),
                {"database_name": database_name},
            )
            connection.execute(sqlalchemy.text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        admin_engine.dispose()


def _set_required_settings_env(monkeypatch: pytest.MonkeyPatch, dsn: str) -> None:
    monkeypatch.setenv("DP_PG_DSN", dsn)
    monkeypatch.setenv("DP_RAW_ZONE_PATH", "data_platform/raw")
    monkeypatch.setenv("DP_ICEBERG_WAREHOUSE_PATH", "data_platform/iceberg/warehouse")
    monkeypatch.setenv("DP_DUCKDB_PATH", "data_platform/duckdb/data_platform.duckdb")


def _create_engine(dsn: str) -> Any:
    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL submit_candidate tests require SQLAlchemy",
    )
    return sqlalchemy.create_engine(_sqlalchemy_postgres_uri(dsn))


def _candidate_count(engine: Any) -> int:
    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL submit_candidate tests require SQLAlchemy",
    )
    with engine.connect() as connection:
        return int(
            connection.execute(
                sqlalchemy.text("SELECT count(*) FROM data_platform.candidate_queue")
            ).scalar_one()
        )
