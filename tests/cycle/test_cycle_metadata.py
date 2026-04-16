from __future__ import annotations

import os
from collections.abc import Generator
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast, get_args
from uuid import uuid4

import pytest

from data_platform.config import reset_settings_cache
from data_platform.cycle import (
    CYCLE_ID_PATTERN,
    CYCLE_METADATA_TABLE,
    CycleAlreadyExists,
    CycleMetadata,
    CycleNotFound,
    CycleStatus,
    InvalidCycleId,
    InvalidCycleTransition,
    create_cycle,
    get_cycle,
    transition_cycle_status,
)

EXPECTED_CYCLE_COLUMNS = [
    "cycle_id",
    "cycle_date",
    "status",
    "cutoff_submitted_at",
    "cutoff_ingest_seq",
    "candidate_count",
    "selection_frozen_at",
    "created_at",
    "updated_at",
]
ORDERED_STATUSES: list[CycleStatus] = [
    "phase0",
    "phase1",
    "phase2",
    "phase3",
    "published",
]


@pytest.fixture()
def postgres_dsn() -> Generator[str]:
    admin_dsn = os.environ.get("DATABASE_URL") or os.environ.get("DP_PG_DSN")
    if not admin_dsn:
        pytest.skip("PostgreSQL cycle metadata tests require DATABASE_URL or DP_PG_DSN")

    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL cycle metadata tests require SQLAlchemy",
    )
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL cycle metadata tests require the migration runner",
    )
    make_url = pytest.importorskip(
        "sqlalchemy.engine",
        reason="PostgreSQL cycle metadata tests require SQLAlchemy",
    ).make_url
    sqlalchemy_error = pytest.importorskip(
        "sqlalchemy.exc",
        reason="PostgreSQL cycle metadata tests require SQLAlchemy",
    ).SQLAlchemyError

    admin_engine = sqlalchemy.create_engine(
        runner_module._sqlalchemy_postgres_uri(admin_dsn),
        isolation_level="AUTOCOMMIT",
    )
    database_name = f"dp_cycle_metadata_test_{uuid4().hex}"
    try:
        with admin_engine.connect() as connection:
            connection.execute(sqlalchemy.text(f'CREATE DATABASE "{database_name}"'))
    except sqlalchemy_error as exc:
        admin_engine.dispose()
        pytest.skip(
            "PostgreSQL cycle metadata tests require permission to create "
            f"test databases: {exc}"
        )

    test_dsn = str(
        make_url(runner_module._sqlalchemy_postgres_uri(admin_dsn)).set(database=database_name)
    )
    try:
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


@pytest.fixture()
def migrated_cycle_dsn(
    postgres_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Generator[str]:
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL cycle metadata tests require the migration runner",
    )
    monkeypatch.setenv("DP_PG_DSN", postgres_dsn)
    monkeypatch.setenv("DP_RAW_ZONE_PATH", str(tmp_path / "raw"))
    monkeypatch.setenv("DP_ICEBERG_WAREHOUSE_PATH", str(tmp_path / "iceberg" / "warehouse"))
    monkeypatch.setenv("DP_DUCKDB_PATH", str(tmp_path / "duckdb" / "data_platform.duckdb"))
    reset_settings_cache()

    applied_versions = runner_module.MigrationRunner().apply_pending(postgres_dsn)
    assert applied_versions == ["0001", "0002", "0003"]
    assert runner_module.MigrationRunner().apply_pending(postgres_dsn) == []
    try:
        yield postgres_dsn
    finally:
        reset_settings_cache()


def test_cycle_metadata_model_exposes_contract_types_and_constants() -> None:
    assert CYCLE_METADATA_TABLE == "data_platform.cycle_metadata"
    assert CYCLE_ID_PATTERN.fullmatch("CYCLE_20260416") is not None
    assert CYCLE_ID_PATTERN.fullmatch("20260416") is None
    assert get_args(CycleStatus) == (
        "pending",
        "phase0",
        "phase1",
        "phase2",
        "phase3",
        "published",
        "failed",
    )

    assert is_dataclass(CycleMetadata)
    assert [field.name for field in fields(CycleMetadata)] == EXPECTED_CYCLE_COLUMNS
    assert CycleMetadata.__slots__ == tuple(EXPECTED_CYCLE_COLUMNS)

    now = datetime.now(UTC)
    cycle = CycleMetadata(
        cycle_id="CYCLE_20260416",
        cycle_date=date(2026, 4, 16),
        status="pending",
        cutoff_submitted_at=None,
        cutoff_ingest_seq=None,
        candidate_count=0,
        selection_frozen_at=None,
        created_at=now,
        updated_at=now,
    )
    assert cycle.cycle_id == "CYCLE_20260416"
    assert cycle.status == "pending"


@pytest.mark.parametrize(
    "cycle_id",
    ["", "20260416", "CYCLE_2026-04-16", "CYCLE_2026041", "CYCLE_202604160"],
)
def test_repository_rejects_invalid_cycle_id_before_db_access(cycle_id: str) -> None:
    with pytest.raises(InvalidCycleId, match="CYCLE_YYYYMMDD"):
        get_cycle(cycle_id)

    with pytest.raises(InvalidCycleId, match="CYCLE_YYYYMMDD"):
        transition_cycle_status(cycle_id, "phase0")


def test_repository_rejects_invalid_target_status_before_db_access() -> None:
    with pytest.raises(InvalidCycleTransition, match="cycle status must be one of"):
        transition_cycle_status("CYCLE_20260416", cast(CycleStatus, "done"))


def test_migration_creates_cycle_metadata_schema(migrated_cycle_dsn: str) -> None:
    engine = _create_engine(migrated_cycle_dsn)
    try:
        with engine.connect() as connection:
            assert (
                connection.execute(
                    _text("SELECT to_regclass('data_platform.cycle_metadata')")
                ).scalar_one()
                == "data_platform.cycle_metadata"
            )
            assert _enum_labels(connection, "cycle_status") == [
                "pending",
                "phase0",
                "phase1",
                "phase2",
                "phase3",
                "published",
                "failed",
            ]

            table_columns = connection.execute(
                _text(
                    """
                    SELECT column_name, data_type, udt_schema, udt_name, is_nullable,
                           column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'data_platform'
                      AND table_name = 'cycle_metadata'
                    ORDER BY ordinal_position
                    """
                )
            ).mappings()
            columns = {str(row["column_name"]): row for row in table_columns}
            assert list(columns) == EXPECTED_CYCLE_COLUMNS
            assert columns["cycle_id"]["data_type"] == "text"
            assert columns["cycle_date"]["data_type"] == "date"
            assert columns["status"]["udt_name"] == "cycle_status"
            assert columns["status"]["column_default"] == (
                "'pending'::data_platform.cycle_status"
            )
            assert columns["cutoff_submitted_at"]["data_type"] == "timestamp with time zone"
            assert columns["cutoff_ingest_seq"]["data_type"] == "bigint"
            assert columns["candidate_count"]["data_type"] == "integer"
            assert columns["candidate_count"]["column_default"] == "0"
            assert columns["selection_frozen_at"]["data_type"] == "timestamp with time zone"
            assert columns["created_at"]["column_default"] == "now()"
            assert columns["updated_at"]["column_default"] == "now()"
            for column_name in EXPECTED_CYCLE_COLUMNS:
                expected_nullable = (
                    "YES"
                    if column_name
                    in {"cutoff_submitted_at", "cutoff_ingest_seq", "selection_frozen_at"}
                    else "NO"
                )
                assert columns[column_name]["is_nullable"] == expected_nullable

            assert {
                str(row[0])
                for row in connection.execute(
                    _text(
                        """
                        SELECT constraint_name
                        FROM information_schema.table_constraints
                        WHERE table_schema = 'data_platform'
                          AND table_name = 'cycle_metadata'
                        """
                    )
                )
            } >= {
                "cycle_metadata_pkey",
                "cycle_metadata_cycle_date_key",
                "cycle_metadata_candidate_count_nonnegative",
            }
    finally:
        engine.dispose()


def test_cycle_metadata_rejects_negative_candidate_count(migrated_cycle_dsn: str) -> None:
    sqlalchemy_exc = pytest.importorskip(
        "sqlalchemy.exc",
        reason="PostgreSQL cycle metadata tests require SQLAlchemy",
    ).IntegrityError
    engine = _create_engine(migrated_cycle_dsn)
    try:
        with pytest.raises(sqlalchemy_exc):
            with engine.begin() as connection:
                connection.execute(
                    _text(
                        """
                        INSERT INTO data_platform.cycle_metadata (
                            cycle_id,
                            cycle_date,
                            candidate_count
                        )
                        VALUES ('CYCLE_20260416', DATE '2026-04-16', -1)
                        """
                    )
                )

        with engine.begin() as connection:
            connection.execute(
                _text(
                    """
                    INSERT INTO data_platform.cycle_metadata (cycle_id, cycle_date)
                    VALUES ('CYCLE_20260416', DATE '2026-04-16')
                    """
                )
            )

        with pytest.raises(sqlalchemy_exc):
            with engine.begin() as connection:
                connection.execute(
                    _text(
                        """
                        UPDATE data_platform.cycle_metadata
                        SET candidate_count = -1
                        WHERE cycle_id = 'CYCLE_20260416'
                        """
                    )
                )
    finally:
        engine.dispose()


def test_create_cycle_returns_pending_metadata(migrated_cycle_dsn: str) -> None:
    cycle = create_cycle(date(2026, 4, 16))

    assert cycle.cycle_id == "CYCLE_20260416"
    assert cycle.cycle_date == date(2026, 4, 16)
    assert cycle.status == "pending"
    assert cycle.cutoff_submitted_at is None
    assert cycle.cutoff_ingest_seq is None
    assert cycle.candidate_count == 0
    assert cycle.selection_frozen_at is None
    assert cycle.created_at is not None
    assert cycle.updated_at is not None
    assert get_cycle("CYCLE_20260416") == cycle


def test_create_cycle_rejects_duplicate_cycle_date(migrated_cycle_dsn: str) -> None:
    create_cycle(date(2026, 4, 16))

    with pytest.raises(CycleAlreadyExists, match="2026-04-16"):
        create_cycle(date(2026, 4, 16))

    assert _fetch_scalar(
        migrated_cycle_dsn,
        "SELECT count(*) FROM data_platform.cycle_metadata",
    ) == 1


def test_get_cycle_raises_for_missing_cycle(migrated_cycle_dsn: str) -> None:
    with pytest.raises(CycleNotFound, match="CYCLE_20990101"):
        get_cycle("CYCLE_20990101")


def test_transition_cycle_status_allows_ordered_path(migrated_cycle_dsn: str) -> None:
    cycle = create_cycle(date(2026, 4, 16))

    for status in ORDERED_STATUSES:
        cycle = transition_cycle_status(cycle.cycle_id, status)
        assert cycle.status == status

    assert get_cycle("CYCLE_20260416").status == "published"


def test_transition_cycle_status_rejects_direct_publish(migrated_cycle_dsn: str) -> None:
    create_cycle(date(2026, 4, 16))

    with pytest.raises(InvalidCycleTransition, match="from 'pending' to 'published'"):
        transition_cycle_status("CYCLE_20260416", "published")

    assert get_cycle("CYCLE_20260416").status == "pending"


def test_transition_cycle_status_allows_failed_from_nonterminal_states(
    migrated_cycle_dsn: str,
) -> None:
    source_paths: list[list[CycleStatus]] = [
        [],
        ["phase0"],
        ["phase0", "phase1"],
        ["phase0", "phase1", "phase2"],
        ["phase0", "phase1", "phase2", "phase3"],
    ]

    for index, source_path in enumerate(source_paths, start=16):
        cycle = create_cycle(date(2026, 4, index))
        for status in source_path:
            cycle = transition_cycle_status(cycle.cycle_id, status)

        failed_cycle = transition_cycle_status(cycle.cycle_id, "failed")

        assert failed_cycle.status == "failed"
        with pytest.raises(InvalidCycleTransition):
            transition_cycle_status(cycle.cycle_id, "phase0")


def test_transition_cycle_status_rejects_changes_after_published(
    migrated_cycle_dsn: str,
) -> None:
    cycle = create_cycle(date(2026, 4, 16))
    for status in ORDERED_STATUSES:
        cycle = transition_cycle_status(cycle.cycle_id, status)

    with pytest.raises(InvalidCycleTransition, match="from 'published' to 'failed'"):
        transition_cycle_status(cycle.cycle_id, "failed")


def test_transition_cycle_status_raises_for_missing_cycle(migrated_cycle_dsn: str) -> None:
    with pytest.raises(CycleNotFound, match="CYCLE_20990101"):
        transition_cycle_status("CYCLE_20990101", "phase0")


def _create_engine(dsn: str, **kwargs: object) -> Any:
    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL cycle metadata tests require SQLAlchemy",
    )
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL cycle metadata tests require the migration runner",
    )
    return sqlalchemy.create_engine(runner_module._sqlalchemy_postgres_uri(dsn), **kwargs)


def _text(sql: str) -> Any:
    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL cycle metadata tests require SQLAlchemy",
    )
    return sqlalchemy.text(sql)


def _enum_labels(connection: Any, type_name: str) -> list[str]:
    rows = connection.execute(
        _text(
            """
            SELECT pg_enum.enumlabel
            FROM pg_enum
            JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
            JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
            WHERE pg_namespace.nspname = 'data_platform'
              AND pg_type.typname = :type_name
            ORDER BY pg_enum.enumsortorder
            """
        ),
        {"type_name": type_name},
    ).scalars()
    return [str(row) for row in rows]


def _fetch_scalar(dsn: str, sql: str) -> object:
    engine = _create_engine(dsn)
    try:
        with engine.connect() as connection:
            return connection.execute(_text(sql)).scalar_one()
    finally:
        engine.dispose()
