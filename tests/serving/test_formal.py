from __future__ import annotations

import os
from collections.abc import Generator, Mapping
from dataclasses import FrozenInstanceError, fields, is_dataclass
from datetime import UTC, date, datetime
import importlib.util
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from data_platform.cycle.manifest import (
    CyclePublishManifest,
    FormalTableSnapshot,
    PublishManifestNotFound,
)

FORMAL_DEPS_MISSING = (
    importlib.util.find_spec("pyarrow") is None
    or importlib.util.find_spec("pyiceberg") is None
)
pytestmark = pytest.mark.skipif(
    FORMAL_DEPS_MISSING,
    reason="formal serving tests require PyArrow and PyIceberg",
)


FORMAL_IDENTIFIER = "formal.recommendation_set"


@pytest.fixture()
def pa_module() -> Any:
    return pytest.importorskip("pyarrow", reason="formal serving tests require PyArrow")


@pytest.fixture()
def memory_catalog_class() -> Any:
    module = pytest.importorskip(
        "pyiceberg.catalog.memory",
        reason="formal serving tests require PyIceberg",
    )
    return module.InMemoryCatalog


@pytest.fixture()
def formal_module() -> Any:
    from data_platform.serving import formal

    return formal


def test_formal_object_model_exposes_contract(
    formal_module: Any,
    pa_module: Any,
) -> None:
    payload = pa_module.table(
        {"version": ["v1"], "score": [1]},
        schema=formal_schema(pa_module),
    )
    formal_object = formal_module.FormalObject(
        cycle_id="CYCLE_20260416",
        object_type="recommendation_set",
        snapshot_id=123,
        payload=payload,
    )

    assert is_dataclass(formal_module.FormalObject)
    assert [field.name for field in fields(formal_module.FormalObject)] == [
        "cycle_id",
        "object_type",
        "snapshot_id",
        "payload",
    ]
    assert formal_module.FormalObject.__slots__ == (
        "cycle_id",
        "object_type",
        "snapshot_id",
        "payload",
    )
    assert formal_object.payload == payload
    with pytest.raises(FrozenInstanceError):
        formal_object.snapshot_id = 456


def test_formal_table_identifier_validates_object_type(formal_module: Any) -> None:
    assert formal_module.formal_table_identifier("recommendation_set") == FORMAL_IDENTIFIER

    for object_type in ["formal.recommendation_set", "recommendation-set", "", " raw "]:
        with pytest.raises(formal_module.FormalObjectTypeInvalid):
            formal_module.formal_table_identifier(object_type)


def test_latest_and_by_id_read_manifest_snapshots_not_formal_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    formal_module: Any,
    memory_catalog_class: Any,
    pa_module: Any,
) -> None:
    catalog = create_formal_catalog(tmp_path, memory_catalog_class, pa_module)
    old_snapshot_id = write_formal_snapshot(catalog, pa_module, version="old", score=1)
    latest_snapshot_id = write_formal_snapshot(
        catalog,
        pa_module,
        version="latest",
        score=2,
    )
    unpublished_head_snapshot_id = write_formal_snapshot(
        catalog,
        pa_module,
        version="unpublished-head",
        score=3,
    )
    old_manifest = make_manifest("CYCLE_20260416", old_snapshot_id)
    latest_manifest = make_manifest("CYCLE_20260417", latest_snapshot_id)

    monkeypatch.setattr(formal_module, "load_catalog", lambda: catalog)
    monkeypatch.setattr(
        formal_module,
        "get_latest_publish_manifest",
        lambda: latest_manifest,
    )
    monkeypatch.setattr(
        formal_module,
        "get_publish_manifest",
        lambda cycle_id: {
            "CYCLE_20260416": old_manifest,
            "CYCLE_20260417": latest_manifest,
        }[cycle_id],
    )

    latest = formal_module.get_formal_latest("recommendation_set")
    old = formal_module.get_formal_by_id("CYCLE_20260416", "recommendation_set")

    assert latest.cycle_id == "CYCLE_20260417"
    assert latest.snapshot_id == latest_snapshot_id
    assert latest.snapshot_id != unpublished_head_snapshot_id
    assert latest.payload.column("version").to_pylist() == ["latest"]
    assert latest.payload.column("score").to_pylist() == [2]

    assert old.cycle_id == "CYCLE_20260416"
    assert old.snapshot_id == old_snapshot_id
    assert old.payload.column("version").to_pylist() == ["old"]
    assert old.payload.column("score").to_pylist() == [1]


def test_by_snapshot_reads_only_published_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    formal_module: Any,
    memory_catalog_class: Any,
    pa_module: Any,
) -> None:
    catalog = create_formal_catalog(tmp_path, memory_catalog_class, pa_module)
    published_snapshot_id = write_formal_snapshot(
        catalog,
        pa_module,
        version="published",
        score=10,
    )
    published_manifest = make_manifest("CYCLE_20260416", published_snapshot_id)

    def published_lookup(snapshot_id: int, table_identifier: str) -> CyclePublishManifest:
        assert table_identifier == FORMAL_IDENTIFIER
        if snapshot_id == published_snapshot_id:
            return published_manifest
        raise formal_module.FormalSnapshotNotPublished(snapshot_id, table_identifier)

    monkeypatch.setattr(formal_module, "load_catalog", lambda: catalog)
    monkeypatch.setattr(
        formal_module,
        "get_publish_manifest_for_snapshot",
        published_lookup,
    )

    formal_object = formal_module.get_formal_by_snapshot(
        published_snapshot_id,
        "recommendation_set",
    )

    assert formal_object.cycle_id == "CYCLE_20260416"
    assert formal_object.snapshot_id == published_snapshot_id
    assert formal_object.payload.column("version").to_pylist() == ["published"]
    assert formal_object.payload.column("score").to_pylist() == [10]


def test_by_snapshot_rejects_unpublished_current_head_before_reading_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    formal_module: Any,
    memory_catalog_class: Any,
    pa_module: Any,
) -> None:
    catalog = create_formal_catalog(tmp_path, memory_catalog_class, pa_module)
    unpublished_snapshot_id = write_formal_snapshot(
        catalog,
        pa_module,
        version="head",
        score=99,
    )

    def unpublished_lookup(snapshot_id: int, table_identifier: str) -> CyclePublishManifest:
        assert snapshot_id == unpublished_snapshot_id
        raise formal_module.FormalSnapshotNotPublished(snapshot_id, table_identifier)

    def fail_load_catalog() -> object:
        pytest.fail("unpublished snapshot must be rejected before Iceberg read")

    monkeypatch.setattr(formal_module, "load_catalog", fail_load_catalog)
    monkeypatch.setattr(
        formal_module,
        "get_publish_manifest_for_snapshot",
        unpublished_lookup,
    )

    with pytest.raises(formal_module.FormalSnapshotNotPublished):
        formal_module.get_formal_by_snapshot(unpublished_snapshot_id, "recommendation_set")


def test_by_snapshot_uses_db_backed_manifest_lookup(
    formal_postgres_env: str,
    monkeypatch: pytest.MonkeyPatch,
    formal_module: Any,
    pa_module: Any,
) -> None:
    target_snapshot_id = 98123
    unpublished_snapshot_id = 98124
    target_cycle_id = "CYCLE_20260201"

    for day in range(1, 26):
        _publish_formal_manifest(
            date(2026, 1, day),
            {"formal.other_object": 70000 + day},
        )
    _publish_formal_manifest(
        date(2026, 2, 1),
        {FORMAL_IDENTIFIER: target_snapshot_id},
    )
    for day in range(2, 10):
        _publish_formal_manifest(
            date(2026, 2, day),
            {"formal.other_object": 80000 + day},
        )

    monkeypatch.setattr(
        formal_module,
        "load_catalog",
        lambda: _fake_snapshot_catalog(
            pa_module,
            expected_snapshot_id=target_snapshot_id,
        ),
    )

    formal_object = formal_module.get_formal_by_snapshot(
        target_snapshot_id,
        "recommendation_set",
    )

    assert formal_object.cycle_id == target_cycle_id
    assert formal_object.snapshot_id == target_snapshot_id
    assert formal_object.payload.column("version").to_pylist() == [
        f"snapshot-{target_snapshot_id}"
    ]
    assert formal_object.payload.column("score").to_pylist() == [target_snapshot_id]

    monkeypatch.setattr(
        formal_module,
        "load_catalog",
        lambda: pytest.fail("unpublished snapshot must not read formal table head"),
    )
    with pytest.raises(formal_module.FormalSnapshotNotPublished):
        formal_module.get_formal_by_snapshot(
            unpublished_snapshot_id,
            "recommendation_set",
        )


def test_missing_manifest_raises_formal_manifest_not_found(
    monkeypatch: pytest.MonkeyPatch,
    formal_module: Any,
) -> None:
    def missing_latest() -> CyclePublishManifest:
        raise PublishManifestNotFound()

    def missing_cycle(cycle_id: str) -> CyclePublishManifest:
        raise PublishManifestNotFound(cycle_id)

    monkeypatch.setattr(formal_module, "get_latest_publish_manifest", missing_latest)
    monkeypatch.setattr(formal_module, "get_publish_manifest", missing_cycle)

    with pytest.raises(formal_module.FormalManifestNotFound):
        formal_module.get_formal_latest("recommendation_set")
    with pytest.raises(formal_module.FormalManifestNotFound):
        formal_module.get_formal_by_id("CYCLE_20260416", "recommendation_set")


def test_manifest_without_object_type_raises_table_snapshot_not_found(
    monkeypatch: pytest.MonkeyPatch,
    formal_module: Any,
) -> None:
    manifest = make_manifest(
        "CYCLE_20260416",
        snapshot_id=123,
        table_identifier="formal.other_object",
    )

    def fail_load_catalog() -> object:
        pytest.fail("missing manifest entry must be rejected before Iceberg read")

    monkeypatch.setattr(formal_module, "get_latest_publish_manifest", lambda: manifest)
    monkeypatch.setattr(formal_module, "load_catalog", fail_load_catalog)

    with pytest.raises(formal_module.FormalTableSnapshotNotFound):
        formal_module.get_formal_latest("recommendation_set")


def formal_schema(pa_module: Any) -> Any:
    return pa_module.schema(
        [
            pa_module.field("version", pa_module.string()),
            pa_module.field("score", pa_module.int64()),
        ]
    )


def create_formal_catalog(
    tmp_path: Path,
    memory_catalog_class: Any,
    pa_module: Any,
) -> Any:
    catalog = memory_catalog_class(
        "test",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )
    catalog.create_namespace_if_not_exists(("formal",))
    catalog.create_table(FORMAL_IDENTIFIER, schema=formal_schema(pa_module))
    return catalog


def write_formal_snapshot(
    catalog: Any,
    pa_module: Any,
    *,
    version: str,
    score: int,
) -> int:
    table = catalog.load_table(FORMAL_IDENTIFIER)
    table.overwrite(
        pa_module.table(
            {"version": [version], "score": [score]},
            schema=formal_schema(pa_module),
        )
    )
    snapshot = table.refresh().current_snapshot()
    if snapshot is None:
        raise AssertionError("formal table overwrite did not create a snapshot")
    return int(snapshot.snapshot_id)


def make_manifest(
    cycle_id: str,
    snapshot_id: int,
    *,
    table_identifier: str = FORMAL_IDENTIFIER,
) -> CyclePublishManifest:
    return CyclePublishManifest(
        published_cycle_id=cycle_id,
        published_at=datetime(2026, 4, 16, 10, 30, tzinfo=UTC),
        formal_table_snapshots={
            table_identifier: FormalTableSnapshot(
                table=table_identifier,
                snapshot_id=snapshot_id,
            )
        },
    )


@pytest.fixture()
def postgres_dsn() -> Generator[str]:
    admin_dsn = os.environ.get("DATABASE_URL") or os.environ.get("DP_PG_DSN")
    if not admin_dsn:
        pytest.skip("PostgreSQL formal serving tests require DATABASE_URL or DP_PG_DSN")

    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL formal serving tests require SQLAlchemy",
    )
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL formal serving tests require the migration runner",
    )
    make_url = pytest.importorskip(
        "sqlalchemy.engine",
        reason="PostgreSQL formal serving tests require SQLAlchemy",
    ).make_url
    sqlalchemy_error = pytest.importorskip(
        "sqlalchemy.exc",
        reason="PostgreSQL formal serving tests require SQLAlchemy",
    ).SQLAlchemyError

    admin_engine = _create_engine(admin_dsn, isolation_level="AUTOCOMMIT")
    database_name = f"dp_formal_serving_test_{uuid4().hex}"
    try:
        with admin_engine.connect() as connection:
            connection.execute(sqlalchemy.text(f'CREATE DATABASE "{database_name}"'))
    except sqlalchemy_error as exc:
        admin_engine.dispose()
        pytest.skip(
            "PostgreSQL formal serving tests require permission to create "
            f"test databases: {exc}"
        )

    test_dsn = str(
        make_url(runner_module._sqlalchemy_postgres_uri(admin_dsn)).set(
            database=database_name,
        )
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
def migrated_postgres_dsn(postgres_dsn: str) -> str:
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL formal serving tests require the migration runner",
    )
    runner_module.MigrationRunner().apply_pending(postgres_dsn)
    assert runner_module.MigrationRunner().apply_pending(postgres_dsn) == []
    return postgres_dsn


@pytest.fixture()
def formal_postgres_env(
    migrated_postgres_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[str]:
    monkeypatch.setenv("DP_PG_DSN", migrated_postgres_dsn)
    yield migrated_postgres_dsn


def _publish_formal_manifest(
    cycle_date: date,
    snapshots: Mapping[str, int],
) -> None:
    from data_platform.cycle import (
        create_cycle,
        publish_manifest,
        transition_cycle_status,
    )

    cycle = create_cycle(cycle_date)
    for status in ("phase0", "phase1", "phase2", "phase3"):
        transition_cycle_status(cycle.cycle_id, status)
    publish_manifest(cycle.cycle_id, snapshots)


def _fake_snapshot_catalog(
    pa_module: Any,
    *,
    expected_snapshot_id: int,
) -> object:
    class FakeScan:
        def to_arrow(self) -> Any:
            return pa_module.table(
                {
                    "version": [f"snapshot-{expected_snapshot_id}"],
                    "score": [expected_snapshot_id],
                },
                schema=formal_schema(pa_module),
            )

    class FakeTable:
        def scan(self, *, snapshot_id: int) -> FakeScan:
            assert snapshot_id == expected_snapshot_id
            return FakeScan()

    class FakeCatalog:
        def load_table(self, table_identifier: str) -> FakeTable:
            assert table_identifier == FORMAL_IDENTIFIER
            return FakeTable()

    return FakeCatalog()


def _create_engine(dsn: str, **kwargs: object) -> Any:
    sqlalchemy = pytest.importorskip(
        "sqlalchemy",
        reason="PostgreSQL formal serving tests require SQLAlchemy",
    )
    runner_module = pytest.importorskip(
        "data_platform.ddl.runner",
        reason="PostgreSQL formal serving tests require the migration runner",
    )
    return sqlalchemy.create_engine(runner_module._sqlalchemy_postgres_uri(dsn), **kwargs)
