from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest
from pyiceberg.catalog.memory import InMemoryCatalog

from data_platform.ddl.iceberg_tables import TIMESTAMP_TYPE
from data_platform.serving.schema_evolution import (
    SchemaChange,
    apply_schema_evolution,
    plan_schema_evolution,
    run_canonical_backfill,
)


def test_plan_schema_evolution_add_column_requires_backfill() -> None:
    current_schema = pa.schema([pa.field("ts_code", pa.string())])
    target_schema = current_schema.append(pa.field("source_run_id", pa.string()))

    plan = plan_schema_evolution("canonical.stock_basic", current_schema, target_schema)

    assert plan.changes == [
        SchemaChange(
            kind="add_column",
            field_name="source_run_id",
            from_type=None,
            to_type="string",
        )
    ]
    assert plan.rejections == []
    assert plan.requires_backfill is True


def test_plan_schema_evolution_allows_whitelisted_type_widening() -> None:
    current_schema = pa.schema(
        [
            pa.field("trade_count", pa.int32()),
            pa.field("price", pa.float32()),
            pa.field("name", pa.string()),
        ]
    )
    target_schema = pa.schema(
        [
            pa.field("trade_count", pa.int64()),
            pa.field("price", pa.float64()),
            pa.field("name", pa.string()),
        ]
    )

    plan = plan_schema_evolution("canonical.fact_price_bar", current_schema, target_schema)

    assert plan.changes == [
        SchemaChange(
            kind="widen_type",
            field_name="trade_count",
            from_type="int32",
            to_type="int64",
        ),
        SchemaChange(
            kind="widen_type",
            field_name="price",
            from_type="float32",
            to_type="float64",
        ),
    ]
    assert plan.rejections == []
    assert plan.requires_backfill is False


@pytest.mark.parametrize(
    "target_schema, expected_rejection",
    [
        (
            pa.schema([pa.field("ts_code", pa.string())]),
            "drop column is not supported",
        ),
        (
            pa.schema(
                [
                    pa.field("ts_code", pa.string()),
                    pa.field("security_name", pa.string()),
                ]
            ),
            "rename column is not supported",
        ),
        (
            pa.schema(
                [
                    pa.field("ts_code", pa.string()),
                    pa.field("name", pa.int32()),
                ]
            ),
            "type change is not allowed",
        ),
    ],
)
def test_plan_schema_evolution_rejects_breaking_changes(
    target_schema: pa.Schema,
    expected_rejection: str,
) -> None:
    current_schema = pa.schema(
        [
            pa.field("ts_code", pa.string()),
            pa.field("name", pa.string()),
        ]
    )

    plan = plan_schema_evolution("canonical.stock_basic", current_schema, target_schema)

    assert any(expected_rejection in rejection for rejection in plan.rejections)


def test_apply_schema_evolution_add_column_preserves_old_snapshot(
    tmp_path: Path,
) -> None:
    catalog = _create_catalog(tmp_path)
    table = catalog.create_table(
        "canonical.stock_basic",
        schema=pa.schema([pa.field("ts_code", pa.string())]),
    )
    table.append(pa.table({"ts_code": ["000001.SZ"]}))
    table = table.refresh()
    old_snapshot = table.current_snapshot()
    assert old_snapshot is not None

    target_schema = pa.schema(
        [
            pa.field("ts_code", pa.string()),
            pa.field("source_run_id", pa.string()),
        ]
    )
    dry_run_plan = apply_schema_evolution(
        catalog,  # type: ignore[arg-type]
        "canonical.stock_basic",
        target_schema,
    )
    applied_plan = apply_schema_evolution(
        catalog,  # type: ignore[arg-type]
        "canonical.stock_basic",
        target_schema,
        dry_run=False,
    )

    refreshed = catalog.load_table("canonical.stock_basic")
    current_rows = refreshed.scan().to_arrow()
    old_rows = refreshed.scan(snapshot_id=old_snapshot.snapshot_id).to_arrow()

    assert dry_run_plan.requires_backfill is True
    assert applied_plan == dry_run_plan
    assert current_rows.schema.names == ["ts_code", "source_run_id"]
    assert current_rows.to_pylist() == [
        {"ts_code": "000001.SZ", "source_run_id": None}
    ]
    assert old_rows.schema.names == ["ts_code"]
    assert old_rows.to_pylist() == [{"ts_code": "000001.SZ"}]


def test_apply_schema_evolution_widens_column_type(tmp_path: Path) -> None:
    catalog = _create_catalog(tmp_path)
    catalog.create_table(
        "canonical.fact_price_bar",
        schema=pa.schema([pa.field("trade_count", pa.int32())]),
    )
    target_schema = pa.schema([pa.field("trade_count", pa.int64())])

    plan = apply_schema_evolution(
        catalog,  # type: ignore[arg-type]
        "canonical.fact_price_bar",
        target_schema,
        dry_run=False,
    )

    refreshed = catalog.load_table("canonical.fact_price_bar")
    assert plan.rejections == []
    assert plan.changes == [
        SchemaChange(
            kind="widen_type",
            field_name="trade_count",
            from_type="int32",
            to_type="int64",
        )
    ]
    assert refreshed.schema().as_arrow().field("trade_count").type.equals(pa.int64())


def test_run_canonical_backfill_dry_run_does_not_write_snapshot(tmp_path: Path) -> None:
    catalog = _create_catalog(tmp_path)
    catalog.create_table("canonical.backfill_target", schema=_backfill_target_schema())
    duckdb_path = tmp_path / "backfill.duckdb"
    _write_backfill_source(duckdb_path)

    result = run_canonical_backfill(
        catalog,  # type: ignore[arg-type]
        duckdb_path,
        "canonical.backfill_target",
        "SELECT id, value FROM source_rows",
        dry_run=True,
    )

    assert result is None
    assert catalog.load_table("canonical.backfill_target").current_snapshot() is None


def test_run_canonical_backfill_writes_with_write_result(tmp_path: Path) -> None:
    catalog = _create_catalog(tmp_path)
    catalog.create_table("canonical.backfill_target", schema=_backfill_target_schema())
    duckdb_path = tmp_path / "backfill.duckdb"
    _write_backfill_source(duckdb_path)

    result = run_canonical_backfill(
        catalog,  # type: ignore[arg-type]
        duckdb_path,
        "canonical.backfill_target",
        "SELECT id, value FROM source_rows",
    )

    rows = catalog.load_table("canonical.backfill_target").scan().to_arrow()
    assert result is not None
    assert result.table == "canonical.backfill_target"
    assert result.row_count == 2
    assert result.snapshot_id
    assert rows.schema.names == ["id", "value", "canonical_loaded_at"]
    assert [row["id"] for row in rows.to_pylist()] == ["a", "b"]


def _create_catalog(tmp_path: Path) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test", warehouse=f"file://{tmp_path / 'warehouse'}")
    catalog.create_namespace_if_not_exists(("canonical",))
    return catalog


def _backfill_target_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("value", pa.int64()),
            pa.field("canonical_loaded_at", TIMESTAMP_TYPE),
        ]
    )


def _write_backfill_source(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE OR REPLACE TABLE source_rows AS
            SELECT 'a'::VARCHAR AS id, 1::BIGINT AS value
            UNION ALL
            SELECT 'b'::VARCHAR AS id, 2::BIGINT AS value
            """
        )
    finally:
        connection.close()
