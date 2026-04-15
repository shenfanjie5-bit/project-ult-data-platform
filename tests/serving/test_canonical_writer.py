from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
from typing import Sequence

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest
from pyiceberg.catalog.memory import InMemoryCatalog

from data_platform.ddl.iceberg_tables import CANONICAL_STOCK_BASIC_SPEC
from data_platform.serving import canonical_writer
from data_platform.serving.canonical_writer import (
    CANONICAL_STOCK_BASIC_IDENTIFIER,
    FORBIDDEN_PAYLOAD_FIELDS,
    load_canonical_stock_basic,
)


StockBasicRow = tuple[str, str, str, str, str, str, date, bool, str]


@dataclass(frozen=True, slots=True)
class FakeSettings:
    duckdb_path: Path


def test_load_canonical_stock_basic_overwrites_idempotently(tmp_path: Path) -> None:
    catalog = create_stock_basic_catalog(tmp_path)
    duckdb_path = tmp_path / "staging.duckdb"
    rows = stock_basic_rows()
    write_staging_stock_basic(duckdb_path, rows)

    first = load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]
    second = load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]

    assert first.table == CANONICAL_STOCK_BASIC_IDENTIFIER
    assert first.row_count == len(rows)
    assert second.row_count == len(rows)
    assert second.snapshot_id != first.snapshot_id

    table = catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER)
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None
    assert current_snapshot.snapshot_id == second.snapshot_id

    current_rows = table.scan().to_arrow()
    old_rows = table.scan(snapshot_id=first.snapshot_id).to_arrow()
    assert current_rows.num_rows == len(rows)
    assert old_rows.num_rows == len(rows)
    assert current_rows.schema.names == CANONICAL_STOCK_BASIC_SPEC.schema.names


def test_load_canonical_stock_basic_does_not_propagate_queue_fields(
    tmp_path: Path,
) -> None:
    catalog = create_stock_basic_catalog(tmp_path)
    duckdb_path = tmp_path / "staging.duckdb"
    write_staging_stock_basic(duckdb_path, stock_basic_rows(), include_queue_fields=True)

    load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]

    payload = catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER).scan().to_arrow()
    assert not FORBIDDEN_PAYLOAD_FIELDS.intersection(
        field_name.lower() for field_name in payload.schema.names
    )


@pytest.mark.parametrize(
    "target_schema, expected_error",
    [
        (
            pa.schema(
                [
                    field
                    for field in CANONICAL_STOCK_BASIC_SPEC.schema
                    if field.name != "market"
                ]
            ),
            "extra payload fields: market",
        ),
        (
            CANONICAL_STOCK_BASIC_SPEC.schema.append(pa.field("extra", pa.string())),
            "missing payload fields: extra",
        ),
    ],
)
def test_load_canonical_stock_basic_rejects_target_payload_field_mismatch(
    tmp_path: Path,
    target_schema: pa.Schema,
    expected_error: str,
) -> None:
    catalog = create_stock_basic_catalog(tmp_path, schema=target_schema)
    duckdb_path = tmp_path / "staging.duckdb"
    write_staging_stock_basic(duckdb_path, stock_basic_rows())

    with pytest.raises(ValueError, match=expected_error):
        load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]

    assert catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER).current_snapshot() is None


def test_overwrite_failure_does_not_refresh_or_report_snapshot(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "staging.duckdb"
    rows = stock_basic_rows()
    write_staging_stock_basic(duckdb_path, rows)

    class FailingTable:
        schema = CANONICAL_STOCK_BASIC_SPEC.schema

        def __init__(self) -> None:
            self.overwritten_rows: int | None = None
            self.refresh_called = False

        def overwrite(self, table_arrow: pa.Table) -> None:
            self.overwritten_rows = table_arrow.num_rows
            raise RuntimeError("overwrite failed")

        def refresh(self) -> FailingTable:
            self.refresh_called = True
            return self

    class FakeCatalog:
        def __init__(self) -> None:
            self.table = FailingTable()

        def load_table(self, identifier: str) -> FailingTable:
            assert identifier == CANONICAL_STOCK_BASIC_IDENTIFIER
            return self.table

    catalog = FakeCatalog()

    with pytest.raises(RuntimeError, match="overwrite failed"):
        load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]

    assert catalog.table.overwritten_rows == len(rows)
    assert catalog.table.refresh_called is False


def test_cli_writes_result_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    catalog = create_stock_basic_catalog(tmp_path)
    duckdb_path = tmp_path / "staging.duckdb"
    write_staging_stock_basic(duckdb_path, stock_basic_rows())
    monkeypatch.setattr(canonical_writer, "load_catalog", lambda: catalog)
    monkeypatch.setattr(canonical_writer, "get_settings", lambda: FakeSettings(duckdb_path))

    exit_code = canonical_writer.main(["--table", "stock_basic"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["table"] == CANONICAL_STOCK_BASIC_IDENTIFIER
    assert payload["row_count"] == len(stock_basic_rows())
    assert catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER).current_snapshot() is not None


def test_cli_reports_failure_as_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = canonical_writer.main(["--table", "canonical_entity"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"] == "ValueError"
    assert "unsupported canonical table" in payload["detail"]


def create_stock_basic_catalog(
    tmp_path: Path,
    *,
    schema: pa.Schema = CANONICAL_STOCK_BASIC_SPEC.schema,
) -> InMemoryCatalog:
    warehouse_path = tmp_path / "warehouse"
    catalog = InMemoryCatalog("test", warehouse=f"file://{warehouse_path}")
    catalog.create_namespace_if_not_exists(("canonical",))
    catalog.create_table(CANONICAL_STOCK_BASIC_IDENTIFIER, schema=schema)
    return catalog


def write_staging_stock_basic(
    duckdb_path: Path,
    rows: Sequence[StockBasicRow],
    *,
    include_queue_fields: bool = False,
) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        queue_columns = (
            ', "submitted_at" TIMESTAMP, "ingest_seq" BIGINT'
            if include_queue_fields
            else ""
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE stg_stock_basic (
                "ts_code" VARCHAR,
                "symbol" VARCHAR,
                "name" VARCHAR,
                "area" VARCHAR,
                "industry" VARCHAR,
                "market" VARCHAR,
                "list_date" DATE,
                "is_active" BOOLEAN,
                "source_run_id" VARCHAR
                {queue_columns}
            )
            """
        )
        for row in rows:
            if include_queue_fields:
                connection.execute(
                    """
                    INSERT INTO stg_stock_basic VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, 1
                    )
                    """,
                    row,
                )
            else:
                connection.execute(
                    """
                    INSERT INTO stg_stock_basic VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
    finally:
        connection.close()


def stock_basic_rows() -> list[StockBasicRow]:
    return [
        (
            "000001.SZ",
            "000001",
            "Ping An Bank",
            "Shenzhen",
            "Bank",
            "Main",
            date(1991, 4, 3),
            True,
            "run-001",
        ),
        (
            "000002.SZ",
            "000002",
            "Vanke A",
            "Shenzhen",
            "Real Estate",
            "Main",
            date(1991, 1, 29),
            True,
            "run-001",
        ),
    ]
