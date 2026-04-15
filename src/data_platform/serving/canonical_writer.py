"""Canonical Iceberg writers for Lite-mode staging outputs."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import logging
from pathlib import Path
import sys
from time import perf_counter
from typing import NoReturn, Sequence

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from data_platform.config import get_settings
from data_platform.serving.catalog import load_catalog


logger = logging.getLogger(__name__)

TABLE_STOCK_BASIC = "stock_basic"
CANONICAL_STOCK_BASIC_IDENTIFIER = "canonical.stock_basic"
FORBIDDEN_PAYLOAD_FIELDS = frozenset({"submitted_at", "ingest_seq"})

STOCK_BASIC_SELECT = """
SELECT
    CAST("ts_code" AS VARCHAR) AS "ts_code",
    CAST("symbol" AS VARCHAR) AS "symbol",
    CAST("name" AS VARCHAR) AS "name",
    CAST("area" AS VARCHAR) AS "area",
    CAST("industry" AS VARCHAR) AS "industry",
    CAST("market" AS VARCHAR) AS "market",
    CAST("list_date" AS DATE) AS "list_date",
    CAST("is_active" AS BOOLEAN) AS "is_active",
    CAST("source_run_id" AS VARCHAR) AS "source_run_id",
    CAST(current_timestamp AS TIMESTAMP) AS "canonical_loaded_at"
FROM stg_stock_basic
"""


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Result of one canonical table overwrite."""

    table: str
    snapshot_id: int
    row_count: int
    duration_ms: int


class _JsonErrorArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError(message)


def load_canonical_stock_basic(
    catalog: SqlCatalog,
    duckdb_path: Path,
    *,
    allow_empty: bool = False,
) -> WriteResult:
    """Load DuckDB stg_stock_basic into canonical.stock_basic via full overwrite."""

    start = perf_counter()
    table = catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER)
    table_arrow = _read_stg_stock_basic(duckdb_path)
    _validate_no_forbidden_payload_fields(table_arrow)
    _validate_payload_fields_match_target(CANONICAL_STOCK_BASIC_IDENTIFIER, table, table_arrow)
    _validate_non_empty_staging(table_arrow, allow_empty=allow_empty)

    table.overwrite(table_arrow)
    refreshed_table = table.refresh()
    snapshot_id = _current_snapshot_id(refreshed_table)
    duration_ms = int((perf_counter() - start) * 1000)
    result = WriteResult(
        table=CANONICAL_STOCK_BASIC_IDENTIFIER,
        snapshot_id=snapshot_id,
        row_count=table_arrow.num_rows,
        duration_ms=duration_ms,
    )
    logger.info(
        "canonical_write",
        extra={
            "table": result.table,
            "rows": result.row_count,
            "snapshot": result.snapshot_id,
        },
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = _JsonErrorArgumentParser(
            description="Load staging data into canonical Iceberg tables."
        )
        parser.add_argument("--table", required=True, help="canonical table to load")
        parser.add_argument(
            "--allow-empty",
            action="store_true",
            help="intentionally publish an empty canonical table",
        )
        args = parser.parse_args(argv)
        if args.table != TABLE_STOCK_BASIC:
            msg = f"unsupported canonical table: {args.table}"
            raise ValueError(msg)
        result = load_canonical_stock_basic(
            load_catalog(),
            get_settings().duckdb_path,
            allow_empty=args.allow_empty,
        )
    except Exception as exc:
        error_payload = {"error": type(exc).__name__, "detail": str(exc)}
        print(json.dumps(error_payload, sort_keys=True), file=sys.stderr)
        return 1

    print(json.dumps(asdict(result), sort_keys=True))
    return 0


def _read_stg_stock_basic(duckdb_path: Path) -> pa.Table:
    connection = duckdb.connect(str(duckdb_path))
    try:
        return connection.execute(STOCK_BASIC_SELECT).to_arrow_table()
    finally:
        connection.close()


def _validate_no_forbidden_payload_fields(table_arrow: pa.Table) -> None:
    forbidden_fields = sorted(
        FORBIDDEN_PAYLOAD_FIELDS.intersection(
            field_name.lower() for field_name in table_arrow.schema.names
        )
    )
    if not forbidden_fields:
        return

    msg = "canonical payload must not include producer queue fields: "
    raise ValueError(msg + ", ".join(forbidden_fields))


def _validate_payload_fields_match_target(
    identifier: str,
    table: Table,
    table_arrow: pa.Table,
) -> None:
    target_fields = set(_table_field_names(table))
    payload_fields = set(table_arrow.schema.names)
    if target_fields == payload_fields:
        return

    missing = sorted(target_fields - payload_fields)
    extra = sorted(payload_fields - target_fields)
    details = []
    if missing:
        details.append("missing payload fields: " + ", ".join(missing))
    if extra:
        details.append("extra payload fields: " + ", ".join(extra))
    msg = f"{identifier} payload field set does not match target schema"
    if details:
        msg = msg + " (" + "; ".join(details) + ")"
    raise ValueError(msg)


def _validate_non_empty_staging(table_arrow: pa.Table, *, allow_empty: bool) -> None:
    if table_arrow.num_rows > 0 or allow_empty:
        return

    msg = (
        "stg_stock_basic produced zero rows; refusing to overwrite "
        "canonical.stock_basic without allow_empty=True"
    )
    raise ValueError(msg)


def _table_field_names(table: Table) -> list[str]:
    table_schema = table.schema
    schema = table_schema() if callable(table_schema) else table_schema
    if isinstance(schema, pa.Schema):
        return list(schema.names)

    as_arrow = getattr(schema, "as_arrow", None)
    if callable(as_arrow):
        arrow_schema = as_arrow()
        if isinstance(arrow_schema, pa.Schema):
            return list(arrow_schema.names)

    fields = getattr(schema, "fields", None)
    if fields is not None:
        return [str(field.name) for field in fields]

    msg = "Iceberg table schema cannot be inspected for field names"
    raise TypeError(msg)


def _current_snapshot_id(table: Table) -> int:
    snapshot = table.current_snapshot()
    if snapshot is None:
        msg = "canonical.stock_basic overwrite did not create a current snapshot"
        raise RuntimeError(msg)
    return int(snapshot.snapshot_id)


__all__ = ["WriteResult", "load_canonical_stock_basic", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
