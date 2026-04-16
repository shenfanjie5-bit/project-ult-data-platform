"""Canonical read APIs backed by DuckDB Iceberg scans."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from functools import lru_cache
import json
from pathlib import Path
import re
from threading import RLock
from typing import Any

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]

from data_platform.config import get_settings


CANONICAL_NAMESPACE = "canonical"
TABLE_STOCK_BASIC = "stock_basic"
CANONICAL_MART_SNAPSHOT_SET_FILE = "_mart_snapshot_set.json"
CANONICAL_MART_TABLES = frozenset(
    {
        "dim_security",
        "dim_index",
        "fact_price_bar",
        "fact_financial_indicator",
        "fact_event",
    }
)
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONNECTION_LOCK = RLock()


class CanonicalTableNotFound(LookupError):
    """Raised when a canonical Iceberg table cannot be read."""

    def __init__(self, table: str) -> None:
        self.table = table
        super().__init__(f"canonical table not found: {table}")


class UnsupportedFilter(ValueError):
    """Raised when a read_canonical filter is outside the supported contract."""

    def __init__(self, filter_spec: object) -> None:
        self.filter_spec = filter_spec
        super().__init__(f"unsupported canonical filter: {filter_spec!r}")


def read_canonical(
    table: str,
    columns: list[str] | None = None,
    filters: list[tuple[str, str, Any]] | None = None,
) -> pa.Table:
    """Read one canonical Iceberg table as a PyArrow table."""

    _validate_identifier(table)
    select_list = _compile_select_list(columns)
    where_clause, parameters = _compile_filters(filters)
    sql = f"""
SELECT {select_list}
FROM {_canonical_table_expression(table)}
{where_clause}
"""

    with with_duckdb_connection() as connection:
        try:
            return connection.execute(sql, parameters).to_arrow_table()
        except duckdb.Error as exc:
            if _is_missing_table_error(exc, table):
                raise CanonicalTableNotFound(table) from exc
            raise


def get_canonical_stock_basic(active_only: bool = True) -> pa.Table:
    """Read canonical.stock_basic, filtering to active rows by default."""

    filters = [("is_active", "=", True)] if active_only else None
    return read_canonical(TABLE_STOCK_BASIC, filters=filters)


@contextmanager
def with_duckdb_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield the process-global DuckDB connection for canonical reads."""

    connection = _duckdb_connection()
    with _CONNECTION_LOCK:
        yield connection


@lru_cache(maxsize=1)
def _duckdb_connection() -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    connection = duckdb.connect(str(settings.duckdb_path))
    _load_iceberg_extension(connection)
    return connection


def _load_iceberg_extension(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        connection.execute("LOAD iceberg")
    except duckdb.Error:
        connection.execute("INSTALL iceberg")
        connection.execute("LOAD iceberg")


def _compile_select_list(columns: list[str] | None) -> str:
    if columns is None:
        return "*"
    if not columns:
        msg = "columns must not be empty"
        raise ValueError(msg)
    return ", ".join(_quote_identifier(column) for column in columns)


def _compile_filters(
    filters: list[tuple[str, str, Any]] | None,
) -> tuple[str, list[Any]]:
    if not filters:
        return "", []

    clauses: list[str] = []
    parameters: list[Any] = []
    for filter_spec in filters:
        if len(filter_spec) != 3:
            raise UnsupportedFilter(filter_spec)

        column, operator, value = filter_spec
        quoted_column = _quote_identifier(column)
        normalized_operator = operator.strip().lower()
        if normalized_operator == "=":
            clauses.append(f"{quoted_column} = ?")
            parameters.append(value)
        elif normalized_operator == "in":
            values = _filter_values(filter_spec, value)
            if values:
                placeholders = ", ".join("?" for _ in values)
                clauses.append(f"{quoted_column} IN ({placeholders})")
                parameters.extend(values)
            else:
                clauses.append("FALSE")
        else:
            raise UnsupportedFilter(filter_spec)

    return "WHERE " + " AND ".join(clauses), parameters


def _filter_values(filter_spec: object, value: object) -> list[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise UnsupportedFilter(filter_spec)
    return list(value)


def _canonical_table_expression(table: str) -> str:
    snapshot_entry = _canonical_mart_snapshot_entry(table)
    if snapshot_entry is not None:
        metadata_location = str(snapshot_entry["metadata_location"])
        snapshot_id = int(snapshot_entry["snapshot_id"])
        return (
            f"iceberg_scan({_sql_string_literal(metadata_location)}, "
            f"snapshot_from_id = {snapshot_id})"
        )
    if table in CANONICAL_MART_TABLES:
        raise CanonicalTableNotFound(table)

    latest_metadata_location = _latest_metadata_location(table)
    return f"iceberg_scan({_sql_string_literal(str(latest_metadata_location))})"


def _canonical_table_location(table: str) -> Path:
    warehouse_path = get_settings().iceberg_warehouse_path.expanduser()
    return warehouse_path / CANONICAL_NAMESPACE / table


def _latest_metadata_location(table: str) -> Path:
    metadata_dir = _canonical_table_location(table) / "metadata"
    metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
    if metadata_files:
        return metadata_files[-1]
    raise CanonicalTableNotFound(table)


def _canonical_mart_snapshot_entry(table: str) -> dict[str, str | int] | None:
    manifest_path = _canonical_mart_snapshot_manifest_path()
    if not manifest_path.exists():
        return None

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    tables = payload.get("tables", {})
    if not isinstance(tables, dict):
        return None
    entry = tables.get(table)
    if not isinstance(entry, dict):
        return None
    metadata_location = entry.get("metadata_location")
    snapshot_id = entry.get("snapshot_id")
    if isinstance(metadata_location, str) and isinstance(snapshot_id, (int, str)):
        return {"metadata_location": metadata_location, "snapshot_id": snapshot_id}
    return None


def _canonical_mart_snapshot_manifest_path() -> Path:
    warehouse_path = get_settings().iceberg_warehouse_path.expanduser()
    return warehouse_path / CANONICAL_NAMESPACE / CANONICAL_MART_SNAPSHOT_SET_FILE


def _quote_identifier(identifier: str) -> str:
    _validate_identifier(identifier)
    return f'"{identifier}"'


def _validate_identifier(identifier: str) -> None:
    if _IDENTIFIER_PATTERN.fullmatch(identifier):
        return
    msg = f"invalid SQL identifier: {identifier!r}"
    raise ValueError(msg)


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _is_missing_table_error(exc: duckdb.Error, table: str) -> bool:
    detail = str(exc).lower()
    expected_location = str(_canonical_table_location(table)).lower()
    missing_markers = ("does not exist", "no such file", "not found", "cannot open")
    return (
        table.lower() in detail or expected_location in detail
    ) and any(marker in detail for marker in missing_markers)


__all__ = [
    "CanonicalTableNotFound",
    "UnsupportedFilter",
    "get_canonical_stock_basic",
    "read_canonical",
    "with_duckdb_connection",
]
