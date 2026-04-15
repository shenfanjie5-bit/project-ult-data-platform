from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import duckdb
import pytest

from data_platform.serving import reader
from data_platform.serving.reader import (
    CanonicalTableNotFound,
    UnsupportedFilter,
    get_canonical_stock_basic,
    read_canonical,
    with_duckdb_connection,
)


StockBasicRow = tuple[str, str, str, str, str, str, date, bool, str, datetime]


@dataclass(frozen=True, slots=True)
class FakeSettings:
    duckdb_path: Path
    iceberg_warehouse_path: Path


@pytest.fixture
def stock_basic_duckdb(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    duckdb_path = tmp_path / "canonical.duckdb"
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
CREATE TABLE stock_basic (
    ts_code VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    area VARCHAR,
    industry VARCHAR,
    market VARCHAR,
    list_date DATE,
    is_active BOOLEAN,
    source_run_id VARCHAR,
    canonical_loaded_at TIMESTAMP
)
"""
        )
        connection.executemany(
            """
INSERT INTO stock_basic VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
            stock_basic_rows(),
        )
    finally:
        connection.close()

    monkeypatch.setattr(
        reader,
        "get_settings",
        lambda: FakeSettings(
            duckdb_path=duckdb_path,
            iceberg_warehouse_path=tmp_path / "warehouse",
        ),
    )
    monkeypatch.setattr(reader, "_load_iceberg_extension", lambda connection: None)
    monkeypatch.setattr(
        reader,
        "_canonical_table_expression",
        lambda table: reader._quote_identifier(table),
    )
    reader._duckdb_connection.cache_clear()
    yield duckdb_path
    reader._duckdb_connection.cache_clear()


def test_read_canonical_selects_requested_columns(stock_basic_duckdb: Path) -> None:
    table = read_canonical("stock_basic", columns=["ts_code", "name"])

    assert table.schema.names == ["ts_code", "name"]
    assert table.column("ts_code").to_pylist() == [
        "000001.SZ",
        "000002.SZ",
        "300001.SZ",
    ]


def test_read_canonical_applies_equals_filter(stock_basic_duckdb: Path) -> None:
    table = read_canonical("stock_basic", filters=[("market", "=", "主板")])

    assert table.num_rows == 2
    assert table.column("ts_code").to_pylist() == ["000001.SZ", "000002.SZ"]


def test_read_canonical_applies_in_filter(stock_basic_duckdb: Path) -> None:
    table = read_canonical(
        "stock_basic",
        columns=["ts_code", "market"],
        filters=[("market", "in", ["主板", "创业板"])],
    )

    assert table.num_rows == 3
    assert table.schema.names == ["ts_code", "market"]


def test_get_canonical_stock_basic_filters_active_rows_by_default(
    stock_basic_duckdb: Path,
) -> None:
    active = get_canonical_stock_basic()
    all_rows = get_canonical_stock_basic(active_only=False)

    assert active.num_rows == 2
    assert active.column("ts_code").to_pylist() == ["000001.SZ", "300001.SZ"]
    assert all_rows.num_rows == 3


def test_read_canonical_raises_for_missing_table(stock_basic_duckdb: Path) -> None:
    with pytest.raises(CanonicalTableNotFound) as exc_info:
        read_canonical("missing_table")

    assert exc_info.value.table == "missing_table"


@pytest.mark.parametrize(
    "filters",
    [
        [("market", ">", "主板")],
        [("market", "like", "主%")],
        [("market", "in", "主板")],
    ],
)
def test_read_canonical_rejects_unsupported_filters(
    stock_basic_duckdb: Path,
    filters: list[tuple[str, str, object]],
) -> None:
    with pytest.raises(UnsupportedFilter):
        read_canonical("stock_basic", filters=filters)


def test_with_duckdb_connection_uses_cached_connection(stock_basic_duckdb: Path) -> None:
    with with_duckdb_connection() as first:
        with with_duckdb_connection() as second:
            assert second is first


def stock_basic_rows() -> list[StockBasicRow]:
    loaded_at = datetime(2026, 4, 15, 10, 30, 0)
    return [
        (
            "000001.SZ",
            "000001",
            "平安银行",
            "深圳",
            "银行",
            "主板",
            date(1991, 4, 3),
            True,
            "run-1",
            loaded_at,
        ),
        (
            "000002.SZ",
            "000002",
            "万科A",
            "深圳",
            "房地产",
            "主板",
            date(1991, 1, 29),
            False,
            "run-1",
            loaded_at,
        ),
        (
            "300001.SZ",
            "300001",
            "特锐德",
            "青岛",
            "电气设备",
            "创业板",
            date(2009, 10, 30),
            True,
            "run-1",
            loaded_at,
        ),
    ]
