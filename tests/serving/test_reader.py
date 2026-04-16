from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

import duckdb
import pytest
from pyiceberg.catalog.memory import InMemoryCatalog

from data_platform.ddl.iceberg_tables import (
    CANONICAL_MART_TABLE_SPECS,
    CANONICAL_STOCK_BASIC_SPEC,
    ensure_tables,
)
from data_platform.serving import reader
from data_platform.serving.canonical_writer import (
    CANONICAL_MART_LOAD_SPECS,
    load_canonical_marts,
    load_canonical_stock_basic,
    load_canonical_table,
)
from data_platform.serving.reader import (
    CanonicalTableNotFound,
    UnsupportedFilter,
    get_canonical_stock_basic,
    read_canonical,
    with_duckdb_connection,
)
from tests.serving.test_canonical_writer import (
    create_catalog,
    stock_basic_rows as staging_stock_basic_rows,
    write_mart_relations,
    write_staging_stock_basic,
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
        connection.execute(
            """
CREATE TABLE fact_price_bar (
    ts_code VARCHAR,
    trade_date DATE,
    freq VARCHAR,
    open DECIMAL(38, 18),
    close DECIMAL(38, 18),
    source_run_id VARCHAR,
    raw_loaded_at TIMESTAMP,
    canonical_loaded_at TIMESTAMP
)
"""
        )
        connection.executemany(
            """
INSERT INTO fact_price_bar VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""",
            price_bar_rows(),
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


def test_read_canonical_reads_fact_price_bar_with_filter(stock_basic_duckdb: Path) -> None:
    table = read_canonical(
        "fact_price_bar",
        columns=["ts_code", "trade_date", "freq"],
        filters=[("freq", "=", "daily")],
    )

    assert table.schema.names == ["ts_code", "trade_date", "freq"]
    assert table.num_rows == 1
    assert table.column("ts_code").to_pylist() == ["000001.SZ"]
    assert table.column("freq").to_pylist() == ["daily"]


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


def test_read_canonical_uses_real_iceberg_scan_for_stock_basic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_duckdb_iceberg_extension()

    warehouse_path = tmp_path / "warehouse"
    catalog = InMemoryCatalog("test", warehouse=str(warehouse_path))
    ensure_tables(catalog, [CANONICAL_STOCK_BASIC_SPEC])
    staging_duckdb_path = tmp_path / "staging.duckdb"
    write_staging_stock_basic(staging_duckdb_path, staging_stock_basic_rows())
    load_canonical_stock_basic(catalog, staging_duckdb_path)  # type: ignore[arg-type]

    monkeypatch.setattr(
        reader,
        "get_settings",
        lambda: FakeSettings(
            duckdb_path=tmp_path / "reader.duckdb",
            iceberg_warehouse_path=warehouse_path,
        ),
    )
    reader._duckdb_connection.cache_clear()
    try:
        table = read_canonical(
            "stock_basic",
            columns=["ts_code", "name"],
            filters=[("is_active", "=", True)],
        )

        assert table.schema.names == ["ts_code", "name"]
        assert table.num_rows == 2
        assert table.column("ts_code").to_pylist() == ["000001.SZ", "000002.SZ"]

        with pytest.raises(CanonicalTableNotFound):
            read_canonical("missing_table")
    finally:
        reader._duckdb_connection.cache_clear()


def test_read_canonical_uses_mart_snapshot_set_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_duckdb_iceberg_extension()

    catalog = create_catalog(tmp_path, CANONICAL_MART_TABLE_SPECS)
    duckdb_path = tmp_path / "marts.duckdb"
    write_mart_relations(duckdb_path)
    load_canonical_marts(catalog, duckdb_path)  # type: ignore[arg-type]

    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute("UPDATE mart_fact_price_bar SET freq = 'weekly'")
    finally:
        connection.close()
    load_canonical_table(
        catalog,
        duckdb_path,
        CANONICAL_MART_LOAD_SPECS[2],
    )  # type: ignore[arg-type]

    monkeypatch.setattr(
        reader,
        "get_settings",
        lambda: FakeSettings(
            duckdb_path=tmp_path / "reader.duckdb",
            iceberg_warehouse_path=tmp_path / "warehouse",
        ),
    )
    reader._duckdb_connection.cache_clear()
    try:
        daily = read_canonical(
            "fact_price_bar",
            columns=["freq"],
            filters=[("freq", "=", "daily")],
        )
        weekly = read_canonical(
            "fact_price_bar",
            columns=["freq"],
            filters=[("freq", "=", "weekly")],
        )

        assert daily.column("freq").to_pylist() == ["daily"]
        assert weekly.num_rows == 0
    finally:
        reader._duckdb_connection.cache_clear()


def test_read_canonical_rejects_unpublished_mart_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = create_catalog(tmp_path, [CANONICAL_MART_TABLE_SPECS[2]])
    duckdb_path = tmp_path / "marts.duckdb"
    write_mart_relations(duckdb_path)
    load_canonical_table(
        catalog,
        duckdb_path,
        CANONICAL_MART_LOAD_SPECS[2],
    )  # type: ignore[arg-type]

    monkeypatch.setattr(
        reader,
        "get_settings",
        lambda: FakeSettings(
            duckdb_path=tmp_path / "reader.duckdb",
            iceberg_warehouse_path=tmp_path / "warehouse",
        ),
    )
    reader._duckdb_connection.cache_clear()
    try:
        with pytest.raises(CanonicalTableNotFound):
            read_canonical("fact_price_bar")
    finally:
        reader._duckdb_connection.cache_clear()


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


def price_bar_rows() -> list[tuple[str, date, str, str, str, str, datetime, datetime]]:
    loaded_at = datetime(2026, 4, 15, 10, 30, 0)
    return [
        (
            "000001.SZ",
            date(2026, 4, 15),
            "daily",
            "10.000000000000000000",
            "10.500000000000000000",
            "run-1",
            loaded_at,
            loaded_at,
        ),
        (
            "000001.SZ",
            date(2026, 4, 15),
            "weekly",
            "10.000000000000000000",
            "10.500000000000000000",
            "run-1",
            loaded_at,
            loaded_at,
        ),
    ]


def _require_duckdb_iceberg_extension() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("LOAD iceberg")
    except duckdb.Error as exc:
        pytest.skip(
            "DuckDB iceberg extension is not available in this sandbox without "
            f"network install: {exc}"
        )
    finally:
        connection.close()
