from __future__ import annotations

from datetime import date
from decimal import Decimal
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest

from data_platform.raw import RawWriter
from tests.dbt.test_intermediate_models import (
    _create_all_intermediate_tables,
    _create_all_staging_views,
)
from tests.dbt.test_tushare_staging_models import (
    _asset_by_dataset,
    _run_dbt_wrapper,
    _sample_table_with_values,
    _write_all_tushare_raw_fixtures,
    _write_duckdb_only_profile,
    require_working_dbt,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"
MARTS_DIR = DBT_PROJECT_DIR / "models" / "marts"
MARTS_V2_DIR = DBT_PROJECT_DIR / "models" / "marts_v2"
MARTS_LINEAGE_DIR = DBT_PROJECT_DIR / "models" / "marts_lineage"
MART_MODEL_NAMES = [
    "mart_dim_security",
    "mart_dim_index",
    "mart_fact_price_bar",
    "mart_fact_financial_indicator",
    "mart_fact_event",
    "mart_fact_market_daily_feature",
    "mart_fact_index_price_bar",
    "mart_fact_forecast_event",
]


def test_marts_sql_and_schema_contracts_are_present() -> None:
    yaml = pytest.importorskip("yaml")

    parsed_schema = yaml.safe_load((MARTS_DIR / "_schema.yml").read_text())
    declared_models = {model["name"]: model for model in parsed_schema["models"]}

    assert set(declared_models) == set(MART_MODEL_NAMES)
    for model_name in MART_MODEL_NAMES:
        model_sql = (MARTS_DIR / f"{model_name}.sql").read_text()
        lowered_sql = model_sql.lower()

        assert "{{ ref('int_" in model_sql
        assert "{{ ref('stg_" not in model_sql
        assert "select *" not in lowered_sql
        assert "canonical." not in lowered_sql
        assert "formal." not in lowered_sql
        assert "iceberg_scan" not in lowered_sql
        assert "submitted_at" not in lowered_sql
        assert "ingest_seq" not in lowered_sql

    dim_security_columns = _schema_columns(declared_models["mart_dim_security"])
    assert {"ts_code", "list_date"}.issubset(dim_security_columns)
    assert {"not_null", "unique"}.issubset(
        _test_names(_schema_column(declared_models["mart_dim_security"], "ts_code"))
    )
    dim_security_relationship = _relationship_test(
        _schema_column(declared_models["mart_dim_security"], "ts_code")
    )
    assert dim_security_relationship["to"] == "ref('int_security_master')"
    assert dim_security_relationship["field"] == "ts_code"
    assert dim_security_relationship["config"]["severity"] == "error"
    assert "not_null" in _test_names(
        _schema_column(declared_models["mart_dim_security"], "list_date")
    )

    dim_index_columns = _schema_columns(declared_models["mart_dim_index"])
    assert {"index_code", "first_effective_date", "latest_effective_date"}.issubset(
        dim_index_columns
    )
    assert {"not_null", "unique"}.issubset(
        _test_names(_schema_column(declared_models["mart_dim_index"], "index_code"))
    )

    price_tests = declared_models["mart_fact_price_bar"]["tests"]
    price_unique_test = next(
        test["unique_combination_of_columns"]
        for test in price_tests
        if _model_test_name(test) == "unique_combination_of_columns"
    )
    price_sql = (MARTS_DIR / "mart_fact_price_bar.sql").read_text().lower()
    assert "try_cast" not in price_sql
    assert price_unique_test["combination_of_columns"] == ["ts_code", "trade_date", "freq"]
    assert "not_null" in _test_names(
        _schema_column(declared_models["mart_fact_price_bar"], "trade_date")
    )

    financial_tests = declared_models["mart_fact_financial_indicator"]["tests"]
    financial_unique_test = next(
        test["unique_combination_of_columns"]
        for test in financial_tests
        if _model_test_name(test) == "unique_combination_of_columns"
    )
    assert financial_unique_test["combination_of_columns"] == [
        "ts_code",
        "end_date",
        "report_type",
    ]
    assert "not_null" in _test_names(
        _schema_column(declared_models["mart_fact_financial_indicator"], "end_date")
    )

    event_tests = declared_models["mart_fact_event"]["tests"]
    assert any(_model_test_name(test) == "unique_combination_of_columns" for test in event_tests)
    assert "not_null" in _test_names(
        _schema_column(declared_models["mart_fact_event"], "event_date")
    )

    market_tests = declared_models["mart_fact_market_daily_feature"]["tests"]
    market_unique_test = next(
        test["unique_combination_of_columns"]
        for test in market_tests
        if _model_test_name(test) == "unique_combination_of_columns"
    )
    assert market_unique_test["combination_of_columns"] == ["ts_code", "trade_date"]

    index_price_tests = declared_models["mart_fact_index_price_bar"]["tests"]
    index_price_unique_test = next(
        test["unique_combination_of_columns"]
        for test in index_price_tests
        if _model_test_name(test) == "unique_combination_of_columns"
    )
    assert index_price_unique_test["combination_of_columns"] == ["index_code", "trade_date"]
    assert "accepted_values" in _test_names(
        _schema_column(declared_models["mart_fact_index_price_bar"], "is_open")
    )

    forecast_tests = declared_models["mart_fact_forecast_event"]["tests"]
    forecast_unique_test = next(
        test["unique_combination_of_columns"]
        for test in forecast_tests
        if _model_test_name(test) == "unique_combination_of_columns"
    )
    assert forecast_unique_test["combination_of_columns"] == [
        "ts_code",
        "ann_date",
        "end_date",
        "update_flag",
        "forecast_type",
    ]


def test_marts_models_execute_with_duckdb_raw_fixture(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    _write_all_tushare_raw_fixtures(raw_zone_path, tmp_path)

    connection = duckdb.connect(":memory:")
    try:
        _create_all_staging_views(connection, raw_zone_path)
        _create_all_intermediate_tables(connection)
        _create_all_mart_tables(connection)

        security_row = connection.execute(
            """
            select ts_code, symbol, name, market, industry, list_date, is_active
            from mart_dim_security
            """
        ).fetchone()
        security_numeric_types = connection.execute(
            """
            select typeof(reg_capital), typeof(employees)
            from mart_dim_security
            """
        ).fetchone()
        index_row = connection.execute(
            """
            select index_code, index_name, index_market, index_category,
                   first_effective_date, latest_effective_date
            from mart_dim_index
            """
        ).fetchone()
        price_summary = connection.execute(
            """
            select count(*), count(distinct freq), typeof(any_value(open)),
                   typeof(any_value(adj_factor))
            from mart_fact_price_bar
            """
        ).fetchone()
        financial_row = connection.execute(
            """
            select is_latest, total_revenue, total_assets, n_cashflow_act, roe
            from mart_fact_financial_indicator
            """
        ).fetchone()
        event_summary = connection.execute(
            """
            select count(*), count(distinct event_type)
            from mart_fact_event
            """
        ).fetchone()
        event_columns = [
            row[1]
            for row in connection.execute("pragma table_info('mart_fact_event')").fetchall()
        ]
        market_summary = connection.execute(
            """
            select count(*), typeof(any_value(close)), typeof(any_value(up_limit)),
                   typeof(any_value(net_mf_amount))
            from mart_fact_market_daily_feature
            """
        ).fetchone()
        index_price_row = connection.execute(
            """
            select index_code, is_open, typeof(close), typeof(amount)
            from mart_fact_index_price_bar
            """
        ).fetchone()
        forecast_row = connection.execute(
            """
            select forecast_type, typeof(p_change_min), typeof(net_profit_max), update_flag
            from mart_fact_forecast_event
            """
        ).fetchone()
    finally:
        connection.close()

    assert security_row == (
        "000001.SZ",
        "000001",
        "name-fixture",
        "market-fixture",
        "industry-fixture",
        date(2026, 4, 15),
        True,
    )
    assert security_numeric_types == ("DECIMAL(38,18)", "DECIMAL(38,18)")
    assert index_row == (
        "000300.SH",
        "index_name-fixture",
        "market-fixture",
        "category-fixture",
        date(2026, 4, 15),
        date(2026, 4, 15),
    )
    assert price_summary == (3, 3, "DECIMAL(38,18)", "DECIMAL(38,18)")
    assert financial_row == (
        True,
        Decimal("1.123456789012345678"),
        Decimal("1.123456789012345678"),
        Decimal("1.123456789012345678"),
        Decimal("1.123456789012345678"),
    )
    assert event_summary == (16, 16)
    assert {"body", "content", "text"}.isdisjoint(event_columns)
    assert market_summary == (1, "DECIMAL(38,18)", "DECIMAL(38,18)", "DECIMAL(38,18)")
    assert index_price_row == ("000300.SH", True, "DECIMAL(38,18)", "DECIMAL(38,18)")
    assert forecast_row == (
        "type-fixture",
        "DECIMAL(38,18)",
        "DECIMAL(38,18)",
        "0",
    )


def test_event_v2_and_lineage_marts_preserve_namechange_fixture(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    _write_all_tushare_raw_fixtures(raw_zone_path, tmp_path)

    connection = duckdb.connect(":memory:")
    try:
        _create_all_staging_views(connection, raw_zone_path)
        _create_all_intermediate_tables(connection)

        expected_namechange_row = connection.execute(
            """
            select ts_code, start_date
            from stg_namechange
            """
        ).fetchone()
        int_namechange_count = connection.execute(
            """
            select count(*)
            from int_event_timeline
            where event_type = 'name_change'
              and source_interface_id = 'namechange'
            """
        ).fetchone()[0]

        _create_mart_table(connection, "mart_fact_event_v2", MARTS_V2_DIR)
        _create_mart_table(connection, "mart_lineage_fact_event", MARTS_LINEAGE_DIR)

        v2_event_types = {
            row[0]
            for row in connection.execute(
                """
                select distinct event_type
                from mart_fact_event_v2
                """
            ).fetchall()
        }
        lineage_source_interface_ids = {
            row[0]
            for row in connection.execute(
                """
                select distinct source_interface_id
                from mart_lineage_fact_event
                """
            ).fetchall()
        }
        v2_columns = _table_columns(connection, "mart_fact_event_v2")
        lineage_columns = _table_columns(connection, "mart_lineage_fact_event")
        v2_pk_rows = connection.execute(
            """
            select event_type, entity_id, event_date, event_key
            from mart_fact_event_v2
            order by event_type, entity_id, event_date, event_key
            """
        ).fetchall()
        lineage_pk_rows = connection.execute(
            """
            select event_type, entity_id, event_date, event_key
            from mart_lineage_fact_event
            order by event_type, entity_id, event_date, event_key
            """
        ).fetchall()
        v2_namechange_row = connection.execute(
            """
            select entity_id, event_date
            from mart_fact_event_v2
            where event_type = 'name_change'
            """
        ).fetchone()
        lineage_namechange_row = connection.execute(
            """
            select entity_id, event_date, source_interface_id
            from mart_lineage_fact_event
            where event_type = 'name_change'
            """
        ).fetchone()
    finally:
        connection.close()

    lineage_column_names = {
        "source_provider",
        "source_interface_id",
        "source_run_id",
        "raw_loaded_at",
    }

    assert int_namechange_count == 1
    assert "name_change" in v2_event_types
    assert "namechange" in lineage_source_interface_ids
    assert lineage_column_names.isdisjoint(v2_columns)
    assert lineage_column_names.issubset(lineage_columns)
    assert v2_pk_rows == lineage_pk_rows
    assert v2_namechange_row == expected_namechange_row
    assert lineage_namechange_row == (*expected_namechange_row, "namechange")


def test_event_v2_and_lineage_marts_preserve_block_trade_fixture(tmp_path: Path) -> None:
    """M1.8 — block_trade flows through int_event_timeline → mart_fact_event_v2
    + mart_lineage_fact_event with v2/lineage canonical PK parity.

    The intermediate uniqueness contract was widened in M1.8 to include
    `summary` (encoding buyer/seller/price/vol/amount), allowing intra-day
    block_trade rows to flow through. Row identity is enforced here at the
    mart-level event_key + canonical PK parity (v2_pk_rows == lineage_pk_rows
    below), and at writer-side validation."""

    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    _write_all_tushare_raw_fixtures(raw_zone_path, tmp_path)
    _write_same_day_block_trade_rows(raw_zone_path, tmp_path)

    connection = duckdb.connect(":memory:")
    try:
        _create_all_staging_views(connection, raw_zone_path)
        _create_all_intermediate_tables(connection)

        expected_block_trade_row = connection.execute(
            """
            select ts_code, trade_date
            from stg_block_trade
            order by buyer, seller
            """
        ).fetchall()
        int_block_trade_count = connection.execute(
            """
            select count(*)
            from int_event_timeline
            where event_type = 'block_trade'
              and source_interface_id = 'block_trade'
            """
        ).fetchone()[0]
        int_block_trade_duplicate_count = connection.execute(
            """
            select count(*)
            from (
                select event_type, ts_code, event_date, title, related_date, reference_url
                from int_event_timeline
                where event_type = 'block_trade'
                group by event_type, ts_code, event_date, title, related_date, reference_url
                having count(*) > 1
            )
            """
        ).fetchone()[0]
        int_block_trade_summary_count = connection.execute(
            """
            select count(distinct summary)
            from int_event_timeline
            where event_type = 'block_trade'
            """
        ).fetchone()[0]

        _create_mart_table(connection, "mart_fact_event_v2", MARTS_V2_DIR)
        _create_mart_table(connection, "mart_lineage_fact_event", MARTS_LINEAGE_DIR)

        v2_event_types = {
            row[0]
            for row in connection.execute(
                """
                select distinct event_type
                from mart_fact_event_v2
                """
            ).fetchall()
        }
        lineage_source_interface_ids = {
            row[0]
            for row in connection.execute(
                """
                select distinct source_interface_id
                from mart_lineage_fact_event
                """
            ).fetchall()
        }
        v2_columns = _table_columns(connection, "mart_fact_event_v2")
        lineage_columns = _table_columns(connection, "mart_lineage_fact_event")
        v2_pk_rows = connection.execute(
            """
            select event_type, entity_id, event_date, event_key
            from mart_fact_event_v2
            order by event_type, entity_id, event_date, event_key
            """
        ).fetchall()
        lineage_pk_rows = connection.execute(
            """
            select event_type, entity_id, event_date, event_key
            from mart_lineage_fact_event
            order by event_type, entity_id, event_date, event_key
            """
        ).fetchall()
        v2_block_trade_row = connection.execute(
            """
            select entity_id, event_date
            from mart_fact_event_v2
            where event_type = 'block_trade'
            order by event_key
            """
        ).fetchall()
        lineage_block_trade_row = connection.execute(
            """
            select entity_id, event_date, source_interface_id
            from mart_lineage_fact_event
            where event_type = 'block_trade'
            order by event_key
            """
        ).fetchall()
        v2_block_trade_event_keys = connection.execute(
            """
            select count(distinct event_key)
            from mart_fact_event_v2
            where event_type = 'block_trade'
            """
        ).fetchone()[0]
    finally:
        connection.close()

    lineage_column_names = {
        "source_provider",
        "source_interface_id",
        "source_run_id",
        "raw_loaded_at",
    }

    assert int_block_trade_count == 2
    assert int_block_trade_duplicate_count == 1
    assert int_block_trade_summary_count == 2
    assert "block_trade" in v2_event_types
    assert "block_trade" in lineage_source_interface_ids
    assert lineage_column_names.isdisjoint(v2_columns)
    assert lineage_column_names.issubset(lineage_columns)
    assert v2_pk_rows == lineage_pk_rows
    assert v2_block_trade_event_keys == 2
    assert v2_block_trade_row == expected_block_trade_row
    assert lineage_block_trade_row == [
        (*row, "block_trade") for row in expected_block_trade_row
    ]


def _write_same_day_block_trade_rows(raw_zone_path: Path, tmp_path: Path) -> None:
    asset = _asset_by_dataset("block_trade")
    block_trade_rows = pa.concat_tables(
        [
            _sample_table_with_values(
                asset,
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260415",
                    "buyer": "buyer-a",
                    "seller": "seller-a",
                    "price": "10.10",
                    "vol": "100",
                    "amount": "1010",
                },
            ),
            _sample_table_with_values(
                asset,
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260415",
                    "buyer": "buyer-b",
                    "seller": "seller-b",
                    "price": "10.20",
                    "vol": "200",
                    "amount": "2040",
                },
            ),
        ]
    )
    RawWriter(
        raw_zone_path=raw_zone_path,
        iceberg_warehouse_path=tmp_path / "iceberg" / "warehouse",
    ).write_arrow(
        "tushare",
        "block_trade",
        date(2026, 4, 15),
        str(uuid4()),
        block_trade_rows,
    )


# M1.13 expansion (precondition 9 closure) — 8 parity tests mirroring
# `test_event_v2_and_lineage_marts_preserve_block_trade_fixture`.
# Each test writes 1 raw row (the M1.11 sign-off rows) for one
# candidate event_timeline source, materializes int_event_timeline →
# mart_fact_event_v2 + mart_lineage_fact_event, then asserts:
#   - int_event_timeline carries the correct event_type literal +
#     source_interface_id
#   - mart_fact_event_v2.event_type contains the new value
#   - mart_lineage_fact_event.source_interface_id contains the new
#     value
#   - lineage columns are present only on the lineage mart
#   - canonical PK (event_type, entity_id, event_date, event_key) is
#     identical between v2 and lineage marts (writer-side pairing
#     guarantee).


def _assert_event_v2_and_lineage_pair_preserved(
    tmp_path: Path,
    *,
    expected_event_type: str,
    expected_source_interface_id: str,
) -> None:
    """Run the materialization + parity assertions for one M1.13 source."""

    duckdb = pytest.importorskip("duckdb")

    raw_zone_path = tmp_path / "raw"
    _write_all_tushare_raw_fixtures(raw_zone_path, tmp_path)

    connection = duckdb.connect(":memory:")
    try:
        _create_all_staging_views(connection, raw_zone_path)
        _create_all_intermediate_tables(connection)

        int_count = connection.execute(
            f"""
            select count(*)
            from int_event_timeline
            where event_type = '{expected_event_type}'
              and source_interface_id = '{expected_source_interface_id}'
            """
        ).fetchone()[0]

        _create_mart_table(connection, "mart_fact_event_v2", MARTS_V2_DIR)
        _create_mart_table(connection, "mart_lineage_fact_event", MARTS_LINEAGE_DIR)

        v2_event_types = {
            row[0]
            for row in connection.execute(
                "select distinct event_type from mart_fact_event_v2"
            ).fetchall()
        }
        lineage_source_interface_ids = {
            row[0]
            for row in connection.execute(
                "select distinct source_interface_id from mart_lineage_fact_event"
            ).fetchall()
        }
        v2_columns = _table_columns(connection, "mart_fact_event_v2")
        lineage_columns = _table_columns(connection, "mart_lineage_fact_event")
        v2_pk_rows = connection.execute(
            """
            select event_type, entity_id, event_date, event_key
            from mart_fact_event_v2
            order by event_type, entity_id, event_date, event_key
            """
        ).fetchall()
        lineage_pk_rows = connection.execute(
            """
            select event_type, entity_id, event_date, event_key
            from mart_lineage_fact_event
            order by event_type, entity_id, event_date, event_key
            """
        ).fetchall()
    finally:
        connection.close()

    lineage_column_names = {
        "source_provider",
        "source_interface_id",
        "source_run_id",
        "raw_loaded_at",
    }

    assert int_count == 1
    assert expected_event_type in v2_event_types
    assert expected_source_interface_id in lineage_source_interface_ids
    assert lineage_column_names.isdisjoint(v2_columns)
    assert lineage_column_names.issubset(lineage_columns)
    assert v2_pk_rows == lineage_pk_rows


def test_event_v2_and_lineage_marts_preserve_pledge_stat_fixture(tmp_path: Path) -> None:
    """M1.13 — pledge_stat flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="pledge_summary",
        expected_source_interface_id="pledge_stat",
    )


def test_event_v2_and_lineage_marts_preserve_pledge_detail_fixture(tmp_path: Path) -> None:
    """M1.13 — pledge_detail flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="pledge_event",
        expected_source_interface_id="pledge_detail",
    )


def test_event_v2_and_lineage_marts_preserve_repurchase_fixture(tmp_path: Path) -> None:
    """M1.13 — repurchase flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="share_repurchase",
        expected_source_interface_id="repurchase",
    )


def test_event_v2_and_lineage_marts_preserve_stk_holdertrade_fixture(tmp_path: Path) -> None:
    """M1.13 — stk_holdertrade flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="shareholder_trade",
        expected_source_interface_id="stk_holdertrade",
    )


def test_event_v2_and_lineage_marts_preserve_stk_surv_fixture(tmp_path: Path) -> None:
    """M1.13 — stk_surv flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="institutional_survey",
        expected_source_interface_id="stk_surv",
    )


def test_event_v2_and_lineage_marts_preserve_limit_list_ths_fixture(tmp_path: Path) -> None:
    """M1.13 — limit_list_ths flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="price_limit_status",
        expected_source_interface_id="limit_list_ths",
    )


def test_event_v2_and_lineage_marts_preserve_limit_list_d_fixture(tmp_path: Path) -> None:
    """M1.13 — limit_list_d flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="price_limit_event",
        expected_source_interface_id="limit_list_d",
    )


def test_event_v2_and_lineage_marts_preserve_hm_detail_fixture(tmp_path: Path) -> None:
    """M1.13 — hm_detail flows through int_event_timeline → marts."""
    _assert_event_v2_and_lineage_pair_preserved(
        tmp_path,
        expected_event_type="hot_money_trade",
        expected_source_interface_id="hm_detail",
    )


def test_price_mart_rejects_malformed_numeric_values() -> None:
    duckdb = pytest.importorskip("duckdb")

    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            create table int_price_bars_adjusted (
                ts_code varchar,
                trade_date date,
                freq varchar,
                open varchar,
                high varchar,
                low varchar,
                close varchar,
                pre_close varchar,
                change varchar,
                pct_chg varchar,
                vol varchar,
                amount varchar,
                adj_factor varchar,
                source_run_id varchar,
                raw_loaded_at timestamp
            )
            """
        )
        connection.execute(
            """
            insert into int_price_bars_adjusted values (
                '000001.SZ',
                date '2026-04-15',
                'daily',
                'not-a-decimal',
                '11.0',
                '9.0',
                '10.5',
                '10.1',
                '0.4',
                '3.96',
                '1000',
                '10500',
                '1.0',
                'run-001',
                timestamp '2026-04-15 10:30:00'
            )
            """
        )
        model_sql = _render_mart_model("mart_fact_price_bar")

        with pytest.raises(duckdb.Error, match="not-a-decimal"):
            connection.execute(f'create table "mart_fact_price_bar" as {model_sql}')
    finally:
        connection.close()


def test_security_mart_rejects_malformed_numeric_values() -> None:
    duckdb = pytest.importorskip("duckdb")

    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            create table int_security_master (
                ts_code varchar,
                symbol varchar,
                name varchar,
                market varchar,
                industry varchar,
                list_date date,
                is_active boolean,
                area varchar,
                fullname varchar,
                exchange varchar,
                curr_type varchar,
                list_status varchar,
                delist_date date,
                setup_date date,
                province varchar,
                city varchar,
                reg_capital varchar,
                employees varchar,
                main_business varchar,
                latest_namechange_name varchar,
                latest_namechange_start_date date,
                latest_namechange_end_date date,
                latest_namechange_ann_date date,
                latest_namechange_reason varchar,
                source_run_id varchar,
                raw_loaded_at timestamp
            )
            """
        )
        connection.execute(
            """
            insert into int_security_master values (
                '000001.SZ',
                '000001',
                'Ping An Bank',
                'Main',
                'Bank',
                date '1991-04-03',
                true,
                'Shenzhen',
                'Ping An Bank Co Ltd',
                'SZSE',
                'CNY',
                'L',
                null,
                date '1987-12-22',
                'Guangdong',
                'Shenzhen',
                'not-a-decimal',
                '100',
                'Banking',
                'Ping An Bank',
                date '2020-01-01',
                null,
                date '2020-01-01',
                'rename',
                'run-001',
                timestamp '2026-04-15 10:30:00'
            )
            """
        )
        model_sql = _render_mart_model("mart_dim_security")

        with pytest.raises(duckdb.Error, match="not-a-decimal"):
            connection.execute(f'create table "mart_dim_security" as {model_sql}')
    finally:
        connection.close()


def test_dbt_run_and_test_marts_with_rawwriter_fixture(tmp_path: Path) -> None:
    require_working_dbt()

    raw_zone_path = tmp_path / "raw"
    _write_all_tushare_raw_fixtures(raw_zone_path, tmp_path)
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    _write_duckdb_only_profile(profiles_dir / "profiles.yml")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles_dir)
    env["DP_RAW_ZONE_PATH"] = str(raw_zone_path)
    env["DP_DUCKDB_PATH"] = str(tmp_path / "data_platform.duckdb")

    run_result = _run_dbt_wrapper(
        [
            "run",
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(tmp_path / "run"),
            "--select",
            "staging",
            "intermediate",
            "marts",
        ],
        env=env,
    )
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    test_result = _run_dbt_wrapper(
        [
            "test",
            "--profiles-dir",
            str(profiles_dir),
            "--target-path",
            str(tmp_path / "test"),
            "--select",
            "staging",
            "intermediate",
            "marts",
        ],
        env=env,
    )
    assert test_result.returncode == 0, test_result.stdout + test_result.stderr


def _create_all_mart_tables(connection: Any) -> None:
    for model_name in MART_MODEL_NAMES:
        _create_mart_table(connection, model_name, MARTS_DIR)


def _create_mart_table(connection: Any, model_name: str, models_dir: Path) -> None:
    model_sql = _render_mart_model(model_name, models_dir)
    connection.execute(f'create table "{model_name}" as {model_sql}')


def _render_mart_model(model_name: str, models_dir: Path = MARTS_DIR) -> str:
    jinja2 = pytest.importorskip("jinja2")

    environment = jinja2.Environment()
    environment.globals["config"] = lambda **_kwargs: ""
    environment.globals["ref"] = lambda ref_name: f'"{ref_name}"'

    return (
        environment.from_string((models_dir / f"{model_name}.sql").read_text())
        .render()
        .strip()
    )


def _table_columns(connection: Any, table_name: str) -> set[str]:
    return {row[1] for row in connection.execute(f"pragma table_info('{table_name}')").fetchall()}


def _schema_columns(model: dict[str, Any]) -> set[str]:
    return {column["name"] for column in model.get("columns", [])}


def _schema_column(model: dict[str, Any], column_name: str) -> dict[str, Any]:
    for column in model.get("columns", []):
        if column["name"] == column_name:
            return column
    raise AssertionError(f"missing schema column: {column_name}")


def _test_names(column: dict[str, Any]) -> set[str]:
    return {_model_test_name(test) for test in column.get("tests", [])}


def _model_test_name(test: str | dict[str, Any]) -> str:
    if isinstance(test, str):
        return test
    return next(iter(test))


def _relationship_test(column: dict[str, Any]) -> dict[str, Any]:
    for test in column.get("tests", []):
        if isinstance(test, dict) and "relationships" in test:
            return test["relationships"]
    raise AssertionError(f"missing relationship test for column: {column['name']}")
