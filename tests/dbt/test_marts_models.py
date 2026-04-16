from __future__ import annotations

from datetime import date
from decimal import Decimal
import os
from pathlib import Path
from typing import Any

import pytest

from tests.dbt.test_intermediate_models import (
    _create_all_intermediate_tables,
    _create_all_staging_views,
)
from tests.dbt.test_tushare_staging_models import (
    _run_dbt_wrapper,
    _write_all_tushare_raw_fixtures,
    _write_duckdb_only_profile,
    require_working_dbt,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "src" / "data_platform" / "dbt"
MARTS_DIR = DBT_PROJECT_DIR / "models" / "marts"
MART_MODEL_NAMES = [
    "mart_dim_security",
    "mart_dim_index",
    "mart_fact_price_bar",
    "mart_fact_financial_indicator",
    "mart_fact_event",
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
        None,
        None,
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
    assert event_summary == (6, 6)
    assert {"body", "content", "text"}.isdisjoint(event_columns)


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
        model_sql = _render_mart_model(model_name)
        connection.execute(f'create table "{model_name}" as {model_sql}')


def _render_mart_model(model_name: str) -> str:
    jinja2 = pytest.importorskip("jinja2")

    environment = jinja2.Environment()
    environment.globals["config"] = lambda **_kwargs: ""
    environment.globals["ref"] = lambda ref_name: f'"{ref_name}"'

    return (
        environment.from_string((MARTS_DIR / f"{model_name}.sql").read_text())
        .render()
        .strip()
    )


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
