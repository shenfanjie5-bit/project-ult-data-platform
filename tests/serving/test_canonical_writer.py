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

from data_platform.ddl.iceberg_tables import (
    CANONICAL_MART_TABLE_SPECS,
    CANONICAL_STOCK_BASIC_SPEC,
    TableSpec,
)
from data_platform.serving import canonical_writer
from data_platform.serving.canonical_writer import (
    CANONICAL_MART_LOAD_SPECS,
    CANONICAL_MART_SNAPSHOT_SET_FILE,
    CANONICAL_STOCK_BASIC_IDENTIFIER,
    FORBIDDEN_PAYLOAD_FIELDS,
    CanonicalLoadSpec,
    load_canonical_marts,
    load_canonical_stock_basic,
    load_canonical_table,
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


def test_load_canonical_stock_basic_rejects_empty_staging_before_overwrite(
    tmp_path: Path,
) -> None:
    catalog = create_stock_basic_catalog(tmp_path)
    duckdb_path = tmp_path / "staging.duckdb"
    rows = stock_basic_rows()
    write_staging_stock_basic(duckdb_path, rows)
    first = load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]
    write_staging_stock_basic(duckdb_path, [])

    with pytest.raises(ValueError, match="zero rows"):
        load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]

    table = catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER)
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None
    assert current_snapshot.snapshot_id == first.snapshot_id
    assert table.scan().to_arrow().num_rows == len(rows)


def test_load_canonical_stock_basic_allows_empty_staging_with_explicit_flag(
    tmp_path: Path,
) -> None:
    catalog = create_stock_basic_catalog(tmp_path)
    duckdb_path = tmp_path / "staging.duckdb"
    rows = stock_basic_rows()
    write_staging_stock_basic(duckdb_path, rows)
    first = load_canonical_stock_basic(catalog, duckdb_path)  # type: ignore[arg-type]
    write_staging_stock_basic(duckdb_path, [])

    second = load_canonical_stock_basic(  # type: ignore[arg-type]
        catalog,
        duckdb_path,
        allow_empty=True,
    )

    table = catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER)
    assert second.row_count == 0
    assert second.snapshot_id != first.snapshot_id
    assert table.scan().to_arrow().num_rows == 0
    assert table.scan(snapshot_id=first.snapshot_id).to_arrow().num_rows == len(rows)


def test_canonical_load_spec_rejects_queue_fields() -> None:
    with pytest.raises(ValueError, match="producer queue fields"):
        CanonicalLoadSpec(
            identifier="canonical.bad",
            duckdb_relation="bad_relation",
            required_columns=("submitted_at",),
        )


def test_load_canonical_marts_writes_all_tables_in_fixed_order(tmp_path: Path) -> None:
    catalog = create_catalog(tmp_path, CANONICAL_MART_TABLE_SPECS)
    duckdb_path = tmp_path / "marts.duckdb"
    write_mart_relations(duckdb_path)

    results = load_canonical_marts(catalog, duckdb_path)  # type: ignore[arg-type]
    manifest_path = tmp_path / "warehouse" / "canonical" / CANONICAL_MART_SNAPSHOT_SET_FILE
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert [result.table for result in results] == [
        spec.identifier for spec in CANONICAL_MART_LOAD_SPECS
    ]
    assert [result.row_count for result in results] == [1] * len(CANONICAL_MART_LOAD_SPECS)
    assert all(result.snapshot_id for result in results)

    for load_spec, table_spec in zip(
        CANONICAL_MART_LOAD_SPECS,
        CANONICAL_MART_TABLE_SPECS,
        strict=True,
    ):
        payload = catalog.load_table(load_spec.identifier).scan().to_arrow()
        assert payload.num_rows == 1
        assert payload.schema.names == table_spec.schema.names
        assert "canonical_loaded_at" in payload.schema.names

        table_name = load_spec.identifier.rsplit(".", maxsplit=1)[-1]
        assert manifest["tables"][table_name]["identifier"] == load_spec.identifier
        assert manifest["tables"][table_name]["snapshot_id"] == results[
            CANONICAL_MART_LOAD_SPECS.index(load_spec)
        ].snapshot_id
        assert manifest["tables"][table_name]["metadata_location"].endswith(".metadata.json")


def test_load_canonical_marts_preflights_all_relations_before_overwrite(
    tmp_path: Path,
) -> None:
    catalog = create_catalog(tmp_path, CANONICAL_MART_TABLE_SPECS)
    duckdb_path = tmp_path / "marts.duckdb"
    write_mart_relations(duckdb_path)
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute("ALTER TABLE mart_fact_price_bar DROP COLUMN adj_factor")
    finally:
        connection.close()

    with pytest.raises(duckdb.Error, match="adj_factor"):
        load_canonical_marts(catalog, duckdb_path)  # type: ignore[arg-type]

    for load_spec in CANONICAL_MART_LOAD_SPECS:
        assert catalog.load_table(load_spec.identifier).current_snapshot() is None

    manifest_path = tmp_path / "warehouse" / "canonical" / CANONICAL_MART_SNAPSHOT_SET_FILE
    assert not manifest_path.exists()


def test_load_canonical_table_rejects_missing_relation_column(tmp_path: Path) -> None:
    catalog = create_stock_basic_catalog(tmp_path)
    duckdb_path = tmp_path / "staging.duckdb"
    write_staging_stock_basic(duckdb_path, stock_basic_rows())
    bad_spec = CanonicalLoadSpec(
        identifier=CANONICAL_STOCK_BASIC_IDENTIFIER,
        duckdb_relation="stg_stock_basic",
        required_columns=(
            *canonical_writer.STOCK_BASIC_LOAD_SPEC.required_columns,
            "missing_from_target",
        ),
    )

    with pytest.raises(duckdb.Error, match="missing_from_target"):
        load_canonical_table(catalog, duckdb_path, bad_spec)  # type: ignore[arg-type]

    assert catalog.load_table(CANONICAL_STOCK_BASIC_IDENTIFIER).current_snapshot() is None


def test_load_canonical_table_rejects_public_single_mart_publish(tmp_path: Path) -> None:
    catalog = create_catalog(tmp_path, [CANONICAL_MART_TABLE_SPECS[0]])
    duckdb_path = tmp_path / "marts.duckdb"
    write_mart_relations(duckdb_path)

    with pytest.raises(ValueError, match="publish marts with load_canonical_marts"):
        load_canonical_table(  # type: ignore[arg-type]
            catalog,
            duckdb_path,
            CANONICAL_MART_LOAD_SPECS[0],
        )

    assert catalog.load_table("canonical.dim_security").current_snapshot() is None


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


def test_cli_writes_marts_result_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    catalog = create_catalog(tmp_path, CANONICAL_MART_TABLE_SPECS)
    duckdb_path = tmp_path / "marts.duckdb"
    write_mart_relations(duckdb_path)
    monkeypatch.setattr(canonical_writer, "load_catalog", lambda: catalog)
    monkeypatch.setattr(canonical_writer, "get_settings", lambda: FakeSettings(duckdb_path))

    exit_code = canonical_writer.main(["--table", "marts", "--json"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert [item["table"] for item in payload] == [
        spec.identifier for spec in CANONICAL_MART_LOAD_SPECS
    ]
    assert [item["row_count"] for item in payload] == [1] * len(CANONICAL_MART_LOAD_SPECS)


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


@pytest.mark.parametrize(
    "argv, expected_detail",
    [
        ([], "the following arguments are required: --table"),
        (["--table", "stock_basic", "--unknown"], "unrecognized arguments: --unknown"),
    ],
)
def test_cli_reports_argument_failures_as_json(
    argv: list[str],
    expected_detail: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = canonical_writer.main(argv)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"] == "ValueError"
    assert expected_detail in payload["detail"]


def create_catalog(tmp_path: Path, specs: Sequence[TableSpec]) -> InMemoryCatalog:
    warehouse_path = tmp_path / "warehouse"
    catalog = InMemoryCatalog("test", warehouse=f"file://{warehouse_path}")
    catalog.create_namespace_if_not_exists(("canonical",))
    for spec in specs:
        catalog.create_table(f"{spec.namespace}.{spec.name}", schema=spec.schema)
    return catalog


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


def write_mart_relations(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_dim_security AS
            SELECT
                '000001.SZ'::VARCHAR AS ts_code,
                '000001'::VARCHAR AS symbol,
                'Ping An Bank'::VARCHAR AS name,
                'Main'::VARCHAR AS market,
                'Bank'::VARCHAR AS industry,
                DATE '1991-04-03' AS list_date,
                TRUE AS is_active,
                'Shenzhen'::VARCHAR AS area,
                'Ping An Bank Co Ltd'::VARCHAR AS fullname,
                'SZSE'::VARCHAR AS exchange,
                'CNY'::VARCHAR AS curr_type,
                'L'::VARCHAR AS list_status,
                NULL::DATE AS delist_date,
                DATE '1987-12-22' AS setup_date,
                'Guangdong'::VARCHAR AS province,
                'Shenzhen'::VARCHAR AS city,
                CAST(1000.000000000000000000 AS DECIMAL(38, 18)) AS reg_capital,
                CAST(100.000000000000000000 AS DECIMAL(38, 18)) AS employees,
                'Banking'::VARCHAR AS main_business,
                'Ping An Bank'::VARCHAR AS latest_namechange_name,
                DATE '2020-01-01' AS latest_namechange_start_date,
                NULL::DATE AS latest_namechange_end_date,
                DATE '2020-01-01' AS latest_namechange_ann_date,
                'rename'::VARCHAR AS latest_namechange_reason,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_dim_index AS
            SELECT
                '000300.SH'::VARCHAR AS index_code,
                'CSI 300'::VARCHAR AS index_name,
                'SSE'::VARCHAR AS index_market,
                'broad'::VARCHAR AS index_category,
                DATE '2026-04-15' AS first_effective_date,
                DATE '2026-04-15' AS latest_effective_date,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_fact_price_bar AS
            SELECT
                '000001.SZ'::VARCHAR AS ts_code,
                DATE '2026-04-15' AS trade_date,
                'daily'::VARCHAR AS freq,
                CAST(10.000000000000000000 AS DECIMAL(38, 18)) AS open,
                CAST(11.000000000000000000 AS DECIMAL(38, 18)) AS high,
                CAST(9.000000000000000000 AS DECIMAL(38, 18)) AS low,
                CAST(10.500000000000000000 AS DECIMAL(38, 18)) AS close,
                CAST(10.100000000000000000 AS DECIMAL(38, 18)) AS pre_close,
                CAST(0.400000000000000000 AS DECIMAL(38, 18)) AS change,
                CAST(3.960000000000000000 AS DECIMAL(38, 18)) AS pct_chg,
                CAST(1000.000000000000000000 AS DECIMAL(38, 18)) AS vol,
                CAST(10500.000000000000000000 AS DECIMAL(38, 18)) AS amount,
                CAST(1.000000000000000000 AS DECIMAL(38, 18)) AS adj_factor,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_fact_financial_indicator AS
            SELECT
                '000001.SZ'::VARCHAR AS ts_code,
                DATE '2026-03-31' AS end_date,
                DATE '2026-04-15' AS ann_date,
                DATE '2026-04-16' AS f_ann_date,
                '1'::VARCHAR AS report_type,
                '1'::VARCHAR AS comp_type,
                '0'::VARCHAR AS update_flag,
                TRUE AS is_latest,
                CAST(1.000000000000000000 AS DECIMAL(38, 18)) AS basic_eps,
                CAST(1.000000000000000000 AS DECIMAL(38, 18)) AS diluted_eps,
                CAST(100.000000000000000000 AS DECIMAL(38, 18)) AS total_revenue,
                CAST(90.000000000000000000 AS DECIMAL(38, 18)) AS revenue,
                CAST(80.000000000000000000 AS DECIMAL(38, 18)) AS operate_profit,
                CAST(70.000000000000000000 AS DECIMAL(38, 18)) AS total_profit,
                CAST(60.000000000000000000 AS DECIMAL(38, 18)) AS n_income,
                CAST(50.000000000000000000 AS DECIMAL(38, 18)) AS n_income_attr_p,
                CAST(40.000000000000000000 AS DECIMAL(38, 18)) AS money_cap,
                CAST(300.000000000000000000 AS DECIMAL(38, 18)) AS total_cur_assets,
                CAST(500.000000000000000000 AS DECIMAL(38, 18)) AS total_assets,
                CAST(200.000000000000000000 AS DECIMAL(38, 18)) AS total_cur_liab,
                CAST(250.000000000000000000 AS DECIMAL(38, 18)) AS total_liab,
                CAST(240.000000000000000000 AS DECIMAL(38, 18))
                    AS total_hldr_eqy_exc_min_int,
                CAST(500.000000000000000000 AS DECIMAL(38, 18)) AS total_liab_hldr_eqy,
                CAST(60.000000000000000000 AS DECIMAL(38, 18)) AS net_profit,
                CAST(55.000000000000000000 AS DECIMAL(38, 18)) AS n_cashflow_act,
                CAST(45.000000000000000000 AS DECIMAL(38, 18)) AS n_cashflow_inv_act,
                CAST(35.000000000000000000 AS DECIMAL(38, 18)) AS n_cash_flows_fnc_act,
                CAST(25.000000000000000000 AS DECIMAL(38, 18)) AS n_incr_cash_cash_equ,
                CAST(15.000000000000000000 AS DECIMAL(38, 18)) AS free_cashflow,
                CAST(1.000000000000000000 AS DECIMAL(38, 18)) AS eps,
                CAST(1.000000000000000000 AS DECIMAL(38, 18)) AS dt_eps,
                CAST(30.000000000000000000 AS DECIMAL(38, 18)) AS grossprofit_margin,
                CAST(20.000000000000000000 AS DECIMAL(38, 18)) AS netprofit_margin,
                CAST(10.000000000000000000 AS DECIMAL(38, 18)) AS roe,
                CAST(5.000000000000000000 AS DECIMAL(38, 18)) AS roa,
                CAST(50.000000000000000000 AS DECIMAL(38, 18)) AS debt_to_assets,
                CAST(12.000000000000000000 AS DECIMAL(38, 18)) AS or_yoy,
                CAST(8.000000000000000000 AS DECIMAL(38, 18)) AS netprofit_yoy,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_fact_event AS
            SELECT
                'announcement'::VARCHAR AS event_type,
                '000001.SZ'::VARCHAR AS ts_code,
                DATE '2026-04-15' AS event_date,
                'Announcement'::VARCHAR AS title,
                'Quarterly report'::VARCHAR AS summary,
                NULL::VARCHAR AS event_subtype,
                NULL::DATE AS related_date,
                'https://example.test/report'::VARCHAR AS reference_url,
                '10:30:00'::VARCHAR AS rec_time,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_fact_market_daily_feature AS
            SELECT
                '000001.SZ'::VARCHAR AS ts_code,
                DATE '2026-04-15' AS trade_date,
                CAST(10.500000000000000000 AS DECIMAL(38, 18)) AS close,
                CAST(1.000000000000000000 AS DECIMAL(38, 18)) AS turnover_rate,
                CAST(1.100000000000000000 AS DECIMAL(38, 18)) AS turnover_rate_f,
                CAST(1.200000000000000000 AS DECIMAL(38, 18)) AS volume_ratio,
                CAST(8.000000000000000000 AS DECIMAL(38, 18)) AS pe,
                CAST(9.000000000000000000 AS DECIMAL(38, 18)) AS pe_ttm,
                CAST(1.300000000000000000 AS DECIMAL(38, 18)) AS pb,
                CAST(2.000000000000000000 AS DECIMAL(38, 18)) AS ps,
                CAST(2.100000000000000000 AS DECIMAL(38, 18)) AS ps_ttm,
                CAST(3.000000000000000000 AS DECIMAL(38, 18)) AS dv_ratio,
                CAST(3.100000000000000000 AS DECIMAL(38, 18)) AS dv_ttm,
                CAST(1000.000000000000000000 AS DECIMAL(38, 18)) AS total_share,
                CAST(800.000000000000000000 AS DECIMAL(38, 18)) AS float_share,
                CAST(600.000000000000000000 AS DECIMAL(38, 18)) AS free_share,
                CAST(100000.000000000000000000 AS DECIMAL(38, 18)) AS total_mv,
                CAST(80000.000000000000000000 AS DECIMAL(38, 18)) AS circ_mv,
                CAST(11.550000000000000000 AS DECIMAL(38, 18)) AS up_limit,
                CAST(9.450000000000000000 AS DECIMAL(38, 18)) AS down_limit,
                CAST(10.000000000000000000 AS DECIMAL(38, 18)) AS buy_sm_vol,
                CAST(100.000000000000000000 AS DECIMAL(38, 18)) AS buy_sm_amount,
                CAST(9.000000000000000000 AS DECIMAL(38, 18)) AS sell_sm_vol,
                CAST(90.000000000000000000 AS DECIMAL(38, 18)) AS sell_sm_amount,
                CAST(20.000000000000000000 AS DECIMAL(38, 18)) AS buy_md_vol,
                CAST(200.000000000000000000 AS DECIMAL(38, 18)) AS buy_md_amount,
                CAST(18.000000000000000000 AS DECIMAL(38, 18)) AS sell_md_vol,
                CAST(180.000000000000000000 AS DECIMAL(38, 18)) AS sell_md_amount,
                CAST(30.000000000000000000 AS DECIMAL(38, 18)) AS buy_lg_vol,
                CAST(300.000000000000000000 AS DECIMAL(38, 18)) AS buy_lg_amount,
                CAST(25.000000000000000000 AS DECIMAL(38, 18)) AS sell_lg_vol,
                CAST(250.000000000000000000 AS DECIMAL(38, 18)) AS sell_lg_amount,
                CAST(40.000000000000000000 AS DECIMAL(38, 18)) AS buy_elg_vol,
                CAST(400.000000000000000000 AS DECIMAL(38, 18)) AS buy_elg_amount,
                CAST(35.000000000000000000 AS DECIMAL(38, 18)) AS sell_elg_vol,
                CAST(350.000000000000000000 AS DECIMAL(38, 18)) AS sell_elg_amount,
                CAST(13.000000000000000000 AS DECIMAL(38, 18)) AS net_mf_vol,
                CAST(130.000000000000000000 AS DECIMAL(38, 18)) AS net_mf_amount,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_fact_index_price_bar AS
            SELECT
                '000300.SH'::VARCHAR AS index_code,
                DATE '2026-04-15' AS trade_date,
                CAST(4000.000000000000000000 AS DECIMAL(38, 18)) AS open,
                CAST(4050.000000000000000000 AS DECIMAL(38, 18)) AS high,
                CAST(3980.000000000000000000 AS DECIMAL(38, 18)) AS low,
                CAST(4020.000000000000000000 AS DECIMAL(38, 18)) AS close,
                CAST(3990.000000000000000000 AS DECIMAL(38, 18)) AS pre_close,
                CAST(30.000000000000000000 AS DECIMAL(38, 18)) AS change,
                CAST(0.750000000000000000 AS DECIMAL(38, 18)) AS pct_chg,
                CAST(200000.000000000000000000 AS DECIMAL(38, 18)) AS vol,
                CAST(900000.000000000000000000 AS DECIMAL(38, 18)) AS amount,
                'SSE'::VARCHAR AS exchange,
                TRUE AS is_open,
                DATE '2026-04-14' AS pretrade_date,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE mart_fact_forecast_event AS
            SELECT
                '000001.SZ'::VARCHAR AS ts_code,
                DATE '2026-04-15' AS ann_date,
                DATE '2026-03-31' AS end_date,
                'increase'::VARCHAR AS forecast_type,
                CAST(5.000000000000000000 AS DECIMAL(38, 18)) AS p_change_min,
                CAST(10.000000000000000000 AS DECIMAL(38, 18)) AS p_change_max,
                CAST(100.000000000000000000 AS DECIMAL(38, 18)) AS net_profit_min,
                CAST(110.000000000000000000 AS DECIMAL(38, 18)) AS net_profit_max,
                CAST(90.000000000000000000 AS DECIMAL(38, 18)) AS last_parent_net,
                DATE '2026-04-14' AS first_ann_date,
                'Forecast summary'::VARCHAR AS summary,
                'Demand change'::VARCHAR AS change_reason,
                '0'::VARCHAR AS update_flag,
                'run-001'::VARCHAR AS source_run_id,
                TIMESTAMP '2026-04-15 10:30:00' AS raw_loaded_at
            """
        )
    finally:
        connection.close()


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
