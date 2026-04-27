"""Canonical Iceberg writers for Lite-mode staging outputs."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import logging
from pathlib import Path
import re
import sys
from time import perf_counter
from typing import Final, NoReturn, Sequence
from urllib.parse import unquote, urlparse
from uuid import uuid4

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from data_platform.config import get_settings
from data_platform.serving.catalog import load_catalog


logger = logging.getLogger(__name__)

TABLE_STOCK_BASIC = "stock_basic"
TABLE_MARTS = "marts"
CANONICAL_STOCK_BASIC_IDENTIFIER = "canonical.stock_basic"
CANONICAL_LOADED_AT_COLUMN = "canonical_loaded_at"
CANONICAL_MART_SNAPSHOT_SET_FILE = "_mart_snapshot_set.json"
FORBIDDEN_PAYLOAD_FIELDS = frozenset({"submitted_at", "ingest_seq"})
_IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Result of one canonical table overwrite."""

    table: str
    snapshot_id: int
    row_count: int
    duration_ms: int


@dataclass(frozen=True, slots=True)
class CanonicalLoadSpec:
    """DuckDB relation to canonical Iceberg table load contract."""

    identifier: str
    duckdb_relation: str
    required_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        identifier = self.identifier.strip()
        duckdb_relation = self.duckdb_relation.strip()
        if not identifier:
            msg = "canonical identifier must not be empty"
            raise ValueError(msg)
        if not duckdb_relation:
            msg = "DuckDB relation must not be empty"
            raise ValueError(msg)
        if not self.required_columns:
            msg = "canonical load required_columns must not be empty"
            raise ValueError(msg)

        _validate_qualified_identifier(identifier)
        _validate_qualified_identifier(duckdb_relation)
        for column in self.required_columns:
            _validate_identifier(column)

        forbidden_fields = sorted(
            FORBIDDEN_PAYLOAD_FIELDS.intersection(
                column.lower() for column in self.required_columns
            )
        )
        if forbidden_fields:
            msg = "canonical load spec must not include producer queue fields: "
            raise ValueError(msg + ", ".join(forbidden_fields))

        object.__setattr__(self, "identifier", identifier)
        object.__setattr__(self, "duckdb_relation", duckdb_relation)


@dataclass(frozen=True, slots=True)
class _PreparedCanonicalLoad:
    spec: CanonicalLoadSpec
    table: Table
    table_arrow: pa.Table


class _JsonErrorArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError(message)


def _quote_qualified_identifier(identifier: str) -> str:
    return ".".join(_quote_identifier(part) for part in identifier.split("."))


def _validate_qualified_identifier(identifier: str) -> None:
    parts = identifier.split(".")
    if not parts:
        msg = f"invalid SQL identifier: {identifier!r}"
        raise ValueError(msg)
    for part in parts:
        _validate_identifier(part)


def _quote_identifier(identifier: str) -> str:
    _validate_identifier(identifier)
    return f'"{identifier}"'


def _validate_identifier(identifier: str) -> None:
    if _IDENTIFIER_PATTERN.fullmatch(identifier):
        return
    msg = f"invalid SQL identifier: {identifier!r}"
    raise ValueError(msg)


STOCK_BASIC_LOAD_SPEC: Final[CanonicalLoadSpec] = CanonicalLoadSpec(
    identifier=CANONICAL_STOCK_BASIC_IDENTIFIER,
    duckdb_relation="stg_stock_basic",
    required_columns=(
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "market",
        "list_date",
        "is_active",
        "source_run_id",
    ),
)

CANONICAL_MART_LOAD_SPECS: Final[tuple[CanonicalLoadSpec, ...]] = (
    CanonicalLoadSpec(
        identifier="canonical.dim_security",
        duckdb_relation="mart_dim_security",
        required_columns=(
            "ts_code",
            "symbol",
            "name",
            "market",
            "industry",
            "list_date",
            "is_active",
            "area",
            "fullname",
            "exchange",
            "curr_type",
            "list_status",
            "delist_date",
            "setup_date",
            "province",
            "city",
            "reg_capital",
            "employees",
            "main_business",
            "latest_namechange_name",
            "latest_namechange_start_date",
            "latest_namechange_end_date",
            "latest_namechange_ann_date",
            "latest_namechange_reason",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.dim_index",
        duckdb_relation="mart_dim_index",
        required_columns=(
            "index_code",
            "index_name",
            "index_market",
            "index_category",
            "first_effective_date",
            "latest_effective_date",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.fact_price_bar",
        duckdb_relation="mart_fact_price_bar",
        required_columns=(
            "ts_code",
            "trade_date",
            "freq",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
            "adj_factor",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.fact_financial_indicator",
        duckdb_relation="mart_fact_financial_indicator",
        required_columns=(
            "ts_code",
            "end_date",
            "ann_date",
            "f_ann_date",
            "report_type",
            "comp_type",
            "update_flag",
            "is_latest",
            "basic_eps",
            "diluted_eps",
            "total_revenue",
            "revenue",
            "operate_profit",
            "total_profit",
            "n_income",
            "n_income_attr_p",
            "money_cap",
            "total_cur_assets",
            "total_assets",
            "total_cur_liab",
            "total_liab",
            "total_hldr_eqy_exc_min_int",
            "total_liab_hldr_eqy",
            "net_profit",
            "n_cashflow_act",
            "n_cashflow_inv_act",
            "n_cash_flows_fnc_act",
            "n_incr_cash_cash_equ",
            "free_cashflow",
            "eps",
            "dt_eps",
            "grossprofit_margin",
            "netprofit_margin",
            "roe",
            "roa",
            "debt_to_assets",
            "or_yoy",
            "netprofit_yoy",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.fact_event",
        duckdb_relation="mart_fact_event",
        required_columns=(
            "event_type",
            "ts_code",
            "event_date",
            "title",
            "summary",
            "event_subtype",
            "related_date",
            "reference_url",
            "rec_time",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.fact_market_daily_feature",
        duckdb_relation="mart_fact_market_daily_feature",
        required_columns=(
            "ts_code",
            "trade_date",
            "close",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dv_ratio",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
            "up_limit",
            "down_limit",
            "buy_sm_vol",
            "buy_sm_amount",
            "sell_sm_vol",
            "sell_sm_amount",
            "buy_md_vol",
            "buy_md_amount",
            "sell_md_vol",
            "sell_md_amount",
            "buy_lg_vol",
            "buy_lg_amount",
            "sell_lg_vol",
            "sell_lg_amount",
            "buy_elg_vol",
            "buy_elg_amount",
            "sell_elg_vol",
            "sell_elg_amount",
            "net_mf_vol",
            "net_mf_amount",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.fact_index_price_bar",
        duckdb_relation="mart_fact_index_price_bar",
        required_columns=(
            "index_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
            "exchange",
            "is_open",
            "pretrade_date",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
    CanonicalLoadSpec(
        identifier="canonical.fact_forecast_event",
        duckdb_relation="mart_fact_forecast_event",
        required_columns=(
            "ts_code",
            "ann_date",
            "end_date",
            "forecast_type",
            "p_change_min",
            "p_change_max",
            "net_profit_min",
            "net_profit_max",
            "last_parent_net",
            "first_ann_date",
            "summary",
            "change_reason",
            "update_flag",
            "source_run_id",
            "raw_loaded_at",
        ),
    ),
)


def load_canonical_table(
    catalog: SqlCatalog,
    duckdb_path: Path,
    spec: CanonicalLoadSpec,
    *,
    allow_empty: bool = False,
) -> WriteResult:
    """Load one DuckDB relation into its canonical Iceberg table via full overwrite."""

    _reject_public_mart_load(spec)
    start = perf_counter()
    prepared = _prepare_canonical_load(
        catalog,
        duckdb_path,
        spec,
        allow_empty=allow_empty,
    )
    result, _refreshed_table = _overwrite_prepared_load(prepared, started_at=start)
    return result


def _reject_public_mart_load(spec: CanonicalLoadSpec) -> None:
    mart_identifiers = {mart_spec.identifier for mart_spec in CANONICAL_MART_LOAD_SPECS}
    if spec.identifier not in mart_identifiers:
        return

    msg = (
        f"{spec.identifier} is part of the canonical mart snapshot set; "
        "publish marts with load_canonical_marts"
    )
    raise ValueError(msg)


def _prepare_canonical_load(
    catalog: SqlCatalog,
    duckdb_path: Path,
    spec: CanonicalLoadSpec,
    *,
    allow_empty: bool,
) -> _PreparedCanonicalLoad:
    table = catalog.load_table(spec.identifier)
    target_columns = _table_field_names(table)
    table_arrow = _read_duckdb_relation(duckdb_path, spec, target_columns)
    _validate_no_forbidden_payload_fields(table_arrow)
    _validate_payload_fields_match_target(spec.identifier, table, table_arrow)
    _validate_non_empty_staging(spec, table_arrow, allow_empty=allow_empty)
    return _PreparedCanonicalLoad(spec=spec, table=table, table_arrow=table_arrow)


def _overwrite_prepared_load(
    prepared: _PreparedCanonicalLoad,
    *,
    started_at: float,
) -> tuple[WriteResult, Table]:
    prepared.table.overwrite(prepared.table_arrow)
    refreshed_table = prepared.table.refresh()
    snapshot_id = _current_snapshot_id(refreshed_table, prepared.spec.identifier)
    duration_ms = int((perf_counter() - started_at) * 1000)
    result = WriteResult(
        table=prepared.spec.identifier,
        snapshot_id=snapshot_id,
        row_count=prepared.table_arrow.num_rows,
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
    return result, refreshed_table


def load_canonical_stock_basic(
    catalog: SqlCatalog,
    duckdb_path: Path,
    *,
    allow_empty: bool = False,
) -> WriteResult:
    """Load DuckDB stg_stock_basic into canonical.stock_basic via full overwrite."""

    return load_canonical_table(
        catalog,
        duckdb_path,
        STOCK_BASIC_LOAD_SPEC,
        allow_empty=allow_empty,
    )


def load_canonical_marts(
    catalog: SqlCatalog,
    duckdb_path: Path,
    *,
    allow_empty: bool = False,
) -> list[WriteResult]:
    """Load all canonical mart tables in the project-defined dependency order."""

    prepared_loads = [
        _prepare_canonical_load(
            catalog,
            duckdb_path,
            spec,
            allow_empty=allow_empty,
        )
        for spec in CANONICAL_MART_LOAD_SPECS
    ]

    results: list[WriteResult] = []
    snapshot_set_tables: dict[str, dict[str, int | str]] = {}
    load_id = str(uuid4())
    for prepared in prepared_loads:
        result, refreshed_table = _overwrite_prepared_load(
            prepared,
            started_at=perf_counter(),
        )
        results.append(result)
        snapshot_set_tables[_table_name_from_identifier(result.table)] = {
            "identifier": result.table,
            "snapshot_id": result.snapshot_id,
            "metadata_location": str(_local_path_from_location(refreshed_table.metadata_location)),
        }

    _write_mart_snapshot_set_manifest(
        prepared_loads[0].table,
        load_id=load_id,
        tables=snapshot_set_tables,
    )
    return results


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
        parser.add_argument(
            "--json",
            action="store_true",
            help="emit JSON output",
        )
        args = parser.parse_args(argv)
        result: WriteResult | list[WriteResult]
        if args.table == TABLE_STOCK_BASIC:
            result = load_canonical_stock_basic(
                load_catalog(),
                get_settings().duckdb_path,
                allow_empty=args.allow_empty,
            )
        elif args.table == TABLE_MARTS:
            result = load_canonical_marts(
                load_catalog(),
                get_settings().duckdb_path,
                allow_empty=args.allow_empty,
            )
        else:
            msg = f"unsupported canonical table: {args.table}"
            raise ValueError(msg)
    except Exception as exc:
        error_payload = {"error": type(exc).__name__, "detail": str(exc)}
        print(json.dumps(error_payload, sort_keys=True), file=sys.stderr)
        return 1

    print(json.dumps(_serialize_result(result), sort_keys=True))
    return 0


def _read_duckdb_relation(
    duckdb_path: Path,
    spec: CanonicalLoadSpec,
    target_columns: Sequence[str],
) -> pa.Table:
    select_expressions: list[str] = []
    for column in spec.required_columns:
        quoted_column = _quote_identifier(column)
        select_expressions.append(f"{quoted_column} AS {quoted_column}")
    if CANONICAL_LOADED_AT_COLUMN in target_columns:
        quoted_column = _quote_identifier(CANONICAL_LOADED_AT_COLUMN)
        select_expressions.append(f"CAST(current_timestamp AS TIMESTAMP) AS {quoted_column}")

    sql = f"""
SELECT
    {",\n    ".join(select_expressions)}
FROM {_quote_qualified_identifier(spec.duckdb_relation)}
"""
    connection = duckdb.connect(str(duckdb_path))
    try:
        return connection.execute(sql).to_arrow_table()
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


def _validate_non_empty_staging(
    spec: CanonicalLoadSpec,
    table_arrow: pa.Table,
    *,
    allow_empty: bool,
) -> None:
    if table_arrow.num_rows > 0 or allow_empty:
        return

    msg = (
        f"{spec.duckdb_relation} produced zero rows; refusing to overwrite "
        f"{spec.identifier} without allow_empty=True"
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


def _current_snapshot_id(table: Table, identifier: str) -> int:
    snapshot = table.current_snapshot()
    if snapshot is None:
        msg = f"{identifier} overwrite did not create a current snapshot"
        raise RuntimeError(msg)
    return int(snapshot.snapshot_id)


def _write_mart_snapshot_set_manifest(
    table: Table,
    *,
    load_id: str,
    tables: dict[str, dict[str, int | str]],
) -> None:
    manifest_path = _mart_snapshot_set_manifest_path(table)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "load_id": load_id,
        "published_at": datetime.now(UTC).isoformat(),
        "tables": tables,
    }
    temp_path = manifest_path.with_name(f".{manifest_path.name}.{load_id}.tmp")
    temp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    temp_path.replace(manifest_path)


def _mart_snapshot_set_manifest_path(table: Table) -> Path:
    table_location = _local_path_from_location(table.location())
    return table_location.parent / CANONICAL_MART_SNAPSHOT_SET_FILE


def _local_path_from_location(location: str) -> Path:
    parsed = urlparse(location)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    if parsed.scheme:
        msg = "canonical mart snapshot set manifest requires a local file Iceberg warehouse"
        raise ValueError(msg)
    return Path(location)


def _table_name_from_identifier(identifier: str) -> str:
    return identifier.rsplit(".", maxsplit=1)[-1]


def _serialize_result(result: WriteResult | list[WriteResult]) -> object:
    if isinstance(result, list):
        return [asdict(item) for item in result]
    return asdict(result)


__all__ = [
    "CANONICAL_MART_LOAD_SPECS",
    "CANONICAL_MART_SNAPSHOT_SET_FILE",
    "CanonicalLoadSpec",
    "WriteResult",
    "load_canonical_marts",
    "load_canonical_stock_basic",
    "load_canonical_table",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
