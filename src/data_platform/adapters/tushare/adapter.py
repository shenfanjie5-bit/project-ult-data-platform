"""Tushare adapter for Raw Zone structured market data assets."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]

from data_platform.adapters.base import AdapterFetchError, AssetSpec, BaseAdapter, FetchParams
from data_platform.adapters.tushare.assets import (
    ALLOW_NULL_IDENTITY_METADATA_KEY,
    ALLOW_NULL_IDENTITY_METADATA_VALUE,
    EVENT_METADATA_FIELDS,
    REFERENCE_DATA_IDENTITY_FIELDS,
    TUSHARE_ASSETS,
    TUSHARE_STOCK_BASIC_ASSET_NAME,
)
from data_platform.raw import RawArtifact, RawWriter

TOKEN_ENV_VAR = "DP_TUSHARE_TOKEN"
STOCK_BASIC_IDENTITY_FIELDS = ("ts_code",)
MARKET_DATA_IDENTITY_FIELDS = ("ts_code", "trade_date")
MARKET_DATASETS = frozenset({"daily", "weekly", "monthly", "adj_factor", "daily_basic"})
EVENT_DATASETS = frozenset(EVENT_METADATA_FIELDS)
STOCK_TS_CODE_DATASETS = frozenset(
    {
        "stock_basic",
        "daily",
        "weekly",
        "monthly",
        "adj_factor",
        "daily_basic",
        "stock_company",
        "namechange",
        *EVENT_DATASETS,
    }
)
DATE_IDENTITY_FIELDS = frozenset(
    {"trade_date", "cal_date", "start_date", "in_date", "ann_date", "end_date", "float_date"}
)
STOCK_BASIC_TS_CODE_PATTERN = re.compile(r"\d{6}\.(?:SH|SZ|BJ)")
TRADE_DATE_PATTERN = re.compile(r"\d{8}")


@dataclass(frozen=True, slots=True)
class _TushareFetchSpec:
    asset: AssetSpec
    method_name: str
    identity_fields: tuple[str, ...]
    partition_date_field: str | None = None
    partition_request_params: tuple[str, ...] = ()
    date_param_names: tuple[str, ...] = ()


class AdapterConfigError(RuntimeError):
    """Raised when an adapter is missing required runtime configuration."""


class UpstreamSchemaError(ValueError):
    """Raised when an upstream Tushare response does not match the declared schema."""


class UpstreamEmptyResult(ValueError):
    """Raised when Tushare returns no rows for a Raw Zone event partition."""


class _TushareClient(Protocol):
    def stock_basic(self, **kwargs: Any) -> Any:
        """Return stock_basic rows from Tushare Pro."""

    def daily(self, **kwargs: Any) -> Any:
        """Return daily bar rows from Tushare Pro."""

    def weekly(self, **kwargs: Any) -> Any:
        """Return weekly bar rows from Tushare Pro."""

    def monthly(self, **kwargs: Any) -> Any:
        """Return monthly bar rows from Tushare Pro."""

    def adj_factor(self, **kwargs: Any) -> Any:
        """Return adjustment factor rows from Tushare Pro."""

    def daily_basic(self, **kwargs: Any) -> Any:
        """Return daily basic rows from Tushare Pro."""

    def index_basic(self, **kwargs: Any) -> Any:
        """Return index_basic rows from Tushare Pro."""

    def index_daily(self, **kwargs: Any) -> Any:
        """Return index_daily rows from Tushare Pro."""

    def index_weight(self, **kwargs: Any) -> Any:
        """Return index_weight rows from Tushare Pro."""

    def index_member(self, **kwargs: Any) -> Any:
        """Return index_member rows from Tushare Pro."""

    def index_classify(self, **kwargs: Any) -> Any:
        """Return index_classify rows from Tushare Pro."""

    def trade_cal(self, **kwargs: Any) -> Any:
        """Return trade_cal rows from Tushare Pro."""

    def stock_company(self, **kwargs: Any) -> Any:
        """Return stock_company rows from Tushare Pro."""

    def namechange(self, **kwargs: Any) -> Any:
        """Return namechange rows from Tushare Pro."""

    def anns(self, **kwargs: Any) -> Any:
        """Return announcement metadata rows from Tushare Pro."""

    def suspend_d(self, **kwargs: Any) -> Any:
        """Return suspend/resume event rows from Tushare Pro."""

    def dividend(self, **kwargs: Any) -> Any:
        """Return dividend event rows from Tushare Pro."""

    def share_float(self, **kwargs: Any) -> Any:
        """Return restricted-share unlock event rows from Tushare Pro."""

    def stk_holdernumber(self, **kwargs: Any) -> Any:
        """Return shareholder count event rows from Tushare Pro."""

    def disclosure_date(self, **kwargs: Any) -> Any:
        """Return disclosure calendar event rows from Tushare Pro."""


class TushareAdapter(BaseAdapter):
    """Tushare reference adapter exposing Raw Zone structured assets."""

    def __init__(
        self,
        *,
        token: str | None = None,
        client: _TushareClient | None = None,
        max_retries: int = 3,
    ) -> None:
        resolved_token = token or os.environ.get(TOKEN_ENV_VAR)
        if not resolved_token:
            msg = f"{TOKEN_ENV_VAR} is required for the Tushare adapter"
            raise AdapterConfigError(msg)

        self._token = resolved_token
        self._client = client
        self._quota_config: dict[str, Any] = {
            "requests_per_minute": 200,
            "daily_credit_quota": None,
        }
        super().__init__(max_retries=max_retries)

    def source_id(self) -> str:
        return "tushare"

    def get_assets(self) -> list[AssetSpec]:
        return list(TUSHARE_ASSETS)

    def get_resources(self) -> dict[str, Any]:
        return {"token_env": TOKEN_ENV_VAR}

    def get_staging_dbt_models(self) -> list[str]:
        return [f"stg_{asset.dataset}" for asset in TUSHARE_ASSETS]

    def get_quota_config(self) -> dict[str, Any]:
        return dict(self._quota_config)

    def _fetch(self, asset_id: str, params: FetchParams) -> pa.Table:
        spec = _fetch_spec_by_asset_name(asset_id)
        request_params = dict(params)
        _validate_date_params(spec.asset.dataset, request_params, spec.date_param_names)
        request_params["fields"] = _fields_csv(spec.asset)

        fetch_method = getattr(self._get_client(), spec.method_name)
        result = fetch_method(**request_params)
        if spec.asset.dataset in REFERENCE_DATA_IDENTITY_FIELDS:
            return _to_reference_table(spec.asset.dataset, result, spec.asset.schema)
        if spec.asset.dataset in EVENT_METADATA_FIELDS:
            return _to_event_table(spec.asset.dataset, result, spec.asset.schema)
        return _to_asset_table(result, spec.asset, spec.identity_fields)

    def _get_client(self) -> _TushareClient:
        if self._client is not None:
            return self._client

        try:
            import tushare as ts  # type: ignore[import-untyped]
        except ModuleNotFoundError as exc:
            msg = "tushare>=1.4 is required to fetch Tushare assets"
            raise AdapterConfigError(msg) from exc

        client = cast(_TushareClient, ts.pro_api(self._token))
        self._client = client
        return client


def run_tushare_asset(
    asset_id: str,
    partition_date: date,
    params: FetchParams | None = None,
) -> RawArtifact:
    """Fetch a Tushare asset and write it as a Raw Zone Parquet artifact."""

    adapter = TushareAdapter()
    asset = _asset_by_name(adapter, asset_id)
    table = adapter.fetch(asset.name, _fetch_params_for_raw_partition(asset, partition_date, params))

    if not isinstance(table, pa.Table):
        msg = f"Tushare fetch returned unsupported result for asset={asset.name!r}"
        raise TypeError(msg)

    if asset.partition == "daily":
        spec = _fetch_spec_by_asset_name(asset.name)
        _validate_raw_partition_date(table, asset, partition_date, spec.partition_date_field)

    return RawWriter().write_arrow(
        adapter.source_id(),
        asset.dataset,
        partition_date,
        str(uuid.uuid4()),
        table,
    )


def run_stock_basic(asset_id: str, partition_date: date) -> RawArtifact:
    """Compatibility wrapper for the original stock_basic Raw runner."""

    return run_tushare_asset(asset_id, partition_date)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch Tushare assets into the Raw Zone.")
    parser.add_argument("--asset", required=True)
    parser.add_argument("--date", required=True, help="Raw partition date in YYYYMMDD format.")
    args = parser.parse_args(argv)

    try:
        partition_date = _parse_partition_date(args.date)
        artifact = run_tushare_asset(args.asset, partition_date)
    except Exception as exc:
        _print_error(exc, args.asset)
        return 1

    print(artifact.path)
    return 0


def _asset_by_name(adapter: TushareAdapter, asset_id: str) -> AssetSpec:
    for asset in adapter.get_assets():
        if asset.name == asset_id:
            return asset
    msg = f"unsupported Tushare asset: {asset_id!r}"
    raise ValueError(msg)


def _fetch_spec_by_asset_name(asset_id: str) -> _TushareFetchSpec:
    try:
        return _FETCH_SPECS_BY_ASSET_NAME[asset_id]
    except KeyError as exc:
        msg = f"unsupported Tushare asset: {asset_id!r}"
        raise ValueError(msg) from exc


def _fetch_params_for_raw_partition(
    asset: AssetSpec,
    partition_date: date,
    params: FetchParams | None,
) -> dict[str, Any]:
    fetch_params = dict(params or {})
    if asset.partition == "daily":
        spec = _fetch_spec_by_asset_name(asset.name)
        expected_trade_date = f"{partition_date:%Y%m%d}"
        for param_name in spec.partition_request_params:
            if param_name not in fetch_params:
                fetch_params[param_name] = expected_trade_date
                continue

            _validate_date_param(asset.dataset, param_name, fetch_params[param_name])
            if str(fetch_params[param_name]) != expected_trade_date:
                msg = (
                    f"Tushare {asset.dataset} {param_name} {fetch_params[param_name]!r} "
                    f"does not match Raw partition date {expected_trade_date!r}"
                )
                raise ValueError(msg)
    return fetch_params


def _parse_partition_date(value: str) -> date:
    if not TRADE_DATE_PATTERN.fullmatch(value):
        msg = f"date must use YYYYMMDD format: {value!r}"
        raise ValueError(msg)

    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        msg = f"date must use YYYYMMDD format: {value!r}"
        raise ValueError(msg) from exc


def _to_stock_basic_table(value: Any) -> pa.Table:
    spec = _fetch_spec_by_asset_name(TUSHARE_STOCK_BASIC_ASSET_NAME)
    return _to_asset_table(value, spec.asset, spec.identity_fields)


def _to_reference_table(dataset: str, result: Any, schema: pa.Schema) -> pa.Table:
    try:
        spec = _FETCH_SPECS_BY_DATASET[dataset]
        identity_fields = REFERENCE_DATA_IDENTITY_FIELDS[dataset]
    except KeyError as exc:
        msg = f"unsupported Tushare reference dataset: {dataset!r}"
        raise ValueError(msg) from exc

    asset = AssetSpec(
        name=spec.asset.name,
        dataset=spec.asset.dataset,
        partition=spec.asset.partition,
        schema=schema,
    )
    return _to_asset_table(result, asset, identity_fields)


def _to_event_table(dataset: str, result: Any, schema: pa.Schema) -> pa.Table:
    try:
        spec = _FETCH_SPECS_BY_DATASET[dataset]
        identity_fields = EVENT_IDENTITY_FIELDS[dataset]
        event_date_fields = EVENT_DATE_FIELDS[dataset]
    except KeyError as exc:
        msg = f"unsupported Tushare event dataset: {dataset!r}"
        raise ValueError(msg) from exc

    asset = AssetSpec(
        name=spec.asset.name,
        dataset=spec.asset.dataset,
        partition=spec.asset.partition,
        schema=schema,
    )
    table = _to_asset_table(result, asset, identity_fields)
    if table.num_rows == 0:
        msg = f"Tushare {asset.dataset} response returned an empty table"
        raise UpstreamEmptyResult(msg)
    _validate_event_date_columns(table, asset, event_date_fields)
    return table


def _to_asset_table(
    value: Any,
    asset: AssetSpec,
    identity_fields: tuple[str, ...],
) -> pa.Table:
    if isinstance(value, pa.Table):
        _validate_asset_fields(value.column_names, asset)
        _validate_asset_identity_columns(value, asset, identity_fields)
        return value.select(asset.schema.names).cast(asset.schema)

    field_names = _field_names_from_result(value)
    if field_names is not None:
        _validate_asset_fields(field_names, asset)

    rows = _records_from_result(value, asset)
    _validate_asset_records(rows, asset, identity_fields)
    columns = {
        field.name: [_normalize_value(row[field.name], field.type) for row in rows]
        for field in asset.schema
    }
    return pa.table(columns, schema=asset.schema)


def _validate_asset_fields(field_names: Sequence[str], asset: AssetSpec) -> None:
    available = set(field_names)
    missing = [field_name for field_name in asset.schema.names if field_name not in available]
    if missing:
        joined = ", ".join(missing)
        msg = f"Tushare {asset.dataset} response missing required fields: {joined}"
        raise UpstreamSchemaError(msg)


def _field_names_from_result(value: Any) -> list[str] | None:
    columns = getattr(value, "columns", None)
    if columns is None:
        return None

    try:
        return [str(field_name) for field_name in columns]
    except TypeError:
        return None


def _records_from_result(value: Any, asset: AssetSpec) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return _coerce_record_list(value, asset)

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        records = to_dict("records")
        if isinstance(records, list):
            return _coerce_record_list(records, asset)

    msg = f"Tushare {asset.dataset} result must be a pandas DataFrame, Arrow table, or row list"
    raise TypeError(msg)


def _coerce_record_list(rows: list[Any], asset: AssetSpec) -> list[Mapping[str, Any]]:
    records: list[Mapping[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            msg = (
                f"Tushare {asset.dataset} row "
                f"{index} must be a mapping, got {type(row).__name__}"
            )
            raise TypeError(msg)
        records.append(row)
    return records


def _validate_asset_records(
    rows: Sequence[Mapping[str, Any]],
    asset: AssetSpec,
    identity_fields: tuple[str, ...],
) -> None:
    for index, row in enumerate(rows):
        missing = [field_name for field_name in asset.schema.names if field_name not in row]
        if missing:
            joined = ", ".join(missing)
            msg = f"Tushare {asset.dataset} row {index} missing required fields: {joined}"
            raise UpstreamSchemaError(msg)

        for field_name in identity_fields:
            _validate_identity_value(row[field_name], index, field_name, asset)


def _validate_asset_identity_columns(
    table: pa.Table,
    asset: AssetSpec,
    identity_fields: tuple[str, ...],
) -> None:
    for field_name in identity_fields:
        values = table[field_name].to_pylist()
        for index, value in enumerate(values):
            _validate_identity_value(value, index, field_name, asset)


def _validate_identity_value(
    value: Any,
    row_index: int,
    field_name: str,
    asset: AssetSpec,
) -> None:
    if _is_nullish(value):
        if _allows_null_identity(asset.schema, field_name):
            return
        msg = f"Tushare {asset.dataset} row {row_index} has null identity field: {field_name}"
        raise ValueError(msg)

    if not pd.api.types.is_scalar(value):
        msg = (
            f"Tushare {asset.dataset} row {row_index} has non-scalar identity field: "
            f"{field_name}"
        )
        raise ValueError(msg)

    normalized_value = str(value)
    if not normalized_value.strip():
        if _allows_null_identity(asset.schema, field_name):
            return
        msg = f"Tushare {asset.dataset} row {row_index} has blank identity field: {field_name}"
        raise ValueError(msg)

    if field_name == "ts_code" and asset.dataset in STOCK_TS_CODE_DATASETS and (
        normalized_value != normalized_value.strip()
        or not STOCK_BASIC_TS_CODE_PATTERN.fullmatch(normalized_value)
    ):
        msg = f"Tushare {asset.dataset} row {row_index} has malformed identity field: {field_name}"
        raise ValueError(msg)

    if field_name in DATE_IDENTITY_FIELDS and not _is_valid_trade_date(normalized_value):
        msg = f"Tushare {asset.dataset} row {row_index} has malformed identity field: {field_name}"
        raise ValueError(msg)


def _validate_date_params(
    dataset: str,
    params: Mapping[str, Any],
    param_names: tuple[str, ...],
) -> None:
    for param_name in param_names:
        value = params.get(param_name)
        if value is None:
            continue
        _validate_date_param(dataset, param_name, value)


def _validate_date_param(dataset: str, param_name: str, value: Any) -> None:
    if not pd.api.types.is_scalar(value) or _is_nullish(value):
        msg = f"Tushare {dataset} {param_name} must be a YYYYMMDD string"
        raise ValueError(msg)
    if not _is_valid_trade_date(str(value)):
        msg = f"Tushare {dataset} {param_name} must be a valid YYYYMMDD date: {value!r}"
        raise ValueError(msg)


def _validate_raw_partition_date(
    table: pa.Table,
    asset: AssetSpec,
    partition_date: date,
    partition_date_field: str | None,
) -> None:
    if partition_date_field is None:
        msg = f"Tushare {asset.dataset} daily partition date field is not configured"
        raise ValueError(msg)

    expected_trade_date = f"{partition_date:%Y%m%d}"
    if partition_date_field not in table.column_names:
        msg = f"Tushare {asset.dataset} response missing required fields: {partition_date_field}"
        raise ValueError(msg)

    for index, value in enumerate(table[partition_date_field].to_pylist()):
        if str(value) != expected_trade_date:
            msg = (
                f"Tushare {asset.dataset} row {index} {partition_date_field} {value!r} "
                f"does not match Raw partition date {expected_trade_date!r}"
            )
            raise ValueError(msg)


def _validate_event_date_columns(
    table: pa.Table,
    asset: AssetSpec,
    event_date_fields: tuple[str, ...],
) -> None:
    for field_name in event_date_fields:
        if field_name not in table.column_names:
            msg = f"Tushare {asset.dataset} response missing required fields: {field_name}"
            raise UpstreamSchemaError(msg)

        for index, value in enumerate(table[field_name].to_pylist()):
            if not pd.api.types.is_scalar(value) or _is_nullish(value):
                msg = (
                    f"Tushare {asset.dataset} row {index} has null event date field: "
                    f"{field_name}"
                )
                raise ValueError(msg)
            if not _is_valid_trade_date(str(value)):
                msg = (
                    f"Tushare {asset.dataset} row {index} has malformed event date field: "
                    f"{field_name}"
                )
                raise ValueError(msg)


def _allows_null_identity(schema: pa.Schema, field_name: str) -> bool:
    field = schema.field(field_name)
    metadata = field.metadata or {}
    return metadata.get(ALLOW_NULL_IDENTITY_METADATA_KEY) == ALLOW_NULL_IDENTITY_METADATA_VALUE


def _is_valid_trade_date(value: str) -> bool:
    if not TRADE_DATE_PATTERN.fullmatch(value):
        return False
    try:
        parsed = datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return False
    return f"{parsed:%Y%m%d}" == value


def _fields_csv(asset: AssetSpec) -> str:
    return ",".join(asset.schema.names)


def _normalize_value(value: Any, data_type: pa.DataType) -> Any:
    if _is_nullish(value):
        return None
    if pa.types.is_string(data_type):
        return _normalize_string(value)
    return value


def _normalize_string(value: Any) -> str | None:
    if _is_nullish(value):
        return None
    return str(value)


def _is_nullish(value: Any) -> bool:
    if value is None:
        return True

    result = pd.isna(value)
    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _print_error(exc: BaseException, asset_id: str) -> None:
    payload = {"error": str(exc), "asset": asset_id, "error_type": _error_type(exc)}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)


def _error_type(exc: BaseException) -> str:
    cause = exc.cause if isinstance(exc, AdapterFetchError) else exc
    if isinstance(cause, AdapterConfigError):
        return "config_error"
    if isinstance(cause, UpstreamSchemaError):
        return "upstream_missing_columns"
    if isinstance(cause, UpstreamEmptyResult):
        return "upstream_empty_table"
    return "runtime_error"


EVENT_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "anns": ("ts_code", "ann_date", "title", "url"),
    "suspend_d": ("ts_code", "trade_date"),
    "dividend": ("ts_code", "ann_date", "end_date"),
    "share_float": ("ts_code", "ann_date", "float_date"),
    "stk_holdernumber": ("ts_code", "ann_date", "end_date"),
    "disclosure_date": ("ts_code", "ann_date", "end_date"),
}
EVENT_DATE_FIELDS: dict[str, tuple[str, ...]] = {
    "anns": ("ann_date",),
    "suspend_d": ("trade_date",),
    "dividend": ("ann_date",),
    "share_float": ("ann_date", "float_date"),
    "stk_holdernumber": ("ann_date", "end_date"),
    "disclosure_date": ("ann_date", "end_date"),
}
_METHOD_BY_DATASET = {
    "stock_basic": "stock_basic",
    "daily": "daily",
    "weekly": "weekly",
    "monthly": "monthly",
    "adj_factor": "adj_factor",
    "daily_basic": "daily_basic",
    "index_basic": "index_basic",
    "index_daily": "index_daily",
    "index_weight": "index_weight",
    "index_member": "index_member",
    "index_classify": "index_classify",
    "trade_cal": "trade_cal",
    "stock_company": "stock_company",
    "namechange": "namechange",
    "anns": "anns",
    "suspend_d": "suspend_d",
    "dividend": "dividend",
    "share_float": "share_float",
    "stk_holdernumber": "stk_holdernumber",
    "disclosure_date": "disclosure_date",
}
_IDENTITY_FIELDS_BY_DATASET = {
    "stock_basic": STOCK_BASIC_IDENTITY_FIELDS,
    **{dataset: MARKET_DATA_IDENTITY_FIELDS for dataset in MARKET_DATASETS},
    **REFERENCE_DATA_IDENTITY_FIELDS,
    **EVENT_IDENTITY_FIELDS,
}
_PARTITION_DATE_FIELD_BY_DATASET = {
    **{dataset: "trade_date" for dataset in MARKET_DATASETS},
    "index_daily": "trade_date",
    "index_weight": "trade_date",
    "index_member": "in_date",
    "trade_cal": "cal_date",
    "namechange": "start_date",
    "anns": "ann_date",
    "suspend_d": "trade_date",
    "dividend": "ann_date",
    "share_float": "ann_date",
    "stk_holdernumber": "ann_date",
    "disclosure_date": "ann_date",
}
_PARTITION_REQUEST_PARAMS_BY_DATASET = {
    **{dataset: ("trade_date",) for dataset in MARKET_DATASETS},
    "index_daily": ("trade_date",),
    "index_weight": ("trade_date",),
    "index_member": ("start_date", "end_date"),
    "trade_cal": ("start_date", "end_date"),
    "namechange": ("start_date", "end_date"),
    "anns": ("ann_date",),
    "suspend_d": ("trade_date",),
    "dividend": ("ann_date",),
    "share_float": ("ann_date",),
    "stk_holdernumber": ("ann_date",),
    "disclosure_date": ("ann_date",),
}
_DATE_PARAM_NAMES_BY_DATASET = {
    **{dataset: ("trade_date", "start_date", "end_date") for dataset in MARKET_DATASETS},
    "index_daily": ("trade_date", "start_date", "end_date"),
    "index_weight": ("trade_date", "start_date", "end_date"),
    "index_member": ("start_date", "end_date"),
    "trade_cal": ("start_date", "end_date"),
    "namechange": ("start_date", "end_date"),
    "anns": ("ann_date", "start_date", "end_date"),
    "suspend_d": ("trade_date", "start_date", "end_date"),
    "dividend": ("ann_date", "record_date", "ex_date", "pay_date", "start_date", "end_date"),
    "share_float": ("ann_date", "float_date", "start_date", "end_date"),
    "stk_holdernumber": ("ann_date", "end_date", "start_date"),
    "disclosure_date": (
        "ann_date",
        "end_date",
        "pre_date",
        "actual_date",
        "modify_date",
        "start_date",
    ),
}
_FETCH_SPECS_BY_ASSET_NAME = {
    _asset.name: _TushareFetchSpec(
        asset=_asset,
        method_name=_METHOD_BY_DATASET[_asset.dataset],
        identity_fields=_IDENTITY_FIELDS_BY_DATASET[_asset.dataset],
        partition_date_field=_PARTITION_DATE_FIELD_BY_DATASET.get(_asset.dataset),
        partition_request_params=_PARTITION_REQUEST_PARAMS_BY_DATASET.get(_asset.dataset, ()),
        date_param_names=_DATE_PARAM_NAMES_BY_DATASET.get(_asset.dataset, ()),
    )
    for _asset in TUSHARE_ASSETS
}
_FETCH_SPECS_BY_DATASET = {
    _fetch_spec.asset.dataset: _fetch_spec for _fetch_spec in _FETCH_SPECS_BY_ASSET_NAME.values()
}


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AdapterConfigError",
    "TOKEN_ENV_VAR",
    "TushareAdapter",
    "_to_event_table",
    "_to_reference_table",
    "main",
    "run_stock_basic",
    "run_tushare_asset",
]
