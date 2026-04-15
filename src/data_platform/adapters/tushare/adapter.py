"""Minimal Tushare adapter for the stock_basic API."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any, Protocol, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]

from data_platform.adapters.base import AssetSpec, BaseAdapter, FetchParams, QuotaConfig
from data_platform.adapters.tushare.assets import (
    TUSHARE_ASSETS,
    TUSHARE_STOCK_BASIC_ASSET_NAME,
    TUSHARE_STOCK_BASIC_FIELDS,
    TUSHARE_STOCK_BASIC_FIELDS_CSV,
    TUSHARE_STOCK_BASIC_SCHEMA,
)
from data_platform.raw import RawArtifact, RawWriter

TOKEN_ENV_VAR = "DP_TUSHARE_TOKEN"
STOCK_BASIC_IDENTITY_FIELDS = ("ts_code",)


class AdapterConfigError(RuntimeError):
    """Raised when an adapter is missing required runtime configuration."""


class _TushareClient(Protocol):
    def stock_basic(self, **kwargs: Any) -> Any:
        """Return stock_basic rows from Tushare Pro."""


class TushareAdapter(BaseAdapter):
    """Tushare reference adapter exposing only the stock_basic asset."""

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
        self._quota_config = QuotaConfig(requests_per_minute=200, daily_credit_quota=None)
        super().__init__(max_retries=max_retries)

    def source_id(self) -> str:
        return "tushare"

    def get_assets(self) -> list[AssetSpec]:
        return list(TUSHARE_ASSETS)

    def get_resources(self) -> dict[str, Any]:
        return {"token_env": TOKEN_ENV_VAR}

    def get_staging_dbt_models(self) -> list[str]:
        return ["stg_stock_basic"]

    def get_quota_config(self) -> QuotaConfig:
        return self._quota_config

    def _fetch(self, asset_id: str, params: FetchParams) -> pa.Table:
        if asset_id != TUSHARE_STOCK_BASIC_ASSET_NAME:
            msg = f"unsupported Tushare asset: {asset_id!r}"
            raise ValueError(msg)

        request_params = dict(params)
        request_params["fields"] = TUSHARE_STOCK_BASIC_FIELDS_CSV

        result = self._get_client().stock_basic(**request_params)
        return _to_stock_basic_table(result)

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


def run_stock_basic(asset_id: str, partition_date: date) -> RawArtifact:
    """Fetch a Tushare asset and write it as a Raw Zone Parquet artifact."""

    adapter = TushareAdapter()
    asset = _asset_by_name(adapter, asset_id)
    table = adapter.fetch(asset.name, {})

    if not isinstance(table, pa.Table):
        msg = f"Tushare fetch returned unsupported result for asset={asset.name!r}"
        raise TypeError(msg)

    return RawWriter().write_arrow(
        adapter.source_id(),
        asset.dataset,
        partition_date,
        str(uuid.uuid4()),
        table,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch Tushare assets into the Raw Zone.")
    parser.add_argument("--asset", required=True)
    parser.add_argument("--date", required=True, help="Raw partition date in YYYYMMDD format.")
    args = parser.parse_args(argv)

    try:
        partition_date = _parse_partition_date(args.date)
        artifact = run_stock_basic(args.asset, partition_date)
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


def _parse_partition_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        msg = f"date must use YYYYMMDD format: {value!r}"
        raise ValueError(msg) from exc


def _to_stock_basic_table(value: Any) -> pa.Table:
    if isinstance(value, pa.Table):
        _validate_stock_basic_fields(value.column_names)
        _validate_stock_basic_identity_columns(value)
        return value.select(TUSHARE_STOCK_BASIC_FIELDS).cast(TUSHARE_STOCK_BASIC_SCHEMA)

    field_names = _field_names_from_result(value)
    if field_names is not None:
        _validate_stock_basic_fields(field_names)

    rows = _records_from_result(value)
    _validate_stock_basic_records(rows)
    columns = {
        field_name: [_normalize_string(row[field_name]) for row in rows]
        for field_name in TUSHARE_STOCK_BASIC_FIELDS
    }
    return pa.table(columns, schema=TUSHARE_STOCK_BASIC_SCHEMA)


def _validate_stock_basic_fields(field_names: Sequence[str]) -> None:
    available = set(field_names)
    missing = [
        field_name for field_name in TUSHARE_STOCK_BASIC_FIELDS if field_name not in available
    ]
    if missing:
        joined = ", ".join(missing)
        msg = f"Tushare stock_basic response missing required fields: {joined}"
        raise ValueError(msg)


def _field_names_from_result(value: Any) -> list[str] | None:
    columns = getattr(value, "columns", None)
    if columns is None:
        return None

    try:
        return [str(field_name) for field_name in columns]
    except TypeError:
        return None


def _records_from_result(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return _coerce_record_list(value)

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        records = to_dict("records")
        if isinstance(records, list):
            return _coerce_record_list(records)

    msg = "Tushare stock_basic result must be a pandas DataFrame, Arrow table, or row list"
    raise TypeError(msg)


def _coerce_record_list(rows: list[Any]) -> list[Mapping[str, Any]]:
    records: list[Mapping[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            msg = (
                "Tushare stock_basic row "
                f"{index} must be a mapping, got {type(row).__name__}"
            )
            raise TypeError(msg)
        records.append(row)
    return records


def _validate_stock_basic_records(rows: Sequence[Mapping[str, Any]]) -> None:
    for index, row in enumerate(rows):
        missing = [
            field_name for field_name in TUSHARE_STOCK_BASIC_FIELDS if field_name not in row
        ]
        if missing:
            joined = ", ".join(missing)
            msg = f"Tushare stock_basic row {index} missing required fields: {joined}"
            raise ValueError(msg)

        for field_name in STOCK_BASIC_IDENTITY_FIELDS:
            if _is_nullish(row[field_name]):
                msg = f"Tushare stock_basic row {index} has null identity field: {field_name}"
                raise ValueError(msg)


def _validate_stock_basic_identity_columns(table: pa.Table) -> None:
    for field_name in STOCK_BASIC_IDENTITY_FIELDS:
        values = table[field_name].to_pylist()
        for index, value in enumerate(values):
            if _is_nullish(value):
                msg = f"Tushare stock_basic row {index} has null identity field: {field_name}"
                raise ValueError(msg)


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
    payload = {"error": str(exc), "asset": asset_id}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AdapterConfigError",
    "TOKEN_ENV_VAR",
    "TushareAdapter",
    "main",
    "run_stock_basic",
]
