"""Asset definitions for the Tushare adapter."""

from __future__ import annotations

import pyarrow as pa  # type: ignore[import-untyped]

from data_platform.adapters.base import AssetSpec

TUSHARE_STOCK_BASIC_ASSET_NAME = "tushare_stock_basic"
TUSHARE_DAILY_ASSET_NAME = "tushare_daily"
TUSHARE_WEEKLY_ASSET_NAME = "tushare_weekly"
TUSHARE_MONTHLY_ASSET_NAME = "tushare_monthly"
TUSHARE_ADJ_FACTOR_ASSET_NAME = "tushare_adj_factor"
TUSHARE_DAILY_BASIC_ASSET_NAME = "tushare_daily_basic"

TUSHARE_STOCK_BASIC_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("symbol", pa.string()),
        ("name", pa.string()),
        ("area", pa.string()),
        ("industry", pa.string()),
        ("fullname", pa.string()),
        ("enname", pa.string()),
        ("cnspell", pa.string()),
        ("market", pa.string()),
        ("exchange", pa.string()),
        ("curr_type", pa.string()),
        ("list_status", pa.string()),
        ("list_date", pa.string()),
        ("delist_date", pa.string()),
        ("is_hs", pa.string()),
        ("act_name", pa.string()),
        ("act_ent_type", pa.string()),
    ]
)

TUSHARE_BAR_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("pre_close", pa.float64()),
        ("change", pa.float64()),
        ("pct_chg", pa.float64()),
        ("vol", pa.float64()),
        ("amount", pa.float64()),
    ]
)

TUSHARE_ADJ_FACTOR_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("adj_factor", pa.float64()),
    ]
)

TUSHARE_DAILY_BASIC_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("close", pa.float64()),
        ("turnover_rate", pa.float64()),
        ("turnover_rate_f", pa.float64()),
        ("volume_ratio", pa.float64()),
        ("pe", pa.float64()),
        ("pe_ttm", pa.float64()),
        ("pb", pa.float64()),
        ("ps", pa.float64()),
        ("ps_ttm", pa.float64()),
        ("dv_ratio", pa.float64()),
        ("dv_ttm", pa.float64()),
        ("total_share", pa.float64()),
        ("float_share", pa.float64()),
        ("free_share", pa.float64()),
        ("total_mv", pa.float64()),
        ("circ_mv", pa.float64()),
    ]
)

TUSHARE_STOCK_BASIC_FIELDS = tuple(TUSHARE_STOCK_BASIC_SCHEMA.names)
TUSHARE_STOCK_BASIC_FIELDS_CSV = ",".join(TUSHARE_STOCK_BASIC_FIELDS)
TUSHARE_BAR_FIELDS = tuple(TUSHARE_BAR_SCHEMA.names)
TUSHARE_BAR_FIELDS_CSV = ",".join(TUSHARE_BAR_FIELDS)
TUSHARE_ADJ_FACTOR_FIELDS = tuple(TUSHARE_ADJ_FACTOR_SCHEMA.names)
TUSHARE_ADJ_FACTOR_FIELDS_CSV = ",".join(TUSHARE_ADJ_FACTOR_FIELDS)
TUSHARE_DAILY_BASIC_FIELDS = tuple(TUSHARE_DAILY_BASIC_SCHEMA.names)
TUSHARE_DAILY_BASIC_FIELDS_CSV = ",".join(TUSHARE_DAILY_BASIC_FIELDS)

TUSHARE_STOCK_BASIC_ASSET = AssetSpec(
    name=TUSHARE_STOCK_BASIC_ASSET_NAME,
    dataset="stock_basic",
    partition="static",
    schema=TUSHARE_STOCK_BASIC_SCHEMA,
)

TUSHARE_DAILY_ASSET = AssetSpec(
    name=TUSHARE_DAILY_ASSET_NAME,
    dataset="daily",
    partition="daily",
    schema=TUSHARE_BAR_SCHEMA,
)

TUSHARE_WEEKLY_ASSET = AssetSpec(
    name=TUSHARE_WEEKLY_ASSET_NAME,
    dataset="weekly",
    partition="daily",
    schema=TUSHARE_BAR_SCHEMA,
)

TUSHARE_MONTHLY_ASSET = AssetSpec(
    name=TUSHARE_MONTHLY_ASSET_NAME,
    dataset="monthly",
    partition="daily",
    schema=TUSHARE_BAR_SCHEMA,
)

TUSHARE_ADJ_FACTOR_ASSET = AssetSpec(
    name=TUSHARE_ADJ_FACTOR_ASSET_NAME,
    dataset="adj_factor",
    partition="daily",
    schema=TUSHARE_ADJ_FACTOR_SCHEMA,
)

TUSHARE_DAILY_BASIC_ASSET = AssetSpec(
    name=TUSHARE_DAILY_BASIC_ASSET_NAME,
    dataset="daily_basic",
    partition="daily",
    schema=TUSHARE_DAILY_BASIC_SCHEMA,
)

TUSHARE_ASSETS = [
    TUSHARE_STOCK_BASIC_ASSET,
    TUSHARE_DAILY_ASSET,
    TUSHARE_WEEKLY_ASSET,
    TUSHARE_MONTHLY_ASSET,
    TUSHARE_ADJ_FACTOR_ASSET,
    TUSHARE_DAILY_BASIC_ASSET,
]

__all__ = [
    "TUSHARE_ADJ_FACTOR_ASSET",
    "TUSHARE_ADJ_FACTOR_ASSET_NAME",
    "TUSHARE_ADJ_FACTOR_FIELDS",
    "TUSHARE_ADJ_FACTOR_FIELDS_CSV",
    "TUSHARE_ADJ_FACTOR_SCHEMA",
    "TUSHARE_ASSETS",
    "TUSHARE_BAR_FIELDS",
    "TUSHARE_BAR_FIELDS_CSV",
    "TUSHARE_BAR_SCHEMA",
    "TUSHARE_DAILY_ASSET",
    "TUSHARE_DAILY_ASSET_NAME",
    "TUSHARE_DAILY_BASIC_ASSET",
    "TUSHARE_DAILY_BASIC_ASSET_NAME",
    "TUSHARE_DAILY_BASIC_FIELDS",
    "TUSHARE_DAILY_BASIC_FIELDS_CSV",
    "TUSHARE_DAILY_BASIC_SCHEMA",
    "TUSHARE_MONTHLY_ASSET",
    "TUSHARE_MONTHLY_ASSET_NAME",
    "TUSHARE_STOCK_BASIC_ASSET",
    "TUSHARE_STOCK_BASIC_ASSET_NAME",
    "TUSHARE_STOCK_BASIC_FIELDS",
    "TUSHARE_STOCK_BASIC_FIELDS_CSV",
    "TUSHARE_STOCK_BASIC_SCHEMA",
    "TUSHARE_WEEKLY_ASSET",
    "TUSHARE_WEEKLY_ASSET_NAME",
]
