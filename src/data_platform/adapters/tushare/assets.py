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

TUSHARE_RAW_NUMERIC_TYPE = pa.string()

TUSHARE_BAR_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("open", TUSHARE_RAW_NUMERIC_TYPE),
        ("high", TUSHARE_RAW_NUMERIC_TYPE),
        ("low", TUSHARE_RAW_NUMERIC_TYPE),
        ("close", TUSHARE_RAW_NUMERIC_TYPE),
        ("pre_close", TUSHARE_RAW_NUMERIC_TYPE),
        ("change", TUSHARE_RAW_NUMERIC_TYPE),
        ("pct_chg", TUSHARE_RAW_NUMERIC_TYPE),
        ("vol", TUSHARE_RAW_NUMERIC_TYPE),
        ("amount", TUSHARE_RAW_NUMERIC_TYPE),
    ]
)

TUSHARE_ADJ_FACTOR_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("adj_factor", TUSHARE_RAW_NUMERIC_TYPE),
    ]
)

TUSHARE_DAILY_BASIC_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("close", TUSHARE_RAW_NUMERIC_TYPE),
        ("turnover_rate", TUSHARE_RAW_NUMERIC_TYPE),
        ("turnover_rate_f", TUSHARE_RAW_NUMERIC_TYPE),
        ("volume_ratio", TUSHARE_RAW_NUMERIC_TYPE),
        ("pe", TUSHARE_RAW_NUMERIC_TYPE),
        ("pe_ttm", TUSHARE_RAW_NUMERIC_TYPE),
        ("pb", TUSHARE_RAW_NUMERIC_TYPE),
        ("ps", TUSHARE_RAW_NUMERIC_TYPE),
        ("ps_ttm", TUSHARE_RAW_NUMERIC_TYPE),
        ("dv_ratio", TUSHARE_RAW_NUMERIC_TYPE),
        ("dv_ttm", TUSHARE_RAW_NUMERIC_TYPE),
        ("total_share", TUSHARE_RAW_NUMERIC_TYPE),
        ("float_share", TUSHARE_RAW_NUMERIC_TYPE),
        ("free_share", TUSHARE_RAW_NUMERIC_TYPE),
        ("total_mv", TUSHARE_RAW_NUMERIC_TYPE),
        ("circ_mv", TUSHARE_RAW_NUMERIC_TYPE),
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
