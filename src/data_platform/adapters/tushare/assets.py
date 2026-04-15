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
TUSHARE_INDEX_BASIC_ASSET_NAME = "tushare_index_basic"
TUSHARE_INDEX_DAILY_ASSET_NAME = "tushare_index_daily"
TUSHARE_INDEX_WEIGHT_ASSET_NAME = "tushare_index_weight"
TUSHARE_INDEX_MEMBER_ASSET_NAME = "tushare_index_member"
TUSHARE_INDEX_CLASSIFY_ASSET_NAME = "tushare_index_classify"
TUSHARE_TRADE_CAL_ASSET_NAME = "tushare_trade_cal"
TUSHARE_STOCK_COMPANY_ASSET_NAME = "tushare_stock_company"
TUSHARE_NAMECHANGE_ASSET_NAME = "tushare_namechange"
TUSHARE_ANNS_ASSET_NAME = "tushare_anns"
TUSHARE_SUSPEND_D_ASSET_NAME = "tushare_suspend_d"
TUSHARE_DIVIDEND_ASSET_NAME = "tushare_dividend"
TUSHARE_SHARE_FLOAT_ASSET_NAME = "tushare_share_float"
TUSHARE_STK_HOLDERNUMBER_ASSET_NAME = "tushare_stk_holdernumber"
TUSHARE_DISCLOSURE_DATE_ASSET_NAME = "tushare_disclosure_date"

ALLOW_NULL_IDENTITY_METADATA_KEY = b"data_platform.allow_null_identity"
ALLOW_NULL_IDENTITY_METADATA_VALUE = b"true"


def _string_field(name: str, *, allow_null_identity: bool = False) -> pa.Field:
    metadata = None
    if allow_null_identity:
        metadata = {ALLOW_NULL_IDENTITY_METADATA_KEY: ALLOW_NULL_IDENTITY_METADATA_VALUE}
    return pa.field(name, pa.string(), metadata=metadata)


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

TUSHARE_INDEX_BASIC_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("name", pa.string()),
        ("fullname", pa.string()),
        ("market", pa.string()),
        ("publisher", pa.string()),
        ("index_type", pa.string()),
        ("category", pa.string()),
        ("base_date", pa.string()),
        ("base_point", TUSHARE_RAW_NUMERIC_TYPE),
        ("list_date", pa.string()),
        ("weight_rule", pa.string()),
        ("desc", pa.string()),
        ("exp_date", pa.string()),
    ]
)

TUSHARE_INDEX_DAILY_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("close", TUSHARE_RAW_NUMERIC_TYPE),
        ("open", TUSHARE_RAW_NUMERIC_TYPE),
        ("high", TUSHARE_RAW_NUMERIC_TYPE),
        ("low", TUSHARE_RAW_NUMERIC_TYPE),
        ("pre_close", TUSHARE_RAW_NUMERIC_TYPE),
        ("change", TUSHARE_RAW_NUMERIC_TYPE),
        ("pct_chg", TUSHARE_RAW_NUMERIC_TYPE),
        ("vol", TUSHARE_RAW_NUMERIC_TYPE),
        ("amount", TUSHARE_RAW_NUMERIC_TYPE),
    ]
)

TUSHARE_INDEX_WEIGHT_SCHEMA = pa.schema(
    [
        ("index_code", pa.string()),
        ("con_code", pa.string()),
        ("trade_date", pa.string()),
        ("weight", TUSHARE_RAW_NUMERIC_TYPE),
    ]
)

TUSHARE_INDEX_MEMBER_SCHEMA = pa.schema(
    [
        ("index_code", pa.string()),
        ("index_name", pa.string()),
        ("con_code", pa.string()),
        ("con_name", pa.string()),
        ("in_date", pa.string()),
        ("out_date", pa.string()),
        ("is_new", pa.string()),
    ]
)

TUSHARE_INDEX_CLASSIFY_SCHEMA = pa.schema(
    [
        ("index_code", pa.string()),
        ("industry_name", pa.string()),
        ("level", pa.string()),
        ("industry_code", pa.string()),
        ("is_pub", pa.string()),
        ("parent_code", pa.string()),
        ("src", pa.string()),
    ]
)

TUSHARE_TRADE_CAL_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("cal_date", pa.string()),
        ("is_open", pa.string()),
        ("pretrade_date", pa.string()),
    ]
)

TUSHARE_STOCK_COMPANY_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("exchange", pa.string()),
        ("chairman", pa.string()),
        ("manager", pa.string()),
        ("secretary", pa.string()),
        ("reg_capital", TUSHARE_RAW_NUMERIC_TYPE),
        ("setup_date", pa.string()),
        ("province", pa.string()),
        ("city", pa.string()),
        ("introduction", pa.string()),
        ("website", pa.string()),
        ("email", pa.string()),
        ("office", pa.string()),
        ("employees", TUSHARE_RAW_NUMERIC_TYPE),
        ("main_business", pa.string()),
        ("business_scope", pa.string()),
    ]
)

TUSHARE_NAMECHANGE_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("name", pa.string()),
        ("start_date", pa.string()),
        ("end_date", pa.string()),
        ("ann_date", pa.string()),
        ("change_reason", pa.string()),
    ]
)

TUSHARE_ANNS_SCHEMA = pa.schema(
    [
        _string_field("ts_code", allow_null_identity=True),
        ("ann_date", pa.string()),
        ("name", pa.string()),
        ("title", pa.string()),
        ("url", pa.string()),
        ("rec_time", pa.string()),
    ]
)

TUSHARE_SUSPEND_D_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_date", pa.string()),
        ("suspend_timing", pa.string()),
        ("suspend_type", pa.string()),
    ]
)

TUSHARE_DIVIDEND_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("end_date", pa.string()),
        ("ann_date", pa.string()),
        ("div_proc", pa.string()),
        ("stk_div", TUSHARE_RAW_NUMERIC_TYPE),
        ("stk_bo_rate", TUSHARE_RAW_NUMERIC_TYPE),
        ("stk_co_rate", TUSHARE_RAW_NUMERIC_TYPE),
        ("cash_div", TUSHARE_RAW_NUMERIC_TYPE),
        ("cash_div_tax", TUSHARE_RAW_NUMERIC_TYPE),
        ("record_date", pa.string()),
        ("ex_date", pa.string()),
        ("pay_date", pa.string()),
        ("div_listdate", pa.string()),
        ("imp_ann_date", pa.string()),
        ("base_date", pa.string()),
        ("base_share", TUSHARE_RAW_NUMERIC_TYPE),
    ]
)

TUSHARE_SHARE_FLOAT_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("ann_date", pa.string()),
        ("float_date", pa.string()),
        ("float_share", TUSHARE_RAW_NUMERIC_TYPE),
        ("float_ratio", TUSHARE_RAW_NUMERIC_TYPE),
        ("holder_name", pa.string()),
        ("share_type", pa.string()),
    ]
)

TUSHARE_STK_HOLDERNUMBER_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("ann_date", pa.string()),
        ("end_date", pa.string()),
        ("holder_num", TUSHARE_RAW_NUMERIC_TYPE),
    ]
)

TUSHARE_DISCLOSURE_DATE_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("ann_date", pa.string()),
        ("end_date", pa.string()),
        ("pre_date", pa.string()),
        ("actual_date", pa.string()),
        ("modify_date", pa.string()),
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
TUSHARE_INDEX_BASIC_FIELDS = tuple(TUSHARE_INDEX_BASIC_SCHEMA.names)
TUSHARE_INDEX_BASIC_FIELDS_CSV = ",".join(TUSHARE_INDEX_BASIC_FIELDS)
TUSHARE_INDEX_DAILY_FIELDS = tuple(TUSHARE_INDEX_DAILY_SCHEMA.names)
TUSHARE_INDEX_DAILY_FIELDS_CSV = ",".join(TUSHARE_INDEX_DAILY_FIELDS)
TUSHARE_INDEX_WEIGHT_FIELDS = tuple(TUSHARE_INDEX_WEIGHT_SCHEMA.names)
TUSHARE_INDEX_WEIGHT_FIELDS_CSV = ",".join(TUSHARE_INDEX_WEIGHT_FIELDS)
TUSHARE_INDEX_MEMBER_FIELDS = tuple(TUSHARE_INDEX_MEMBER_SCHEMA.names)
TUSHARE_INDEX_MEMBER_FIELDS_CSV = ",".join(TUSHARE_INDEX_MEMBER_FIELDS)
TUSHARE_INDEX_CLASSIFY_FIELDS = tuple(TUSHARE_INDEX_CLASSIFY_SCHEMA.names)
TUSHARE_INDEX_CLASSIFY_FIELDS_CSV = ",".join(TUSHARE_INDEX_CLASSIFY_FIELDS)
TUSHARE_TRADE_CAL_FIELDS = tuple(TUSHARE_TRADE_CAL_SCHEMA.names)
TUSHARE_TRADE_CAL_FIELDS_CSV = ",".join(TUSHARE_TRADE_CAL_FIELDS)
TUSHARE_STOCK_COMPANY_FIELDS = tuple(TUSHARE_STOCK_COMPANY_SCHEMA.names)
TUSHARE_STOCK_COMPANY_FIELDS_CSV = ",".join(TUSHARE_STOCK_COMPANY_FIELDS)
TUSHARE_NAMECHANGE_FIELDS = tuple(TUSHARE_NAMECHANGE_SCHEMA.names)
TUSHARE_NAMECHANGE_FIELDS_CSV = ",".join(TUSHARE_NAMECHANGE_FIELDS)
TUSHARE_ANNS_FIELDS = tuple(TUSHARE_ANNS_SCHEMA.names)
TUSHARE_ANNS_FIELDS_CSV = ",".join(TUSHARE_ANNS_FIELDS)
TUSHARE_SUSPEND_D_FIELDS = tuple(TUSHARE_SUSPEND_D_SCHEMA.names)
TUSHARE_SUSPEND_D_FIELDS_CSV = ",".join(TUSHARE_SUSPEND_D_FIELDS)
TUSHARE_DIVIDEND_FIELDS = tuple(TUSHARE_DIVIDEND_SCHEMA.names)
TUSHARE_DIVIDEND_FIELDS_CSV = ",".join(TUSHARE_DIVIDEND_FIELDS)
TUSHARE_SHARE_FLOAT_FIELDS = tuple(TUSHARE_SHARE_FLOAT_SCHEMA.names)
TUSHARE_SHARE_FLOAT_FIELDS_CSV = ",".join(TUSHARE_SHARE_FLOAT_FIELDS)
TUSHARE_STK_HOLDERNUMBER_FIELDS = tuple(TUSHARE_STK_HOLDERNUMBER_SCHEMA.names)
TUSHARE_STK_HOLDERNUMBER_FIELDS_CSV = ",".join(TUSHARE_STK_HOLDERNUMBER_FIELDS)
TUSHARE_DISCLOSURE_DATE_FIELDS = tuple(TUSHARE_DISCLOSURE_DATE_SCHEMA.names)
TUSHARE_DISCLOSURE_DATE_FIELDS_CSV = ",".join(TUSHARE_DISCLOSURE_DATE_FIELDS)

REFERENCE_DATA_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "index_basic": ("ts_code",),
    "index_daily": ("ts_code", "trade_date"),
    "index_weight": ("index_code", "con_code", "trade_date"),
    "index_member": ("index_code", "con_code", "in_date"),
    "index_classify": ("index_code",),
    "trade_cal": ("cal_date", "is_open"),
    "stock_company": ("ts_code",),
    "namechange": ("ts_code", "start_date"),
}

EVENT_METADATA_FIELDS: dict[str, tuple[str, ...]] = {
    "anns": TUSHARE_ANNS_FIELDS,
    "suspend_d": TUSHARE_SUSPEND_D_FIELDS,
    "dividend": TUSHARE_DIVIDEND_FIELDS,
    "share_float": TUSHARE_SHARE_FLOAT_FIELDS,
    "stk_holdernumber": TUSHARE_STK_HOLDERNUMBER_FIELDS,
    "disclosure_date": TUSHARE_DISCLOSURE_DATE_FIELDS,
}

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

TUSHARE_INDEX_BASIC_ASSET = AssetSpec(
    name=TUSHARE_INDEX_BASIC_ASSET_NAME,
    dataset="index_basic",
    partition="static",
    schema=TUSHARE_INDEX_BASIC_SCHEMA,
)

TUSHARE_INDEX_DAILY_ASSET = AssetSpec(
    name=TUSHARE_INDEX_DAILY_ASSET_NAME,
    dataset="index_daily",
    partition="daily",
    schema=TUSHARE_INDEX_DAILY_SCHEMA,
)

TUSHARE_INDEX_WEIGHT_ASSET = AssetSpec(
    name=TUSHARE_INDEX_WEIGHT_ASSET_NAME,
    dataset="index_weight",
    partition="daily",
    schema=TUSHARE_INDEX_WEIGHT_SCHEMA,
)

TUSHARE_INDEX_MEMBER_ASSET = AssetSpec(
    name=TUSHARE_INDEX_MEMBER_ASSET_NAME,
    dataset="index_member",
    partition="daily",
    schema=TUSHARE_INDEX_MEMBER_SCHEMA,
)

TUSHARE_INDEX_CLASSIFY_ASSET = AssetSpec(
    name=TUSHARE_INDEX_CLASSIFY_ASSET_NAME,
    dataset="index_classify",
    partition="static",
    schema=TUSHARE_INDEX_CLASSIFY_SCHEMA,
)

TUSHARE_TRADE_CAL_ASSET = AssetSpec(
    name=TUSHARE_TRADE_CAL_ASSET_NAME,
    dataset="trade_cal",
    partition="daily",
    schema=TUSHARE_TRADE_CAL_SCHEMA,
)

TUSHARE_STOCK_COMPANY_ASSET = AssetSpec(
    name=TUSHARE_STOCK_COMPANY_ASSET_NAME,
    dataset="stock_company",
    partition="static",
    schema=TUSHARE_STOCK_COMPANY_SCHEMA,
)

TUSHARE_NAMECHANGE_ASSET = AssetSpec(
    name=TUSHARE_NAMECHANGE_ASSET_NAME,
    dataset="namechange",
    partition="daily",
    schema=TUSHARE_NAMECHANGE_SCHEMA,
)

TUSHARE_ANNS_ASSET = AssetSpec(
    name=TUSHARE_ANNS_ASSET_NAME,
    dataset="anns",
    partition="daily",
    schema=TUSHARE_ANNS_SCHEMA,
)

TUSHARE_SUSPEND_D_ASSET = AssetSpec(
    name=TUSHARE_SUSPEND_D_ASSET_NAME,
    dataset="suspend_d",
    partition="daily",
    schema=TUSHARE_SUSPEND_D_SCHEMA,
)

TUSHARE_DIVIDEND_ASSET = AssetSpec(
    name=TUSHARE_DIVIDEND_ASSET_NAME,
    dataset="dividend",
    partition="daily",
    schema=TUSHARE_DIVIDEND_SCHEMA,
)

TUSHARE_SHARE_FLOAT_ASSET = AssetSpec(
    name=TUSHARE_SHARE_FLOAT_ASSET_NAME,
    dataset="share_float",
    partition="daily",
    schema=TUSHARE_SHARE_FLOAT_SCHEMA,
)

TUSHARE_STK_HOLDERNUMBER_ASSET = AssetSpec(
    name=TUSHARE_STK_HOLDERNUMBER_ASSET_NAME,
    dataset="stk_holdernumber",
    partition="daily",
    schema=TUSHARE_STK_HOLDERNUMBER_SCHEMA,
)

TUSHARE_DISCLOSURE_DATE_ASSET = AssetSpec(
    name=TUSHARE_DISCLOSURE_DATE_ASSET_NAME,
    dataset="disclosure_date",
    partition="daily",
    schema=TUSHARE_DISCLOSURE_DATE_SCHEMA,
)

TUSHARE_ASSETS = [
    TUSHARE_STOCK_BASIC_ASSET,
    TUSHARE_DAILY_ASSET,
    TUSHARE_WEEKLY_ASSET,
    TUSHARE_MONTHLY_ASSET,
    TUSHARE_ADJ_FACTOR_ASSET,
    TUSHARE_DAILY_BASIC_ASSET,
    TUSHARE_INDEX_BASIC_ASSET,
    TUSHARE_INDEX_DAILY_ASSET,
    TUSHARE_INDEX_WEIGHT_ASSET,
    TUSHARE_INDEX_MEMBER_ASSET,
    TUSHARE_INDEX_CLASSIFY_ASSET,
    TUSHARE_TRADE_CAL_ASSET,
    TUSHARE_STOCK_COMPANY_ASSET,
    TUSHARE_NAMECHANGE_ASSET,
    TUSHARE_ANNS_ASSET,
    TUSHARE_SUSPEND_D_ASSET,
    TUSHARE_DIVIDEND_ASSET,
    TUSHARE_SHARE_FLOAT_ASSET,
    TUSHARE_STK_HOLDERNUMBER_ASSET,
    TUSHARE_DISCLOSURE_DATE_ASSET,
]

__all__ = [
    "ALLOW_NULL_IDENTITY_METADATA_KEY",
    "ALLOW_NULL_IDENTITY_METADATA_VALUE",
    "EVENT_METADATA_FIELDS",
    "REFERENCE_DATA_IDENTITY_FIELDS",
    "TUSHARE_ADJ_FACTOR_ASSET",
    "TUSHARE_ADJ_FACTOR_ASSET_NAME",
    "TUSHARE_ADJ_FACTOR_FIELDS",
    "TUSHARE_ADJ_FACTOR_FIELDS_CSV",
    "TUSHARE_ADJ_FACTOR_SCHEMA",
    "TUSHARE_ANNS_ASSET",
    "TUSHARE_ANNS_ASSET_NAME",
    "TUSHARE_ANNS_FIELDS",
    "TUSHARE_ANNS_FIELDS_CSV",
    "TUSHARE_ANNS_SCHEMA",
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
    "TUSHARE_DISCLOSURE_DATE_ASSET",
    "TUSHARE_DISCLOSURE_DATE_ASSET_NAME",
    "TUSHARE_DISCLOSURE_DATE_FIELDS",
    "TUSHARE_DISCLOSURE_DATE_FIELDS_CSV",
    "TUSHARE_DISCLOSURE_DATE_SCHEMA",
    "TUSHARE_DIVIDEND_ASSET",
    "TUSHARE_DIVIDEND_ASSET_NAME",
    "TUSHARE_DIVIDEND_FIELDS",
    "TUSHARE_DIVIDEND_FIELDS_CSV",
    "TUSHARE_DIVIDEND_SCHEMA",
    "TUSHARE_INDEX_BASIC_ASSET",
    "TUSHARE_INDEX_BASIC_ASSET_NAME",
    "TUSHARE_INDEX_BASIC_FIELDS",
    "TUSHARE_INDEX_BASIC_FIELDS_CSV",
    "TUSHARE_INDEX_BASIC_SCHEMA",
    "TUSHARE_INDEX_CLASSIFY_ASSET",
    "TUSHARE_INDEX_CLASSIFY_ASSET_NAME",
    "TUSHARE_INDEX_CLASSIFY_FIELDS",
    "TUSHARE_INDEX_CLASSIFY_FIELDS_CSV",
    "TUSHARE_INDEX_CLASSIFY_SCHEMA",
    "TUSHARE_INDEX_DAILY_ASSET",
    "TUSHARE_INDEX_DAILY_ASSET_NAME",
    "TUSHARE_INDEX_DAILY_FIELDS",
    "TUSHARE_INDEX_DAILY_FIELDS_CSV",
    "TUSHARE_INDEX_DAILY_SCHEMA",
    "TUSHARE_INDEX_MEMBER_ASSET",
    "TUSHARE_INDEX_MEMBER_ASSET_NAME",
    "TUSHARE_INDEX_MEMBER_FIELDS",
    "TUSHARE_INDEX_MEMBER_FIELDS_CSV",
    "TUSHARE_INDEX_MEMBER_SCHEMA",
    "TUSHARE_INDEX_WEIGHT_ASSET",
    "TUSHARE_INDEX_WEIGHT_ASSET_NAME",
    "TUSHARE_INDEX_WEIGHT_FIELDS",
    "TUSHARE_INDEX_WEIGHT_FIELDS_CSV",
    "TUSHARE_INDEX_WEIGHT_SCHEMA",
    "TUSHARE_MONTHLY_ASSET",
    "TUSHARE_MONTHLY_ASSET_NAME",
    "TUSHARE_NAMECHANGE_ASSET",
    "TUSHARE_NAMECHANGE_ASSET_NAME",
    "TUSHARE_NAMECHANGE_FIELDS",
    "TUSHARE_NAMECHANGE_FIELDS_CSV",
    "TUSHARE_NAMECHANGE_SCHEMA",
    "TUSHARE_SHARE_FLOAT_ASSET",
    "TUSHARE_SHARE_FLOAT_ASSET_NAME",
    "TUSHARE_SHARE_FLOAT_FIELDS",
    "TUSHARE_SHARE_FLOAT_FIELDS_CSV",
    "TUSHARE_SHARE_FLOAT_SCHEMA",
    "TUSHARE_STOCK_BASIC_ASSET",
    "TUSHARE_STOCK_BASIC_ASSET_NAME",
    "TUSHARE_STOCK_BASIC_FIELDS",
    "TUSHARE_STOCK_BASIC_FIELDS_CSV",
    "TUSHARE_STOCK_BASIC_SCHEMA",
    "TUSHARE_STOCK_COMPANY_ASSET",
    "TUSHARE_STOCK_COMPANY_ASSET_NAME",
    "TUSHARE_STOCK_COMPANY_FIELDS",
    "TUSHARE_STOCK_COMPANY_FIELDS_CSV",
    "TUSHARE_STOCK_COMPANY_SCHEMA",
    "TUSHARE_STK_HOLDERNUMBER_ASSET",
    "TUSHARE_STK_HOLDERNUMBER_ASSET_NAME",
    "TUSHARE_STK_HOLDERNUMBER_FIELDS",
    "TUSHARE_STK_HOLDERNUMBER_FIELDS_CSV",
    "TUSHARE_STK_HOLDERNUMBER_SCHEMA",
    "TUSHARE_SUSPEND_D_ASSET",
    "TUSHARE_SUSPEND_D_ASSET_NAME",
    "TUSHARE_SUSPEND_D_FIELDS",
    "TUSHARE_SUSPEND_D_FIELDS_CSV",
    "TUSHARE_SUSPEND_D_SCHEMA",
    "TUSHARE_TRADE_CAL_ASSET",
    "TUSHARE_TRADE_CAL_ASSET_NAME",
    "TUSHARE_TRADE_CAL_FIELDS",
    "TUSHARE_TRADE_CAL_FIELDS_CSV",
    "TUSHARE_TRADE_CAL_SCHEMA",
    "TUSHARE_WEEKLY_ASSET",
    "TUSHARE_WEEKLY_ASSET_NAME",
]
