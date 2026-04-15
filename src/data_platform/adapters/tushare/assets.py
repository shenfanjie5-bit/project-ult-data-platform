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
TUSHARE_INCOME_ASSET_NAME = "tushare_income"
TUSHARE_BALANCESHEET_ASSET_NAME = "tushare_balancesheet"
TUSHARE_CASHFLOW_ASSET_NAME = "tushare_cashflow"
TUSHARE_FINA_INDICATOR_ASSET_NAME = "tushare_fina_indicator"

FINANCIAL_VERSION_FIELDS: tuple[str, ...] = (
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
    "update_flag",
)

_FINANCIAL_VERSION_SCHEMA_FIELDS = [
    ("ts_code", pa.string()),
    ("ann_date", pa.string()),
    ("f_ann_date", pa.string()),
    ("end_date", pa.string()),
    ("report_type", pa.string()),
    ("comp_type", pa.string()),
    ("update_flag", pa.string()),
]

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

TUSHARE_INCOME_SCHEMA = pa.schema(
    [
        *_FINANCIAL_VERSION_SCHEMA_FIELDS,
        ("basic_eps", pa.float64()),
        ("diluted_eps", pa.float64()),
        ("total_revenue", pa.float64()),
        ("revenue", pa.float64()),
        ("total_cogs", pa.float64()),
        ("operate_profit", pa.float64()),
        ("total_profit", pa.float64()),
        ("income_tax", pa.float64()),
        ("n_income", pa.float64()),
        ("n_income_attr_p", pa.float64()),
        ("minority_gain", pa.float64()),
        ("ebit", pa.float64()),
        ("ebitda", pa.float64()),
    ]
)

TUSHARE_BALANCESHEET_SCHEMA = pa.schema(
    [
        *_FINANCIAL_VERSION_SCHEMA_FIELDS,
        ("total_share", pa.float64()),
        ("money_cap", pa.float64()),
        ("accounts_receiv", pa.float64()),
        ("inventories", pa.float64()),
        ("fixed_assets", pa.float64()),
        ("total_cur_assets", pa.float64()),
        ("total_nca", pa.float64()),
        ("total_assets", pa.float64()),
        ("total_cur_liab", pa.float64()),
        ("total_ncl", pa.float64()),
        ("total_liab", pa.float64()),
        ("total_hldr_eqy_exc_min_int", pa.float64()),
        ("total_hldr_eqy_inc_min_int", pa.float64()),
        ("total_liab_hldr_eqy", pa.float64()),
    ]
)

TUSHARE_CASHFLOW_SCHEMA = pa.schema(
    [
        *_FINANCIAL_VERSION_SCHEMA_FIELDS,
        ("net_profit", pa.float64()),
        ("c_fr_sale_sg", pa.float64()),
        ("c_inf_fr_operate_a", pa.float64()),
        ("c_paid_goods_s", pa.float64()),
        ("c_paid_to_for_empl", pa.float64()),
        ("c_paid_for_taxes", pa.float64()),
        ("n_cashflow_act", pa.float64()),
        ("n_cashflow_inv_act", pa.float64()),
        ("n_cash_flows_fnc_act", pa.float64()),
        ("n_incr_cash_cash_equ", pa.float64()),
        ("c_cash_equ_beg_period", pa.float64()),
        ("c_cash_equ_end_period", pa.float64()),
        ("free_cashflow", pa.float64()),
    ]
)

TUSHARE_FINA_INDICATOR_SCHEMA = pa.schema(
    [
        *_FINANCIAL_VERSION_SCHEMA_FIELDS,
        ("eps", pa.float64()),
        ("dt_eps", pa.float64()),
        ("total_revenue_ps", pa.float64()),
        ("revenue_ps", pa.float64()),
        ("bps", pa.float64()),
        ("ocfps", pa.float64()),
        ("roe", pa.float64()),
        ("roe_waa", pa.float64()),
        ("roa", pa.float64()),
        ("netprofit_margin", pa.float64()),
        ("grossprofit_margin", pa.float64()),
        ("current_ratio", pa.float64()),
        ("quick_ratio", pa.float64()),
        ("debt_to_assets", pa.float64()),
        ("assets_turn", pa.float64()),
        ("op_yoy", pa.float64()),
        ("netprofit_yoy", pa.float64()),
        ("tr_yoy", pa.float64()),
        ("or_yoy", pa.float64()),
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
TUSHARE_INCOME_FIELDS = tuple(TUSHARE_INCOME_SCHEMA.names)
TUSHARE_INCOME_FIELDS_CSV = ",".join(TUSHARE_INCOME_FIELDS)
TUSHARE_BALANCESHEET_FIELDS = tuple(TUSHARE_BALANCESHEET_SCHEMA.names)
TUSHARE_BALANCESHEET_FIELDS_CSV = ",".join(TUSHARE_BALANCESHEET_FIELDS)
TUSHARE_CASHFLOW_FIELDS = tuple(TUSHARE_CASHFLOW_SCHEMA.names)
TUSHARE_CASHFLOW_FIELDS_CSV = ",".join(TUSHARE_CASHFLOW_FIELDS)
TUSHARE_FINA_INDICATOR_FIELDS = tuple(TUSHARE_FINA_INDICATOR_SCHEMA.names)
TUSHARE_FINA_INDICATOR_FIELDS_CSV = ",".join(TUSHARE_FINA_INDICATOR_FIELDS)

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

TUSHARE_INCOME_ASSET = AssetSpec(
    name=TUSHARE_INCOME_ASSET_NAME,
    dataset="income",
    partition="daily",
    schema=TUSHARE_INCOME_SCHEMA,
)

TUSHARE_BALANCESHEET_ASSET = AssetSpec(
    name=TUSHARE_BALANCESHEET_ASSET_NAME,
    dataset="balancesheet",
    partition="daily",
    schema=TUSHARE_BALANCESHEET_SCHEMA,
)

TUSHARE_CASHFLOW_ASSET = AssetSpec(
    name=TUSHARE_CASHFLOW_ASSET_NAME,
    dataset="cashflow",
    partition="daily",
    schema=TUSHARE_CASHFLOW_SCHEMA,
)

TUSHARE_FINA_INDICATOR_ASSET = AssetSpec(
    name=TUSHARE_FINA_INDICATOR_ASSET_NAME,
    dataset="fina_indicator",
    partition="daily",
    schema=TUSHARE_FINA_INDICATOR_SCHEMA,
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
    TUSHARE_INCOME_ASSET,
    TUSHARE_BALANCESHEET_ASSET,
    TUSHARE_CASHFLOW_ASSET,
    TUSHARE_FINA_INDICATOR_ASSET,
]

__all__ = [
    "FINANCIAL_VERSION_FIELDS",
    "REFERENCE_DATA_IDENTITY_FIELDS",
    "TUSHARE_ADJ_FACTOR_ASSET",
    "TUSHARE_ADJ_FACTOR_ASSET_NAME",
    "TUSHARE_ADJ_FACTOR_FIELDS",
    "TUSHARE_ADJ_FACTOR_FIELDS_CSV",
    "TUSHARE_ADJ_FACTOR_SCHEMA",
    "TUSHARE_ASSETS",
    "TUSHARE_BALANCESHEET_ASSET",
    "TUSHARE_BALANCESHEET_ASSET_NAME",
    "TUSHARE_BALANCESHEET_FIELDS",
    "TUSHARE_BALANCESHEET_FIELDS_CSV",
    "TUSHARE_BALANCESHEET_SCHEMA",
    "TUSHARE_BAR_FIELDS",
    "TUSHARE_BAR_FIELDS_CSV",
    "TUSHARE_BAR_SCHEMA",
    "TUSHARE_CASHFLOW_ASSET",
    "TUSHARE_CASHFLOW_ASSET_NAME",
    "TUSHARE_CASHFLOW_FIELDS",
    "TUSHARE_CASHFLOW_FIELDS_CSV",
    "TUSHARE_CASHFLOW_SCHEMA",
    "TUSHARE_DAILY_ASSET",
    "TUSHARE_DAILY_ASSET_NAME",
    "TUSHARE_DAILY_BASIC_ASSET",
    "TUSHARE_DAILY_BASIC_ASSET_NAME",
    "TUSHARE_DAILY_BASIC_FIELDS",
    "TUSHARE_DAILY_BASIC_FIELDS_CSV",
    "TUSHARE_DAILY_BASIC_SCHEMA",
    "TUSHARE_FINA_INDICATOR_ASSET",
    "TUSHARE_FINA_INDICATOR_ASSET_NAME",
    "TUSHARE_FINA_INDICATOR_FIELDS",
    "TUSHARE_FINA_INDICATOR_FIELDS_CSV",
    "TUSHARE_FINA_INDICATOR_SCHEMA",
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
    "TUSHARE_INCOME_ASSET",
    "TUSHARE_INCOME_ASSET_NAME",
    "TUSHARE_INCOME_FIELDS",
    "TUSHARE_INCOME_FIELDS_CSV",
    "TUSHARE_INCOME_SCHEMA",
    "TUSHARE_MONTHLY_ASSET",
    "TUSHARE_MONTHLY_ASSET_NAME",
    "TUSHARE_NAMECHANGE_ASSET",
    "TUSHARE_NAMECHANGE_ASSET_NAME",
    "TUSHARE_NAMECHANGE_FIELDS",
    "TUSHARE_NAMECHANGE_FIELDS_CSV",
    "TUSHARE_NAMECHANGE_SCHEMA",
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
    "TUSHARE_TRADE_CAL_ASSET",
    "TUSHARE_TRADE_CAL_ASSET_NAME",
    "TUSHARE_TRADE_CAL_FIELDS",
    "TUSHARE_TRADE_CAL_FIELDS_CSV",
    "TUSHARE_TRADE_CAL_SCHEMA",
    "TUSHARE_WEEKLY_ASSET",
    "TUSHARE_WEEKLY_ASSET_NAME",
]
