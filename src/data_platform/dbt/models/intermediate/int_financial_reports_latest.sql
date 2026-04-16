{{ config(materialized="table") }}

with versions as (
    select
        ts_code,
        ann_date,
        f_ann_date,
        end_date,
        report_type,
        comp_type,
        update_flag
    from {{ ref('stg_income') }}

    union

    select
        ts_code,
        ann_date,
        f_ann_date,
        end_date,
        report_type,
        comp_type,
        update_flag
    from {{ ref('stg_balancesheet') }}

    union

    select
        ts_code,
        ann_date,
        f_ann_date,
        end_date,
        report_type,
        comp_type,
        update_flag
    from {{ ref('stg_cashflow') }}

    union

    select
        ts_code,
        ann_date,
        f_ann_date,
        end_date,
        report_type,
        comp_type,
        update_flag
    from {{ ref('stg_fina_indicator') }}
),

ranked_versions as (
    select
        *,
        row_number() over (
            partition by ts_code, end_date, report_type
            order by ann_date desc nulls last,
                     f_ann_date desc nulls last,
                     update_flag desc nulls last
        ) = 1 as is_latest
    from versions
)

select
    versions.ts_code,
    versions.end_date,
    versions.ann_date,
    versions.f_ann_date,
    versions.report_type,
    versions.comp_type,
    versions.update_flag,
    versions.is_latest,
    income.basic_eps,
    income.diluted_eps,
    income.total_revenue,
    income.revenue,
    income.operate_profit,
    income.total_profit,
    income.n_income,
    income.n_income_attr_p,
    balancesheet.money_cap,
    balancesheet.total_cur_assets,
    balancesheet.total_assets,
    balancesheet.total_cur_liab,
    balancesheet.total_liab,
    balancesheet.total_hldr_eqy_exc_min_int,
    balancesheet.total_liab_hldr_eqy,
    cashflow.net_profit,
    cashflow.n_cashflow_act,
    cashflow.n_cashflow_inv_act,
    cashflow.n_cash_flows_fnc_act,
    cashflow.n_incr_cash_cash_equ,
    cashflow.free_cashflow,
    fina_indicator.eps,
    fina_indicator.dt_eps,
    fina_indicator.grossprofit_margin,
    fina_indicator.netprofit_margin,
    fina_indicator.roe,
    fina_indicator.roa,
    fina_indicator.debt_to_assets,
    fina_indicator.or_yoy,
    fina_indicator.netprofit_yoy,
    coalesce(
        income.source_run_id,
        balancesheet.source_run_id,
        cashflow.source_run_id,
        fina_indicator.source_run_id
    ) as source_run_id,
    coalesce(
        income.raw_loaded_at,
        balancesheet.raw_loaded_at,
        cashflow.raw_loaded_at,
        fina_indicator.raw_loaded_at
    ) as raw_loaded_at
from ranked_versions as versions
left join {{ ref('stg_income') }} as income
    on versions.ts_code = income.ts_code
    and versions.ann_date = income.ann_date
    and versions.f_ann_date = income.f_ann_date
    and versions.end_date = income.end_date
    and versions.report_type = income.report_type
    and versions.comp_type = income.comp_type
    and versions.update_flag = income.update_flag
left join {{ ref('stg_balancesheet') }} as balancesheet
    on versions.ts_code = balancesheet.ts_code
    and versions.ann_date = balancesheet.ann_date
    and versions.f_ann_date = balancesheet.f_ann_date
    and versions.end_date = balancesheet.end_date
    and versions.report_type = balancesheet.report_type
    and versions.comp_type = balancesheet.comp_type
    and versions.update_flag = balancesheet.update_flag
left join {{ ref('stg_cashflow') }} as cashflow
    on versions.ts_code = cashflow.ts_code
    and versions.ann_date = cashflow.ann_date
    and versions.f_ann_date = cashflow.f_ann_date
    and versions.end_date = cashflow.end_date
    and versions.report_type = cashflow.report_type
    and versions.comp_type = cashflow.comp_type
    and versions.update_flag = cashflow.update_flag
left join {{ ref('stg_fina_indicator') }} as fina_indicator
    on versions.ts_code = fina_indicator.ts_code
    and versions.ann_date = fina_indicator.ann_date
    and versions.f_ann_date = fina_indicator.f_ann_date
    and versions.end_date = fina_indicator.end_date
    and versions.report_type = fina_indicator.report_type
    and versions.comp_type = fina_indicator.comp_type
    and versions.update_flag = fina_indicator.update_flag
