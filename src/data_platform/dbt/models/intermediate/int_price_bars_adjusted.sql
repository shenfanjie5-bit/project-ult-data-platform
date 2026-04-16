{{ config(materialized="table") }}

with bars as (
    select
        ts_code,
        trade_date,
        'daily' as freq,
        open,
        high,
        low,
        close,
        pre_close,
        change,
        pct_chg,
        vol,
        amount,
        source_run_id,
        raw_loaded_at
    from {{ ref('stg_daily') }}

    union all

    select
        ts_code,
        trade_date,
        'weekly' as freq,
        open,
        high,
        low,
        close,
        pre_close,
        change,
        pct_chg,
        vol,
        amount,
        source_run_id,
        raw_loaded_at
    from {{ ref('stg_weekly') }}

    union all

    select
        ts_code,
        trade_date,
        'monthly' as freq,
        open,
        high,
        low,
        close,
        pre_close,
        change,
        pct_chg,
        vol,
        amount,
        source_run_id,
        raw_loaded_at
    from {{ ref('stg_monthly') }}
),

adj_factor as (
    select
        ts_code,
        trade_date,
        adj_factor
    from {{ ref('stg_adj_factor') }}
)

select
    bars.ts_code,
    bars.trade_date,
    bars.freq,
    bars.open,
    bars.high,
    bars.low,
    bars.close,
    bars.pre_close,
    bars.change,
    bars.pct_chg,
    bars.vol,
    bars.amount,
    adj_factor.adj_factor,
    bars.source_run_id,
    bars.raw_loaded_at
from bars
left join adj_factor
    on bars.ts_code = adj_factor.ts_code
    and bars.trade_date = adj_factor.trade_date
