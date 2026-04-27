{{ config(materialized="table") }}

select
    ts_code,
    ann_date,
    end_date,
    forecast_type,
    cast(nullif(trim(cast(p_change_min as varchar)), '') as decimal(38, 18))
        as p_change_min,
    cast(nullif(trim(cast(p_change_max as varchar)), '') as decimal(38, 18))
        as p_change_max,
    cast(nullif(trim(cast(net_profit_min as varchar)), '') as decimal(38, 18))
        as net_profit_min,
    cast(nullif(trim(cast(net_profit_max as varchar)), '') as decimal(38, 18))
        as net_profit_max,
    cast(nullif(trim(cast(last_parent_net as varchar)), '') as decimal(38, 18))
        as last_parent_net,
    first_ann_date,
    summary,
    change_reason,
    update_flag,
    source_run_id,
    raw_loaded_at
from {{ ref('int_forecast_events') }}
