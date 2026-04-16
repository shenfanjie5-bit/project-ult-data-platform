{{ config(materialized="table") }}

select
    ts_code,
    trade_date,
    freq,
    cast(nullif(trim(cast(open as varchar)), '') as decimal(38, 18)) as open,
    cast(nullif(trim(cast(high as varchar)), '') as decimal(38, 18)) as high,
    cast(nullif(trim(cast(low as varchar)), '') as decimal(38, 18)) as low,
    cast(nullif(trim(cast(close as varchar)), '') as decimal(38, 18)) as close,
    cast(nullif(trim(cast(pre_close as varchar)), '') as decimal(38, 18)) as pre_close,
    cast(nullif(trim(cast(change as varchar)), '') as decimal(38, 18)) as change,
    cast(nullif(trim(cast(pct_chg as varchar)), '') as decimal(38, 18)) as pct_chg,
    cast(nullif(trim(cast(vol as varchar)), '') as decimal(38, 18)) as vol,
    cast(nullif(trim(cast(amount as varchar)), '') as decimal(38, 18)) as amount,
    cast(nullif(trim(cast(adj_factor as varchar)), '') as decimal(38, 18)) as adj_factor,
    source_run_id,
    raw_loaded_at
from {{ ref('int_price_bars_adjusted') }}
