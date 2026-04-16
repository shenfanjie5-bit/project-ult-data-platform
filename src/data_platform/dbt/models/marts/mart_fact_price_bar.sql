{{ config(materialized="table") }}

select
    ts_code,
    trade_date,
    freq,
    try_cast(nullif(trim(cast(open as varchar)), '') as decimal(38, 18)) as open,
    try_cast(nullif(trim(cast(high as varchar)), '') as decimal(38, 18)) as high,
    try_cast(nullif(trim(cast(low as varchar)), '') as decimal(38, 18)) as low,
    try_cast(nullif(trim(cast(close as varchar)), '') as decimal(38, 18)) as close,
    try_cast(nullif(trim(cast(pre_close as varchar)), '') as decimal(38, 18)) as pre_close,
    try_cast(nullif(trim(cast(change as varchar)), '') as decimal(38, 18)) as change,
    try_cast(nullif(trim(cast(pct_chg as varchar)), '') as decimal(38, 18)) as pct_chg,
    try_cast(nullif(trim(cast(vol as varchar)), '') as decimal(38, 18)) as vol,
    try_cast(nullif(trim(cast(amount as varchar)), '') as decimal(38, 18)) as amount,
    try_cast(nullif(trim(cast(adj_factor as varchar)), '') as decimal(38, 18)) as adj_factor,
    source_run_id,
    raw_loaded_at
from {{ ref('int_price_bars_adjusted') }}
