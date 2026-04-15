{{ config(materialized="view") }}

with raw_stock_basic as (
    select *
    from read_parquet(
        '{{ dp_raw_path("tushare", "stock_basic") }}',
        hive_partitioning=1,
        filename=1
    )
)

select
    cast(trim(cast(ts_code as varchar)) as varchar) as ts_code,
    cast(trim(cast(symbol as varchar)) as varchar) as symbol,
    cast(trim(cast(name as varchar)) as varchar) as name,
    cast(nullif(trim(cast(area as varchar)), '') as varchar) as area,
    cast(nullif(trim(cast(industry as varchar)), '') as varchar) as industry,
    cast(nullif(trim(cast(market as varchar)), '') as varchar) as market,
    strptime(nullif(trim(cast(list_date as varchar)), ''), '%Y%m%d')::date as list_date,
    cast(upper(trim(cast(list_status as varchar))) = 'L' as boolean) as is_active,
    cast(nullif(regexp_extract(filename, '([^/]+)[.]parquet$', 1), '') as varchar)
        as source_run_id,
    cast(current_timestamp as timestamp) as raw_loaded_at
from raw_stock_basic
