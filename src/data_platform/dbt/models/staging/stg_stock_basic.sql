{{ config(materialized="view") }}

with raw_manifest_files as (
    select
        partition_date,
        artifacts,
        filename
    from read_json_auto(
        '{{ dp_raw_manifest_path("tushare", "stock_basic") }}',
        hive_partitioning=1,
        filename=1
    )
),

raw_artifacts as (
    select
        cast(artifact.run_id as varchar) as source_run_id,
        cast(artifact.written_at as timestamp) as raw_loaded_at,
        cast(coalesce(artifact.partition_date, raw_manifest_files.partition_date) as date)
            as partition_date,
        strftime(
            cast(coalesce(artifact.partition_date, raw_manifest_files.partition_date) as date),
            '%Y%m%d'
        ) as partition_yyyymmdd
    from raw_manifest_files
    cross join unnest(raw_manifest_files.artifacts) as artifact_item(artifact)
),

ranked_raw_artifacts as (
    select
        *,
        row_number() over (
            order by raw_loaded_at desc, partition_date desc, source_run_id desc
        ) as artifact_rank
    from raw_artifacts
),

latest_raw_artifact as (
    select
        source_run_id,
        raw_loaded_at,
        partition_yyyymmdd
    from ranked_raw_artifacts
    where artifact_rank = 1
),

raw_stock_basic as (
    select
        ts_code,
        symbol,
        name,
        area,
        industry,
        market,
        list_status,
        list_date,
        dt,
        filename
    from read_parquet(
        '{{ dp_raw_path("tushare", "stock_basic") }}',
        hive_partitioning=1,
        filename=1,
        union_by_name=1
    )
),

selected_stock_basic as (
    select
        raw_stock_basic.*,
        latest_raw_artifact.source_run_id,
        latest_raw_artifact.raw_loaded_at
    from raw_stock_basic
    inner join latest_raw_artifact
        on cast(raw_stock_basic.dt as varchar) = latest_raw_artifact.partition_yyyymmdd
        and regexp_extract(raw_stock_basic.filename, '([^/]+)[.]parquet$', 1)
            = latest_raw_artifact.source_run_id
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
    source_run_id,
    raw_loaded_at
from selected_stock_basic
