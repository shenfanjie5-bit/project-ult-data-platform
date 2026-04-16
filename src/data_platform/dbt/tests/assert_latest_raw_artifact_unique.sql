{{ config(severity="error") }}
-- depends_on: {{ ref('stg_stock_basic') }}

with raw_manifest_files as (
    select
        cast(source_id as varchar) as manifest_source_id,
        cast(dataset as varchar) as manifest_dataset,
        cast(partition_date as date) as manifest_partition_date,
        artifacts,
        filename as manifest_filename
    from read_json_auto(
        '{{ env_var("DP_RAW_ZONE_PATH", "./data_platform/raw").rstrip("/") }}/tushare/*/**/_manifest.json',
        hive_partitioning=1,
        filename=1
    )
),

raw_artifacts as (
    select
        manifest_filename,
        manifest_source_id,
        manifest_dataset,
        manifest_partition_date,
        cast(artifact.run_id as varchar) as source_run_id,
        try_cast(artifact.written_at as timestamp) as raw_loaded_at,
        rank() over (
            partition by manifest_source_id, manifest_dataset, manifest_partition_date
            order by
                try_cast(artifact.written_at as timestamp) desc nulls last,
                cast(artifact.run_id as varchar) desc nulls last
        ) as artifact_rank
    from raw_manifest_files
    cross join unnest(raw_manifest_files.artifacts) as artifact_item(artifact)
),

latest_ties as (
    select
        manifest_source_id,
        manifest_dataset,
        manifest_partition_date,
        raw_loaded_at,
        count(*) as latest_artifact_count
    from raw_artifacts
    where artifact_rank = 1
    group by
        manifest_source_id,
        manifest_dataset,
        manifest_partition_date,
        raw_loaded_at
)

select
    manifest_source_id,
    manifest_dataset,
    manifest_partition_date,
    raw_loaded_at,
    latest_artifact_count
from latest_ties
where latest_artifact_count > 1
