{{ config(materialized="table") }}

with northbound_metrics as (
    select
        security_id,
        holder_id,
        report_date,
        cast('holding_amount' as varchar) as z_score_metric,
        holding_amount as metric_value
    from {{ ref('mart_fact_holding_position_v2') }}
    where holding_source = 'northbound_hold'

    union all

    select
        security_id,
        holder_id,
        report_date,
        cast('holding_ratio' as varchar) as z_score_metric,
        holding_ratio as metric_value
    from {{ ref('mart_fact_holding_position_v2') }}
    where holding_source = 'northbound_hold'
),

windowed_metrics as (
    select
        security_id,
        holder_id,
        report_date,
        z_score_metric,
        cast(8 as integer) as lookback_observations,
        min(report_date) over metric_window as window_start_date,
        max(report_date) over metric_window as window_end_date,
        count(metric_value) over metric_window as observation_count,
        metric_value,
        avg(metric_value) over metric_window as metric_mean,
        stddev_samp(metric_value) over metric_window as metric_stddev
    from northbound_metrics
    window metric_window as (
        partition by security_id, holder_id, z_score_metric
        order by report_date
        rows between 7 preceding and current row
    )
)

select
    security_id,
    holder_id,
    report_date,
    z_score_metric,
    lookback_observations,
    window_start_date,
    window_end_date,
    observation_count,
    metric_value,
    metric_mean,
    metric_stddev,
    case
        when metric_stddev is null or metric_stddev = 0
            then cast(null as double)
        else (metric_value - metric_mean) / metric_stddev
    end as metric_z_score
from windowed_metrics
