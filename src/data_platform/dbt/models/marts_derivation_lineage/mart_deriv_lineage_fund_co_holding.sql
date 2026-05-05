{{ config(materialized="table") }}

with source_rows as (
    select
        co_holding.report_date,
        co_holding.security_id_left,
        co_holding.security_id_right,
        lineage.report_date as source_report_date,
        lineage.source_interface_id,
        lineage.source_run_id,
        lineage.raw_loaded_at
    from {{ ref('mart_deriv_fund_co_holding') }} as co_holding
    inner join {{ ref('mart_lineage_fact_holding_position') }} as lineage
        on co_holding.report_date = lineage.report_date
        and lineage.holding_source = 'fund_portfolio'
        and lineage.security_id in (
            co_holding.security_id_left,
            co_holding.security_id_right
        )
)

select
    report_date,
    security_id_left,
    security_id_right,
    cast('mart_fact_holding_position_v2' as varchar) as source_mart,
    min(source_report_date) as source_window_start_date,
    max(source_report_date) as source_window_end_date,
    string_agg(distinct source_interface_id, ',') as source_interface_ids,
    count(*) as source_row_count,
    count(*) as source_lineage_row_count,
    string_agg(distinct cast(source_run_id as varchar), ',') as source_run_ids,
    min(raw_loaded_at) as raw_loaded_at_min,
    max(raw_loaded_at) as raw_loaded_at_max,
    concat(
        'source_rows=',
        cast(count(*) as varchar),
        ';lineage_rows=',
        cast(count(*) as varchar)
    ) as source_lineage_summary
from source_rows
group by
    report_date,
    security_id_left,
    security_id_right
