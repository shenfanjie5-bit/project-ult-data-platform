{{ config(materialized="table") }}

select
    event_type,
    ts_code,
    event_date,
    title,
    summary,
    event_subtype,
    related_date,
    reference_url,
    rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('int_event_timeline') }}
