{{ config(materialized="table") }}

select
    index_code,
    max(index_name) as index_name,
    max(index_market) as index_market,
    max(index_category) as index_category,
    min(effective_date) as first_effective_date,
    max(effective_date) as latest_effective_date,
    max(source_run_id) as source_run_id,
    max(raw_loaded_at) as raw_loaded_at
from {{ ref('int_index_membership') }}
where index_code is not null
group by index_code
