{{ config(materialized="table") }}

select
    ts_code,
    symbol,
    name,
    market,
    industry,
    list_date,
    is_active,
    area,
    fullname,
    exchange,
    curr_type,
    list_status,
    delist_date,
    setup_date,
    province,
    city,
    cast(nullif(trim(cast(reg_capital as varchar)), '') as decimal(38, 18))
        as reg_capital,
    cast(nullif(trim(cast(employees as varchar)), '') as decimal(38, 18))
        as employees,
    main_business,
    latest_namechange_name,
    latest_namechange_start_date,
    latest_namechange_end_date,
    latest_namechange_ann_date,
    latest_namechange_reason,
    source_run_id,
    raw_loaded_at
from {{ ref('int_security_master') }}
