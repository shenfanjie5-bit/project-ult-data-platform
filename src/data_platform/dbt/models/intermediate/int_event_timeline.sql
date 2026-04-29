{{ config(materialized="table") }}

select
    'announcement' as event_type,
    cast('anns' as varchar) as source_interface_id,
    ts_code,
    ann_date as event_date,
    title,
    name as summary,
    cast(null as varchar) as event_subtype,
    cast(null as date) as related_date,
    url as reference_url,
    rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_anns') }}

union all

select
    'suspend' as event_type,
    cast('suspend_d' as varchar) as source_interface_id,
    ts_code,
    trade_date as event_date,
    'Trading suspension' as title,
    suspend_timing as summary,
    suspend_type as event_subtype,
    trade_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_suspend_d') }}

union all

select
    'dividend' as event_type,
    cast('dividend' as varchar) as source_interface_id,
    ts_code,
    ann_date as event_date,
    'Dividend' as title,
    concat('cash_div=', coalesce(cash_div, ''), ';stk_div=', coalesce(stk_div, '')) as summary,
    div_proc as event_subtype,
    end_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_dividend') }}

union all

select
    'share_float' as event_type,
    cast('share_float' as varchar) as source_interface_id,
    ts_code,
    float_date as event_date,
    'Share float' as title,
    holder_name as summary,
    share_type as event_subtype,
    ann_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_share_float') }}

union all

select
    'holder_number' as event_type,
    cast('stk_holdernumber' as varchar) as source_interface_id,
    ts_code,
    ann_date as event_date,
    'Holder number' as title,
    holder_num as summary,
    cast(null as varchar) as event_subtype,
    end_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_stk_holdernumber') }}

union all

select
    'disclosure_date' as event_type,
    cast('disclosure_date' as varchar) as source_interface_id,
    ts_code,
    coalesce(actual_date, pre_date, ann_date, modify_date) as event_date,
    'Disclosure date' as title,
    concat(
        'pre_date=', coalesce(cast(pre_date as varchar), ''),
        ';actual_date=', coalesce(cast(actual_date as varchar), '')
    ) as summary,
    cast(null as varchar) as event_subtype,
    end_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_disclosure_date') }}

union all

select
    'name_change' as event_type,
    cast('namechange' as varchar) as source_interface_id,
    ts_code,
    start_date as event_date,
    'Name change' as title,
    name as summary,
    change_reason as event_subtype,
    ann_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_namechange') }}

union all

select
    'block_trade' as event_type,
    cast('block_trade' as varchar) as source_interface_id,
    ts_code,
    trade_date as event_date,
    'Block trade' as title,
    concat(
        'buyer=', coalesce(buyer, ''),
        ';seller=', coalesce(seller, ''),
        ';price=', coalesce(price, ''),
        ';vol=', coalesce(vol, ''),
        ';amount=', coalesce(amount, '')
    ) as summary,
    cast(null as varchar) as event_subtype,
    trade_date as related_date,
    cast(null as varchar) as reference_url,
    cast(null as varchar) as rec_time,
    source_run_id,
    raw_loaded_at
from {{ ref('stg_block_trade') }}
