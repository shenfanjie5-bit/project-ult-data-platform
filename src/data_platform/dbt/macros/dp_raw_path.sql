{% macro dp_raw_path(source_id, dataset) -%}
{%- set raw_zone_path = env_var("DP_RAW_ZONE_PATH", "./data_platform/raw") -%}
{{ raw_zone_path ~ "/" ~ source_id ~ "/" ~ dataset ~ "/**/*.parquet" }}
{%- endmacro %}
