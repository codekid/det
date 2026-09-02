{% macro det_lake_bronze_path(provider, table) -%}
{#
  Bronze Iceberg / JSONL root for a dataset.

  Layout 2: DET_LAKE_PATH_BRONZE/{provider}/{table} (flattened; no /bronze/).
  Layout 1 fallback: DET_LAKE_PATH/bronze/{provider}/{table}.
#}
{{ env_var('DET_LAKE_PATH_BRONZE', env_var('DET_LAKE_PATH', '../data/lake') ~ '/bronze') }}/{{ provider }}/{{ table }}
{%- endmacro %}
