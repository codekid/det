{% macro det_lake_bronze_path(provider, table) -%}
{#
  Bronze Iceberg / JSONL root for a dataset. Use from SQL models.

  Layout 2: DET_LAKE_PATH_BRONZE/{provider}/{table} (flattened; no /bronze/).
  Layout 1 fallback: DET_LAKE_PATH/bronze/{provider}/{table}.

  Do not call this from sources.yml meta.external_location — dbt does not
  expand project macros there. Scaffold inlines the same env_var expression.
#}
{{ env_var('DET_LAKE_PATH_BRONZE', env_var('DET_LAKE_PATH', '../data/lake') ~ '/bronze') }}/{{ provider }}/{{ table }}
{%- endmacro %}
