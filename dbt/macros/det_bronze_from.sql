{% macro det_bronze_from(dataset, source_name=none) %}
{#
  Filesystem / Iceberg bronze (default): dbt source ``source_name.dataset``
  (e.g. bronze_noaa.storm_events). Pass source_name explicitly so multi-provider
  projects parse correctly when ``det dbt -p`` sets DET_BRONZE_SCHEMA for one
  pipeline. Falls back to DET_BRONZE_SCHEMA / "bronze" when omitted.
  Iceberg: sources.yml iceberg_scan when DET_BRONZE_SOURCE=iceberg.
  DuckDB bronze: native table source_name.dataset when DET_BRONZE_SOURCE=duckdb.
#}
{%- set src = source_name if source_name is not none else env_var("DET_BRONZE_SCHEMA", "bronze") -%}
{%- if env_var("DET_BRONZE_SOURCE", "filesystem") == "duckdb" -%}
{{ src }}.{{ dataset }}
{%- else -%}
{{ source(src, dataset) }}
{%- endif -%}
{% endmacro %}
