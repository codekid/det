{% macro det_bronze_from(dataset) %}
{#
  Filesystem bronze (default): dbt source named DET_BRONZE_SCHEMA (bronze_{provider})
  with nested lake path bronze/{provider}/{name}/**/data.jsonl.
  DuckDB bronze: native table DET_BRONZE_SCHEMA.dataset (e.g. bronze_noaa.storm_events).
  `det dbt --pipeline` sets DET_BRONZE_SOURCE / DET_BRONZE_SCHEMA from the pipeline.
#}
{%- if env_var("DET_BRONZE_SOURCE", "filesystem") == "duckdb" -%}
{{ env_var("DET_BRONZE_SCHEMA", "bronze") }}.{{ dataset }}
{%- else -%}
{{ source(env_var("DET_BRONZE_SCHEMA", "bronze"), dataset) }}
{%- endif -%}
{% endmacro %}
