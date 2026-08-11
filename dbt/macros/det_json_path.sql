{#
  DuckDB JSON path helpers for scaffolded nested flatten / relation unnest.
  Scaffold emits these macros; other engines would swap bodies later.
#}

{% macro det_json_path_text(col, path) -%}
(
  case
    when {{ col }} is null then null
    else json_extract_string({{ col }}, '{{ path }}')
  end
)
{%- endmacro %}

{% macro det_json_path_string(col, path) -%}
nullif(trim({{ det_json_path_text(col, path) }}), '')
{%- endmacro %}

{% macro det_json_path_integer(col, path) -%}
try_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as integer)
{%- endmacro %}

{% macro det_json_path_double(col, path) -%}
try_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as double)
{%- endmacro %}

{% macro det_json_path_boolean(col, path) -%}
try_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as boolean)
{%- endmacro %}

{% macro det_unnest_json_array(col) -%}
unnest(CAST({{ col }} AS JSON[]))
{%- endmacro %}
