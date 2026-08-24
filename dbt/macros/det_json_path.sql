{#
  JSON path helpers for scaffolded nested flatten / relation unnest.
  DuckDB uses json_extract_string; BigQuery uses json_value.
#}

{% macro det_json_path_text(col, path) -%}
(
  case
    when {% if target.name == 'bigquery' %}{{ det_bq_col(col) }}{% else %}{{ col }}{% endif %} is null then null
    {% if target.name == 'bigquery' %}
    else json_value(
      safe.parse_json(
        if(
          typeof({{ det_bq_col(col) }}) = 'JSON',
          to_json_string({{ det_bq_col(col) }}),
          cast({{ det_bq_col(col) }} as string)
        )
      ),
      '{{ path }}'
    )
    {% else %}
    else json_extract_string({{ col }}, '{{ path }}')
    {% endif %}
  end
)
{%- endmacro %}

{% macro det_json_path_string(col, path) -%}
nullif(trim({{ det_json_path_text(col, path) }}), '')
{%- endmacro %}

{% macro det_json_path_integer(col, path) -%}
{% if target.name == 'bigquery' %}
safe_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as int64)
{% else %}
try_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as integer)
{% endif %}
{%- endmacro %}

{% macro det_json_path_double(col, path) -%}
{% if target.name == 'bigquery' %}
safe_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as float64)
{% else %}
try_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as double)
{% endif %}
{%- endmacro %}

{% macro det_json_path_boolean(col, path) -%}
{% if target.name == 'bigquery' %}
safe_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as bool)
{% else %}
try_cast(nullif(trim({{ det_json_path_text(col, path) }}), '') as boolean)
{% endif %}
{%- endmacro %}

{% macro det_unnest_json_array(col) -%}
{% if target.name == 'bigquery' %}
unnest(json_query_array(
  safe.parse_json(
    if(
      typeof({{ det_bq_col(col) }}) = 'JSON',
      to_json_string({{ det_bq_col(col) }}),
      cast({{ det_bq_col(col) }} as string)
    )
  )
))
{% else %}
unnest(CAST({{ col }} AS JSON[]))
{% endif %}
{%- endmacro %}
