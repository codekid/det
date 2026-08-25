{#
  Unwrap DuckDB JSON scalars (quoted empties / quoted numbers) and cast to
  typed silver columns. BigQuery reads typed Iceberg columns via safe_cast.
#}

{% macro det_bq_col(col) -%}
`{{ col }}`
{%- endmacro %}

{% macro det_json_scalar_text(col) -%}
(
  {% if target.name == 'bigquery' %}
  cast({{ det_bq_col(col) }} as string)
  {% else %}
  case
    when "{{ col }}" is null then null
    when typeof("{{ col }}") = 'JSON' then json_extract_string("{{ col }}", '$')
    else cast("{{ col }}" as varchar)
  end
  {% endif %}
)
{%- endmacro %}

{% macro det_as_string(col) -%}
nullif(trim({{ det_json_scalar_text(col) }}), '')
{%- endmacro %}

{% macro det_as_integer(col) -%}
{% if target.name == 'bigquery' %}
safe_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as int64)
{% else %}
try_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as integer)
{% endif %}
{%- endmacro %}

{% macro det_as_double(col) -%}
{% if target.name == 'bigquery' %}
safe_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as float64)
{% else %}
try_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as double)
{% endif %}
{%- endmacro %}

{% macro det_as_boolean(col) -%}
{% if target.name == 'bigquery' %}
safe_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as bool)
{% else %}
try_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as boolean)
{% endif %}
{%- endmacro %}
