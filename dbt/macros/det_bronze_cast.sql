{#
  Unwrap DuckDB JSON scalars (quoted empties / quoted numbers) and cast to
  typed silver columns. Also works for native VARCHAR/INTEGER/DOUBLE.
#}

{% macro det_json_scalar_text(col) -%}
(
  case
    when {{ col }} is null then null
    when typeof({{ col }}) = 'JSON' then json_extract_string({{ col }}, '$')
    else cast({{ col }} as varchar)
  end
)
{%- endmacro %}

{% macro det_as_string(col) -%}
nullif(trim({{ det_json_scalar_text(col) }}), '')
{%- endmacro %}

{% macro det_as_integer(col) -%}
try_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as integer)
{%- endmacro %}

{% macro det_as_double(col) -%}
try_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as double)
{%- endmacro %}

{% macro det_as_boolean(col) -%}
try_cast(nullif(trim({{ det_json_scalar_text(col) }}), '') as boolean)
{%- endmacro %}
