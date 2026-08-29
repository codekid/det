{# Hand-written gold / cross-engine SQL helpers. #}

{% macro det_cast_string(col) -%}
{% if target.name == 'bigquery' %}
cast({{ col }} as string)
{% else %}
cast({{ col }} as varchar)
{% endif %}
{%- endmacro %}

{% macro det_try_cast_double(expr) -%}
{% if target.name == 'bigquery' %}
safe_cast({{ expr }} as float64)
{% else %}
try_cast({{ expr }} as double)
{% endif %}
{%- endmacro %}

{% macro det_timestamp_minus_hours(ts_expr, hours_expr) -%}
{% if target.name == 'bigquery' %}
timestamp_sub({{ ts_expr }}, interval {{ hours_expr }} hour)
{% else %}
{{ ts_expr }} - ({{ hours_expr }} * interval '1 hour')
{% endif %}
{%- endmacro %}

{% macro det_regexp_strip_alpha(expr) -%}
{% if target.name == 'bigquery' %}
regexp_replace({{ expr }}, r'[a-z]', '')
{% else %}
regexp_replace({{ expr }}, '[a-z]', '', 'g')
{% endif %}
{%- endmacro %}
