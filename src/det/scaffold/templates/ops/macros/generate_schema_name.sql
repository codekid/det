{% macro generate_schema_name(custom_schema_name, node) -%}
  {#- Use custom schema as-is (bronze_noaa / silver_noaa), not main_<custom>. -#}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
