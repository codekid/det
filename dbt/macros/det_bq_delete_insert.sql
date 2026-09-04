{#
  Enable incremental_strategy='delete+insert' on BigQuery.

  Stock dbt-bigquery only allows merge / insert_overwrite / microbatch. Relation
  silver with load: parent_replace needs delete-then-insert on delete_key (not
  row-unique), which merge cannot express. These overrides keep other strategies
  identical to upstream and route delete+insert through get_delete_insert_merge_sql.
#}

{% macro dbt_bigquery_validate_get_incremental_strategy(config) %}
  {%- set strategy = config.get("incremental_strategy") or 'merge' -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'insert_overwrite', 'microbatch', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['merge', 'insert_overwrite', 'microbatch', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% if strategy == 'microbatch' %}
    {% do bq_validate_microbatch_config(config) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}


{% macro bq_generate_incremental_build_sql(
    strategy, tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions, incremental_predicates
) %}
  {% if strategy == 'delete+insert' %}
    {%- if tmp_relation_exists -%}
      {% set source_sql = tmp_relation %}
    {%- else -%}
      {% set source_sql %}
        (
            {{ sql }}
        )
      {%- endset %}
    {%- endif -%}
    {% set build_sql = get_delete_insert_merge_sql(
        target_relation, source_sql, unique_key, dest_columns, incremental_predicates
    ) %}
  {% elif strategy == 'insert_overwrite' %}
    {% set build_sql = bq_generate_incremental_insert_overwrite_build_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
    ) %}
  {% elif strategy == 'microbatch' %}
    {% set build_sql = bq_generate_microbatch_build_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
    ) %}
  {% else %} {# strategy == 'merge' #}
    {% set build_sql = bq_generate_incremental_merge_build_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, incremental_predicates
    ) %}
  {% endif %}

  {{ return(build_sql) }}
{% endmacro %}
