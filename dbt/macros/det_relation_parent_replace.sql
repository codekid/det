{#
  parent_replace: clear silver children when the relation array (or an ancestor
  array) is empty/null on bronze. Stock delete+insert only sees cross-join
  unnest rows, so empty arrays never contribute delete keys. This pre-hook
  deletes by those keys without inserting key-only rows.
#}

{% macro det_json_array_is_empty(col) -%}
(
  {% if target.name == 'bigquery' -%}
  (
    {{ col }} is null
    or array_length(
      json_query_array(
        safe.parse_json(
          if(
            typeof({{ col }}) = 'JSON',
            to_json_string({{ col }}),
            cast({{ col }} as string)
          )
        )
      )
    ) = 0
  )
  {%- else -%}
  (
    {{ col }} is null
    or len(cast({{ col }} as JSON[])) = 0
  )
  {%- endif %}
)
{%- endmacro %}


{% macro det_relation_watermark_predicate(alias, watermark, lookback=none, pipeline_name=none) -%}
  {% set catchup_map = var('det_catchup_by_pipeline', {}) %}
  {% set catchup_runs = [] %}
  {% if pipeline_name and catchup_map is mapping and pipeline_name in catchup_map %}
    {% set catchup_runs = catchup_map[pipeline_name] %}
  {% endif %}
  {% if catchup_runs %}
  {{ alias }}.{{ watermark }} in (
    {% for r in catchup_runs %}
      '{{ r }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
  )
  {% elif lookback %}
  {{ alias }}.{{ watermark }} >= (
      select coalesce(
          max({{ watermark }}) - interval '{{ lookback }}',
          '0001-01-01'
      )
      from {{ this }}
  )
  {% else %}
  {{ alias }}.{{ watermark }} > (
      select coalesce(max({{ watermark }}), '0001-01-01')
      from {{ this }}
  )
  {% endif %}
{%- endmacro %}


{% macro _det_relation_empty_keys_subquery(level_k) -%}
  {%- set path_chain = config.get('det_relation_path_chain') -%}
  {%- set parent_key = config.get('det_parent_key') -%}
  {%- set spine = config.get('det_relation_spine') or [] -%}
  {%- set sql_table = config.get('det_sql_table') -%}
  {%- set sql_schema = config.get('det_sql_schema') -%}
  {%- set watermark = config.get('det_watermark') or '__extract_run_datetime' -%}
  {%- set lookback = config.get('det_lookback') -%}
  {%- set pipeline_name = config.get('det_pipeline_name') -%}
  {%- set key_cols = [parent_key] -%}
  {%- for sp in spine -%}
    {%- if sp['level_idx'] < level_k -%}
      {%- do key_cols.append(sp['name']) -%}
    {%- endif -%}
  {%- endfor -%}
        select
            _parent.{{ parent_key }} as {{ parent_key }}
{%- for sp in spine if sp['level_idx'] < level_k %}
            ,
{%- if sp['kind'] == 'index' %}
            t{{ sp['level_idx'] }}.__rel_index as {{ sp['name'] }}
{%- else %}
{%- set path_macro = sp.get('json_path_macro', 'det_json_path_string') -%}
{%- if path_macro == 'det_json_path_integer' %}
            {{ det_json_path_integer('t' ~ sp['level_idx'] ~ '._rel', '$.' ~ sp['field']) }} as {{ sp['name'] }}
{%- elif path_macro == 'det_json_path_double' %}
            {{ det_json_path_double('t' ~ sp['level_idx'] ~ '._rel', '$.' ~ sp['field']) }} as {{ sp['name'] }}
{%- elif path_macro == 'det_json_path_boolean' %}
            {{ det_json_path_boolean('t' ~ sp['level_idx'] ~ '._rel', '$.' ~ sp['field']) }} as {{ sp['name'] }}
{%- else %}
            {{ det_json_path_string('t' ~ sp['level_idx'] ~ '._rel', '$.' ~ sp['field']) }} as {{ sp['name'] }}
{%- endif %}
{%- endif %}
{%- endfor %}
        from {{ det_bronze_from(sql_table, sql_schema) }} as _parent
{%- for step_i in range(level_k) %}
{%- set step = path_chain[step_i] %}
{%- if target.name == 'bigquery' %}
{#- Subquery keeps tN._rel / tN.__rel_index like DuckDB WITH ORDINALITY aliases. -#}
{%- if step_i == 0 %}
        cross join (
          select __el as _rel, __off as __rel_index
          from unnest(
            json_query_array(
              safe.parse_json(
                if(
                  typeof(_parent.{{ step }}) = 'JSON',
                  to_json_string(_parent.{{ step }}),
                  cast(_parent.{{ step }} as string)
                )
              )
            )
          ) as __el with offset as __off
        ) as t{{ step_i }}
{%- else %}
        cross join (
          select __el as _rel, __off as __rel_index
          from unnest(
            json_query_array(
              safe.parse_json(
                if(
                  typeof(t{{ step_i - 1 }}._rel) = 'JSON',
                  to_json_string(t{{ step_i - 1 }}._rel),
                  cast(t{{ step_i - 1 }}._rel as string)
                )
              ),
              '$.{{ step }}'
            )
          ) as __el with offset as __off
        ) as t{{ step_i }}
{%- endif %}
{%- elif step_i == 0 %}
        cross join unnest(cast(_parent.{{ step }} as JSON[])) with ordinality as t{{ step_i }}(_rel, __rel_index)
{%- else %}
        cross join unnest(cast(json_extract(t{{ step_i - 1 }}._rel, '$.{{ step }}') as JSON[])) with ordinality as t{{ step_i }}(_rel, __rel_index)
{%- endif %}
{%- endfor %}
        where
{%- if level_k == 0 %}
            {{ det_json_array_is_empty('_parent.' ~ path_chain[0]) }}
{%- else %}
            {{ det_json_array_is_empty("json_extract(t" ~ (level_k - 1) ~ "._rel, '$." ~ path_chain[level_k] ~ "')") }}
{%- endif %}
            and {{ det_relation_watermark_predicate('_parent', watermark, lookback, pipeline_name) }}
{%- endmacro %}


{% macro det_relation_clear_empty_arrays() %}
  {%- if not is_incremental() or not config.get('det_parent_replace') -%}
    select 1 as __det_noop where false
  {%- else -%}
  {%- set path_chain = config.get('det_relation_path_chain') -%}
  {%- set parent_key = config.get('det_parent_key') -%}
  {%- set spine = config.get('det_relation_spine') or [] -%}
  {%- if not path_chain or not parent_key -%}
    select 1 as __det_noop where false
  {%- else -%}
delete from {{ this }} as __det_dest
where
  {%- for level_k in range(path_chain | length) %}
  {%- set key_cols = [parent_key] -%}
  {%- for sp in spine -%}
    {%- if sp['level_idx'] < level_k -%}
      {%- do key_cols.append(sp['name']) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set key_csv = key_cols | join(', ') -%}
  {%- if not loop.first %}
  or {% endif -%}
  ({{ key_csv }}) in (
    select distinct {{ key_csv }}
    from (
{{ _det_relation_empty_keys_subquery(level_k) }}
    ) as __det_empty_keys_{{ level_k }}
  )
  {%- endfor %}
  {%- endif -%}
  {%- endif -%}
{% endmacro %}
