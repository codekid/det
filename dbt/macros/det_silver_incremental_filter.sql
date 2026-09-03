{% macro det_silver_incremental_filter(
    watermark,
    unique_key,
    pipeline_name,
    lookback=none
) %}
{#-
  Incremental WHERE for scaffolded silver models.

  Catch-up: when var det_catchup_by_pipeline[pipeline_name] is a non-empty list,
  restrict to those __extract_run_datetime values (run-list heal).
  Otherwise watermark > max(this) or >= max - lookback.
-#}
{% if is_incremental() %}
  {% set catchup_map = var('det_catchup_by_pipeline', {}) %}
  {% set catchup_runs = [] %}
  {% if catchup_map is mapping and pipeline_name in catchup_map %}
    {% set catchup_runs = catchup_map[pipeline_name] %}
  {% endif %}
  {% if catchup_runs %}
    where __extract_run_datetime in (
      {% for r in catchup_runs %}
        '{{ r }}'{% if not loop.last %}, {% endif %}
      {% endfor %}
    )
  {% elif lookback %}
    where {{ watermark }} >= (
        select coalesce(
            max({{ watermark }}) - interval '{{ lookback }}',
            '0001-01-01'
        )
        from {{ this }}
    )
  {% else %}
    where {{ watermark }} > (
        select coalesce(max({{ watermark }}), '0001-01-01')
        from {{ this }}
    )
  {% endif %}
{% endif %}
{% endmacro %}


{% macro det_silver_catchup_guard(watermark, unique_key, pipeline_name) %}
{#-
  After catch-up IN-filter: keep rows whose unique_key is missing in silver or
  whose watermark is strictly newer than silver (never demote).
  Emits nothing when not in catch-up mode / not incremental.
-#}
{% if is_incremental() %}
  {% set catchup_map = var('det_catchup_by_pipeline', {}) %}
  {% set catchup_runs = [] %}
  {% if catchup_map is mapping and pipeline_name in catchup_map %}
    {% set catchup_runs = catchup_map[pipeline_name] %}
  {% endif %}
  {% if catchup_runs %}
silver_keys as (
    select
        {% for col in unique_key %}
        {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
        , {{ watermark }} as __det_silver_wm
    from {{ this }}
),
base as (
    select i.*
    from stg_filtered i
    left join silver_keys s
      on {% for col in unique_key %}
      i.{{ col }} = s.{{ col }}{% if not loop.last %} and {% endif %}
      {% endfor %}
    where s.{{ unique_key[0] }} is null
       or i.{{ watermark }} > s.__det_silver_wm
)
  {% else %}
base as (
    select * from stg_filtered
)
  {% endif %}
{% else %}
base as (
    select * from stg_filtered
)
{% endif %}
{% endmacro %}
