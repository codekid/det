{% macro det_catchup_coverage_predicate(pipeline_name, alias=none) %}
{#-
  Match coverage keys from the immutable catch-up manifest.

  DuckDB: read_json(DET_CATCHUP_MANIFEST_PATH) + unnest runs.
  BigQuery: EXISTS against DET_CATCHUP_BQ_RELATION (external table over
  sibling .runs.jsonl on GCS; set by det dbt --catchup).
-#}
  {%- if alias -%}{%- set p = alias ~ '.' -%}{%- else -%}{%- set p = '' -%}{%- endif -%}
  {%- if target.name == 'bigquery' -%}
    {%- set bq_rel = env_var('DET_CATCHUP_BQ_RELATION', '') -%}
    {%- if not bq_rel -%}
      {{ exceptions.raise_compiler_error(
        "BigQuery catch-up requires DET_CATCHUP_BQ_RELATION "
        "(det dbt --catchup on a gs:// ops lake)."
      ) }}
    {%- endif -%}
    exists (
      select 1
      from {{ bq_rel }} as _det_run
      where _det_run.pipeline = '{{ pipeline_name }}'
        and {{ p }}__extract_run_datetime = timestamp(_det_run.extract_run_datetime)
        and {{ p }}__interval_start_datetime = timestamp(_det_run.interval_start)
        and {{ p }}__interval_end_datetime = timestamp(_det_run.interval_end)
    )
  {%- else -%}
    exists (
      select 1
      from read_json('{{ env_var("DET_CATCHUP_MANIFEST_PATH") }}') as _det_cm,
      unnest(_det_cm.runs) as _det_u(_det_run)
      where _det_run.pipeline = '{{ pipeline_name }}'
        and {{ p }}__extract_run_datetime = cast(_det_run.extract_run_datetime as timestamptz)
        and {{ p }}__interval_start_datetime = cast(_det_run.interval_start as timestamptz)
        and {{ p }}__interval_end_datetime = cast(_det_run.interval_end as timestamptz)
    )
  {%- endif -%}
{% endmacro %}


{% macro det_silver_incremental_filter(
    watermark,
    unique_key,
    pipeline_name,
    lookback=none
) %}
{#-
  Incremental WHERE for scaffolded silver models.

  Catch-up: when var det_catchup is true, restrict via DuckDB read_json of
  DET_CATCHUP_MANIFEST_PATH or BigQuery DET_CATCHUP_BQ_RELATION. Otherwise
  watermark > max(this) or >= max - lookback.
-#}
{% if is_incremental() %}
  {% if var('det_catchup', false) %}
    where {{ det_catchup_coverage_predicate(pipeline_name) }}
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
  After catch-up filter: keep rows whose unique_key is missing in silver or
  whose watermark is strictly newer than silver (never demote).
  Emits nothing when not in catch-up mode / not incremental.
-#}
{% if is_incremental() %}
  {% if var('det_catchup', false) %}
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
