{{ config(tags=['ops']) }}

-- sum(error) / sum(attempts) over score_hours (receipt started_at) vs seed
-- max_error_rate. Skip the row when max_error_rate is null. Empty seed → pass.

with expected as (
  select
    pipeline,
    command,
    score_hours,
    max_error_rate
  from {{ ref('ops_slo_expected') }}
  where max_error_rate is not null
),
windowed as (
  select
    e.pipeline,
    e.command,
    e.max_error_rate,
    coalesce(count(r.attempt_id), 0) as attempts,
    coalesce(sum(case when r.status = 'error' then 1 else 0 end), 0) as errors
  from expected e
  left join {{ ref('stg_det__run_receipts') }} r
    on r.pipeline = e.pipeline
   and r.command = e.command
   and r.started_at >= {{ det_timestamp_minus_hours('current_timestamp', 'e.score_hours') }}
  group by 1, 2, 3
)

select
  pipeline,
  command,
  max_error_rate,
  attempts,
  errors,
  case
    when attempts = 0 then null
    else {{ det_try_cast_double('errors') }} / attempts
  end as error_rate
from windowed
where attempts = 0
   or ({{ det_try_cast_double('errors') }} / attempts) > max_error_rate
