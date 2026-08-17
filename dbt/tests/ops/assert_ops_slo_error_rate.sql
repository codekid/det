{{ config(tags=['ops']) }}

-- sum(error) / sum(attempts) over score_hours vs seed max_error_rate.
-- Skip the row when max_error_rate is null. Empty seed → pass.

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
    coalesce(sum(d.attempts), 0) as attempts,
    coalesce(sum(d.error), 0) as errors
  from expected e
  left join {{ ref('det__ops_run_daily') }} d
    on d.pipeline = e.pipeline
   and d.command = e.command
   and d.attempt_date >= cast(
     current_timestamp - (e.score_hours * interval '1 hour') as date
   )
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
    else errors::double / attempts
  end as error_rate
from windowed
where attempts = 0
   or (errors::double / attempts) > max_error_rate
