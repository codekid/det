{{ config(tags=['ops']) }}

-- Max of daily mart p95_ms in the score window vs seed p95_ms.
-- Skip the row when p95_ms is null. Empty seed → pass.

with expected as (
  select
    pipeline,
    command,
    score_hours,
    p95_ms
  from {{ ref('ops_slo_expected') }}
  where p95_ms is not null
),
windowed as (
  select
    e.pipeline,
    e.command,
    e.p95_ms as max_p95_ms,
    max(d.p95_ms) as observed_p95_ms
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
  max_p95_ms,
  observed_p95_ms
from windowed
where observed_p95_ms is null
   or observed_p95_ms > max_p95_ms
