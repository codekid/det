{{ config(tags=['ops']) }}

-- Each seeded (pipeline, command) must have an ok receipt whose started_at is
-- within recency_hours of now. Empty seed → 0 rows → pass.

with expected as (
  select
    pipeline,
    command,
    recency_hours
  from {{ ref('ops_slo_expected') }}
),
latest_ok as (
  select
    pipeline,
    command,
    max(started_at) as last_ok_at
  from {{ ref('stg_det__run_receipts') }}
  where status = 'ok'
  group by 1, 2
)

select
  e.pipeline,
  e.command,
  e.recency_hours,
  l.last_ok_at
from expected e
left join latest_ok l
  on l.pipeline = e.pipeline
 and l.command = e.command
where l.last_ok_at is null
   or l.last_ok_at < {{ det_timestamp_minus_hours('current_timestamp', 'e.recency_hours') }}
