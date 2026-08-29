{{ config(tags=['ops']) }}

-- p95 of receipt duration_ms over score_hours (started_at) vs seed p95_ms.
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
    {% if target.type == 'bigquery' %}
    approx_quantiles(r.duration_ms, 100)[offset(95)] as observed_p95_ms
    {% else %}
    quantile_cont(r.duration_ms, 0.95) as observed_p95_ms
    {% endif %}
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
  max_p95_ms,
  observed_p95_ms
from windowed
where observed_p95_ms is null
   or observed_p95_ms > max_p95_ms
