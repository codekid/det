{{ config(tags=['ops']) }}

select
  attempt_date,
  pipeline,
  command,
  count(*) as attempts,
  count(*) filter (where status = 'ok') as ok,
  count(*) filter (where status = 'error') as error,
  quantile_cont(duration_ms, 0.50) as p50_ms,
  quantile_cont(duration_ms, 0.95) as p95_ms,
  sum(rows) as rows
from {{ ref('stg_det__run_receipts') }}
group by 1, 2, 3
