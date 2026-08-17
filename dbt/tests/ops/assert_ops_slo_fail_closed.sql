{{ config(tags=['ops']) }}

-- Fail-closed codes in the score window for any seeded pair.
-- lease_held is excluded (operator contention, not a contract break).
-- Empty seed → pass.

with expected as (
  select
    pipeline,
    command,
    score_hours
  from {{ ref('ops_slo_expected') }}
)

select
  r.pipeline,
  r.command,
  r.attempt_id,
  r.error_code,
  r.started_at
from {{ ref('stg_det__run_receipts') }} r
inner join expected e
  on e.pipeline = r.pipeline
 and e.command = r.command
where r.error_code in ('schema_invalid', 'integrity_error', 'secret_not_set')
  and r.started_at >= (current_timestamp - (e.score_hours * interval '1 hour'))
