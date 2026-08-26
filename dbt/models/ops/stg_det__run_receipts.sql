{{ config(tags=['ops']) }}

select
  receipt_version,
  attempt_id,
  attempt_date,
  pipeline,
  command,
  interval_start,
  interval_end,
  extract_run_datetime,
  wire_version,
  status,
  started_at,
  finished_at,
  duration_ms,
  owner,
  destination,
  artifacts,
  raw_bytes,
  {{ adapter.quote('rows') }},
  schema_sha256,
  error_code,
  error_class,
  error_message
from {{ source('det_ops', 'run_receipts') }}
