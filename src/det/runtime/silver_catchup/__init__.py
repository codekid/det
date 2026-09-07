"""Bronze ↔ silver catch-up: latest-per-interval diff, ops manifest, dbt vars.

Permanent façade: ``from det.runtime.silver_catchup import X`` keeps working for
every name previously imported from the monolithic module.
"""

from __future__ import annotations

from det.runtime.silver_catchup.bq_cleanup import (
    apply_bq_catchup_cleanup,
    drop_bq_catchup_external_table,
    list_bq_catchup_external_tables,
    plan_bq_catchup_cleanup,
    resolve_bq_catchup_cleanup_cutoff,
    validate_bq_catchup_cleanup_scope,
)
from det.runtime.silver_catchup.bq_heal import (
    CATCHUP_BQ_EXTERNAL_TABLE_PREFIX,
    _bq_client,
    _bq_project_dataset_location,
    _list_silver_extract_runs_bigquery,
    analytics_target_is_bigquery,
    catchup_bq_external_table_name,
    catchup_bq_relation,
    ensure_bq_catchup_external_table,
)
from det.runtime.silver_catchup.diff import (
    diff_bronze_silver,
    diff_bronze_silver_fleet,
    list_silver_extract_runs,
)
from det.runtime.silver_catchup.duckdb_silver import _list_silver_extract_runs_duckdb
from det.runtime.silver_catchup.ids import (
    MANIFEST_ID_PREFIX,
    MANIFEST_VERSION,
    _coverage_key,
    _norm_ts,
    _runs_jsonl_bytes,
    catchup_content_digest,
    new_catchup_manifest_id,
    parse_duration,
    parse_extract_lookback,
    silver_relation,
    validate_catchup_candidate_scope,
    validate_catchup_content_digest,
    validate_catchup_manifest_id,
)
from det.runtime.silver_catchup.manifest import (
    assert_catchup_digest_matches,
    assert_catchup_runs_sidecar_matches,
    catchup_select_from_manifest,
    catchup_vars_from_manifest,
    load_catchup_runs_from_jsonl,
    manifest_payload_from_catchup,
    plan_catchup_manifest,
    read_catchup_manifest,
    write_catchup_manifest,
)
from det.runtime.silver_catchup.paths import (
    CATCHUP_DIR,
    catchup_manifest_file_path,
    catchup_manifest_ref,
    catchup_runs_file_path,
    catchup_runs_ref,
    catchup_runs_ref_from_manifest,
    manifest_relpath_for_root,
    resolve_ops_lake,
)

__all__ = [
    "CATCHUP_BQ_EXTERNAL_TABLE_PREFIX",
    "CATCHUP_DIR",
    "MANIFEST_ID_PREFIX",
    "MANIFEST_VERSION",
    "_bq_client",
    "_bq_project_dataset_location",
    "_coverage_key",
    "_list_silver_extract_runs_bigquery",
    "_list_silver_extract_runs_duckdb",
    "_norm_ts",
    "_runs_jsonl_bytes",
    "analytics_target_is_bigquery",
    "apply_bq_catchup_cleanup",
    "assert_catchup_digest_matches",
    "assert_catchup_runs_sidecar_matches",
    "catchup_bq_external_table_name",
    "catchup_bq_relation",
    "catchup_content_digest",
    "catchup_manifest_file_path",
    "catchup_manifest_ref",
    "catchup_runs_file_path",
    "catchup_runs_ref",
    "catchup_runs_ref_from_manifest",
    "catchup_select_from_manifest",
    "catchup_vars_from_manifest",
    "diff_bronze_silver",
    "diff_bronze_silver_fleet",
    "drop_bq_catchup_external_table",
    "ensure_bq_catchup_external_table",
    "list_bq_catchup_external_tables",
    "list_silver_extract_runs",
    "load_catchup_runs_from_jsonl",
    "manifest_payload_from_catchup",
    "manifest_relpath_for_root",
    "new_catchup_manifest_id",
    "parse_duration",
    "parse_extract_lookback",
    "plan_bq_catchup_cleanup",
    "plan_catchup_manifest",
    "read_catchup_manifest",
    "resolve_bq_catchup_cleanup_cutoff",
    "resolve_ops_lake",
    "silver_relation",
    "validate_bq_catchup_cleanup_scope",
    "validate_catchup_candidate_scope",
    "validate_catchup_content_digest",
    "validate_catchup_manifest_id",
    "write_catchup_manifest",
]
