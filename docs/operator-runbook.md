# Operator runbook

Day-2 stuck states and deploy profiles. Contracts stay fail-closed — this page
compresses where to look and what to run. Deep authority:

- [publication-contract.md](publication-contract.md) — orphan raw / lease fence
- [api.md](api.md) — approvals and `LeaseHeldError` / `LeaseFencedError`
- [silver-catchup.md](silver-catchup.md) — bronze↔silver heal
- [lake-layout.md](lake-layout.md) — layout 2 roots (derived or explicit split)

Approvals are **audit / intent-binding, not authorization**. Never treat
`det approve` as a security boundary.

---

## Day-2 stuck states

| Symptom | CLI | MCP | Fix | Do not |
| --- | --- | --- | --- | --- |
| Approval stuck `claimed` (crash between claim and consume) | `det list-approvals --status claimed` · `det approval-show <id>` (heartbeat_status); same `--lake-path` / `DET_LAKE_PATH*` as the claimed run | `list_approvals` `status="claimed"` · `describe_approval` | Prefer new `det approve`. Or `det approval-release <id> --force` only after the worker is confirmed dead (warns if heartbeat fresh; pass matching lake). Store: `{ops}/approvals/` or Postgres | Release while a run may still be alive; auto-TTL release; describe approvals as auth |
| Lease held / fence | `det lock-show -p … -s …` · receipts under `{ops}/runs/` | `diagnose_pipeline` · `list_runs` | Wait for TTL / steal, or `det lock-release … --force` after the worker is gone | `--force` release while a writer may still be alive |
| Orphan raw / raw without bronze | Inspect lake paths; `det check` | `diagnose_pipeline` · `diff_partitions` · `sample_raw` | Re-extract same interval, or (after `det approve`) later-turn `det load … --approval <id>` for the committed raw extract_run | Invent dual lake roots; delete raw as first move |
| Prune apply | `det prune … --dry-run` then approve | `prune_dry_run` → show `approval_plan`, **stop** | After confirm: `det approve` → later turn `det prune … --apply --approval <id>` | Chain dry-run → `--apply` in the same turn |
| Silver hole (latest bronze missing in silver) | `det silver-catchup-diff` / plan `--dry-run` | `diff_bronze_silver` · `silver_catchup_dry_run` | After confirm: `det approve` → later `det silver-catchup-plan --apply --manifest-id scm_… --content-digest sha256:… --approval <id>` → separate approve → later `det dbt --catchup --catchup-manifest scm_… --approval <id>` | Full-refresh silver as the first heal; chain dry-run → write in one turn |

Agent policy: MCP dry-run only; mutating CLI only after explicit user confirm, then
`det approve`, then a **later** turn with `--approval`. See [AGENTS.md](../AGENTS.md).

---

## Profiles

Both profiles use **lake layout 2** only.

| | Minimal | Fleet |
| --- | --- | --- |
| **Lake layout** | **2**: `DET_LAKE_PATH` derives `{path}/raw`, `{path}/bronze`, ops=`{path}` | **2**: same derive, **or** explicit `DET_LAKE_PATH_{RAW,BRONZE,OPS}` multi-bucket |
| **Bronze** | Iceberg (recommended) or JSONL (`filesystem`) | Iceberg; optional REST/Glue catalog |
| **Leases** | Lake locks on | Lake locks; Postgres lock backend optional |
| **Analytics** | Local DuckDB | DuckDB and/or BigQuery (`DET_DBT_TARGET`) |
| **Catch-up** | Unused | Mode A lookback and/or BQ heal on `gs://` ops ([silver-catchup.md](silver-catchup.md)) |
| **Orchestration** | CLI | Airflow Compose, ops dbt (`tag:ops`), optional Cube |

### Try-it (layout 2 derived)

```bash
uv sync --extra iceberg --extra examples
export DET_DISCOVER_EXAMPLES=1
export DET_LAKE_PATH="$PWD/data/lake"   # stamps lake_layout: 2
uv run det run -p noaa.storm_events -s 2026-08-06
```
