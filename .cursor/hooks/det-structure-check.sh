#!/usr/bin/env bash
# afterFileEdit: run det structure check when pipelines/ or schemas/ change.
# Fail-open: always exit 0; never block edits.
set -euo pipefail

ROOT="$(pwd)"
INPUT="$(cat || true)"

FILE_PATH="$(
  ROOT="$ROOT" INPUT="$INPUT" python3 - <<'PY'
import json, os, sys
raw = os.environ.get("INPUT") or ""
try:
    data = json.loads(raw) if raw.strip() else {}
except json.JSONDecodeError:
    data = {}
path = data.get("file_path") or data.get("path") or ""
print(path)
PY
)"

case "$FILE_PATH" in
  */configs/pipelines/*|*/schemas/*|configs/pipelines/*|schemas/*) ;;
  *)
    echo '{}'
    exit 0
    ;;
esac

PY=""
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
else
  echo '{}'
  exit 0
fi

export DET_PROJECT_ROOT="$ROOT"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

CHECK_JSON="$("$PY" -m det.runtime.check --project-root "$ROOT" --json 2>/dev/null || true)"
if [[ -z "$CHECK_JSON" ]]; then
  echo '{}'
  exit 0
fi

ROOT="$ROOT" CHECK_JSON="$CHECK_JSON" FILE_PATH="$FILE_PATH" "$PY" - <<'PY'
import json, os

payload = json.loads(os.environ["CHECK_JSON"])
findings = payload.get("findings") or []
if not findings:
    print("{}")
    raise SystemExit(0)

lines = [
    "det check findings after edit to "
    + (os.environ.get("FILE_PATH") or "pipeline/schema")
    + ":"
]
for f in findings:
    loc = f" ({f['path']})" if f.get("path") else ""
    lines.append(
        f"- {f['severity'].upper()} [{f['code']}] {f.get('pipeline','?')}{loc}: {f.get('detail','')}"
    )
msg = "\n".join(lines)
# afterFileEdit is primarily observational; emit both keys for Cursor versions
# that inject agent context from either field.
print(json.dumps({"additional_context": msg, "agent_message": msg}))
PY

exit 0
