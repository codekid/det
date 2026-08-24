from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from det.errors import DetContractError


class SchemaValidationError(DetContractError):
    """Raised when one or more records fail strict JSON Schema validation."""

    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


def load_json_schema(path: Path | str) -> dict[str, Any]:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if p.suffix in {".yaml", ".yml"}:
        schema = yaml.safe_load(text)
    else:
        import json

        schema = json.loads(text)
    if not isinstance(schema, dict):
        raise ValueError(f"Schema must be an object: {p}")
    return schema


def validate_records(
    records: list[dict[str, Any]],
    schema: dict[str, Any],
    *,
    max_errors: int = 20,
) -> None:
    """Strict-validate canonical records. Fail the run on any violation."""
    validator = Draft202012Validator(schema)
    errors: list[str] = []
    for i, record in enumerate(records):
        for err in sorted(validator.iter_errors(record), key=lambda e: list(e.path)):
            errors.append(f"record[{i}] {list(err.path)}: {err.message}")
            if len(errors) >= max_errors:
                break
        if len(errors) >= max_errors:
            break
    if errors:
        preview = errors[:10]
        detail = "\n".join(f"  - {e}" for e in preview)
        if len(errors) > len(preview):
            detail += f"\n  ... and {len(errors) - len(preview)} more"
        raise SchemaValidationError(
            f"JSON Schema validation failed ({len(errors)} error(s)):\n{detail}",
            errors=errors,
        )


def validate_record(record: dict[str, Any], schema: dict[str, Any]) -> None:
    try:
        Draft202012Validator(schema).validate(record)
    except ValidationError as exc:
        raise SchemaValidationError(str(exc), errors=[str(exc)]) from exc
