"""Filesystem project / lake builders for plugin tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from det.runtime.runner import PipelineRunner
from det.runtime.settings import DetSettings, SecretLookup
from det.testing.secrets import secrets_map

_DEFAULT_STUB_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["id"],
    "properties": {"id": {}},
    "additionalProperties": True,
}


@dataclass
class TestProject:
    """Temporary DET project root with configs, schemas, and a filesystem lake."""

    root: Path
    lake_rel: str = "data/lake"
    _written_pipelines: list[str] = field(default_factory=list, repr=False)

    __test__ = False  # not a pytest test class

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    @property
    def lake_path(self) -> Path:
        return (self.root / self.lake_rel).resolve()

    def write_schema(
        self,
        canonical_or_rel: str,
        schema: Mapping[str, Any] | None = None,
    ) -> Path:
        """
        Write a JSON Schema YAML file.

        *canonical_or_rel* is either ``provider/source`` (writes the default
        relative path) or a project-relative path ending in ``.yaml``.
        """
        body = dict(schema) if schema is not None else dict(_DEFAULT_STUB_SCHEMA)
        if canonical_or_rel.endswith((".yaml", ".yml")):
            rel = canonical_or_rel
        else:
            parts = canonical_or_rel.replace(".", "/").strip("/").split("/")
            if len(parts) != 2:
                raise ValueError(
                    f"expected provider/source or relative schema path, got "
                    f"{canonical_or_rel!r}"
                )
            provider, source = parts
            rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(body), encoding="utf-8")
        return path

    def write_pipeline(
        self,
        name: str,
        *,
        source_type: str | None = None,
        schema_rel: str | None = None,
        overrides: Mapping[str, Any] | None = None,
        ingestion: str = "thin",
        destination_type: str = "filesystem",
        extra: Mapping[str, Any] | None = None,
    ) -> Path:
        """Write pipeline YAML. Schema path must already exist (or pass schema_rel)."""
        provider, source = name.split(".", 1)
        rel = schema_rel or f"schemas/{provider}/{source}/{source}.schema.yaml"
        pipe_dir = self.root / "configs" / "pipelines" / provider
        pipe_dir.mkdir(parents=True, exist_ok=True)
        doc: dict[str, Any] = {
            "name": name,
            "source": {
                "type": source_type or name,
                "overrides": dict(overrides or {}),
            },
            "schema": rel,
            "ingestion": {"library": ingestion},
            "destination": {
                "type": destination_type,
                "path": self.lake_rel,
            },
        }
        if extra:
            doc.update(extra)
        path = pipe_dir / f"{source}.yaml"
        path.write_text(yaml.safe_dump(doc), encoding="utf-8")
        self._written_pipelines.append(name)
        return path

    def write_minimal_pipeline(
        self,
        name: str,
        *,
        fixture_rows: Sequence[Mapping[str, Any]] | None = None,
        source_type: str | None = None,
        schema: Mapping[str, Any] | None = None,
        overrides: Mapping[str, Any] | None = None,
        ingestion: str = "thin",
    ) -> Path:
        """
        Stub schema (required ``id``) + pipeline YAML with optional fixture rows.

        Prefer this for smoke tests; hand-edit files for edge cases.
        """
        provider, source = name.split(".", 1)
        self.write_schema(f"{provider}/{source}", schema=schema)
        merged: dict[str, Any] = dict(overrides or {})
        if fixture_rows is not None:
            merged["fixture_records"] = [dict(r) for r in fixture_rows]
        return self.write_pipeline(
            name,
            source_type=source_type,
            overrides=merged or None,
            ingestion=ingestion,
        )

    def settings(
        self,
        *,
        secrets: Mapping[str, str | None] | SecretLookup | None = None,
        lock: bool = False,
        env: Mapping[str, str] | None = None,
    ) -> DetSettings:
        """Build ``DetSettings`` rooted at this project (locks off by default)."""
        resolve: SecretLookup | None
        if secrets is None:
            resolve = None
        elif isinstance(secrets, Mapping):
            resolve = secrets_map(secrets)
        else:
            resolve = secrets
        base_env = {
            "DET_LAKE_MODE": "local",
            "DET_LAKE_PATH": str(self.lake_path),
            "DET_LOCK": "1" if lock else "0",
        }
        if env:
            base_env.update(env)
        return DetSettings.from_env(
            project_root=self.root,
            env=base_env,
            resolve_secret=resolve,
        )

    def runner(
        self,
        *,
        secrets: Mapping[str, str | None] | SecretLookup | None = None,
        lock: bool = False,
        env: Mapping[str, str] | None = None,
    ) -> PipelineRunner:
        return PipelineRunner(settings=self.settings(secrets=secrets, lock=lock, env=env))
