"""Per-pipeline lease overlay config."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, model_validator

from det.runtime.ids import require_sql_ident
from det.runtime.secrets import looks_like_secret_name

class LeaseConfig(BaseModel):
    """Optional per-pipeline lease overlay (env/settings still win when set)."""

    backend: Literal["lake", "postgres"] | None = None
    mode: Literal["exact", "overlap"] | None = None
    pg_dsn_env: str | None = None
    pg_schema: str | None = None
    pg_table: str | None = None

    @model_validator(mode="after")
    def validate_lease_overlay(self) -> LeaseConfig:
        if self.mode == "overlap" and self.backend == "lake":
            raise ValueError(
                "lease.mode 'overlap' requires lease.backend: postgres"
            )
        if self.pg_dsn_env is not None and str(self.pg_dsn_env).strip():
            if not looks_like_secret_name(self.pg_dsn_env):
                raise ValueError(
                    "lease.pg_dsn_env must be an env var name like "
                    f"DET_LOCK_PG_DSN, got {self.pg_dsn_env!r}"
                )
        if self.pg_schema is not None and str(self.pg_schema).strip():
            require_sql_ident(self.pg_schema, what="lease.pg_schema")
        if self.pg_table is not None and str(self.pg_table).strip():
            require_sql_ident(self.pg_table, what="lease.pg_table")
            require_sql_ident(
                f"{str(self.pg_table).strip()}_overlap_excl",
                what="lease.pg_table overlap constraint",
            )
        return self
