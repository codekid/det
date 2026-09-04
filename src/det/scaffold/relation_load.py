"""Resolve ``dbt.stg.relations.*.load`` into stg/silver materialization.

Closed load set (``full_refresh`` | ``parent_replace``) maps to relation **silver**
dbt config. Stg stays a view whenever ``load`` is set. Legacy ``materialized``
alone still drives both layers.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from det.runtime.config import RelationConfig

StgMat = Literal["view", "table"]
SilverMat = Literal["view", "table", "incremental"]
IncStrategy = Literal["delete+insert", "append", "merge"]


class _SpineLike(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def level_idx(self) -> int: ...


@dataclass(frozen=True)
class RelationMaterialization:
    """Resolved stg/silver materialization for one relation model pair."""

    stg_materialized: StgMat
    silver_materialized: SilverMat
    incremental_strategy: IncStrategy | None
    """Set when ``silver_materialized == "incremental"`` (parent_replace)."""

    @property
    def is_parent_replace(self) -> bool:
        return self.silver_materialized == "incremental"


def resolve_relation_materialization(
    rel: RelationConfig,
    *,
    incremental_strategy: IncStrategy = "delete+insert",
) -> RelationMaterialization:
    """
    Map ``load`` / legacy ``materialized`` to stg + silver configs.

    - No ``load``: both layers use ``rel.materialized`` (view|table).
    - ``full_refresh``: stg view, silver table.
    - ``parent_replace``: stg view, silver incremental + strategy.
    """
    if rel.load == "full_refresh":
        return RelationMaterialization(
            stg_materialized="view",
            silver_materialized="table",
            incremental_strategy=None,
        )
    if rel.load == "parent_replace":
        return RelationMaterialization(
            stg_materialized="view",
            silver_materialized="incremental",
            incremental_strategy=incremental_strategy,
        )
    return RelationMaterialization(
        stg_materialized=rel.materialized,
        silver_materialized=rel.materialized,
        incremental_strategy=None,
    )


def relation_dedupe_key(parent_key: str, spine: Sequence[_SpineLike]) -> list[str]:
    """Full grain for ``det_dedupe_latest_run``: parent_key + full spine."""
    return [parent_key, *[e.name for e in spine]]


def relation_delete_key(parent_key: str, spine: Sequence[_SpineLike]) -> list[str]:
    """
    Incremental ``unique_key`` for parent_replace delete+insert.

    ``[parent_key] + ancestor spine`` (exclude this level's own grain/index) so
    vanished children under a touched parent/line are removed.

    dbt ``delete+insert`` deletes target rows whose unique_key values appear in
    the incremental batch, then inserts. BigQuery has no stock ``delete+insert``;
    relation silver forces that strategy on BQ and DET macros
    (``det_bq_delete_insert``) implement delete-then-insert there. Do **not** map
    parent_replace to ``merge``: delete_key is not row-unique, so merge cannot
    drop vanished children. With delete_key ``[parent_key]`` (top-level) or
    ``[parent_key, …ancestor spine]`` (nested), the batch clears all children for
    those parents/lines — including grain values absent from the new explosion —
    before insert.

    Empty/null relation arrays produce no cross-join unnest rows, so they never
    appear in the delete+insert batch. Scaffolded ``parent_replace`` silver sets
    ``pre_hook=det_relation_clear_empty_arrays()`` to delete those keys (and
    ancestor-empty clears for nested relations) without inserting key-only rows.
    """
    if not spine:
        return [parent_key]
    self_level = max(e.level_idx for e in spine)
    ancestors = [e.name for e in spine if e.level_idx < self_level]
    return [parent_key, *ancestors]
