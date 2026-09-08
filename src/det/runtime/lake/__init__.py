"""DET-owned lake I/O: local pathlib, s3://, gs://, and in-memory tests.

The lake root is a runtime location (default ``./data/lake``), not a per-pipeline
contract. ``destination.type`` still only chooses bronze serving.

Layout **2** (default): split roots — explicit ``DET_LAKE_PATH_{RAW,BRONZE,OPS}``
or derived from ``DET_LAKE_PATH`` as ``{path}/raw``, ``{path}/bronze``, ops =
``{path}``. Layout **1** (unified): ``DET_LAKE_LAYOUT=1`` / ``--lake-layout 1``.
Embedders choose arbitrary URIs for explicit split — DET never assigns bucket
names.

``DET_LAKE_MODE`` (local|cloud) is policy around the URI shape — not a second
writer path. Unset defaults to local.
"""

from __future__ import annotations

# Exposed for tests that monkeypatch ``lake_mod.os.replace``.
import os

from det.runtime.lake.backends.base import _Backend
from det.runtime.lake.backends.fsspec_backend import (
    _FsspecBackend,
    _is_not_found,
    _is_precondition_failed,
    _raise_s3_cas,
    _split_gcs_key,
    _split_s3_key,
    _strip_version_prefix,
)
from det.runtime.lake.backends.local import (
    _is_local_sidecar,
    _local_cas_guard,
    _local_cas_lock_path,
    _local_gen_path,
    _local_read_gen,
    _local_write_gen,
    _LocalBackend,
)
from det.runtime.lake.backends.memory import (
    _MEMORY_DIRS,
    _MEMORY_GENS,
    _MEMORY_STORES,
    _MEMORY_VERSIONS,
    _MemoryBackend,
    _MemoryWriter,
)
from det.runtime.lake.errors import ObjectCasUnsupported, ObjectVersionConflict
from det.runtime.lake.mode import (
    _OBJECT_SCHEMES,
    DEFAULT_LAKE_REL,
    ENV_LAKE_MODE,
    ENV_LAKE_PATH_BRONZE,
    ENV_LAKE_PATH_OPS,
    ENV_LAKE_PATH_RAW,
    LakeMode,
    _strip_spec,
    is_lake_uri,
    is_object_lake_spec,
    lake_mode_from_env,
    pick_lake_spec,
    validate_lake_mode,
)
from det.runtime.lake.open import (
    _CLOUD_EXPERIMENTAL_WARNED,
    _import_fsspec,
    _open_memory,
    clear_memory_lakes,
    open_lake,
    relpath,
    reset_lake_mode_warning_for_tests,
)
from det.runtime.lake.ref import LakeRef, _match_rglob
from det.runtime.lake.roots import (
    ENV_LAKE_LAYOUT,
    LakeRoots,
    LakeRootSpecs,
    _join_lake_child,
    _lake_uri_kind,
    _layer_spec,
    is_split_lake_configured,
    lake_layout_from_env,
    lake_layout_preference,
    resolve_lake_root_specs,
    resolve_lake_roots,
    split_lake_specs_from_settings,
    validate_lake_roots,
)

__all__ = [
    "DEFAULT_LAKE_REL",
    "ENV_LAKE_LAYOUT",
    "ENV_LAKE_MODE",
    "ENV_LAKE_PATH_BRONZE",
    "ENV_LAKE_PATH_OPS",
    "ENV_LAKE_PATH_RAW",
    "LakeMode",
    "LakeRef",
    "LakeRootSpecs",
    "LakeRoots",
    "ObjectCasUnsupported",
    "ObjectVersionConflict",
    "clear_memory_lakes",
    "is_lake_uri",
    "is_object_lake_spec",
    "is_split_lake_configured",
    "lake_layout_from_env",
    "lake_layout_preference",
    "lake_mode_from_env",
    "open_lake",
    "os",
    "pick_lake_spec",
    "relpath",
    "reset_lake_mode_warning_for_tests",
    "resolve_lake_root_specs",
    "resolve_lake_roots",
    "split_lake_specs_from_settings",
    "validate_lake_mode",
    "validate_lake_roots",
    "_Backend",
    "_CLOUD_EXPERIMENTAL_WARNED",
    "_FsspecBackend",
    "_LocalBackend",
    "_MEMORY_DIRS",
    "_MEMORY_GENS",
    "_MEMORY_STORES",
    "_MEMORY_VERSIONS",
    "_MemoryBackend",
    "_MemoryWriter",
    "_OBJECT_SCHEMES",
    "_import_fsspec",
    "_is_local_sidecar",
    "_is_not_found",
    "_is_precondition_failed",
    "_join_lake_child",
    "_lake_uri_kind",
    "_layer_spec",
    "_local_cas_guard",
    "_local_cas_lock_path",
    "_local_gen_path",
    "_local_read_gen",
    "_local_write_gen",
    "_match_rglob",
    "_open_memory",
    "_raise_s3_cas",
    "_split_gcs_key",
    "_split_s3_key",
    "_strip_spec",
    "_strip_version_prefix",
]
