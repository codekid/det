"""Plugin-author test helpers (base install; no pytest required for core).

Stable SemVer surface for embedders — see ``docs/api.md`` and
``docs/getting-started-library.md``.
"""

from __future__ import annotations

from det.testing.asserts import assert_no_dlt_artifacts, assert_raw_contract
from det.testing.extract import ExtractFixture, extract_fixture, records_from_fixture
from det.testing.project import TestProject
from det.testing.registry import isolated_registries, register_source_for_tests
from det.testing.run import run_extract_load
from det.testing.secrets import secrets_map

__all__ = [
    "ExtractFixture",
    "TestProject",
    "assert_no_dlt_artifacts",
    "assert_raw_contract",
    "extract_fixture",
    "isolated_registries",
    "records_from_fixture",
    "register_source_for_tests",
    "run_extract_load",
    "secrets_map",
]
