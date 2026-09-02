"""YAML merge helpers for DET dbt scaffolding (sources.yml and _silver__models.yml)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from det.logging import get_logger
from det.runtime.config import DbtDocsConfig, DbtSilverConfig

from .dbt_sql import (
    _META_COLUMN_DESCRIPTIONS,
    ScaffoldAction,
    _is_identity_name,
    _ordered_meta_columns,
    _render,
)

logger = get_logger(__name__)


def _yaml_block(mapping: dict[str, Any], *, indent: int) -> str:
    """Dump a small mapping and indent every line for embedding in sources.yml."""
    raw = yaml.safe_dump(
        mapping,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    ).rstrip()
    prefix = " " * indent
    return "\n".join(prefix + line if line else line for line in raw.splitlines())


def _table_entry_yaml(
    table: str,
    required: list[str],
    *,
    description: str,
    provider: str,
    columns_struct: str,
    column_descriptions: dict[str, str] | None = None,
    schema_properties: dict[str, Any] | None = None,
    bronze_source: str = "filesystem",
) -> str:
    """
    Emit a sources.yml table block as text (not yaml.dump).

    ``external_location`` embeds dbt Jinja; a YAML round-trip mangles quotes.
    """
    descs = column_descriptions or {}
    props = schema_properties or {}

    identity: list[str] = []
    payload: list[str] = []
    seen: set[str] = set()

    def _classify(name: str) -> None:
        if name in seen or name.startswith("__"):
            return
        seen.add(name)
        if name in required or _is_identity_name(name):
            identity.append(name)
        else:
            payload.append(name)

    for col in required:
        _classify(col)
    for name in props:
        if isinstance(name, str):
            _classify(name)

    identity.sort()
    payload.sort()
    ordered_names = [*identity, *payload, *_ordered_meta_columns()]

    columns: list[dict[str, Any]] = []
    for name in ordered_names:
        entry: dict[str, Any] = {"name": name}
        if name in _META_COLUMN_DESCRIPTIONS:
            entry["description"] = _META_COLUMN_DESCRIPTIONS[name]
        elif name in descs:
            entry["description"] = descs[name]
        if name == "__row_hash" or name in required:
            entry["tests"] = ["not_null"]
        columns.append(entry)

    cols_yaml = yaml.safe_dump(
        columns, sort_keys=False, default_flow_style=False, allow_unicode=True, width=1000
    ).rstrip()
    cols_indented = "\n".join(
        "        " + line if line else line for line in cols_yaml.splitlines()
    )
    desc_indented = _yaml_block({"description": description}, indent=8)

    # Layout 1: DET_LAKE_PATH/bronze/{provider}/{table}
    # Layout 2: DET_LAKE_PATH_BRONZE/{provider}/{table} (flattened; no /bronze/).
    # Inline env_var (not the det_lake_bronze_path macro): dbt does not expand
    # project macros when rendering sources.yml meta.external_location.
    bronze_root_jinja = (
        "{{ env_var('DET_LAKE_PATH_BRONZE', "
        "env_var('DET_LAKE_PATH', '../data/lake') ~ '/bronze') }}"
    )
    bronze_path = f"{bronze_root_jinja}/{provider}/{table}"
    if_not_bq = "{% if target.name != 'bigquery' %}"
    endif = "{% endif %}"
    if bronze_source == "iceberg":
        loc = f"iceberg_scan('{bronze_path}')"
        lines = [
            f"      - name: {table}",
            desc_indented,
            "        meta:",
            f'          formatter: "{if_not_bq}template{endif}"',
            f'          external_location: "{if_not_bq}{loc}{endif}"',
            "        columns:",
            cols_indented,
        ]
        return "\n".join(lines)
    loc = (
        f"read_json('{bronze_path}/**/data.jsonl', "
        f"columns={columns_struct}, union_by_name=true, hive_partitioning=false)"
    )
    lines = [
        f"      - name: {table}",
        desc_indented,
        "        meta:",
        f'          formatter: "{if_not_bq}template{endif}"',
        f'          external_location: "{if_not_bq}{loc}{endif}"',
        "        columns:",
        cols_indented,
    ]
    return "\n".join(lines)


def _sources_has_table(text: str, table: str) -> bool:
    return re.search(rf"(?m)^\s+- name:\s*{re.escape(table)}\s*$", text) is not None


def _sources_has_source(text: str, source_name: str) -> bool:
    return re.search(
        rf"(?m)^\s+- name:\s*{re.escape(source_name)}\s*$", text
    ) is not None


def _insert_source_block(text: str, source_only: str, *, source_name: str) -> str:
    """
    Insert a top-level source block in alphabetical order by ``- name:``.

    Table force-replace stays in-place; this only runs when adding a *new* source
    so provider order stays stable across scaffolds.
    """
    matches = list(re.finditer(r"(?m)^(?P<indent>\s*)- name:\s*(?P<name>\S+)\s*$", text))
    # Only consider top-level source entries (indent under ``sources:``, typically 2 spaces).
    source_headers = [
        m
        for m in matches
        if m.group("indent") in {"  ", "\t"}
        or (len(m.group("indent")) == 2 and m.group("indent").isspace())
    ]
    insert_at: int | None = None
    for m in source_headers:
        if m.group("name") > source_name:
            insert_at = m.start()
            break
    if insert_at is None:
        return text.rstrip() + "\n" + source_only
    return text[:insert_at] + source_only + text[insert_at:]


def _append_table_under_source(text: str, source_name: str, table_yaml: str) -> str:
    """Append a table YAML block under the matching top-level source entry."""
    start = re.search(
        rf"(?m)^(?P<indent>\s*)- name:\s*{re.escape(source_name)}\s*$",
        text,
    )
    if start is None:
        if not re.search(r"(?m)^\s*tables:\s*$", text):
            text = text.rstrip() + "\n    tables:\n"
        return text.rstrip() + "\n" + table_yaml + "\n"

    indent = start.group("indent")
    rest = text[start.end():]
    next_source = re.search(rf"(?m)^{re.escape(indent)}- name:\s*\S+\s*$", rest)
    insert_at = start.end() + (next_source.start() if next_source else len(rest))
    before = text[:insert_at].rstrip()
    after = text[insert_at:]
    if "tables:" not in before[start.start():]:
        before = before + "\n    tables:"
    return before + "\n" + table_yaml + "\n" + after


def _merge_sources_table(
    sources_path: Path,
    *,
    source_name: str,
    provider: str,
    table: str,
    required: list[str],
    columns_struct: str,
    force: bool,
    dry_run: bool,
    actions: list[Any],
    table_description: str | None = None,
    column_descriptions: dict[str, str] | None = None,
    schema_properties: dict[str, Any] | None = None,
    bronze_source: str = "filesystem",
) -> None:
    """
    Merge a bronze table into sources.yml under ``bronze_{provider}``.

    sources.yml may contain dbt Jinja, so we avoid a full YAML round-trip and
    append/replace table blocks as text. Each table gets a schema-aware
    ``read_json(..., columns={...})`` external_location.
    """
    table_yaml = _table_entry_yaml(
        table,
        required,
        description=table_description or f"DET bronze {provider}.{table}",
        provider=provider,
        columns_struct=columns_struct,
        column_descriptions=column_descriptions,
        schema_properties=schema_properties,
        bronze_source=bronze_source,
    )

    if not sources_path.exists():
        content = _render(
            "sources.yml.j2",
            source_name=source_name,
            provider=provider,
            tables_yaml=table_yaml + "\n",
        )
        detail = "create sources.yml"
        if dry_run:
            actions.append(ScaffoldAction(path=sources_path, action="would_write", detail=detail))
            return
        sources_path.parent.mkdir(parents=True, exist_ok=True)
        sources_path.write_text(content, encoding="utf-8")
        actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
        logger.info("scaffolded sources.yml", path=str(sources_path), detail=detail)
        return

    text = sources_path.read_text(encoding="utf-8")
    # Drop legacy source-level read_json_auto once tables carry typed locations.
    if "external_location:" in text and "read_json_auto(" in text:
        text = re.sub(
            r"(?ms)^(?P<indent>\s*)meta:\n"
            r"(?:(?P=indent)  .*\n)*?"
            r"(?P=indent)  external_location:.*\n"
            r"(?:(?P=indent)  .*\n)*",
            "",
            text,
            count=1,
        )
    if not _sources_has_source(text, source_name):
        block = _render(
            "sources.yml.j2",
            source_name=source_name,
            provider=provider,
            tables_yaml=table_yaml + "\n",
        )
        # Append only the source entry (skip version: 2 / sources: header).
        source_only = block.split("sources:\n", 1)[-1].rstrip() + "\n"
        text = _insert_source_block(text, source_only, source_name=source_name)
        detail = f"add source {source_name}"
        if dry_run:
            actions.append(ScaffoldAction(path=sources_path, action="would_patch", detail=detail))
            return
        sources_path.write_text(text, encoding="utf-8")
        actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
        return

    if _sources_has_table(text, table):
        if not force:
            actions.append(
                ScaffoldAction(path=sources_path, action="skip", detail=f"table {table} exists")
            )
            return
        # Stop at the next peer table (same indent) or a less-indented source
        # entry — not at more-indented ``columns: - name:`` items.
        start_m = re.search(
            rf"(?m)^(?P<indent>\s*)- name:\s*{re.escape(table)}\s*$", text
        )
        if start_m is None:
            actions.append(
                ScaffoldAction(
                    path=sources_path,
                    action="skip",
                    detail=f"could not replace table {table}",
                )
            )
            return
        indent = start_m.group("indent")
        indent_len = len(indent)
        pattern = re.compile(
            rf"(?m)^{re.escape(indent)}- name:\s*{re.escape(table)}\s*\n"
            rf"(?:(?!^\s{{0,{indent_len}}}- name:)[^\n]*\n)*"
        )
        new_text, n = pattern.subn(table_yaml + "\n", text, count=1)
        if n == 0:
            actions.append(
                ScaffoldAction(
                    path=sources_path,
                    action="skip",
                    detail=f"could not replace table {table}",
                )
            )
            return
        detail = "replace table"
        text = new_text
    else:
        text = _append_table_under_source(text, source_name, table_yaml)
        detail = "add table"

    if dry_run:
        actions.append(ScaffoldAction(path=sources_path, action="would_patch", detail=detail))
        return

    sources_path.write_text(text, encoding="utf-8")
    actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
    logger.info("scaffolded sources.yml", path=str(sources_path), detail=detail)


def _add_col_test(by_col: dict[str, list[Any]], name: str, test: Any) -> None:
    tests = by_col.setdefault(name, [])
    if test == "unique":
        if "unique" not in tests:
            tests.insert(0, "unique")
        return
    if test == "not_null":
        if "not_null" not in tests:
            tests.append("not_null")
        return
    tests.append(test)


def _merge_silver_models_yml(
    models_yml: Path,
    *,
    dataset: str,
    silver: DbtSilverConfig,
    required: list[str],
    force: bool,
    dry_run: bool,
    actions: list[Any],
    model_description: str | None = None,
    column_descriptions: dict[str, str] | None = None,
    docs: DbtDocsConfig | None = None,
) -> None:
    model_name = f"silver_{dataset}"
    if models_yml.exists():
        doc = yaml.safe_load(models_yml.read_text(encoding="utf-8")) or {}
    else:
        doc = {"version": 2, "models": []}

    models = doc.setdefault("models", [])
    existing = next(
        (m for m in models if isinstance(m, dict) and m.get("name") == model_name), None
    )

    by_col: dict[str, list[Any]] = {}
    unique_key = list(silver.unique_key)
    for key in unique_key:
        _add_col_test(by_col, key, "not_null")
        if len(unique_key) == 1:
            _add_col_test(by_col, key, "unique")
    for col in required:
        _add_col_test(by_col, col, "not_null")
    if "__row_hash" not in by_col:
        _add_col_test(by_col, "__row_hash", "not_null")
    for col in silver.not_null:
        _add_col_test(by_col, col, "not_null")
    for col in silver.unique:
        _add_col_test(by_col, col, "unique")
    for col, values in silver.accepted_values.items():
        _add_col_test(
            by_col, col, {"accepted_values": {"values": list(values)}}
        )

    mapped = column_descriptions or {}
    docs_cols = dict(docs.columns) if docs is not None else {}

    def _desc_for(name: str) -> str | None:
        if name in docs_cols:
            return docs_cols[name]
        if name in _META_COLUMN_DESCRIPTIONS:
            return _META_COLUMN_DESCRIPTIONS[name]
        if name in mapped:
            return mapped[name]
        return None

    columns_out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name, tests in by_col.items():
        entry: dict[str, Any] = {"name": name}
        desc = _desc_for(name)
        if desc:
            entry["description"] = desc
        entry["tests"] = tests
        columns_out.append(entry)
        seen.add(name)
    # Docs-only columns (no tests) so analytics overlays aren't dropped.
    for name, text in docs_cols.items():
        if name in seen:
            continue
        columns_out.append({"name": name, "description": text})
        seen.add(name)

    entry = {
        "name": model_name,
        "description": model_description or f"Cleaned, deduped {dataset} (silver)",
        "columns": columns_out,
    }

    if existing is not None and not force:
        actions.append(
            ScaffoldAction(path=models_yml, action="skip", detail=f"model {model_name} exists")
        )
        return

    if existing is None:
        models.append(entry)
        detail = "add model"
    else:
        models[models.index(existing)] = entry
        detail = "replace model"

    if dry_run:
        actions.append(ScaffoldAction(path=models_yml, action="would_patch", detail=detail))
        return

    models_yml.parent.mkdir(parents=True, exist_ok=True)
    models_yml.write_text(
        yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )
    actions.append(ScaffoldAction(path=models_yml, action="write", detail=detail))
