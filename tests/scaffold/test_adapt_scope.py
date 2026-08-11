"""Unit tests for path-scoped dbt.stg AdaptScope compilation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from det.runtime.config import AdaptScope, RelationConfig
from det.scaffold.adapt_scope import (
    compile_adapt_scope,
    compile_relation_adapt,
    compile_stg_fields,
)


def test_compile_relative_default_and_scoped_rename():
    scope = AdaptScope.model_validate(
        {
            "rename": {"city": "ship_city"},
            "geo": {
                "coords": {
                    "rename": {"lat": "ship_lat"},
                }
            },
        }
    )
    flat = compile_adapt_scope(scope, ("shipping_address",), where="test")
    assert (
        flat.rename["shipping_address__city"] == "shipping_address__ship_city"
    )
    assert (
        flat.rename["shipping_address__geo__coords__lat"]
        == "shipping_address__geo__coords__ship_lat"
    )


def test_dotted_relative_matches_nested_scope():
    nested = AdaptScope.model_validate(
        {"geo": {"coords": {"rename": {"lat": "ship_lat"}}}}
    )
    dotted = AdaptScope.model_validate(
        {"rename": {"geo.coords.lat": "ship_lat"}}
    )
    a = compile_adapt_scope(nested, ("shipping_address",), where="test")
    b = compile_adapt_scope(dotted, ("shipping_address",), where="test")
    # Nested keeps intermediate prefix; dotted replaces relative segments as a unit.
    assert a.rename["shipping_address__geo__coords__lat"] == (
        "shipping_address__geo__coords__ship_lat"
    )
    assert b.rename["shipping_address__geo__coords__lat"] == (
        "shipping_address__ship_lat"
    )


def test_reject_restated_scope_prefix():
    scope = AdaptScope.model_validate({"rename": {"shipping_address.city": "x"}})
    with pytest.raises(ValueError, match="restate"):
        compile_adapt_scope(scope, ("shipping_address",), where="test")


def test_relation_not_null_uses_post_rename_names():
    rel = RelationConfig.model_validate(
        {
            "path": "line_items",
            "rename": {"sku": "line_sku"},
            "not_null": ["sku", "quantity"],
            "variant": {
                "product": {
                    "category": {
                        "rename": {"name": "category"},
                        "not_null": ["name"],
                    }
                }
            },
        }
    )
    flat = compile_relation_adapt(rel)
    assert flat.rename["sku"] == "line_sku"
    assert flat.not_null == [
        "line_sku",
        "quantity",
        "variant__product__category__category",
    ]


def test_compile_stg_fields():
    fields = {
        "shipping_address": AdaptScope.model_validate(
            {"rename": {"city": "ship_city"}}
        )
    }
    flat = compile_stg_fields(fields)
    assert flat.rename["shipping_address__city"] == "shipping_address__ship_city"


def test_adapt_scope_rejects_bad_relative_segment():
    with pytest.raises(ValidationError):
        AdaptScope.model_validate({"rename": {"BadName": "x"}})
