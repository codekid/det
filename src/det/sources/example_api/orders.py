from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from det.logging import get_logger
from det.sources.base import Interval, SourceRow
from det.sources.http_json import dig, nest_under_path, write_json_page

logger = get_logger(__name__)

# Offline showcase payload (Shopify-shaped nesting). Override with HTTP by setting
# source.overrides.fixture_records: null and providing EXAMPLE_API_TOKEN.
_DEMO_ORDERS: list[dict[str, Any]] = [
    {
        "id": "ord_1001",
        "email": "buyer@example.com",
        "currency": "USD",
        "status": "1",
        "customer": {
            "id": "cust_9",
            "loyalty": {"tier": {"code": "gold", "name": "Gold"}},
            "default_address": {
                "city": "Austin",
                "geo": {"coords": {"lat": 30.27, "lon": -97.74}},
            },
        },
        "shipping_address": {
            "city": "Austin",
            "country_code": "US",
            "geo": {
                "coords": {"lat": 30.27, "lon": -97.74},
                "accuracy_m": 12.5,
            },
        },
        "discount_codes": [
            {"code": "SAVE10", "amount": "10.00", "discount_type": "percentage"},
            {"code": "FREESHIP", "amount": "5.00", "discount_type": "shipping"},
        ],
        "line_items": [
            {
                "sku": "SKU-A",
                "quantity": 2,
                "variant": {
                    "id": "var_1",
                    "product": {
                        "id": "prod_1",
                        "category": {"id": "cat_apparel", "name": "Apparel"},
                    },
                },
                "price_set": {
                    "shop_money": {"amount": "19.99", "currency_code": "USD"},
                },
                "tax_lines": [
                    {
                        "title": "State Tax",
                        "rate": 0.0825,
                        "price_set": {
                            "shop_money": {
                                "amount": "3.30",
                                "currency_code": "USD",
                            }
                        },
                    },
                    {
                        "title": "Local Tax",
                        "rate": 0.01,
                        "price_set": {
                            "shop_money": {
                                "amount": "0.40",
                                "currency_code": "USD",
                            }
                        },
                    },
                ],
            },
            {
                "sku": "SKU-B",
                "quantity": 1,
                "variant": {
                    "id": "var_2",
                    "product": {
                        "id": "prod_2",
                        "category": {"id": "cat_home", "name": "Home"},
                    },
                },
                "price_set": {
                    "shop_money": {"amount": "49.00", "currency_code": "USD"},
                },
                "tax_lines": [
                    {
                        "title": "State Tax",
                        "rate": 0.0825,
                        "price_set": {
                            "shop_money": {
                                "amount": "4.04",
                                "currency_code": "USD",
                            }
                        },
                    },
                ],
            },
        ],
    },
    {
        "id": "ord_1002",
        "email": "NA",
        "currency": "USD",
        "status": "2",
        "customer": {
            "id": "cust_2",
            "loyalty": {"tier": {"code": "silver", "name": "Silver"}},
            "default_address": {
                "city": "Toronto",
                "geo": {"coords": {"lat": 43.65, "lon": -79.38}},
            },
        },
        "shipping_address": {
            "city": "Toronto",
            "country_code": "CA",
            "geo": {
                "coords": {"lat": 43.65, "lon": -79.38},
                "accuracy_m": 8.0,
            },
        },
        "discount_codes": [
            {"code": "WELCOME", "amount": "15.00", "discount_type": "fixed_amount"},
        ],
        "line_items": [
            {
                "sku": "SKU-C",
                "quantity": 3,
                "variant": {
                    "id": "var_9",
                    "product": {
                        "id": "prod_9",
                        "category": {"id": "cat_gear", "name": "Gear"},
                    },
                },
                "price_set": {
                    "shop_money": {"amount": "12.50", "currency_code": "USD"},
                },
                "tax_lines": [
                    {
                        "title": "GST",
                        "rate": 0.05,
                        "price_set": {
                            "shop_money": {
                                "amount": "1.88",
                                "currency_code": "USD",
                            }
                        },
                    },
                ],
            },
        ],
    },
]


class ExampleApiOrdersSource:
    """
    Nested orders showcase for dbt.stg flatten + relations.

    Defaults include fixture_records so local extract works without a token.

    Interval mode: ``query_params`` (start/end on the request).
    Raw pages are wire-shaped ``{"data": {"orders": [...]}}``; no reshape at extract.
    """

    name = "example_api.orders"

    def defaults(self) -> dict[str, Any]:
        return {
            "base_url": "https://api.example.com",
            "path": "/v1/orders",
            "record_path": "data.orders",
            "auth_env": "EXAMPLE_API_TOKEN",
            "next_url_path": "meta.next",
            "fixture_records": list(_DEMO_ORDERS),
        }

    def extract_to_raw(
        self,
        *,
        config: dict[str, Any],
        interval: Interval,
        data_dir: Path,
    ) -> list[dict[str, Any]]:
        pages_dir = data_dir / "pages"
        pages_dir.mkdir(parents=True, exist_ok=True)
        record_path = config.get("record_path") or "data.orders"
        fixtures = config.get("fixture_records")
        if fixtures is not None:
            return [
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=1,
                    body=nest_under_path(list(fixtures), record_path=record_path),
                    origin="fixture_records",
                )
            ]

        token_env = config.get("auth_env")
        token = os.environ.get(token_env) if token_env else None
        client = RESTClient(
            base_url=config["base_url"],
            auth=BearerTokenAuth(token) if token else None,
            paginator=JSONLinkPaginator(next_url_path=config.get("next_url_path")),
            data_selector=record_path or None,
        )
        params = {"start": interval.start, "end": interval.end}
        logger.info("Fetching example API orders", path=config["path"], params=params)
        artifacts: list[dict[str, Any]] = []
        page_num = 0
        for page in client.paginate(config["path"], params=params):
            rows = [row for row in page if isinstance(row, dict)]
            page_num += 1
            artifacts.append(
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=page_num,
                    body=nest_under_path(rows, record_path=record_path),
                    origin="example_api",
                )
            )
        if not artifacts:
            artifacts.append(
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=1,
                    body=nest_under_path([], record_path=record_path),
                    origin="example_api",
                )
            )
        return artifacts

    def records_from_raw(
        self,
        *,
        config: dict[str, Any],
        raw_dir: Path,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        record_path = config.get("record_path") or "data.orders"
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            orders = dig(payload, record_path)
            if orders is None and isinstance(payload, list):
                orders = payload
            if not isinstance(orders, list):
                raise ValueError(f"No order list at {record_path!r} in {path}")
            for row in orders:
                if isinstance(row, dict):
                    yield SourceRow(data=dict(row), filename=Path(art["path"]).name)

