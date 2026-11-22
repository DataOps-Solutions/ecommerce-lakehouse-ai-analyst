"""Shared fixtures for the test-suite: a tiny hand-checked dataset and Bronze builders."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from src.medallion.bronze import build_bronze_table
from src.models import RAW_ENTITIES

RUN_TS = datetime(2026, 11, 1, 12, 0, 0)


def base_rows() -> dict[str, list[dict[str, str]]]:
    """One fully valid record per entity (batch-001 content)."""
    return {
        "products": [
            {
                "sku": "REM01-NEG-M",
                "product_name": "Remera básica algodón",
                "category": "remera",
                "size": "M",
                "unit_cost": "5000.00",
                "list_price": "12900.00",
                "stock_on_hand": "10",
            },
            {
                "sku": "BUZ01-NEG-M",
                "product_name": "Buzo canguro",
                "category": "buzo",
                "size": "M",
                "unit_cost": "15000.00",
                "list_price": "29900.00",
                "stock_on_hand": "0",
            },
        ],
        "orders": [
            {
                "order_id": "ORD-0000001",
                "customer_id": "CUS-00001",
                "order_ts": "2026-08-01T10:00:00",
                "order_status": "pending_payment",
                "channel": "web",
                "currency": "ARS",
                "shipping_cost": "0.00",
                "discount_amount": "0.00",
            }
        ],
        "order_items": [
            {
                "order_item_id": "OIT-00000001",
                "order_id": "ORD-0000001",
                "sku": "REM01-NEG-M",
                "quantity": "2",
                "unit_price": "12900.00",
                "discount_amount": "0.00",
            }
        ],
        "payments": [
            {
                "payment_id": "PAY-0000001",
                "order_id": "ORD-0000001",
                "payment_ts": "2026-08-01T10:01:00",
                "payment_method": "tarjeta_credito",
                "payment_status": "pending",
                "amount": "25800.00",
                "gateway_fee": "1006.20",
                "currency": "ARS",
            }
        ],
        "returns": [
            {
                "return_id": "RET-0000001",
                "order_id": "ORD-0000001",
                "sku": "REM01-NEG-M",
                "return_ts": "2026-08-15T10:00:00",
                "quantity": "1",
                "refund_amount": "12900.00",
                "reason": "talle_incorrecto",
            }
        ],
        "checkout_events": [
            {
                "event_id": "EVT-00000001",
                "session_id": "SES-0000001",
                "event_ts": "2026-08-01T09:55:00",
                "event_type": "cart_created",
                "channel": "web",
                "payment_method": "",
                "order_id": "",
            },
            {
                "event_id": "EVT-00000002",
                "session_id": "SES-0000001",
                "event_ts": "2026-08-01T09:58:00",
                "event_type": "checkout_started",
                "channel": "web",
                "payment_method": "tarjeta_credito",
                "order_id": "",
            },
            {
                "event_id": "EVT-00000003",
                "session_id": "SES-0000001",
                "event_ts": "2026-08-01T10:01:00",
                "event_type": "payment_approved",
                "channel": "web",
                "payment_method": "tarjeta_credito",
                "order_id": "ORD-0000001",
            },
        ],
    }


def paid_batch2() -> dict[str, list[dict[str, str]]]:
    """batch-002: the base order is paid and its payment approved (later batch wins)."""
    return {
        "orders": [{**base_rows()["orders"][0], "order_status": "paid"}],
        "payments": [{**base_rows()["payments"][0], "payment_status": "approved"}],
    }


def copy_rows(rows: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    return {entity: [dict(r) for r in items] for entity, items in rows.items()}


def make_bronze(
    batches: dict[str, dict[str, list[dict[str, str]]]],
) -> dict[str, pa.Table]:
    """Build the Bronze tables for several batches. Missing entities default to empty."""
    result: dict[str, pa.Table] = {}
    for entity in RAW_ENTITIES:
        tables: list[pa.Table] = []
        for batch_id, sources in batches.items():
            rows = sources.get(entity, [])
            if not rows:
                continue
            tables.append(build_bronze_table(entity, list(rows), batch_id, RUN_TS))
        if not tables:
            raise ValueError(f"no rows for entity {entity}; add at least one record")
        result[entity] = pa.concat_tables(tables, promote_options="permissive")
    return result


def fresh_bronze(
    extra: dict[str, list[dict[str, str]]] | None = None,
    batch2: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, pa.Table]:
    """Base dataset in batch-001, plus optional extra rows and a second batch."""
    batch1 = copy_rows(base_rows())
    for entity, rows in (extra or {}).items():
        batch1.setdefault(entity, []).extend(dict(r) for r in rows)
    batches: dict[str, dict[str, list[dict[str, str]]]] = {"batch-001": batch1}
    if batch2 is not None:
        batches["batch-002"] = batch2
    return make_bronze(batches)
