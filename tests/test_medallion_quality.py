"""Medallion tests: deduplication, quarantine routing, Gold consistency and pipeline idempotency."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from src.config import Settings
from src.data_generator import generate_batches
from src.lakehouse import read_table
from src.medallion.gold import build_gold
from src.medallion.silver import SilverResult, run_silver
from src.models import CleanOrder, CleanOrderItem
from src.pipeline import load_gold_tables, run_pipeline
from tests.helpers import RUN_TS, base_rows, copy_rows, fresh_bronze, make_bronze, paid_batch2


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def _quarantine_reasons(result: SilverResult, entity: str, key: str, value: str) -> str:
    matches = [r for r in _rows(result.quarantine[entity]) if r[key] == value]
    assert matches, f"{value} was not quarantined in {entity}"
    return str(matches[0]["_dq_reasons"])


def _silver_keys(result: SilverResult, entity: str, key: str) -> set[str]:
    return {str(r[key]) for r in _rows(result.tables[entity])}


def test_dedupe_keeps_latest_batch_version() -> None:
    """batch-002 pays the order; Silver must keep only the newest version of ORD-0000001."""
    batch2 = {
        "orders": [
            {
                **base_rows()["orders"][0],
                "order_status": "Paid ",
                "order_ts": "2026-08-01T10:00:00",
            }
        ],
        "payments": [
            {
                **base_rows()["payments"][0],
                "payment_status": "approved",
                "payment_ts": "2026-08-01T10:03:00",
            }
        ],
    }
    bronze = fresh_bronze(batch2=batch2)
    result = run_silver(bronze, RUN_TS)

    orders = _rows(result.tables["orders"])
    assert [o["order_status"] for o in orders] == ["paid"]
    report = {r.entity: r for r in result.reports}
    assert report["orders"].duplicates_removed == 1
    assert report["payments"].duplicates_removed == 1
    payment = _rows(result.tables["payments"])[0]
    assert payment["payment_status"] == "approved"


def test_corrupt_newer_version_does_not_mask_valid_older_one() -> None:
    """A negative-stock update in batch-002 is quarantined; the batch-001 valid row survives."""
    batch2 = {
        "products": [
            {**base_rows()["products"][0], "stock_on_hand": "-4"},
        ]
    }
    result = run_silver(fresh_bronze(batch2=batch2), RUN_TS)

    product = [p for p in _rows(result.tables["products"]) if p["sku"] == "REM01-NEG-M"]
    assert len(product) == 1
    assert product[0]["stock_on_hand"] == 10
    quarantined = _rows(result.quarantine["products"])
    assert len(quarantined) == 1
    assert "invalid_stock" in str(quarantined[0]["_dq_reasons"])


def test_invalid_records_are_quarantined_without_breaking_pipeline() -> None:
    extra = {
        "orders": [
            {
                **base_rows()["orders"][0],
                "order_id": "ORD-0000002",
                "currency": "USD",
            }
        ],
        "order_items": [
            {
                **base_rows()["order_items"][0],
                "order_item_id": "OIT-00000002",
                "order_id": "ORD-0000002",
            },
            {
                **base_rows()["order_items"][0],
                "order_item_id": "OIT-00000003",
                "unit_price": "0.00",
            },
            {
                **base_rows()["order_items"][0],
                "order_item_id": "OIT-00000004",
                "order_id": "ORD-9999999",
            },
        ],
    }
    result = run_silver(fresh_bronze(extra=extra), RUN_TS)

    assert "ORD-0000002" not in _silver_keys(result, "orders", "order_id")
    assert "invalid_currency" in _quarantine_reasons(result, "orders", "order_id", "ORD-0000002")
    assert "orphan_order_item" in _quarantine_reasons(
        result, "order_items", "order_item_id", "OIT-00000002"
    )
    assert "invalid_unit_price" in _quarantine_reasons(
        result, "order_items", "order_item_id", "OIT-00000003"
    )
    assert "orphan_order_item" in _quarantine_reasons(
        result, "order_items", "order_item_id", "OIT-00000004"
    )
    assert "OIT-00000001" in _silver_keys(result, "order_items", "order_item_id")


def test_return_of_item_never_sold_is_quarantined() -> None:
    extra = {
        "returns": [
            {
                **base_rows()["returns"][0],
                "return_id": "RET-0000002",
                "sku": "BUZ01-NEG-M",
            }
        ]
    }
    result = run_silver(fresh_bronze(extra=extra), RUN_TS)
    assert "return_item_not_sold" in _quarantine_reasons(
        result, "returns", "return_id", "RET-0000002"
    )


def test_silver_rows_satisfy_pydantic_contracts() -> None:
    result = run_silver(fresh_bronze(), RUN_TS)
    for row in _rows(result.tables["orders"]):
        CleanOrder.model_validate({**row, "order_status": "paid", "channel": row["channel"]})
    for row in _rows(result.tables["order_items"]):
        item = CleanOrderItem.model_validate(row)
        assert item.unit_price > Decimal(0)


def test_gold_metrics_match_hand_computed_values() -> None:
    """Expected values are computed by hand from the base dataset.

    One paid order (batch-002 marks it paid): 2 x 12.900 gross, one unit returned
    (refund 12.900), fee 1.006,20, no shipping, COGS 2 x 5.000 minus 5.000 restocked.
    net_revenue = 25.800 - 12.900 = 12.900
    net_margin  = 12.900 - 1.006,20 - 0 - 5.000 = 6.893,80
    """
    result = run_silver(fresh_bronze(batch2=paid_batch2()), RUN_TS)
    gold = build_gold(result.tables)

    econ = _rows(gold["mart_daily_unit_economics"])
    assert len(econ) == 1
    row = econ[0]
    assert row["gross_sales"] == Decimal("25800.00")
    assert row["refunds_amount"] == Decimal("12900.00")
    assert row["net_revenue"] == Decimal("12900.00")
    assert row["gateway_fees_amount"] == Decimal("1006.20")
    assert row["cogs_amount"] == Decimal("5000.00")
    assert row["net_margin_amount"] == Decimal("6893.80")
    assert row["units_sold"] == 2

    sku = {r["sku"]: r for r in _rows(gold["mart_sku_health"])}
    assert sku["REM01-NEG-M"]["units_sold"] == 2
    assert sku["REM01-NEG-M"]["units_returned"] == 1
    assert float(sku["REM01-NEG-M"]["return_rate"]) == pytest.approx(0.5)
    assert sku["REM01-NEG-M"]["high_return_flag"] is True
    assert sku["REM01-NEG-M"]["stock_alert"] == "reponer"
    assert sku["BUZ01-NEG-M"]["stock_alert"] == "sin_stock"


def test_funnel_stages_are_sequential() -> None:
    result = run_silver(fresh_bronze(), RUN_TS)
    gold = build_gold(result.tables)
    funnel = _rows(gold["mart_checkout_funnel"])
    assert len(funnel) == 1
    row = funnel[0]
    assert row["carts_created"] == 1
    assert row["checkouts_started"] == 1
    assert row["payments_approved"] == 1
    assert row["payment_method"] == "tarjeta_credito"


@pytest.fixture(scope="module")
def pipeline_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Settings]:
    root = tmp_path_factory.mktemp("lakehouse")
    settings = Settings(
        lakehouse_storage="local",
        lakehouse_local_dir=root / "data",
        output_dir=root / "output",
        data_seed=20260101,
    )
    run_pipeline(settings)
    yield settings


def test_full_pipeline_on_generated_data(pipeline_settings: Settings) -> None:
    silver = {
        e: read_table(pipeline_settings.table_uri("silver", e), None)
        for e in ("products", "orders", "order_items", "payments", "returns", "checkout_events")
    }
    for entity, key in (
        ("products", "sku"),
        ("orders", "order_id"),
        ("order_items", "order_item_id"),
        ("payments", "payment_id"),
        ("returns", "return_id"),
        ("checkout_events", "event_id"),
    ):
        keys = silver[entity].column(key).to_pylist()
        assert len(keys) == len(set(keys)), f"duplicate business key in silver {entity}"

    order_ids = set(silver["orders"].column("order_id").to_pylist())
    assert set(silver["order_items"].column("order_id").to_pylist()) <= order_ids
    assert set(silver["payments"].column("order_id").to_pylist()) <= order_ids
    assert set(silver["returns"].column("order_id").to_pylist()) <= order_ids

    quarantine = read_table(pipeline_settings.table_uri("quarantine", "orders"), None)
    assert quarantine.num_rows > 0
    assert "_dq_reasons" in quarantine.column_names

    sample = silver["orders"].slice(0, 200).to_pylist()
    for row in sample:
        CleanOrder.model_validate({**row, "order_status": row["order_status"]})


def test_full_pipeline_gold_is_consistent_with_silver(pipeline_settings: Settings) -> None:
    gold = load_gold_tables(pipeline_settings)
    silver_orders = read_table(pipeline_settings.table_uri("silver", "orders"), None).to_pylist()
    silver_items = read_table(
        pipeline_settings.table_uri("silver", "order_items"), None
    ).to_pylist()
    silver_products = read_table(
        pipeline_settings.table_uri("silver", "products"), None
    ).to_pylist()
    sold = {
        o["order_id"]: o
        for o in silver_orders
        if o["order_status"] in ("paid", "shipped", "delivered")
    }
    unit_cost = {p["sku"]: Decimal(p["unit_cost"]) for p in silver_products}

    expected_gross = sum(
        (
            Decimal(i["quantity"]) * Decimal(i["unit_price"])
            for i in silver_items
            if i["order_id"] in sold
        ),
        Decimal(0),
    )
    econ_gross = sum(
        (r["gross_sales"] for r in _rows(gold["mart_daily_unit_economics"])), Decimal(0)
    )
    assert econ_gross == expected_gross

    expected_units = sum(int(i["quantity"]) for i in silver_items if i["order_id"] in sold)
    econ_units = sum(r["units_sold"] for r in _rows(gold["mart_daily_unit_economics"]))
    assert econ_units == expected_units

    sku_rows = _rows(gold["mart_sku_health"])
    sku_units = sum(r["units_sold"] for r in sku_rows)
    assert sku_units == expected_units
    for r in sku_rows:
        assert r["units_returned"] <= r["units_sold"] or r["units_sold"] == 0
        assert r["stock_on_hand"] >= 0
        assert unit_cost[r["sku"]] >= 0

    funnel = _rows(gold["mart_checkout_funnel"])
    for r in funnel:
        assert r["carts_created"] >= r["checkouts_started"] >= r["payments_approved"]


def test_rerunning_pipeline_is_idempotent_for_bronze(pipeline_settings: Settings) -> None:
    opts = pipeline_settings.storage_options()
    before = read_table(pipeline_settings.table_uri("bronze", "orders"), opts).num_rows
    run_pipeline(pipeline_settings)
    after = read_table(pipeline_settings.table_uri("bronze", "orders"), opts).num_rows
    assert before == after


def test_generator_is_deterministic_and_contains_defects() -> None:
    first = generate_batches(7)
    second = generate_batches(7)
    assert [b.batch_id for b in first] == ["batch-001", "batch-002"]
    assert first[0].sources["orders"] == second[0].sources["orders"]
    currencies = {o["currency"] for o in first[0].sources["orders"]}
    assert "USD" in currencies


def test_make_bronze_rejects_empty_entity() -> None:
    with pytest.raises(ValueError):
        make_bronze({"batch-001": {"orders": copy_rows(base_rows())["orders"]}})


def test_silver_result_contains_all_entities() -> None:
    result = run_silver(fresh_bronze(), RUN_TS)
    assert set(result.tables) == {
        "products",
        "orders",
        "order_items",
        "payments",
        "returns",
        "checkout_events",
    }
    assert set(result.quarantine) == set(result.tables)
