"""Custom attributes: mapping validation, JSON preservation, privacy on extra fields."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ingestion.csv_adapter import CUSTOM_ATTRIBUTES_COLUMN, ingest_files
from src.ingestion.mapping import MappingError, load_mapping
from src.ingestion.privacy import PrivacyPolicy, looks_personal
from src.medallion.silver import run_silver
from tests.helpers import RUN_TS, base_rows, fresh_bronze
from tests.test_csv_ingestion import ORDER_HEADER, bronze_from_batch, build_dataset, order_row, rows

CUSTOM_HEADER = [*ORDER_HEADER, "Color", "Email de contacto"]
MAPPING_TEXT = """
name: custom_orders
description: test mapping with custom attributes
currency: ARS
format:
  delimiter: ";"
  decimal: ","
  date_format: "%d/%m/%Y %H:%M"
estimation:
  default_cost_ratio: 0.45
  default_gateway_rate: 0.035
sources:
  orders:
    file: pedidos.csv
    columns:
      order_id: ["Número de pedido"]
      order_date: ["Fecha"]
      order_status: ["Estado del pedido"]
      channel: ["Canal"]
      sku: ["SKU"]
      product_name: ["Producto"]
      quantity: ["Cantidad"]
      unit_price: ["Precio unitario"]
      payment_method: ["Medio de pago"]
      payment_status: ["Estado del pago"]
      amount_paid: ["Total pagado"]
      gateway_fee: ["Comisión pasarela"]
      shipping_cost: ["Costo de envío"]
      customer_id: ["ID cliente"]
    custom_fields:
      color: ["Color"]
      contacto_email: ["Email de contacto"]
    sensitive:
      customer_id: pseudonymize
"""


def _mapping(tmp_path: Path, text: str = MAPPING_TEXT) -> Path:
    path = tmp_path / "custom_mapping.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def _custom_dataset(tmp_path: Path) -> Path:
    orders = [
        order_row(**{"Color": "Negro", "Email de contacto": "compras@ejemplo.test"}),
        order_row(**{"Color": "", "Email de contacto": ""}),
    ]
    return build_dataset(tmp_path / "in", orders, header=CUSTOM_HEADER)


# --- mapping validation -------------------------------------------------------------


def test_repository_mapping_without_custom_fields_still_loads() -> None:
    mapping = load_mapping(
        Path(__file__).resolve().parent.parent / "mappings" / "generic_store_orders.yaml"
    )
    assert mapping.sources["orders"].custom_fields == {}


def test_custom_field_names_must_be_snake_case(tmp_path: Path) -> None:
    text = MAPPING_TEXT.replace('color: ["Color"]', 'Color-Talle: ["Color"]')
    with pytest.raises(MappingError, match="invalid custom field name"):
        load_mapping(_mapping(tmp_path, text))


def test_custom_field_cannot_shadow_a_canonical_field(tmp_path: Path) -> None:
    text = MAPPING_TEXT.replace('color: ["Color"]', 'sku: ["Color"]')
    with pytest.raises(MappingError, match="collides"):
        load_mapping(_mapping(tmp_path, text))


def test_custom_field_needs_aliases(tmp_path: Path) -> None:
    text = MAPPING_TEXT.replace('color: ["Color"]', "color: []")
    with pytest.raises(MappingError, match="no column aliases"):
        load_mapping(_mapping(tmp_path, text))


# --- preservation in Silver ----------------------------------------------------------


def test_custom_attributes_are_kept_as_json_and_canonical_columns_are_unchanged(
    tmp_path: Path,
) -> None:
    folder = _custom_dataset(tmp_path)
    result = ingest_files(folder, [_mapping(tmp_path)], salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)

    items = rows(silver.tables["order_items"])
    assert CUSTOM_ATTRIBUTES_COLUMN in silver.tables["order_items"].column_names
    first = items[0]
    assert json.loads(first["custom_attributes"]) == {"color": "Negro"}
    # Canonical values are untouched by the extra attribute.
    assert first["sku"] == "REM01-NEG-M"
    assert first["quantity"] == 2
    # Empty custom cells do not produce empty keys.
    empty_row = [r for r in items if r["custom_attributes"] is None]
    assert empty_row, "a line without custom values must keep a NULL attribute column"


def test_personal_looking_custom_field_is_dropped_before_persisting(tmp_path: Path) -> None:
    folder = _custom_dataset(tmp_path)
    result = ingest_files(folder, [_mapping(tmp_path)], salt="test-salt")
    blob = json.dumps(result.batch.sources, ensure_ascii=False, default=str)
    assert "compras@ejemplo.test" not in blob
    assert "contacto_email" not in blob
    stats = {s.field: s for s in result.report.privacy}
    assert stats["contacto_email"].action == "drop"
    assert stats["contacto_email"].cells_affected == 1


def test_custom_field_can_be_pseudonymized_explicitly(tmp_path: Path) -> None:
    text = MAPPING_TEXT.replace(
        "      customer_id: pseudonymize",
        "      customer_id: pseudonymize\n      color: pseudonymize",
    )
    folder = _custom_dataset(tmp_path)
    result = ingest_files(folder, [_mapping(tmp_path, text)], salt="test-salt")
    value = json.loads(result.batch.sources["order_items"][0][CUSTOM_ATTRIBUTES_COLUMN])
    assert value["color"].startswith("CUS-")
    assert value["color"] != "Negro"


def test_custom_attributes_are_not_reported_as_ignored(tmp_path: Path) -> None:
    folder = _custom_dataset(tmp_path)
    result = ingest_files(folder, [_mapping(tmp_path)], salt="test-salt")
    orders_file = next(f for f in result.report.files if f.source == "orders")
    assert "Color" not in orders_file.ignored_columns
    assert "Email de contacto" not in orders_file.ignored_columns


def test_missing_custom_column_is_not_an_error(tmp_path: Path) -> None:
    folder = build_dataset(tmp_path / "in", [order_row()], header=ORDER_HEADER)
    result = ingest_files(folder, [_mapping(tmp_path)], salt="test-salt")
    assert result.batch.sources["order_items"][0][CUSTOM_ATTRIBUTES_COLUMN] is None


# --- quality gate for JSON ----------------------------------------------------------


def test_invalid_custom_json_is_quarantined_and_valid_json_passes() -> None:
    bad = {**base_rows()["products"][0], "sku": "BAD-NEG-M", "custom_attributes": "{no json"}
    good = {
        **base_rows()["products"][0],
        "sku": "GOOD-NEG-M",
        "custom_attributes": json.dumps({"material": "algodón"}, ensure_ascii=False),
    }
    result = run_silver(fresh_bronze(extra={"products": [bad, good]}), RUN_TS)
    quarantined = {r["sku"]: r["_dq_reasons"] for r in rows(result.quarantine["products"])}
    assert "invalid_custom_attributes" in quarantined["BAD-NEG-M"]
    silver_skus = {r["sku"]: r for r in rows(result.tables["products"])}
    assert "GOOD-NEG-M" in silver_skus
    assert json.loads(silver_skus["GOOD-NEG-M"][CUSTOM_ATTRIBUTES_COLUMN]) == {
        "material": "algodón"
    }


# --- privacy policy on extra fields -------------------------------------------------


@pytest.mark.parametrize(
    ("name", "personal"),
    [
        ("color", False),
        ("material", False),
        ("contacto_email", True),
        ("telefono_b", True),
        ("dni_cliente", True),
        ("nombre_coleccion", True),
    ],
)
def test_heuristic_flags_personal_looking_names(name: str, personal: bool) -> None:
    assert looks_personal(name) is personal


def test_policy_drops_personal_custom_field_unless_overridden() -> None:
    policy = PrivacyPolicy.from_mappings(
        sensitive_by_source={"orders": {"telefono_b": "pseudonymize"}},
        mapped_fields={"orders": {"customer_id"}},
        salt="s",
        custom_by_source={"orders": {"contacto_email", "telefono_b", "color"}},
    )
    assert policy.action_for("orders", "contacto_email") == "drop"
    assert policy.action_for("orders", "telefono_b") == "pseudonymize"
    assert policy.action_for("orders", "color") is None
