"""CSV ingestion tests: mappings, parsing, estimation flags, personal-data hygiene, idempotency."""

from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from src.config import PROJECT_ROOT, Settings
from src.data_generator import RawBatch
from src.ingestion.csv_adapter import (
    CANONICAL_COLUMNS,
    ingest_files,
    parse_money,
    parse_timestamp,
)
from src.ingestion.mapping import MappingError, load_mapping, resolve_columns
from src.ingestion.privacy import PrivacyPolicy, pseudonymize
from src.medallion.bronze import build_bronze_table
from src.medallion.gold import build_gold
from src.medallion.silver import run_silver
from src.pipeline import run_csv_ingestion
from src.reporting.client_summary import build_client_summary, render_client_markdown
from tests.helpers import RUN_TS

SAMPLES = PROJECT_ROOT / "data" / "samples_real"
MAPPINGS = [
    PROJECT_ROOT / "mappings" / "generic_store_orders.yaml",
    PROJECT_ROOT / "mappings" / "costs_and_stock.yaml",
]
ORDER_HEADER = [
    "Número de pedido",
    "Fecha",
    "Estado del pedido",
    "Canal",
    "SKU",
    "Producto",
    "Cantidad",
    "Precio unitario",
    "Descuento ítem",
    "Descuento del pedido",
    "Costo de envío",
    "Medio de pago",
    "Estado del pago",
    "Total pagado",
    "Comisión pasarela",
    "ID cliente",
    "Nombre del cliente",
    "Email",
    "Teléfono",
    "DNI",
    "Dirección",
]


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(header)
        writer.writerows(rows)


def order_row(**overrides: str) -> dict[str, str]:
    base = {
        "Número de pedido": "PED-1",
        "Fecha": "03/09/2026 12:14",
        "Estado del pedido": "Entregado",
        "Canal": "Sitio web",
        "SKU": "REM01-NEG-M",
        "Producto": "Remera básica algodón",
        "Cantidad": "2",
        "Precio unitario": "12.900,00",
        "Descuento ítem": "0,00",
        "Descuento del pedido": "0,00",
        "Costo de envío": "0,00",
        "Medio de pago": "Tarjeta de crédito",
        "Estado del pago": "Aprobado",
        "Total pagado": "25.800,00",
        "Comisión pasarela": "1.006,20",
        "ID cliente": "CL-1",
        "Nombre del cliente": "Cliente de prueba",
        "Email": "cliente@ejemplo.test",
        "Teléfono": "+54 9 11 0000 0001",
        "DNI": "00000001",
        "Dirección": "Calle Falsa 1",
    }
    base.update(overrides)
    return base


def to_rows(header: list[str], dicts: list[dict[str, str]]) -> list[list[str]]:
    return [[d.get(h, "") for h in header] for d in dicts]


def build_dataset(
    folder: Path,
    orders: list[dict[str, str]],
    products: list[list[str]] | None = None,
    returns: list[list[str]] | None = None,
    header: list[str] | None = None,
) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    write_csv(
        folder / "pedidos.csv", header or ORDER_HEADER, to_rows(header or ORDER_HEADER, orders)
    )
    write_csv(
        folder / "costos_stock.csv",
        ["SKU", "Producto", "Categoría", "Talle", "Costo unitario", "Precio de lista", "Stock"],
        products
        or [["REM01-NEG-M", "Remera básica algodón", "remera", "M", "5.000,00", "12.900,00", "10"]],
    )
    write_csv(
        folder / "devoluciones.csv",
        [
            "Número de devolución",
            "Número de pedido",
            "SKU",
            "Fecha de devolución",
            "Cantidad",
            "Monto reembolsado",
            "Motivo",
        ],
        returns or [],
    )
    return folder


def bronze_from_batch(batch: RawBatch) -> dict[str, pa.Table]:
    tables: dict[str, pa.Table] = {}
    for entity in CANONICAL_COLUMNS:
        tables[entity] = build_bronze_table(
            entity,
            list(batch.sources.get(entity, [])),
            batch.batch_id,
            RUN_TS,
            source=batch.source,
            columns=batch.columns[entity],
        )
    return tables


def rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


# --- mappings -------------------------------------------------------------------


def test_repository_mappings_load() -> None:
    orders = load_mapping(MAPPINGS[0])
    costs = load_mapping(MAPPINGS[1])
    assert set(orders.sources) == {"orders"}
    assert set(costs.sources) == {"products", "returns"}
    assert orders.format.decimal == ","
    assert orders.sources["orders"].sensitive["customer_email"] == "drop"


def test_mapping_rejects_unknown_source(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text(
        "name: bad\ndescription: x\nsources:\n  invoices:\n    file: f.csv\n    columns: {}\n",
        encoding="utf-8",
    )
    with pytest.raises(MappingError):
        load_mapping(path)


def test_mapping_rejects_missing_required_field(tmp_path: Path) -> None:
    path = tmp_path / "missing.yaml"
    path.write_text(
        "name: m\ndescription: x\nsources:\n  orders:\n    file: f.csv\n    columns:\n"
        "      order_id: [Pedido]\n",
        encoding="utf-8",
    )
    with pytest.raises(MappingError, match="required"):
        load_mapping(path)


def test_header_matching_ignores_case_accents_and_separators() -> None:
    mapping = load_mapping(MAPPINGS[0])
    headers = [
        "NUMERO_DE_PEDIDO",
        "fecha",
        "estado del pedido",
        "canal",
        "sku",
        "producto",
        "cantidad",
        "precio unitario",
        "medio de pago",
        "Descuento ítem",
    ]
    resolved = resolve_columns(headers, mapping.sources["orders"], "orders")
    assert resolved["order_id"] == "NUMERO_DE_PEDIDO"
    assert resolved["line_discount"] == "Descuento ítem"


def test_missing_required_column_in_file_is_reported(tmp_path: Path) -> None:
    mapping = load_mapping(MAPPINGS[0])
    with pytest.raises(MappingError, match="required fields"):
        resolve_columns(["Fecha", "SKU"], mapping.sources["orders"], "orders")


# --- parsing ----------------------------------------------------------------------


def test_money_parsing_handles_argentine_format() -> None:
    assert parse_money("$ 1.234,50", ",") == "1234.50"
    assert parse_money("12900", ".") == "12900.00"
    assert parse_money("1,234.50", ".") == "1234.50"
    assert parse_money("", ",") == ""
    # Not pesos: left raw so Silver rejects it instead of silently converting it.
    assert parse_money("USD 120,00", ",") == "USD 120,00"


def test_timestamp_parsing_and_raw_fallback() -> None:
    assert parse_timestamp("03/09/2026 12:14", "%d/%m/%Y %H:%M") == "2026-09-03T12:14:00"
    assert parse_timestamp("03/09/2026", None) == "2026-09-03T00:00:00"
    assert parse_timestamp("31/02/2026 10:00", None) == "31/02/2026 10:00"


# --- privacy ----------------------------------------------------------------------


def test_pseudonym_is_deterministic_per_salt() -> None:
    assert pseudonymize("CL-1", "salt-a") == pseudonymize(" cl-1 ", "salt-a")
    assert pseudonymize("CL-1", "salt-a") != pseudonymize("CL-1", "salt-b")
    assert pseudonymize("CL-1", "salt-a").startswith("CUS-")


def test_pseudonymization_requires_salt() -> None:
    with pytest.raises(MappingError, match="PSEUDONYMIZATION_SALT"):
        PrivacyPolicy({("orders", "customer_id"): "pseudonymize"}, salt=None)


def test_privacy_policy_drops_and_counts_cells() -> None:
    policy = PrivacyPolicy({("orders", "customer_email"): "drop"}, salt=None)
    assert policy.apply("orders", "customer_email", "a@b.test") is None
    assert policy.apply("orders", "customer_email", "") is None
    stats = {(s.source, s.field): s.cells_affected for s in policy.stats()}
    assert stats[("orders", "customer_email")] == 1


# --- estimation flags and hand-checked margin ----------------------------------------


def test_margin_is_exact_when_costs_and_fees_are_informed(tmp_path: Path) -> None:
    """Hand-checked: 2 x 12.900 sold, 1 unit refunded at 12.900, fee 1.006,20, COGS 5.000.

    net_revenue = 25.800 - 12.900 = 12.900
    net_margin  = 12.900 - 1.006,20 - 0 - 5.000 = 6.893,80
    """
    folder = build_dataset(
        tmp_path / "in",
        [order_row()],
        returns=[
            ["DEV-1", "PED-1", "REM01-NEG-M", "15/09/2026", "1", "12.900,00", "Talle incorrecto"]
        ],
    )
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    gold = build_gold(silver.tables)
    econ = rows(gold["mart_daily_unit_economics"])
    assert len(econ) == 1
    assert econ[0]["net_margin_amount"] == Decimal("6893.80")
    assert econ[0]["data_quality"] == "exact"
    assert econ[0]["estimated_cost_lines"] == 0
    assert econ[0]["estimated_fee_payments"] == 0


def test_missing_unit_cost_is_estimated_and_flagged(tmp_path: Path) -> None:
    folder = build_dataset(
        tmp_path / "in",
        [order_row()],
        products=[["REM01-NEG-M", "Remera básica algodón", "remera", "M", "", "12.900,00", "10"]],
    )
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    product = rows(silver.tables["products"])[0]
    assert product["unit_cost_source"] == "estimated"
    assert product["unit_cost"] == Decimal("5805.00")  # 45 % of list price, assumption
    econ = rows(build_gold(silver.tables)["mart_daily_unit_economics"])[0]
    assert econ["data_quality"] == "estimated"
    assert econ["estimated_cost_lines"] == 1
    assert result.report.estimation.products_unit_cost_estimated == 1


def test_missing_unit_cost_without_assumption_is_quarantined(tmp_path: Path) -> None:
    folder = build_dataset(
        tmp_path / "in",
        [order_row()],
        products=[["REM01-NEG-M", "Remera básica algodón", "remera", "M", "", "12.900,00", "10"]],
    )
    mapping = tmp_path / "no_ratio.yaml"
    text = (
        MAPPINGS[1]
        .read_text(encoding="utf-8")
        .replace("default_cost_ratio: 0.45", "default_cost_ratio: null")
    )
    mapping.write_text(text, encoding="utf-8")
    result = ingest_files(folder, [MAPPINGS[0], mapping], salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    assert "REM01-NEG-M" not in {r["sku"] for r in rows(silver.tables["products"])}
    reasons = [r["_dq_reasons"] for r in rows(silver.quarantine["products"])]
    assert any("invalid_unit_cost" in reason for reason in reasons)


def test_gateway_fee_is_estimated_when_export_has_no_fee_column(tmp_path: Path) -> None:
    header = [h for h in ORDER_HEADER if h != "Comisión pasarela"]
    folder = build_dataset(tmp_path / "in", [order_row()], header=header)
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    payment = rows(silver.tables["payments"])[0]
    assert payment["gateway_fee_source"] == "estimated"
    # 3,9 % configured for tarjeta_credito on 25.800
    assert payment["gateway_fee"] == Decimal("1006.20")
    assert result.report.estimation.payments_gateway_fee_estimated == 1


def test_exact_gateway_fee_is_flagged_exact(tmp_path: Path) -> None:
    folder = build_dataset(tmp_path / "in", [order_row()])
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    payment = rows(silver.tables["payments"])[0]
    assert payment["gateway_fee_source"] == "exact"
    assert result.report.estimation.payments_gateway_fee_exact == 1


def test_sold_sku_missing_from_catalog_gets_flagged_stub(tmp_path: Path) -> None:
    folder = build_dataset(
        tmp_path / "in",
        [order_row(**{"SKU": "NUEVO-AZU-M", "Producto": "Remera nueva"})],
        products=[],
    )
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    stub = [p for p in rows(silver.tables["products"]) if p["sku"] == "NUEVO-AZU-M"]
    assert len(stub) == 1
    assert stub[0]["category"] == "otros"
    assert stub[0]["unit_cost_source"] == "estimated"
    assert result.report.estimation.products_catalog_stubs == 1


# --- personal data ---------------------------------------------------------------


def test_personal_data_never_reaches_bronze(tmp_path: Path) -> None:
    folder = build_dataset(tmp_path / "in", [order_row()])
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    blob = json.dumps(result.batch.sources, ensure_ascii=False, default=str)
    assert "cliente@ejemplo.test" not in blob
    assert "+54 9 11 0000 0001" not in blob
    assert "Calle Falsa" not in blob
    assert "Cliente de prueba" not in blob
    assert "00000001" not in blob
    assert "CL-1" not in blob  # pseudonymized
    customer = result.batch.sources["orders"][0]["customer_id"]
    assert customer.startswith("CUS-") and customer != "CL-1"
    stats = {(s.field): s for s in result.report.privacy}
    assert stats["customer_email"].action == "drop"
    assert stats["customer_email"].cells_affected == 1
    assert stats["customer_id"].action == "pseudonymize"


def test_report_lists_unmapped_sensitive_looking_columns(tmp_path: Path) -> None:
    header = [*ORDER_HEADER, "Teléfono alternativo", "Observaciones"]
    folder = build_dataset(tmp_path / "in", [order_row()], header=header)
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    files = {f.source: f for f in result.report.files}
    assert "Observaciones" in files["orders"].ignored_columns
    assert any("Teléfono alternativo" in w for w in result.report.warnings)


def test_samples_contain_no_personal_values_in_outputs(tmp_path: Path) -> None:
    result = ingest_files(SAMPLES, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    gold = build_gold(silver.tables)
    summary = build_client_summary(
        gold=gold,
        silver=silver.tables,
        quarantine=silver.quarantine,
        quality=silver.reports,
        ingestion=result.report,
        generated_at="2026-11-22T00:00:00+00:00",
    )
    text = render_client_markdown(summary) + json.dumps(summary.model_dump(mode="json"))
    forbidden = ["@ejemplo.test", "Cliente de prueba", "Calle Falsa", "+54 9", "0000000"]
    for needle in forbidden:
        assert needle not in text
        for table in [*silver.tables.values(), *gold.values(), *silver.quarantine.values()]:
            for column in table.column_names:
                values = table.column(column).to_pylist()
                assert not any(isinstance(v, str) and needle in v for v in values), needle


# --- end to end and idempotency --------------------------------------------------


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        lakehouse_storage="local",
        lakehouse_local_dir=tmp_path / "lake",
        output_dir=tmp_path / "out",
        pseudonymization_salt="test-salt",
    )


def test_csv_run_is_idempotent_and_keeps_pii_out_of_bronze(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    lake = tmp_path / "lake_csv"
    first = run_csv_ingestion(settings, SAMPLES, MAPPINGS, lake)
    assert sum(first.outcome.report.batches_loaded.values()) > 0
    second = run_csv_ingestion(settings, SAMPLES, MAPPINGS, lake)
    assert sum(second.outcome.report.batches_loaded.values()) == 0

    orders = DeltaTable(str(lake / "bronze" / "orders")).to_pyarrow_table()
    assert "customer_email" not in orders.column_names
    assert "customer_name" not in orders.column_names
    assert "Email" not in orders.column_names
    for column in orders.column_names:
        values = orders.column(column).to_pylist()
        assert not any(isinstance(v, str) and "@ejemplo.test" in v for v in values)


def test_csv_run_without_salt_fails_cleanly(tmp_path: Path) -> None:
    with pytest.raises(MappingError):
        ingest_files(SAMPLES, MAPPINGS, salt=None)


def test_wrong_decimal_setting_is_visible_in_quality_not_a_crash(tmp_path: Path) -> None:
    folder = build_dataset(
        tmp_path / "in",
        [order_row(**{"Precio unitario": "abc"})],
    )
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    assert silver.quarantine["order_items"].num_rows == 1
    reasons = rows(silver.quarantine["order_items"])[0]["_dq_reasons"]
    assert "invalid_unit_price" in reasons
    assert Path(folder / "pedidos.csv").exists()


def test_header_money_is_normalized_once(tmp_path: Path) -> None:
    """Regression: shipping "7.890,00" must become 7890.00, not 789000.00."""
    folder = build_dataset(
        tmp_path / "in",
        [
            order_row(**{"Costo de envío": "7.890,00", "Descuento del pedido": "1.000,50"}),
            order_row(**{"Costo de envío": "7.890,00", "Descuento del pedido": "1.000,50"}),
        ],
    )
    result = ingest_files(folder, MAPPINGS, salt="test-salt")
    silver = run_silver(bronze_from_batch(result.batch), RUN_TS)
    order = rows(silver.tables["orders"])[0]
    assert order["shipping_cost"] == Decimal("7890.00")
    assert order["discount_amount"] == Decimal("1000.50")
    assert result.report.estimation.orders_header_conflicts == 0
