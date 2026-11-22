"""Client summary tests: factual findings, plain Spanish, honest caveats."""

from __future__ import annotations

from pathlib import Path

from src.config import PROJECT_ROOT
from src.ingestion.csv_adapter import ingest_files
from src.medallion.gold import build_gold
from src.medallion.silver import run_silver
from src.reporting.client_summary import build_client_summary, render_client_markdown
from tests.helpers import RUN_TS
from tests.test_csv_ingestion import MAPPINGS, bronze_from_batch, build_dataset, order_row

SAMPLES = PROJECT_ROOT / "data" / "samples_real"
JARGON = ("Bronze", "Silver", "Gold", "lakehouse", "cuarentena", "Delta", "dbt", "mart_")


def _summary(input_dir: Path, salt: str = "test-salt"):  # type: ignore[no-untyped-def]
    result = ingest_files(input_dir, MAPPINGS, salt=salt)
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
    return summary


def test_sample_summary_is_plain_spanish_without_internal_jargon() -> None:
    summary = _summary(SAMPLES)
    text = render_client_markdown(summary)
    for word in JARGON:
        assert word not in text, word
    assert summary.totals is not None
    assert summary.totals.net_margin.startswith("$ ")
    assert any("margen neto" in f for f in summary.findings)


def test_sample_summary_reports_estimates_and_exclusions() -> None:
    summary = _summary(SAMPLES)
    caveats = " ".join(summary.caveats)
    assert "costo unitario estimado" in caveats
    assert "comisión estimada" in caveats
    assert summary.excluded_sales_lines > 0
    assert "líneas de venta" in caveats
    assert summary.sold_orders_without_valid_payment >= 1
    quality_names = [q.entity for q in summary.quality]
    assert "Eventos de checkout" not in quality_names  # absent source is not shown as zero


def test_summary_without_valid_sales_makes_no_margin_claims(tmp_path: Path) -> None:
    folder = build_dataset(
        tmp_path / "in",
        [order_row(**{"Estado del pedido": "Reclamo en curso"})],
    )
    summary = _summary(folder)
    text = render_client_markdown(summary)
    assert summary.totals is None
    assert "No hay ventas válidas para analizar." in text
    assert "Margen neto" not in text


def test_summary_is_json_serializable() -> None:
    summary = _summary(SAMPLES)
    payload = summary.model_dump_json()
    assert '"batch_id"' in payload
    assert "cliente@ejemplo.test" not in payload
