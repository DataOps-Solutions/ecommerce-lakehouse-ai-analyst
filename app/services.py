"""Data services for the Streamlit demo.

Every business rule (margins, exclusions, estimates, guardrails) lives in ``src/``. This module
only chooses the data source, runs the existing pipelines, reshapes Gold/Silver tables for
display and formats numbers in Spanish. It has no Streamlit imports, so it can be tested
without a browser.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from src.analyst.bi_agent import BiAnalyst, provider_for
from src.analyst.semantic import load_semantic_layer
from src.config import PROJECT_ROOT, Settings
from src.engine.catalog import LakehouseCatalog
from src.engine.runs import RunTracker
from src.ingestion.csv_adapter import EstimationStats, IngestionReport
from src.ingestion.mapping import MappingError, load_mapping
from src.models import AnalystResponse, EntityQualityReport
from src.pipeline import run_csv_ingestion, run_pipeline
from src.reporting.client_summary import (
    ENTITY_ES,
    REASON_ES,
    STOCK_ES,
    ClientSummary,
    EconTotals,
    build_client_summary,
    econ_totals,
)

SOURCE_SAMPLE = "Demo Comercio Real"
SOURCE_SYNTHETIC = "Demo E-commerce Multicanal"
SOURCE_UPLOAD = "Mis archivos (cargados en esta sesión)"
SAMPLE_DIR = PROJECT_ROOT / "data" / "samples_real"
MAPPING_PATHS: tuple[Path, ...] = (
    PROJECT_ROOT / "mappings" / "generic_store_orders.yaml",
    PROJECT_ROOT / "mappings" / "costs_and_stock.yaml",
)
# Only for the bundled fictitious sample. Uploaded real files require a configured salt.
DEMO_SALT = "sample-data-only-not-secret"
SUGGESTED_QUESTIONS: tuple[str, ...] = (
    "¿Cuál fue el margen neto por canal?",
    "¿Qué SKUs tienen la mayor tasa de devolución? top 10",
    "¿Cuántas alertas de stock hay por categoría?",
    "¿Cuál fue la venta bruta y los descuentos por día?",
)
UPLOAD_SOURCES: tuple[str, ...] = ("orders", "products", "returns")
UPLOAD_LABELS: dict[str, str] = {
    "orders": "Pedidos (obligatorio)",
    "products": "Costos y stock",
    "returns": "Devoluciones",
}


class UploadError(ValueError):
    """Raised when uploaded files cannot be processed; the message is shown to the user."""


@dataclass(frozen=True)
class Dataset:
    """A loaded data source: Gold marts plus the quality and privacy context of its load."""

    name: str
    gold: dict[str, pa.Table]
    silver: dict[str, pa.Table]
    quarantine: dict[str, pa.Table]
    quality: list[EntityQualityReport]
    ingestion: IngestionReport
    summary: ClientSummary
    totals: EconTotals | None


@dataclass(frozen=True)
class Kpis:
    gross_sales: float
    net_revenue: float
    net_margin: float
    margin_pct: float | None
    units: int
    orders: int


def fmt_money(value: float | None) -> str:
    if value is None:
        return "s/d"
    text = f"{value:,.0f}".replace(",", ".")
    return f"$ {text}"


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    if value is None:
        return "s/d"
    text = f"{value * 100:,.{decimals}f}"
    return text.replace(",", "§").replace(".", ",").replace("§", ".") + " %"


def fmt_int(value: float | int | None) -> str:
    if value is None:
        return "s/d"
    return f"{int(value):,}".replace(",", ".")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _frame(table: pa.Table) -> pd.DataFrame:
    """Arrow table to pandas, with decimal columns converted to floats for plotting."""
    df = table.to_pandas()
    for column in df.columns:
        if df[column].dtype != object:
            continue
        converted = pd.to_numeric(df[column], errors="coerce")
        if df[column].notna().any() and converted.notna().sum() == df[column].notna().sum():
            df[column] = converted.astype(float)
    return df


def _dataset(name: str, outcome: Any, ingestion: IngestionReport) -> Dataset:
    summary = build_client_summary(
        gold=outcome.gold,
        silver=outcome.silver,
        quarantine=outcome.quarantine,
        quality=outcome.report.entities,
        ingestion=ingestion,
        generated_at=_now(),
    )
    return Dataset(
        name=name,
        gold=outcome.gold,
        silver=outcome.silver,
        quarantine=outcome.quarantine,
        quality=outcome.report.entities,
        ingestion=ingestion,
        summary=summary,
        totals=econ_totals(outcome.gold),
    )


def _lakehouse_root(settings: Settings) -> Path:
    return settings.resolve_path(settings.lakehouse_local_dir).parent


def _with_demo_salt(settings: Settings) -> Settings:
    if settings.pseudonymization_salt:
        return settings
    return settings.model_copy(update={"pseudonymization_salt": DEMO_SALT})


def load_sample(settings: Settings) -> Dataset:
    """Process the bundled fictitious store exports through the CSV pipeline."""
    result = run_csv_ingestion(
        _with_demo_salt(settings),
        SAMPLE_DIR,
        list(MAPPING_PATHS),
        _lakehouse_root(settings) / "lakehouse_csv",
    )
    return _dataset(SOURCE_SAMPLE, result.outcome, result.ingestion)


def load_synthetic(settings: Settings) -> Dataset:
    """Run the seeded synthetic pipeline (multichannel e-commerce with checkout funnel)."""
    outcome = run_pipeline(settings)
    report = IngestionReport(
        batch_id="sintetico",
        mappings=["dataset sintético (semilla fija)"],
        files=[],
        rows_by_entity={},
        estimation=EstimationStats(),
        privacy=[],
        warnings=[],
    )
    return _dataset(SOURCE_SYNTHETIC, outcome, report)


def load_uploaded(settings: Settings, files: dict[str, bytes]) -> Dataset:
    """Process uploaded exports. File names are taken from the mappings, not from the upload.

    Raises :class:`UploadError` with a message suitable for the user.
    """
    if "orders" not in files:
        raise UploadError("Falta el archivo de pedidos: es obligatorio para el análisis.")
    if not settings.pseudonymization_salt:
        raise UploadError(
            "Para procesar datos reales hay que configurar PSEUDONYMIZATION_SALT en el archivo "
            ".env. Sin ese secreto no se pueden seudonimizar los identificadores de cliente."
        )
    expected: dict[str, str] = {}
    for path in MAPPING_PATHS:
        for source, spec in load_mapping(path).sources.items():
            expected[source] = spec.file
    work_dir = Path(tempfile.mkdtemp(prefix="upload_"))
    input_dir = work_dir / "input"
    input_dir.mkdir()
    for source, content in files.items():
        if source not in expected:
            continue
        (input_dir / expected[source]).write_bytes(content)
    try:
        result = run_csv_ingestion(settings, input_dir, list(MAPPING_PATHS), work_dir / "lakehouse")
    except MappingError as exc:
        raise UploadError(str(exc)) from exc
    return _dataset(SOURCE_UPLOAD, result.outcome, result.ingestion)


def kpis(ds: Dataset) -> Kpis:
    totals = ds.totals
    if totals is None:
        return Kpis(0.0, 0.0, 0.0, None, 0, 0)
    sums = {k: float(v) for k, v in totals.sums.items()}
    net = sums["net_revenue"]
    margin = sums["net_margin_amount"]
    return Kpis(
        gross_sales=sums["gross_sales"],
        net_revenue=net,
        net_margin=margin,
        margin_pct=margin / net if net else None,
        units=totals.units,
        orders=totals.orders,
    )


def channel_frame(ds: Dataset) -> pd.DataFrame:
    econ = _frame(ds.gold["mart_daily_unit_economics"])
    econ = econ[econ["orders_count"] > 0]
    grouped = econ.groupby("channel", as_index=False)[
        ["net_revenue", "net_margin_amount", "orders_count", "units_sold"]
    ].sum()
    grouped["margin_pct"] = grouped["net_margin_amount"] / grouped["net_revenue"].where(
        grouped["net_revenue"] != 0
    )
    return grouped.rename(
        columns={
            "channel": "Canal",
            "net_revenue": "Ingreso neto",
            "net_margin_amount": "Margen neto",
            "orders_count": "Pedidos",
            "units_sold": "Unidades",
            "margin_pct": "Margen %",
        }
    )


def daily_frame(ds: Dataset) -> pd.DataFrame:
    econ = _frame(ds.gold["mart_daily_unit_economics"])
    econ = econ[econ["orders_count"] > 0]
    daily = econ.groupby("order_date", as_index=False)[
        ["gross_sales", "net_revenue", "net_margin_amount"]
    ].sum()
    daily["order_date"] = pd.to_datetime(daily["order_date"])
    return daily.sort_values("order_date").rename(
        columns={
            "order_date": "Fecha",
            "gross_sales": "Ventas brutas",
            "net_revenue": "Ingreso neto",
            "net_margin_amount": "Margen neto",
        }
    )


def sku_frame(ds: Dataset) -> pd.DataFrame:
    return _frame(ds.gold["mart_sku_health"])


def top_return_skus(ds: Dataset, limit: int = 10, min_units: int = 5) -> pd.DataFrame:
    sku = sku_frame(ds)
    sample = sku[sku["units_sold"] >= min_units].copy()
    sample = sample.sort_values(["return_rate", "sku"], ascending=[False, True]).head(limit)
    return sample[["sku", "product_name", "units_sold", "units_returned", "return_rate"]].rename(
        columns={
            "sku": "SKU",
            "product_name": "Producto",
            "units_sold": "Vendidas",
            "units_returned": "Devueltas",
            "return_rate": "Tasa de devolución",
        }
    )


def category_frame(ds: Dataset) -> pd.DataFrame:
    sku = sku_frame(ds)
    sku["en_alerta"] = sku["stock_alert"].isin(["sin_stock", "reponer"]).astype(int)
    grouped = sku.groupby("category", as_index=False)[
        ["units_sold", "units_returned", "en_alerta"]
    ].sum()
    grouped["return_rate"] = grouped["units_returned"] / grouped["units_sold"].where(
        grouped["units_sold"] != 0
    )
    return grouped.rename(
        columns={
            "category": "Categoría",
            "units_sold": "Vendidas",
            "units_returned": "Devueltas",
            "en_alerta": "SKU en alerta de stock",
            "return_rate": "Tasa de devolución",
        }
    )


def stock_alert_frame(ds: Dataset) -> pd.DataFrame:
    sku = sku_frame(ds)
    counts = sku["stock_alert"].value_counts().reset_index()
    counts.columns = ["estado", "SKU"]
    counts["Estado"] = counts["estado"].map(lambda code: STOCK_ES.get(str(code), str(code)))
    return counts[["Estado", "SKU"]]


def quality_frame(ds: Dataset) -> pd.DataFrame:
    rows = [
        {
            "Información": ENTITY_ES.get(item.entity, item.entity),
            "Filas leídas": item.bronze_rows,
            "Filas usadas": item.silver_rows,
            "Filas excluidas": item.quarantine_rows,
        }
        for item in ds.quality
        if item.bronze_rows > 0
    ]
    return pd.DataFrame(rows)


def privacy_frame(ds: Dataset) -> pd.DataFrame:
    action_es = {"drop": "Eliminado antes de guardar", "pseudonymize": "Seudonimizado"}
    rows = [
        {
            "Campo": item.field,
            "Tratamiento": action_es[item.action],
            "Celdas afectadas": item.cells_affected,
        }
        for item in ds.ingestion.privacy
    ]
    return pd.DataFrame(rows)


def quarantine_frame(ds: Dataset) -> pd.DataFrame:
    """Every row sent to quarantine, with the reason in Spanish and the technical code."""
    frames: list[pd.DataFrame] = []
    for entity, table in ds.quarantine.items():
        if table.num_rows == 0:
            continue
        df = table.to_pandas()
        codes = df["_dq_reasons"].astype(str)
        df.insert(0, "Información", ENTITY_ES.get(entity, entity))
        df.insert(
            1,
            "Motivo",
            codes.map(
                lambda text: "; ".join(
                    REASON_ES.get(code, code) for code in text.split("; ") if code
                )
            ),
        )
        df = df.rename(columns={"_dq_reasons": "Código técnico"})
        technical = [c for c in df.columns if c.startswith("_") and c != "_dq_reasons"]
        frames.append(df.drop(columns=technical))
    if not frames:
        return pd.DataFrame(columns=["Información", "Motivo", "Código técnico"])
    return pd.concat(frames, ignore_index=True)


def ask(settings: Settings, ds: Dataset, question: str) -> AnalystResponse:
    """Answer a business question through the guarded analyst over the dataset's Gold marts."""
    layer = load_semantic_layer(settings.resolve_path(settings.semantic_layer_path))
    provider = provider_for(settings)
    return BiAnalyst(settings, provider, layer, ds.gold).answer(question)


def summary_caveats(ds: Dataset) -> list[str]:
    return list(ds.summary.caveats)


def catalog_overview(settings: Settings, workspace_id: str | None = None) -> pd.DataFrame:
    """Tables of a workspace as DuckDB sees them: layer, name, rows and column count."""
    catalog = LakehouseCatalog(settings.workspace(workspace_id), settings.storage_options())
    rows = [
        {
            "Capa": info.layer,
            "Tabla": info.name,
            "Filas": info.rows,
            "Columnas": len(info.columns),
            "Atributos personalizados": "sí" if info.has_custom_attributes else "no",
        }
        for info in catalog.inspect()
    ]
    return pd.DataFrame(rows)


def recent_runs_frame(settings: Settings, workspace_id: str | None = None) -> pd.DataFrame:
    """Latest registered runs of a workspace, in Spanish, for the operations view."""
    tracker = RunTracker(settings.workspace(workspace_id), settings.storage_options())
    rows = [
        {
            "Run": run.run_id,
            "Proceso": run.pipeline_name,
            "Estado": run.status,
            "Inicio": run.started_at.strftime("%Y-%m-%d %H:%M"),
            "Filas de entrada": run.metrics.rows_in,
            "Filas en cuarentena": run.metrics.quarantine_rows,
            "Costo estimado %": run.metrics.estimated_cost_pct,
            "Error": run.error_message or "",
        }
        for run in tracker.list_runs(limit=20)
    ]
    return pd.DataFrame(rows)
