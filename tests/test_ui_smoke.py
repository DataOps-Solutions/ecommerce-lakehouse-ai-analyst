"""Smoke tests for the Streamlit demo: data services and the app script, without a browser."""

from __future__ import annotations

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from app.services import (
    SAMPLE_DIR,
    Dataset,
    UploadError,
    ask,
    category_frame,
    channel_frame,
    daily_frame,
    kpis,
    load_sample,
    load_synthetic,
    load_uploaded,
    quality_frame,
    quarantine_frame,
    top_return_skus,
)
from src.config import PROJECT_ROOT, Settings

DASHBOARD = PROJECT_ROOT / "app" / "dashboard.py"


def _settings(tmp_path: Path, salt: str | None = "test-salt") -> Settings:
    return Settings(
        lakehouse_storage="local",
        lakehouse_local_dir=tmp_path / "lakehouse",
        output_dir=tmp_path / "out",
        pseudonymization_salt=salt,
    )


def _assert_views(ds: Dataset) -> None:
    k = kpis(ds)
    assert k.gross_sales > 0
    assert k.net_revenue > 0
    assert k.units > 0
    channels = channel_frame(ds)
    assert not channels.empty
    assert {"Canal", "Ingreso neto", "Margen neto"} <= set(channels.columns)
    assert not daily_frame(ds).empty
    assert not category_frame(ds).empty
    assert not quality_frame(ds).empty
    assert str(ds.summary.totals and ds.summary.totals.period_start)


def test_sample_dataset_feeds_every_view(tmp_path: Path) -> None:
    ds = load_sample(_settings(tmp_path))
    _assert_views(ds)
    assert not top_return_skus(ds).empty
    quarantine = quarantine_frame(ds)
    assert not quarantine.empty
    assert {"Información", "Motivo", "Código técnico"} <= set(quarantine.columns)
    assert ds.summary.excluded_sales_lines > 0


def test_synthetic_dataset_feeds_every_view(tmp_path: Path) -> None:
    ds = load_synthetic(_settings(tmp_path))
    _assert_views(ds)
    assert ds.summary.cost_exact_share == pytest.approx(1.0)


def test_analyst_answers_from_gold_with_guardrails(tmp_path: Path) -> None:
    ds = load_sample(_settings(tmp_path))
    response = ask(_settings(tmp_path), ds, "¿Cuál fue el margen neto por canal?")
    assert response.status == "ok"
    assert response.tables == ["mart_daily_unit_economics"]
    assert response.sql is not None and response.sql.startswith("SELECT")


def test_uploaded_files_are_processed_like_the_sample(tmp_path: Path) -> None:
    files = {
        "orders": (SAMPLE_DIR / "pedidos.csv").read_bytes(),
        "products": (SAMPLE_DIR / "costos_stock.csv").read_bytes(),
        "returns": (SAMPLE_DIR / "devoluciones.csv").read_bytes(),
    }
    ds = load_uploaded(_settings(tmp_path), files)
    assert ds.totals is not None
    assert kpis(ds).net_margin == pytest.approx(kpis(load_sample(_settings(tmp_path))).net_margin)


def test_upload_requires_orders_file(tmp_path: Path) -> None:
    with pytest.raises(UploadError, match="pedidos"):
        load_uploaded(_settings(tmp_path), {"products": b"SKU\n"})


def test_upload_requires_configured_salt(tmp_path: Path) -> None:
    files = {"orders": (SAMPLE_DIR / "pedidos.csv").read_bytes()}
    with pytest.raises(UploadError, match="PSEUDONYMIZATION_SALT"):
        load_uploaded(_settings(tmp_path, salt=None), files)


def test_dashboard_script_runs_without_exceptions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LAKEHOUSE_LOCAL_DIR", str(tmp_path / "lakehouse"))
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    app = AppTest.from_file(str(DASHBOARD), default_timeout=240)
    app.run()
    assert not app.exception
    assert len(app.tabs) == 3
    assert len(app.metric) >= 4
