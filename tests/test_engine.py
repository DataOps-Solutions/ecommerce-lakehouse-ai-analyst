"""Engine tests: workspace paths, run registry, DuckDB catalog and workspace isolation."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.config import PROJECT_ROOT, Settings
from src.engine.catalog import GOLD_MARTS, LakehouseCatalog, MissingGoldError
from src.engine.runs import RunMetrics, RunTracker
from src.engine.workspace import DEFAULT_WORKSPACE, Workspace
from src.ingestion.mapping import MappingError
from src.pipeline import load_gold_tables, run_csv_ingestion
from tests.test_csv_ingestion import MAPPINGS, build_dataset, order_row

SAMPLES = PROJECT_ROOT / "data" / "samples_real"


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        lakehouse_storage="local",
        lakehouse_local_dir=tmp_path / "lake",
        output_dir=tmp_path / "out",
        pseudonymization_salt="test-salt",
    )


def _ws(tmp_path: Path, workspace_id: str = DEFAULT_WORKSPACE) -> Workspace:
    return _settings(tmp_path).workspace(workspace_id)


# --- workspace ----------------------------------------------------------------------


def test_default_workspace_keeps_the_historical_layout(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    expected = str(tmp_path / "lake" / "bronze" / "orders")
    assert settings.table_uri("bronze", "orders") == expected
    assert settings.workspace().table_uri("bronze", "orders") == expected
    assert settings.workspace().reports_dir(tmp_path / "out") == tmp_path / "out"


def test_named_workspace_is_isolated_under_workspaces(tmp_path: Path) -> None:
    ws = _ws(tmp_path, "acme")
    assert ws.table_uri("gold", "mart_sku_health") == str(
        tmp_path / "lake" / "workspaces" / "acme" / "gold" / "mart_sku_health"
    )
    assert ws.reports_dir(tmp_path / "out") == tmp_path / "out" / "workspaces" / "acme"


def test_s3_workspace_uris_are_prefixed(tmp_path: Path) -> None:
    settings = Settings(
        lakehouse_storage="s3", s3_bucket="bkt", s3_prefix="eco", output_dir=tmp_path
    )
    assert settings.table_uri("silver", "orders") == "s3://bkt/eco/silver/orders"
    assert (
        settings.workspace("acme").table_uri("silver", "orders")
        == "s3://bkt/eco/workspaces/acme/silver/orders"
    )


@pytest.mark.parametrize("bad", ["ACME", "a b", "../x", "", "x" * 49])
def test_invalid_workspace_ids_are_rejected(bad: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="workspace_id"):
        _ws(tmp_path, bad)


# --- run registry -------------------------------------------------------------------


def test_successful_run_is_registered_with_metrics(tmp_path: Path) -> None:
    tracker = RunTracker(_ws(tmp_path), None)
    with tracker.track("unit_test") as handle:
        handle.metrics = RunMetrics(
            rows_in=10, rows_out=8, quarantine_rows=2, estimated_cost_pct=12.5
        )
    run = tracker.get_run(handle.run_id)
    assert run is not None
    assert run.status == "succeeded"
    assert run.pipeline_name == "unit_test"
    assert run.metrics.rows_in == 10
    assert run.metrics.quarantine_rows == 2
    assert run.metrics.estimated_cost_pct == 12.5
    assert run.error_message is None
    history = [e.status for e in tracker.history(handle.run_id)]
    assert history == ["running", "succeeded"]


def test_failed_run_keeps_the_error_and_reraises(tmp_path: Path) -> None:
    tracker = RunTracker(_ws(tmp_path), None)
    with pytest.raises(RuntimeError, match="boom"), tracker.track("broken") as handle:
        raise RuntimeError("boom")
    run = tracker.get_run(handle.run_id)
    assert run is not None
    assert run.status == "failed"
    assert run.error_message is not None and "boom" in run.error_message
    assert run.finished_at is not None


def test_list_runs_returns_latest_state_newest_first(tmp_path: Path) -> None:
    tracker = RunTracker(_ws(tmp_path), None)
    with tracker.track("first") as first:
        first.metrics = RunMetrics(rows_in=1)
    with pytest.raises(ValueError), tracker.track("second"):
        raise ValueError("bad input")
    runs = tracker.list_runs()
    assert [r.pipeline_name for r in runs] == ["second", "first"]
    assert [r.status for r in runs] == ["failed", "succeeded"]
    assert tracker.list_runs(limit=1)[0].pipeline_name == "second"


def test_runs_are_scoped_to_their_workspace(tmp_path: Path) -> None:
    with RunTracker(_ws(tmp_path, "acme"), None).track("only_acme") as handle:
        handle.metrics = RunMetrics()
    assert RunTracker(_ws(tmp_path), None).list_runs() == []
    assert len(RunTracker(_ws(tmp_path, "acme"), None).list_runs()) == 1


def test_csv_run_records_counts_and_estimates(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    lake = tmp_path / "csv_lake"
    run_csv_ingestion(settings, SAMPLES, MAPPINGS, lake, workspace_id="acme")
    tracker = RunTracker(Workspace("acme", "local", lake, "", ""), None)
    runs = tracker.list_runs()
    assert len(runs) == 1
    run = runs[0]
    assert run.status == "succeeded"
    assert run.pipeline_name == "csv_ingestion"
    # Raw rows read from the three sample files: 129 order lines + 31 products + 10 returns.
    assert run.metrics.rows_in == 170
    # products 2 + orders 2 + order_items 6 + payments 5 + returns 3 (same as the committed code).
    assert run.metrics.quarantine_rows == 18
    assert run.metrics.estimated_cost_pct is not None
    assert 0 < run.metrics.estimated_cost_pct < 100
    assert run.metrics.estimated_fee_pct is not None


def test_mapping_error_is_recorded_as_failed_run(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    empty = tmp_path / "empty_input"
    empty.mkdir()
    lake = tmp_path / "csv_lake"
    with pytest.raises(MappingError, match="input file not found"):
        run_csv_ingestion(settings, empty, MAPPINGS, lake)
    runs = RunTracker(Workspace("default", "local", lake, "", ""), None).list_runs()
    assert runs[0].status == "failed"
    assert runs[0].error_message


# --- DuckDB catalog ------------------------------------------------------------------


def test_catalog_inspects_tables_and_reads_gold(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    lake = tmp_path / "csv_lake"
    run_csv_ingestion(settings, SAMPLES, MAPPINGS, lake)
    catalog = LakehouseCatalog(Workspace("default", "local", lake, "", ""), None)
    infos = {t.name: t for t in catalog.inspect()}
    assert {"bronze_orders", "silver_products", "silver_order_items", "mart_sku_health"} <= set(
        infos
    )
    products = infos["silver_products"]
    assert products.rows > 0
    assert products.has_custom_attributes
    assert any(c.name == "sku" for c in products.columns)
    assert all(c.duckdb_type for c in products.columns)
    gold = catalog.read_gold()
    assert set(gold) == set(GOLD_MARTS)


def test_catalog_on_empty_workspace_reports_missing_gold(tmp_path: Path) -> None:
    catalog = LakehouseCatalog(_ws(tmp_path), None)
    assert catalog.inspect() == []
    with pytest.raises(MissingGoldError):
        catalog.read_gold()
    with pytest.raises(RuntimeError, match="make pipeline"):
        load_gold_tables(_settings(tmp_path))


def test_catalog_connection_has_no_external_access(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    run_csv_ingestion(settings, SAMPLES, MAPPINGS, tmp_path / "csv_lake")
    catalog = LakehouseCatalog(Workspace("default", "local", tmp_path / "csv_lake", "", ""), None)
    with catalog.connect() as con, pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_csv('/etc/hostname')").fetchall()


def test_csv_ingestion_in_custom_workspace_stays_separate(tmp_path: Path) -> None:
    folder = build_dataset(tmp_path / "in", [order_row()])
    settings = _settings(tmp_path)
    run_csv_ingestion(settings, folder, MAPPINGS, tmp_path / "csv_lake", workspace_id="acme")
    assert (tmp_path / "csv_lake" / "workspaces" / "acme" / "gold").exists()
    assert not (tmp_path / "csv_lake" / "gold").exists()
