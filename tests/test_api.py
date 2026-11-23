"""HTTP API tests with TestClient: isolation, error contract, onboarding and ingestion."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from src.analyst.bi_agent import MockProvider, PlanResult
from src.analyst.semantic import SemanticLayer
from src.api.main import create_app
from src.config import PROJECT_ROOT, Settings
from src.ingestion.mapping import MappingFile
from src.pipeline import run_csv_ingestion
from tests.test_csv_ingestion import order_row
from tests.test_custom_attributes import CUSTOM_HEADER, MAPPING_TEXT

SAMPLES = PROJECT_ROOT / "data" / "samples_real"
MAPPINGS = [
    PROJECT_ROOT / "mappings" / "generic_store_orders.yaml",
    PROJECT_ROOT / "mappings" / "costs_and_stock.yaml",
]
SALT = "test-salt"


@pytest.fixture
def platform_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "platform"
    monkeypatch.setenv("PLATFORM_LAKEHOUSE_DIR", str(root))
    monkeypatch.setenv("PSEUDONYMIZATION_SALT", SALT)
    monkeypatch.setenv("LLM_PROVIDER", "mock")
    monkeypatch.setenv("LAKEHOUSE_STORAGE", "local")
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    return root


@pytest.fixture
def client(platform_dir: Path) -> TestClient:
    return TestClient(create_app())


def _seed_default(platform_dir: Path) -> None:
    """Load the fictitious store exports into the default workspace of the platform."""
    settings = Settings(
        lakehouse_storage="local",
        lakehouse_local_dir=platform_dir,
        output_dir=platform_dir.parent / "out",
        pseudonymization_salt=SALT,
    )
    run_csv_ingestion(settings, SAMPLES, MAPPINGS, platform_dir)


@pytest.fixture
def seeded(client: TestClient, platform_dir: Path) -> TestClient:
    _seed_default(platform_dir)
    return client


def _error_code(response: Any) -> str:
    return str(response.json()["error"]["code"])


# --- health and selection --------------------------------------------------------------


def test_health_reports_status_and_version(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "0.1.0", "storage": "local"}


def test_invalid_workspace_id_is_400(client: TestClient) -> None:
    response = client.get("/workspaces/ACME_BAD/runs")
    assert response.status_code == 400
    assert _error_code(response) == "invalid_workspace"


def test_workspace_without_data_is_404(client: TestClient) -> None:
    response = client.get("/workspaces/nadie/catalog")
    assert response.status_code == 404
    assert _error_code(response) == "workspace_not_found"


def test_shortcut_routes_read_workspace_from_query_or_header(seeded: TestClient) -> None:
    by_query = seeded.get("/catalog", params={"workspace": "default"})
    assert by_query.status_code == 200
    by_header = seeded.get("/summary", headers={"X-Workspace-Id": "default"})
    assert by_header.status_code == 200
    other = seeded.get("/catalog", headers={"X-Workspace-Id": "acme"})
    assert other.status_code == 404
    assert _error_code(other) == "workspace_not_found"


# --- catalog, runs and summary -------------------------------------------------------


def test_catalog_lists_tables_with_schema_and_custom_flag(seeded: TestClient) -> None:
    body = seeded.get("/workspaces/default/catalog").json()
    tables = {t["name"]: t for t in body["tables"]}
    assert {"silver_products", "silver_order_items", "mart_sku_health"} <= set(tables)
    products = tables["silver_products"]
    assert products["layer"] == "silver"
    assert products["rows"] > 0
    assert {"name": "sku", "type": "VARCHAR"} in products["columns"]
    assert products["has_custom_attributes"] is True
    assert tables["mart_sku_health"]["has_custom_attributes"] is False


def test_runs_list_and_detail(seeded: TestClient) -> None:
    listing = seeded.get("/workspaces/default/runs").json()
    assert len(listing["runs"]) == 1
    run = listing["runs"][0]
    assert run["status"] == "succeeded"
    assert run["pipeline_name"] == "csv_ingestion"
    assert run["metrics"]["rows_in"] == 170
    assert run["metrics"]["quarantine_rows"] == 18

    detail = seeded.get(f"/workspaces/default/runs/{run['run_id']}").json()
    assert [e["status"] for e in detail["history"]] == ["running", "succeeded"]
    assert detail["run"]["run_id"] == run["run_id"]


def test_unknown_run_is_404(seeded: TestClient) -> None:
    response = seeded.get("/workspaces/default/runs/no-existe")
    assert response.status_code == 404
    assert _error_code(response) == "run_not_found"


def test_summary_reports_kpis_and_estimated_quality(seeded: TestClient) -> None:
    body = seeded.get("/workspaces/default/summary").json()
    assert body["kpis"]["net_margin"] == pytest.approx(1114514.13, abs=1)
    assert body["kpis"]["orders"] == 49
    assert body["data_quality"] == "estimado"
    assert 0 < body["cost_exact_share"] < 1
    assert any(q["entity"] == "Pedidos" and q["excluded"] == 2 for q in body["quality"])
    assert body["caveats"]


def test_workspaces_are_isolated(seeded: TestClient) -> None:
    response = seeded.post(
        "/workspaces/acme/ingest/csv",
        data={"mapping": MAPPING_TEXT},
        files=[("files", ("pedidos.csv", _custom_orders_csv(), "text/csv"))],
    )
    assert response.status_code == 201, response.text
    acme_runs = seeded.get("/workspaces/acme/runs").json()["runs"]
    default_runs = seeded.get("/workspaces/default/runs").json()["runs"]
    assert len(acme_runs) == 1
    assert len(default_runs) == 1
    assert acme_runs[0]["run_id"] != default_runs[0]["run_id"]
    acme_rows = _table_rows(seeded, "acme", "silver_products")
    default_rows = _table_rows(seeded, "default", "silver_products")
    assert acme_rows == 1  # one provisional stub: both lines sell REM01-NEG-M
    assert default_rows == 32
    default_summary = seeded.get("/workspaces/default/summary").json()
    assert default_summary["kpis"]["orders"] == 49


def test_workspace_without_gold_is_404(seeded: TestClient, platform_dir: Path) -> None:
    (platform_dir / "workspaces" / "vacio").mkdir(parents=True)
    response = seeded.get("/workspaces/vacio/summary")
    assert response.status_code == 404
    assert _error_code(response) == "no_gold_tables"


# --- analyst -------------------------------------------------------------------------


def test_analyst_answers_with_sql_table_and_exactness(seeded: TestClient) -> None:
    response = seeded.post(
        "/workspaces/default/analyst/query",
        json={"question": "¿Cuál fue el margen neto por canal?"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert body["sql"].startswith("SELECT")
    assert body["tables"] == ["mart_daily_unit_economics"]
    assert body["row_count"] == len(body["rows"]) == 3
    assert body["exactness"] == "estimado"
    assert body["answer"]


def test_analyst_flags_answers_without_economics_as_not_applicable(seeded: TestClient) -> None:
    body = seeded.post(
        "/workspaces/default/analyst/query",
        json={"question": "¿Qué SKUs tienen la mayor tasa de devolución? top 10"},
    ).json()
    assert body["tables"] == ["mart_sku_health"]
    assert body["exactness"] == "no_aplica"


@pytest.mark.parametrize("question", ["", "   "])
def test_empty_question_is_422(seeded: TestClient, question: str) -> None:
    response = seeded.post("/workspaces/default/analyst/query", json={"question": question})
    assert response.status_code == 422


def test_unanswerable_question_is_200_with_status(seeded: TestClient) -> None:
    body = seeded.post(
        "/workspaces/default/analyst/query", json={"question": "¿Cuál es el clima de mañana?"}
    ).json()
    assert body["status"] == "unanswerable"
    assert body["sql"] is None


def test_rejected_sql_is_422(seeded: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    def hostile(self: MockProvider, question: str, layer: SemanticLayer) -> PlanResult:
        return PlanResult(sql="DROP TABLE mart_daily_unit_economics", metrics=())

    monkeypatch.setattr(MockProvider, "plan", hostile)
    response = seeded.post("/workspaces/default/analyst/query", json={"question": "borra todo"})
    assert response.status_code == 422
    assert _error_code(response) == "sql_rejected"


# --- onboarding ----------------------------------------------------------------------


def _table_rows(client: TestClient, workspace: str, table: str) -> int:
    tables = client.get(f"/workspaces/{workspace}/catalog").json()["tables"]
    return int(next(t["rows"] for t in tables if t["name"] == table))


def _custom_orders_csv() -> bytes:
    rows = [
        order_row(**{"Color": "Negro", "Email de contacto": "compras@ejemplo.test"}),
        order_row(**{"Color": "", "Email de contacto": ""}),
    ]
    lines = [";".join(CUSTOM_HEADER)]
    for row in rows:
        lines.append(";".join(row.get(h, "") for h in CUSTOM_HEADER))
    return ("\n".join(lines) + "\n").encode("utf-8-sig")


def test_inspect_csv_suggests_mapping_with_custom_fields(client: TestClient) -> None:
    content = (
        b"Nro Pedido;Fecha;Estado;Canal;SKU;Cantidad;Precio unitario;Medio de pago;"
        b"Color;Email de contacto\n"
        b"PED-1;03/09/2026;Entregado;Web;REM01-NEG-M;2;12.900,50;Transferencia;"
        b"Negro;a@b.test\n"
    )
    response = client.post(
        "/workspaces/default/ingest/inspect-csv",
        files={"file": ("mis_ventas.csv", content, "text/csv")},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["detected_source"] == "orders"
    assert body["delimiter"] == ";"
    assert body["decimal"] == ","
    assert body["canonical_matches"]["order_id"] == "Nro Pedido"
    assert body["suggested_custom_fields"]["color"] == ["Color"]
    assert "email_de_contacto" in body["suggested_custom_fields"]
    assert body["suggested_sensitive"] == {"email_de_contacto": "drop"}
    assert body["mapping_errors"] == []
    assert "custom_fields" in body["mapping_yaml"]
    assert len(body["sample_rows"]) == 1
    # The suggested document must be a valid mapping for the same ingestion contract.
    MappingFile.model_validate(yaml.safe_load(body["mapping_yaml"]))


def test_inspect_empty_file_is_422(client: TestClient) -> None:
    response = client.post(
        "/workspaces/default/ingest/inspect-csv",
        files={"file": ("vacio.csv", b"", "text/csv")},
    )
    assert response.status_code == 422
    assert _error_code(response) == "file_invalid"


# --- ingestion -----------------------------------------------------------------------


def test_ingest_with_custom_mapping_registers_run(client: TestClient, platform_dir: Path) -> None:
    response = client.post(
        "/workspaces/acme/ingest/csv",
        data={"mapping": MAPPING_TEXT},
        files=[("files", ("pedidos.csv", _custom_orders_csv(), "text/csv"))],
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["workspace_id"] == "acme"
    assert body["rows_by_entity"]["order_items"] == 2
    assert {p["field"] for p in body["privacy"]} >= {"contacto_email", "customer_id"}
    runs = client.get("/workspaces/acme/runs").json()["runs"]
    assert runs[0]["run_id"] == body["run_id"]
    assert runs[0]["status"] == "succeeded"
    assert (platform_dir / "workspaces" / "acme").exists()
    stored = client.get("/workspaces/acme/catalog").json()["tables"]
    items = next(t for t in stored if t["name"] == "silver_order_items")
    assert items["has_custom_attributes"] is True


def test_ingest_rejects_missing_file_for_mapping(client: TestClient) -> None:
    response = client.post(
        "/workspaces/acme/ingest/csv",
        data={"mapping": MAPPING_TEXT},
        files=[("files", ("otro.csv", _custom_orders_csv(), "text/csv"))],
    )
    assert response.status_code == 422
    assert _error_code(response) == "mapping_invalid"


def test_ingest_rejects_invalid_mapping(client: TestClient) -> None:
    response = client.post(
        "/workspaces/acme/ingest/csv",
        data={"mapping": "name: x\nsources: {invoices: {file: f.csv, columns: {}}}\n"},
        files=[("files", ("f.csv", b"a;b\n1;2\n", "text/csv"))],
    )
    assert response.status_code == 422
    assert _error_code(response) == "mapping_invalid"


def test_failed_ingestion_is_registered_as_failed_run(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PSEUDONYMIZATION_SALT")
    response = client.post(
        "/workspaces/acme/ingest/csv",
        data={"mapping": MAPPING_TEXT},
        files=[("files", ("pedidos.csv", _custom_orders_csv(), "text/csv"))],
    )
    assert response.status_code == 422
    assert _error_code(response) == "mapping_invalid"
    runs = client.get("/workspaces/acme/runs").json()["runs"]
    assert runs[0]["status"] == "failed"
