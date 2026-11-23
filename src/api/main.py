"""FastAPI application: platform endpoints over runs, catalog, summary, analyst and ingestion.

Run locally with::

    uvicorn src.api.main:app --reload

Workspace selection: path routes take ``{workspace_id}``. The shortcut routes without a path
(``/catalog``, ``/runs``, ``/summary``, ``/analyst/query``) read the workspace from the
``workspace`` query parameter or the ``X-Workspace-Id`` header, defaulting to ``default``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Annotated, Any

import yaml
from fastapi import Depends, FastAPI, File, Form, Header, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from app.services import (
    WorkspaceSnapshot,
    answer_exactness,
    ask_gold,
    load_workspace_snapshot,
    workspace_summary,
)
from src.api.errors import (
    EmptyQuestionError,
    FileInvalidError,
    InvalidWorkspaceError,
    MappingInvalidError,
    NoGoldError,
    RunNotFoundError,
    SqlRejectedError,
    WorkspaceNotFoundError,
    register_error_handlers,
)
from src.api.schemas import (
    AnalystQueryIn,
    AnalystQueryOut,
    CatalogOut,
    ColumnOut,
    EntityQualityOut,
    HealthOut,
    IngestOut,
    InspectOut,
    KpisOut,
    PrivacyOut,
    RunDetailOut,
    RunEventOut,
    RunListOut,
    RunMetricsOut,
    RunOut,
    SummaryOut,
    TableOut,
)
from src.config import Settings, get_settings
from src.engine.catalog import LakehouseCatalog, MissingGoldError
from src.engine.runs import RunRecord, RunTracker
from src.engine.workspace import Workspace
from src.ingestion.mapping import MappingError, MappingFile
from src.ingestion.onboarding import inspect_csv
from src.pipeline import csv_settings, run_csv_ingestion

API_VERSION = "0.1.0"
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_ANALYST_ROWS = 200
DEV_ORIGINS: list[str] = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8501",
    "http://127.0.0.1:8501",
]


def get_app_settings() -> Settings:
    return get_settings()


SettingsDep = Annotated[Settings, Depends(get_app_settings)]


def workspace_from_request(
    workspace: Annotated[str | None, Query()] = None,
    x_workspace_id: Annotated[str | None, Header(alias="X-Workspace-Id")] = None,
) -> str:
    """Workspace named by the query parameter, then the header, then ``default``."""
    return workspace or x_workspace_id or "default"


WorkspaceFromRequest = Annotated[str, Depends(workspace_from_request)]


def platform_settings(settings: Settings) -> Settings:
    """Settings that point at the platform lakehouse (the one CSV ingestion writes to)."""
    return csv_settings(settings, settings.resolve_path(settings.platform_lakehouse_dir))


def _workspace(settings: Settings, workspace_id: str) -> Workspace:
    try:
        return settings.workspace(workspace_id)
    except ValueError as exc:
        raise InvalidWorkspaceError(str(exc)) from exc


def _existing_workspace(settings: Settings, workspace_id: str) -> Workspace:
    workspace = _workspace(settings, workspace_id)
    if settings.lakehouse_storage == "local" and not workspace.local_dir().exists():
        raise WorkspaceNotFoundError(f"el workspace '{workspace_id}' no tiene datos")
    return workspace


def _run_out(run: RunRecord) -> RunOut:
    m = run.metrics
    return RunOut(
        run_id=run.run_id,
        workspace_id=run.workspace_id,
        pipeline_name=run.pipeline_name,
        status=run.status,
        started_at=run.started_at,
        finished_at=run.finished_at,
        metrics=RunMetricsOut(
            rows_in=m.rows_in,
            rows_out=m.rows_out,
            quarantine_rows=m.quarantine_rows,
            estimated_cost_pct=m.estimated_cost_pct,
            estimated_fee_pct=m.estimated_fee_pct,
        ),
        error_message=run.error_message,
    )


def _runs(settings: Settings, workspace_id: str, limit: int) -> RunListOut:
    ps = platform_settings(settings)
    workspace = _existing_workspace(ps, workspace_id)
    tracker = RunTracker(workspace, ps.storage_options())
    return RunListOut(
        workspace_id=workspace.workspace_id,
        runs=[_run_out(r) for r in tracker.list_runs(limit=limit)],
    )


def _run_detail(settings: Settings, workspace_id: str, run_id: str) -> RunDetailOut:
    ps = platform_settings(settings)
    workspace = _existing_workspace(ps, workspace_id)
    history = RunTracker(workspace, ps.storage_options()).history(run_id)
    if not history:
        raise RunNotFoundError(f"no existe el run '{run_id}' en el workspace '{workspace_id}'")
    return RunDetailOut(
        run=_run_out(history[-1]),
        history=[
            RunEventOut(status=e.status, event_at=e.event_at, error_message=e.error_message)
            for e in history
        ],
    )


def _catalog(settings: Settings, workspace_id: str) -> CatalogOut:
    ps = platform_settings(settings)
    workspace = _existing_workspace(ps, workspace_id)
    infos = LakehouseCatalog(workspace, ps.storage_options()).inspect()
    return CatalogOut(
        workspace_id=workspace.workspace_id,
        tables=[
            TableOut(
                name=t.name,
                layer=t.layer,
                rows=t.rows,
                columns=[ColumnOut(name=c.name, type=c.duckdb_type) for c in t.columns],
                has_custom_attributes=t.has_custom_attributes,
            )
            for t in infos
        ],
    )


def _snapshot(settings: Settings, workspace_id: str) -> tuple[Settings, WorkspaceSnapshot]:
    ps = platform_settings(settings)
    _existing_workspace(ps, workspace_id)
    try:
        return ps, load_workspace_snapshot(ps, workspace_id)
    except MissingGoldError as exc:
        raise NoGoldError(str(exc)) from exc


def _summary(settings: Settings, workspace_id: str) -> SummaryOut:
    _ps, snapshot = _snapshot(settings, workspace_id)
    view = workspace_summary(snapshot)
    k = view.kpis
    return SummaryOut(
        workspace_id=snapshot.workspace_id,
        kpis=KpisOut(
            gross_sales=k.gross_sales,
            net_revenue=k.net_revenue,
            net_margin=k.net_margin,
            margin_pct=k.margin_pct,
            units=k.units,
            orders=k.orders,
        ),
        period_start=view.period_start,
        period_end=view.period_end,
        cost_exact_share=view.cost_exact_share,
        fee_exact_share=view.fee_exact_share,
        data_quality=view.data_quality,
        quality=[
            EntityQualityOut(
                entity=q.entity, used=q.used, excluded=q.excluded, top_reasons=q.top_reasons
            )
            for q in view.quality
        ],
        caveats=view.caveats,
    )


def _analyst(settings: Settings, workspace_id: str, body: AnalystQueryIn) -> AnalystQueryOut:
    question = body.question.strip()
    if not question:
        raise EmptyQuestionError("la pregunta no puede estar vacía")
    ps, snapshot = _snapshot(settings, workspace_id)
    response = ask_gold(ps, snapshot.gold, question)
    if response.status == "blocked":
        raise SqlRejectedError(response.error or "la consulta fue rechazada por los guardrails")
    return AnalystQueryOut(
        workspace_id=snapshot.workspace_id,
        status=response.status,
        question=question,
        answer=response.interpretation,
        sql=response.sql,
        tables=response.tables,
        columns=response.columns,
        rows=response.rows[:MAX_ANALYST_ROWS],
        row_count=response.row_count,
        exactness=answer_exactness(response, snapshot.gold),
        provider=response.provider,
        error=response.error,
    )


async def _read_upload(upload: UploadFile) -> bytes:
    content = await upload.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise FileInvalidError(f"el archivo supera el límite de {MAX_UPLOAD_BYTES // 2**20} MB")
    if not content:
        raise FileInvalidError("el archivo está vacío")
    return content


def _parse_mapping_text(text: str) -> tuple[dict[str, Any], MappingFile]:
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise MappingInvalidError(f"el mapeo no es YAML ni JSON válido: {exc}") from exc
    if not isinstance(raw, dict):
        raise MappingInvalidError("el mapeo debe ser un objeto con name, format y sources")
    try:
        return raw, MappingFile.model_validate(raw)
    except ValidationError as exc:
        messages = "; ".join(err["msg"] for err in exc.errors())
        raise MappingInvalidError(f"mapeo inválido: {messages}") from exc
    except ValueError as exc:
        raise MappingInvalidError(f"mapeo inválido: {exc}") from exc


def _ingest(
    settings: Settings, workspace_id: str, mapping_text: str, contents: dict[str, bytes]
) -> IngestOut:
    _workspace(settings, workspace_id)
    raw, mapping = _parse_mapping_text(mapping_text)
    expected = {spec.file for spec in mapping.sources.values()}
    received = set(contents)
    if expected - received:
        raise MappingInvalidError(
            f"faltan archivos según el mapeo: {sorted(expected - received)}. "
            "Cada archivo debe llamarse como indica su campo 'file'."
        )
    if received - expected:
        raise MappingInvalidError(
            f"hay archivos que el mapeo no usa: {sorted(received - expected)}"
        )
    platform_dir = settings.resolve_path(settings.platform_lakehouse_dir)
    with tempfile.TemporaryDirectory(prefix="api_ingest_") as tmp:
        root = Path(tmp)
        input_dir = root / "input"
        input_dir.mkdir()
        for name, content in contents.items():
            (input_dir / name).write_bytes(content)
        mapping_path = root / "mapping.yaml"
        mapping_path.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")
        try:
            result = run_csv_ingestion(
                settings, input_dir, [mapping_path], platform_dir, workspace_id=workspace_id
            )
        except MappingError as exc:
            raise MappingInvalidError(str(exc)) from exc
    report = result.ingestion
    run = result.outcome.report
    return IngestOut(
        run_id=run.run_id,
        workspace_id=workspace_id,
        batch_id=report.batch_id,
        rows_by_entity=report.rows_by_entity,
        quarantine_rows=sum(e.quarantine_rows for e in run.entities),
        privacy=[
            PrivacyOut(
                source=p.source, field=p.field, action=p.action, cells_affected=p.cells_affected
            )
            for p in report.privacy
        ],
        estimation=report.estimation.model_dump(),
        warnings=report.warnings,
    )


def create_app() -> FastAPI:
    app = FastAPI(
        title="Plataforma Lakehouse para e-commerce",
        version=API_VERSION,
        description=(
            "Runs, catálogo, resumen, analista BI gobernado e ingesta de CSV por workspace."
        ),
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=DEV_ORIGINS,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    register_error_handlers(app)

    @app.get("/health", response_model=HealthOut, tags=["plataforma"])
    def health(settings: SettingsDep) -> HealthOut:
        return HealthOut(status="ok", version=API_VERSION, storage=settings.lakehouse_storage)

    @app.get("/workspaces/{workspace_id}/runs", response_model=RunListOut, tags=["runs"])
    def list_runs(
        workspace_id: str,
        settings: SettingsDep,
        limit: Annotated[int, Query(ge=1, le=200)] = 20,
    ) -> RunListOut:
        return _runs(settings, workspace_id, limit)

    @app.get("/workspaces/{workspace_id}/runs/{run_id}", response_model=RunDetailOut, tags=["runs"])
    def get_run(workspace_id: str, run_id: str, settings: SettingsDep) -> RunDetailOut:
        return _run_detail(settings, workspace_id, run_id)

    @app.get("/workspaces/{workspace_id}/catalog", response_model=CatalogOut, tags=["catalogo"])
    def catalog(workspace_id: str, settings: SettingsDep) -> CatalogOut:
        return _catalog(settings, workspace_id)

    @app.get("/workspaces/{workspace_id}/summary", response_model=SummaryOut, tags=["resumen"])
    def summary(workspace_id: str, settings: SettingsDep) -> SummaryOut:
        return _summary(settings, workspace_id)

    @app.post(
        "/workspaces/{workspace_id}/analyst/query",
        response_model=AnalystQueryOut,
        tags=["analista"],
    )
    def analyst_query(
        workspace_id: str, body: AnalystQueryIn, settings: SettingsDep
    ) -> AnalystQueryOut:
        return _analyst(settings, workspace_id, body)

    @app.post(
        "/workspaces/{workspace_id}/ingest/inspect-csv", response_model=InspectOut, tags=["ingesta"]
    )
    async def inspect(
        workspace_id: str, settings: SettingsDep, file: Annotated[UploadFile, File()]
    ) -> InspectOut:
        _workspace(settings, workspace_id)
        content = await _read_upload(file)
        try:
            result = inspect_csv(content, file.filename or "archivo.csv")
        except ValueError as exc:
            raise FileInvalidError(str(exc)) from exc
        return InspectOut(
            filename=result.filename,
            encoding=result.encoding,
            delimiter=result.delimiter,
            decimal=result.decimal,
            headers=result.headers,
            sample_rows=result.sample_rows,
            detected_source=result.detected_source,
            template=result.template,
            canonical_matches=result.canonical_matches,
            missing_required=result.missing_required,
            suggested_custom_fields=result.suggested_custom_fields,
            suggested_sensitive=result.suggested_sensitive,
            mapping_yaml=result.mapping_yaml,
            mapping_errors=result.mapping_errors,
            warnings=result.warnings,
        )

    @app.post(
        "/workspaces/{workspace_id}/ingest/csv",
        response_model=IngestOut,
        status_code=201,
        tags=["ingesta"],
    )
    async def ingest(
        workspace_id: str,
        settings: SettingsDep,
        mapping: Annotated[str, Form()],
        files: Annotated[list[UploadFile], File()],
    ) -> IngestOut:
        contents = {f.filename or "": await _read_upload(f) for f in files}
        return _ingest(settings, workspace_id, mapping, contents)

    @app.get("/catalog", response_model=CatalogOut, tags=["atajos"])
    def catalog_default(workspace: WorkspaceFromRequest, settings: SettingsDep) -> CatalogOut:
        return _catalog(settings, workspace)

    @app.get("/runs", response_model=RunListOut, tags=["atajos"])
    def runs_default(
        workspace: WorkspaceFromRequest,
        settings: SettingsDep,
        limit: Annotated[int, Query(ge=1, le=200)] = 20,
    ) -> RunListOut:
        return _runs(settings, workspace, limit)

    @app.get("/summary", response_model=SummaryOut, tags=["atajos"])
    def summary_default(workspace: WorkspaceFromRequest, settings: SettingsDep) -> SummaryOut:
        return _summary(settings, workspace)

    @app.post("/analyst/query", response_model=AnalystQueryOut, tags=["atajos"])
    def analyst_default(
        body: AnalystQueryIn, workspace: WorkspaceFromRequest, settings: SettingsDep
    ) -> AnalystQueryOut:
        return _analyst(settings, workspace, body)

    return app


app = create_app()
