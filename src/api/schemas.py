"""Request and response models of the HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class HealthOut(BaseModel):
    status: Literal["ok"]
    version: str
    storage: Literal["local", "s3"]


class RunMetricsOut(BaseModel):
    rows_in: int | None
    rows_out: int | None
    quarantine_rows: int | None
    estimated_cost_pct: float | None
    estimated_fee_pct: float | None


class RunOut(BaseModel):
    run_id: str
    workspace_id: str
    pipeline_name: str
    status: Literal["running", "succeeded", "failed"]
    started_at: datetime
    finished_at: datetime | None
    metrics: RunMetricsOut
    error_message: str | None


class RunEventOut(BaseModel):
    status: Literal["running", "succeeded", "failed"]
    event_at: datetime
    error_message: str | None


class RunListOut(BaseModel):
    workspace_id: str
    runs: list[RunOut]


class RunDetailOut(BaseModel):
    run: RunOut
    history: list[RunEventOut]


class ColumnOut(BaseModel):
    name: str
    type: str


class TableOut(BaseModel):
    name: str
    layer: str
    rows: int
    columns: list[ColumnOut]
    has_custom_attributes: bool


class CatalogOut(BaseModel):
    workspace_id: str
    tables: list[TableOut]


class KpisOut(BaseModel):
    gross_sales: float
    net_revenue: float
    net_margin: float
    margin_pct: float | None
    units: int
    orders: int


class EntityQualityOut(BaseModel):
    entity: str
    used: int
    excluded: int
    top_reasons: list[str]


class SummaryOut(BaseModel):
    workspace_id: str
    kpis: KpisOut
    period_start: str | None
    period_end: str | None
    cost_exact_share: float | None
    fee_exact_share: float | None
    data_quality: Literal["exacto", "estimado", "sin_datos"]
    quality: list[EntityQualityOut]
    caveats: list[str]


class AnalystQueryIn(BaseModel):
    question: str = Field(min_length=1, max_length=500)


class AnalystQueryOut(BaseModel):
    workspace_id: str
    status: Literal["ok", "unanswerable", "error"]
    question: str
    answer: str | None
    sql: str | None
    tables: list[str]
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    exactness: Literal["exacto", "estimado", "no_aplica", "sin_datos"]
    provider: str
    error: str | None


class InspectOut(BaseModel):
    filename: str
    encoding: str
    delimiter: str
    decimal: str
    headers: list[str]
    sample_rows: list[dict[str, str]]
    detected_source: str | None
    template: str | None
    canonical_matches: dict[str, str]
    missing_required: list[str]
    suggested_custom_fields: dict[str, list[str]]
    suggested_sensitive: dict[str, str]
    mapping_yaml: str | None
    mapping_errors: list[str]
    warnings: list[str]


class PrivacyOut(BaseModel):
    source: str
    field: str
    action: Literal["drop", "pseudonymize"]
    cells_affected: int


class IngestOut(BaseModel):
    run_id: str
    workspace_id: str
    batch_id: str
    rows_by_entity: dict[str, int]
    quarantine_rows: int
    privacy: list[PrivacyOut]
    estimation: dict[str, int]
    warnings: list[str]
