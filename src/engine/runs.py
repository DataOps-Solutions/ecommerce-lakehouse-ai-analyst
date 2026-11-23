"""Run registry: one auditable record per pipeline execution, stored as a Delta table.

Each state change is an immutable event appended to ``<workspace>/_ops/run_events``. The
current state of a run is its most recent event. A run that stays ``running`` after its
process died is reported as such: the registry never marks it as failed by guessing.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

import pyarrow as pa
from pydantic import BaseModel, Field

from src.engine.workspace import Workspace
from src.lakehouse import read_table, table_exists, write_table

RunStatus = Literal["running", "succeeded", "failed"]

RUN_FIELDS: list[tuple[str, pa.DataType]] = [
    ("run_id", pa.string()),
    ("workspace_id", pa.string()),
    ("pipeline_name", pa.string()),
    ("status", pa.string()),
    ("started_at", pa.timestamp("us")),
    ("finished_at", pa.timestamp("us")),
    ("event_at", pa.timestamp("us")),
    ("rows_in", pa.int64()),
    ("rows_out", pa.int64()),
    ("quarantine_rows", pa.int64()),
    ("estimated_cost_pct", pa.float64()),
    ("estimated_fee_pct", pa.float64()),
    ("error_message", pa.string()),
]
RUN_SCHEMA = pa.schema(RUN_FIELDS)


def utc_now() -> datetime:
    """Naive UTC timestamp, the convention used by every lakehouse table."""
    return datetime.now(UTC).replace(tzinfo=None)


class RunMetrics(BaseModel):
    """Summary numbers of one run. Percentages are 0-100 and None when not applicable."""

    rows_in: int | None = None
    rows_out: int | None = None
    quarantine_rows: int | None = None
    estimated_cost_pct: float | None = None
    estimated_fee_pct: float | None = None


class RunRecord(BaseModel):
    """Current state of one run (its latest event)."""

    run_id: str
    workspace_id: str
    pipeline_name: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    event_at: datetime
    metrics: RunMetrics = Field(default_factory=RunMetrics)
    error_message: str | None = None


@dataclass
class RunHandle:
    """Given to the body of :meth:`RunTracker.track`; set ``metrics`` before it returns."""

    run_id: str
    metrics: RunMetrics = field(default_factory=RunMetrics)


def _event_row(record: RunRecord) -> dict[str, object]:
    m = record.metrics
    return {
        "run_id": record.run_id,
        "workspace_id": record.workspace_id,
        "pipeline_name": record.pipeline_name,
        "status": record.status,
        "started_at": record.started_at,
        "finished_at": record.finished_at,
        "event_at": record.event_at,
        "rows_in": m.rows_in,
        "rows_out": m.rows_out,
        "quarantine_rows": m.quarantine_rows,
        "estimated_cost_pct": m.estimated_cost_pct,
        "estimated_fee_pct": m.estimated_fee_pct,
        "error_message": record.error_message,
    }


def _record_from_row(row: dict[str, object]) -> RunRecord:
    def as_int(value: object) -> int | None:
        return None if value is None else int(str(value))

    def as_float(value: object) -> float | None:
        return None if value is None else float(str(value))

    metrics = RunMetrics(
        rows_in=as_int(row["rows_in"]),
        rows_out=as_int(row["rows_out"]),
        quarantine_rows=as_int(row["quarantine_rows"]),
        estimated_cost_pct=as_float(row["estimated_cost_pct"]),
        estimated_fee_pct=as_float(row["estimated_fee_pct"]),
    )
    finished = row["finished_at"]
    error = row["error_message"]
    return RunRecord(
        run_id=str(row["run_id"]),
        workspace_id=str(row["workspace_id"]),
        pipeline_name=str(row["pipeline_name"]),
        status=str(row["status"]),  # type: ignore[arg-type]
        started_at=row["started_at"],  # type: ignore[arg-type]
        finished_at=finished,  # type: ignore[arg-type]
        event_at=row["event_at"],  # type: ignore[arg-type]
        metrics=metrics,
        error_message=None if error is None else str(error),
    )


class RunTracker:
    """Writes run events for one workspace and reads back the run history."""

    def __init__(self, workspace: Workspace, storage_options: dict[str, str] | None) -> None:
        self._workspace = workspace
        self._opts = storage_options
        self._uri = workspace.run_events_uri()

    @property
    def uri(self) -> str:
        return self._uri

    def _append(self, record: RunRecord) -> RunRecord:
        table = pa.Table.from_pylist([_event_row(record)], schema=RUN_SCHEMA)
        write_table(self._uri, table, self._opts, mode="append")
        return record

    def start(self, pipeline_name: str) -> RunRecord:
        now = utc_now()
        record = RunRecord(
            run_id=uuid4().hex[:12],
            workspace_id=self._workspace.workspace_id,
            pipeline_name=pipeline_name,
            status="running",
            started_at=now,
            event_at=now,
        )
        return self._append(record)

    def succeed(self, run: RunRecord, metrics: RunMetrics) -> RunRecord:
        now = utc_now()
        return self._append(
            run.model_copy(
                update={
                    "status": "succeeded",
                    "finished_at": now,
                    "event_at": now,
                    "metrics": metrics,
                    "error_message": None,
                }
            )
        )

    def fail(self, run: RunRecord, error_message: str) -> RunRecord:
        now = utc_now()
        return self._append(
            run.model_copy(
                update={
                    "status": "failed",
                    "finished_at": now,
                    "event_at": now,
                    "error_message": error_message[:2000],
                }
            )
        )

    @contextmanager
    def track(self, pipeline_name: str) -> Iterator[RunHandle]:
        """Record start, then success with ``handle.metrics`` or failure with the error."""
        run = self.start(pipeline_name)
        handle = RunHandle(run_id=run.run_id)
        try:
            yield handle
        except BaseException as exc:
            self.fail(run, f"{type(exc).__name__}: {exc}")
            raise
        self.succeed(run, handle.metrics)

    def _events(self) -> list[RunRecord]:
        if not table_exists(self._uri, self._opts):
            return []
        table = read_table(self._uri, self._opts)
        return [_record_from_row(row) for row in table.to_pylist()]

    def history(self, run_id: str) -> list[RunRecord]:
        """All events of one run, oldest first."""
        return sorted((e for e in self._events() if e.run_id == run_id), key=lambda e: e.event_at)

    def get_run(self, run_id: str) -> RunRecord | None:
        events = self.history(run_id)
        return events[-1] if events else None

    def list_runs(self, limit: int = 20) -> list[RunRecord]:
        """Current state of the most recent runs, newest first."""
        latest: dict[str, RunRecord] = {}
        for event in self._events():
            known = latest.get(event.run_id)
            if known is None or event.event_at >= known.event_at:
                latest[event.run_id] = event
        runs = sorted(latest.values(), key=lambda r: r.started_at, reverse=True)
        return runs[:limit]
