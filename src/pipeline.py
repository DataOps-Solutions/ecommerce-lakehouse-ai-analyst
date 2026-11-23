"""End-to-end orchestration.

Two entry points share the same Silver -> Gold -> audit materialization:

* :func:`run_pipeline`: deterministic synthetic dataset (demo and tests).
* :func:`run_csv_ingestion`: real exports described by declarative mappings.

Both run inside a :class:`~src.engine.runs.RunTracker` context, so every execution leaves a
record with its status, counts and estimated share of the data. Bronze is append-only and
idempotent per batch; Silver, quarantine and Gold are rebuilt from Bronze on every run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pyarrow as pa

from src.config import Settings
from src.data_generator import generate_batches
from src.engine.catalog import LakehouseCatalog
from src.engine.runs import RunMetrics, RunTracker, utc_now
from src.engine.workspace import Workspace
from src.ingestion.csv_adapter import IngestionReport, ingest_files
from src.lakehouse import read_table, write_table
from src.medallion.bronze import ingest_batch
from src.medallion.gold import GOLD_TABLES, SILVER_ENTITIES, build_gold
from src.medallion.silver import run_silver
from src.models import RAW_ENTITIES, AuditRecord, PipelineRunReport
from src.reporting.client_summary import econ_totals


@dataclass
class RunOutcome:
    """Everything a run produced, kept in memory for reports."""

    report: PipelineRunReport
    silver: dict[str, pa.Table] = field(default_factory=dict)
    quarantine: dict[str, pa.Table] = field(default_factory=dict)
    gold: dict[str, pa.Table] = field(default_factory=dict)


@dataclass
class CsvRunOutcome:
    """A CSV ingestion run: the pipeline outcome plus the ingestion report."""

    outcome: RunOutcome
    ingestion: IngestionReport


def _audit_table(records: list[AuditRecord]) -> pa.Table:
    return pa.Table.from_pylist([r.model_dump() for r in records])


def _run_metrics(outcome: RunOutcome, rows_in: int) -> RunMetrics:
    """Counts and estimated shares for the run registry, from the same numbers as the report."""
    totals = econ_totals(outcome.gold)
    cost_pct = fee_pct = None
    if totals is not None:
        if totals.cost_exact_share is not None:
            cost_pct = round((1 - totals.cost_exact_share) * 100, 2)
        if totals.fee_exact_share is not None:
            fee_pct = round((1 - totals.fee_exact_share) * 100, 2)
    return RunMetrics(
        rows_in=rows_in,
        rows_out=sum(r.silver_rows for r in outcome.report.entities),
        quarantine_rows=sum(r.quarantine_rows for r in outcome.report.entities),
        estimated_cost_pct=cost_pct,
        estimated_fee_pct=fee_pct,
    )


def materialize(
    settings: Settings,
    workspace: Workspace,
    batches_loaded: dict[str, int],
    started: datetime,
    run_id: str,
) -> RunOutcome:
    """Silver, quarantine and Gold from the full Bronze state of a workspace, then audit."""
    opts = settings.storage_options()
    bronze = {
        entity: read_table(workspace.table_uri("bronze", entity), opts) for entity in RAW_ENTITIES
    }
    silver = run_silver(bronze, quarantined_at=started)

    audit: list[AuditRecord] = []

    def record(layer: str, table: str, rows: int) -> None:
        audit.append(
            AuditRecord(
                run_id=run_id,
                layer=layer,
                table=table,
                action="overwrite",
                rows=rows,
                created_at=utc_now(),
            )
        )

    for entity, table in silver.tables.items():
        write_table(workspace.table_uri("silver", entity), table, opts, mode="overwrite")
        record("silver", entity, table.num_rows)
    for entity, table in silver.quarantine.items():
        write_table(workspace.table_uri("quarantine", entity), table, opts, mode="overwrite")
        record("quarantine", entity, table.num_rows)

    gold = build_gold({e: silver.tables[e] for e in SILVER_ENTITIES})
    gold_rows: dict[str, int] = {}
    for name in GOLD_TABLES:
        write_table(workspace.table_uri("gold", name), gold[name], opts, mode="overwrite")
        gold_rows[name] = gold[name].num_rows
        record("gold", name, gold[name].num_rows)

    write_table(
        workspace.table_uri("audit", "pipeline_runs"),
        _audit_table(audit),
        opts,
        mode="append",
    )
    report = PipelineRunReport(
        run_id=run_id,
        started_at=started,
        finished_at=utc_now(),
        storage=settings.lakehouse_storage,
        batches_loaded=batches_loaded,
        entities=silver.reports,
        gold_rows=gold_rows,
    )
    return RunOutcome(
        report=report,
        silver=silver.tables,
        quarantine=silver.quarantine,
        gold=gold,
    )


def run_pipeline(settings: Settings, workspace_id: str | None = None) -> RunOutcome:
    """Synthetic flow: generate the seeded batches, ingest them and materialize."""
    workspace = settings.workspace(workspace_id)
    tracker = RunTracker(workspace, settings.storage_options())
    started = utc_now()
    with tracker.track("synthetic_pipeline") as handle:
        batch_rows: dict[str, int] = {}
        for batch in generate_batches(settings.data_seed):
            appended = ingest_batch(settings, batch, started, workspace)
            batch_rows[batch.batch_id] = sum(appended.values())
        outcome = materialize(settings, workspace, batch_rows, started, handle.run_id)
        handle.metrics = _run_metrics(outcome, rows_in=sum(batch_rows.values()))
    return outcome


def csv_settings(settings: Settings, lakehouse_dir: Path) -> Settings:
    """Settings for a CSV run: its own lakehouse so real and synthetic data never mix."""
    if settings.lakehouse_storage == "s3":
        return settings.model_copy(update={"s3_prefix": f"{settings.s3_prefix}-csv"})
    return settings.model_copy(update={"lakehouse_local_dir": lakehouse_dir})


def run_csv_ingestion(
    settings: Settings,
    input_dir: Path,
    mapping_paths: list[Path],
    lakehouse_dir: Path,
    workspace_id: str | None = None,
) -> CsvRunOutcome:
    """Real-data flow: CSV exports -> raw batch -> Silver -> Gold -> audit.

    Configuration errors raise after being recorded as a ``failed`` run.
    """
    target = csv_settings(settings, lakehouse_dir)
    workspace = target.workspace(workspace_id)
    tracker = RunTracker(workspace, target.storage_options())
    started = utc_now()
    with tracker.track("csv_ingestion") as handle:
        result = ingest_files(input_dir, mapping_paths, settings.pseudonymization_salt)
        appended = ingest_batch(target, result.batch, started, workspace)
        outcome = materialize(
            target,
            workspace,
            {result.batch.batch_id: sum(appended.values())},
            started,
            handle.run_id,
        )
        rows_in = sum(f.rows_read for f in result.report.files)
        handle.metrics = _run_metrics(outcome, rows_in=rows_in)
    return CsvRunOutcome(outcome=outcome, ingestion=result.report)


def load_gold_tables(settings: Settings, workspace_id: str | None = None) -> dict[str, pa.Table]:
    """Read the current Gold snapshot of a workspace through the catalog.

    Raises :class:`~src.engine.catalog.MissingGoldError` if the pipeline has not run yet.
    """
    catalog = LakehouseCatalog(settings.workspace(workspace_id), settings.storage_options())
    return catalog.read_gold()
