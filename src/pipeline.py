"""End-to-end orchestration.

Two entry points share the same Silver -> Gold -> audit materialization:

* :func:`run_pipeline`: deterministic synthetic dataset (demo and tests).
* :func:`run_csv_ingestion`: real exports described by declarative mappings.

Bronze is append-only and idempotent per batch; Silver, quarantine and Gold are rebuilt
deterministically from Bronze on every run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa

from src.config import Settings
from src.data_generator import generate_batches
from src.ingestion.csv_adapter import IngestionReport, ingest_files
from src.lakehouse import read_table, table_exists, write_table
from src.medallion.bronze import ingest_batch
from src.medallion.gold import GOLD_TABLES, SILVER_ENTITIES, build_gold
from src.medallion.silver import run_silver
from src.models import RAW_ENTITIES, AuditRecord, PipelineRunReport


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


def _utc_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _audit_table(records: list[AuditRecord]) -> pa.Table:
    return pa.Table.from_pylist([r.model_dump() for r in records])


def materialize(
    settings: Settings,
    batches_loaded: dict[str, int],
    started: datetime,
    run_id: str,
) -> RunOutcome:
    """Silver, quarantine and Gold from the full Bronze state, then audit."""
    opts = settings.storage_options()
    bronze = {
        entity: read_table(settings.table_uri("bronze", entity), opts) for entity in RAW_ENTITIES
    }
    silver = run_silver(bronze, quarantined_at=started)

    audit: list[AuditRecord] = []

    def record(layer: str, table: str, rows: int) -> None:
        audit.append(
            AuditRecord(
                run_id=run_id,
                layer=layer,
                table=table,
                action="append" if layer == "bronze" else "overwrite",
                rows=rows,
                created_at=_utc_naive(),
            )
        )

    for entity, table in silver.tables.items():
        write_table(settings.table_uri("silver", entity), table, opts, mode="overwrite")
        record("silver", entity, table.num_rows)
    for entity, table in silver.quarantine.items():
        write_table(settings.table_uri("quarantine", entity), table, opts, mode="overwrite")
        record("quarantine", entity, table.num_rows)

    gold = build_gold({e: silver.tables[e] for e in SILVER_ENTITIES})
    gold_rows: dict[str, int] = {}
    for name in GOLD_TABLES:
        write_table(settings.table_uri("gold", name), gold[name], opts, mode="overwrite")
        gold_rows[name] = gold[name].num_rows
        record("gold", name, gold[name].num_rows)

    write_table(
        settings.table_uri("audit", "pipeline_runs"),
        _audit_table(audit),
        opts,
        mode="append",
    )
    report = PipelineRunReport(
        run_id=run_id,
        started_at=started,
        finished_at=_utc_naive(),
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


def run_pipeline(settings: Settings) -> RunOutcome:
    """Synthetic flow: generate the seeded batches, ingest them and materialize."""
    run_id = uuid4().hex[:12]
    started = _utc_naive()
    batch_rows: dict[str, int] = {}
    for batch in generate_batches(settings.data_seed):
        appended = ingest_batch(settings, batch, started)
        batch_rows[batch.batch_id] = sum(appended.values())
    return materialize(settings, batch_rows, started, run_id)


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
) -> CsvRunOutcome:
    """Real-data flow: CSV exports -> raw batch -> Silver -> Gold -> audit."""
    target = csv_settings(settings, lakehouse_dir)
    run_id = uuid4().hex[:12]
    started = _utc_naive()
    result = ingest_files(input_dir, mapping_paths, settings.pseudonymization_salt)
    appended = ingest_batch(target, result.batch, started)
    outcome = materialize(
        target,
        {result.batch.batch_id: sum(appended.values())},
        started,
        run_id,
    )
    return CsvRunOutcome(outcome=outcome, ingestion=result.report)


def load_gold_tables(settings: Settings) -> dict[str, pa.Table]:
    """Read the current Gold snapshot. Raises if the pipeline has not run yet."""
    opts = settings.storage_options()
    missing = [
        name for name in GOLD_TABLES if not table_exists(settings.table_uri("gold", name), opts)
    ]
    if missing:
        raise RuntimeError(
            f"faltan tablas Gold {missing}. Ejecute primero: make pipeline "
            "(o python -m src.main run-pipeline)"
        )
    return {name: read_table(settings.table_uri("gold", name), opts) for name in GOLD_TABLES}
