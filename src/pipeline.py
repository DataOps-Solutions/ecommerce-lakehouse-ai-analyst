"""End-to-end orchestration: raw batches -> Bronze -> Silver (+ quarantine) -> Gold -> audit."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pyarrow as pa

from src.config import Settings
from src.data_generator import generate_batches
from src.lakehouse import read_table, table_exists, write_table
from src.medallion.bronze import ingest_batch
from src.medallion.gold import GOLD_TABLES, SILVER_ENTITIES, build_gold
from src.medallion.silver import run_silver
from src.models import RAW_ENTITIES, AuditRecord, PipelineRunReport


def _utc_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _audit_table(records: list[AuditRecord]) -> pa.Table:
    return pa.Table.from_pylist([r.model_dump() for r in records])


def run_pipeline(settings: Settings) -> PipelineRunReport:
    """Run the full Medallion pipeline. Bronze is append-only and idempotent per batch;
    Silver, quarantine and Gold are rebuilt deterministically from Bronze on every run."""
    opts = settings.storage_options()
    run_id = uuid4().hex[:12]
    started = _utc_naive()

    batch_rows: dict[str, int] = {}
    for batch in generate_batches(settings.data_seed):
        appended = ingest_batch(settings, batch, started)
        batch_rows[batch.batch_id] = sum(appended.values())

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
                action="overwrite" if layer != "bronze" else "append",
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
        settings.table_uri("audit", "pipeline_runs"), _audit_table(audit), opts, mode="append"
    )

    return PipelineRunReport(
        run_id=run_id,
        started_at=started,
        finished_at=_utc_naive(),
        storage=settings.lakehouse_storage,
        batches_loaded=batch_rows,
        entities=silver.reports,
        gold_rows=gold_rows,
    )


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
