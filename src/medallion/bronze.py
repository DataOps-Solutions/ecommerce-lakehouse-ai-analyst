"""Bronze layer: raw, append-only Delta ingestion with load metadata.

Every raw value is stored as a string (no type coercion at this layer). Metadata columns:

* ``_batch_id``: identifier of the landing batch.
* ``_source``: originating system/export name.
* ``_source_seq``: 1-based position of the record inside its batch (lineage + deterministic order).
* ``_ingested_at``: UTC timestamp of the load.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from src.config import Settings
from src.data_generator import RAW_SOURCE, RawBatch
from src.engine.workspace import Workspace
from src.lakehouse import read_table, table_exists, write_table


def build_bronze_table(
    entity: str,
    rows: list[dict[str, object]],
    batch_id: str,
    ingested_at: datetime,
    source: str = RAW_SOURCE,
    columns: tuple[str, ...] | None = None,
) -> pa.Table:
    """Convert raw records of one entity into an Arrow table with metadata columns.

    ``columns`` fixes the schema explicitly (required for empty batches); otherwise it is
    the sorted union of the keys present in ``rows``.
    """
    if columns is None:
        columns = tuple(sorted({key for row in rows for key in row}))
    data: dict[str, Any] = {}
    for col in columns:
        values = [None if row.get(col) is None else str(row.get(col)) for row in rows]
        data[col] = pa.array(values, type=pa.string())
    n = len(rows)
    data["_batch_id"] = pa.array([batch_id] * n, type=pa.string())
    data["_source"] = pa.array([f"{source}.{entity}"] * n, type=pa.string())
    data["_source_seq"] = pa.array(range(1, n + 1), type=pa.int64())
    data["_ingested_at"] = pa.array([ingested_at] * n, type=pa.timestamp("us"))
    return pa.table(data)


def loaded_batch_ids(uri: str, storage_options: dict[str, str] | None) -> set[str]:
    """Return batch ids already present in a Bronze table (for idempotent loads)."""
    table = read_table(uri, storage_options, columns=["_batch_id"])
    unique = pc.unique(table.column("_batch_id")).to_pylist()
    return {str(value) for value in unique}


def ingest_batch(
    settings: Settings,
    batch: RawBatch,
    ingested_at: datetime,
    workspace: Workspace | None = None,
) -> dict[str, int]:
    """Append one raw batch to ``bronze/<entity>``. Re-running the same batch is a no-op.

    Returns the number of rows appended per entity (0 when the batch was already loaded).
    """
    opts = settings.storage_options()
    ws = workspace or settings.workspace()
    appended: dict[str, int] = {}
    for entity, rows in batch.sources.items():
        uri = ws.table_uri("bronze", entity)
        if table_exists(uri, opts) and batch.batch_id in loaded_batch_ids(uri, opts):
            appended[entity] = 0
            continue
        table = build_bronze_table(
            entity,
            list(rows),
            batch.batch_id,
            ingested_at,
            source=batch.source,
            columns=batch.columns.get(entity),
        )
        write_table(uri, table, opts, mode="append")
        appended[entity] = len(rows)
    return appended
