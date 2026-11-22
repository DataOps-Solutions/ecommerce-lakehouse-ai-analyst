"""Thin wrapper around ``deltalake`` that works for local paths and ``s3://`` URIs."""

from __future__ import annotations

from typing import Literal

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def relation_to_arrow(relation: duckdb.DuckDBPyRelation) -> pa.Table:
    """Materialize a DuckDB relation as an Arrow table (DuckDB may return a reader)."""
    result: pa.Table | pa.RecordBatchReader = relation.arrow()
    if isinstance(result, pa.Table):
        return result
    return result.read_all()


def table_exists(uri: str, storage_options: dict[str, str] | None) -> bool:
    """Return True when ``uri`` already contains a Delta table."""
    return bool(DeltaTable.is_deltatable(uri, storage_options=storage_options))


def read_table(
    uri: str, storage_options: dict[str, str] | None, columns: list[str] | None = None
) -> pa.Table:
    """Read the current snapshot of a Delta table into Arrow."""
    table = DeltaTable(uri, storage_options=storage_options)
    return table.to_pyarrow_table(columns=columns)


def read_version(uri: str, storage_options: dict[str, str] | None, version: int) -> pa.Table:
    """Read a historical snapshot (Delta time travel)."""
    table = DeltaTable(uri, version=version, storage_options=storage_options)
    return table.to_pyarrow_table()


def write_table(
    uri: str,
    data: pa.Table,
    storage_options: dict[str, str] | None,
    mode: Literal["append", "overwrite"],
) -> int:
    """Write Arrow data to Delta and return the resulting table version."""
    write_deltalake(
        uri,
        data,
        mode=mode,
        storage_options=storage_options,
        schema_mode="merge" if mode == "append" else "overwrite",
    )
    return int(DeltaTable(uri, storage_options=storage_options).version())
