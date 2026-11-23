"""DuckDB catalog over the Delta tables of one workspace.

Persisted tables are read through ``deltalake`` (local or S3) and registered in an in-memory
DuckDB connection with external access disabled, so SQL can join Bronze, Silver, quarantine
and Gold without copying files. Names follow the guarded analyst's convention:
``bronze_<entity>``, ``silver_<entity>``, ``quarantine_<entity>`` and the Gold marts unchanged.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import duckdb
import pyarrow as pa

from src.engine.workspace import Workspace
from src.lakehouse import read_table, table_exists
from src.models import RAW_ENTITIES

CUSTOM_ATTRIBUTES_COLUMN = "custom_attributes"
GOLD_MARTS: tuple[str, ...] = (
    "mart_daily_unit_economics",
    "mart_checkout_funnel",
    "mart_sku_health",
)
SILVER_ENTITIES: tuple[str, ...] = (
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
    "checkout_events",
)


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    duckdb_type: str


@dataclass(frozen=True)
class TableInfo:
    name: str
    layer: str
    uri: str
    rows: int
    columns: tuple[ColumnInfo, ...]

    @property
    def has_custom_attributes(self) -> bool:
        return any(c.name == CUSTOM_ATTRIBUTES_COLUMN for c in self.columns)


@dataclass(frozen=True)
class CatalogEntry:
    """A Delta table known to the catalog: DuckDB name, layer, storage table name."""

    name: str
    layer: str
    layer_table: str


class MissingGoldError(RuntimeError):
    """Raised when the Gold marts have not been built in this workspace yet."""


class LakehouseCatalog:
    """Catalog of the Delta tables that exist in one workspace."""

    def __init__(self, workspace: Workspace, storage_options: dict[str, str] | None) -> None:
        self._workspace = workspace
        self._opts = storage_options

    def entries(self) -> list[CatalogEntry]:
        """Every table the catalog knows about, whether or not it exists yet."""
        items = [CatalogEntry(f"bronze_{e}", "bronze", e) for e in RAW_ENTITIES]
        items += [CatalogEntry(f"silver_{e}", "silver", e) for e in SILVER_ENTITIES]
        items += [CatalogEntry(f"quarantine_{e}", "quarantine", e) for e in RAW_ENTITIES]
        items += [CatalogEntry(m, "gold", m) for m in GOLD_MARTS]
        return items

    def _existing(self) -> list[CatalogEntry]:
        return [
            e
            for e in self.entries()
            if table_exists(self._workspace.table_uri(e.layer, e.layer_table), self._opts)
        ]

    def existing_names(self) -> list[str]:
        return [e.name for e in self._existing()]

    @contextmanager
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Connection with every existing workspace table registered under its catalog name."""
        con = duckdb.connect(config={"enable_external_access": False})
        try:
            for entry in self._existing():
                uri = self._workspace.table_uri(entry.layer, entry.layer_table)
                con.register(entry.name, read_table(uri, self._opts))
            yield con
        finally:
            con.close()

    def inspect(self) -> list[TableInfo]:
        """Schema, row count and location of every existing table, as DuckDB sees them."""
        infos: list[TableInfo] = []
        with self.connect() as con:
            for entry in self._existing():
                described = con.execute(f'DESCRIBE "{entry.name}"').fetchall()
                count_row = con.execute(f'SELECT count(*) FROM "{entry.name}"').fetchone()
                rows = int(count_row[0]) if count_row else 0
                infos.append(
                    TableInfo(
                        name=entry.name,
                        layer=entry.layer,
                        uri=self._workspace.table_uri(entry.layer, entry.layer_table),
                        rows=rows,
                        columns=tuple(ColumnInfo(str(r[0]), str(r[1])) for r in described),
                    )
                )
        return infos

    def read_gold(self) -> dict[str, pa.Table]:
        """Current Gold marts of the workspace. Raises :class:`MissingGoldError` if absent."""
        missing = [
            m
            for m in GOLD_MARTS
            if not table_exists(self._workspace.table_uri("gold", m), self._opts)
        ]
        if missing:
            raise MissingGoldError(
                f"faltan tablas Gold {missing}. Ejecute primero: make pipeline "
                "(o python -m src.main run-pipeline)"
            )
        return {m: read_table(self._workspace.table_uri("gold", m), self._opts) for m in GOLD_MARTS}
