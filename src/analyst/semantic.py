"""Loader and governance checks for ``semantic_layer/metrics.yaml``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import sqlglot
import yaml
from pydantic import BaseModel, Field, model_validator
from sqlglot import exp
from sqlglot.errors import SqlglotError


class ColumnSpec(BaseModel):
    name: str
    type: str
    role: Literal["dimension", "measure", "attribute"]
    description: str
    synonyms: list[str] = Field(default_factory=list)


class TableSpec(BaseModel):
    name: str
    layer: Literal["gold"]
    description: str
    grain: str
    columns: list[ColumnSpec]

    @property
    def column_names(self) -> set[str]:
        return {c.name.lower() for c in self.columns}


class MetricSpec(BaseModel):
    name: str
    display_name: str
    table: str
    sql: str
    unit: Literal["currency", "ratio", "count"]
    synonyms: list[str] = Field(default_factory=list)
    definition: str


class SemanticLayer(BaseModel):
    version: int
    domain: str
    currency: str
    description: str
    restricted_layers: list[str]
    authorized_tables: list[TableSpec]
    metrics: list[MetricSpec]

    @model_validator(mode="after")
    def _validate_governance(self) -> SemanticLayer:
        table_names = [t.name for t in self.authorized_tables]
        if len(set(table_names)) != len(table_names):
            raise ValueError("duplicated table names in authorized_tables")
        metric_names = [m.name for m in self.metrics]
        if len(set(metric_names)) != len(metric_names):
            raise ValueError("duplicated metric names")
        overlap = set(table_names) & {r for r in self.restricted_layers}
        if overlap:
            raise ValueError(f"restricted layers cannot be authorized: {sorted(overlap)}")
        by_name = {t.name: t for t in self.authorized_tables}
        for metric in self.metrics:
            if metric.table not in by_name:
                raise ValueError(
                    f"metric {metric.name} points to unauthorized table {metric.table}"
                )
            allowed = by_name[metric.table].column_names
            for column in _referenced_columns(metric.sql):
                if column not in allowed:
                    raise ValueError(
                        f"metric {metric.name} references unknown column {column} in {metric.table}"
                    )
        return self

    def table(self, name: str) -> TableSpec | None:
        for t in self.authorized_tables:
            if t.name == name:
                return t
        return None

    @property
    def table_names(self) -> set[str]:
        return {t.name for t in self.authorized_tables}

    @property
    def all_column_names(self) -> set[str]:
        return {c for t in self.authorized_tables for c in t.column_names}


def _referenced_columns(expression: str) -> set[str]:
    try:
        parsed = sqlglot.parse_one(f"SELECT {expression} FROM _t", read="duckdb")
    except SqlglotError as exc:
        raise ValueError(f"invalid metric SQL '{expression}': {exc}") from exc
    names: set[str] = set()
    for col in parsed.find_all(exp.Column):
        if isinstance(col.this, exp.Star):
            continue
        names.add(col.name.lower())
    return names


def parse_semantic_layer(raw: dict[str, Any]) -> SemanticLayer:
    """Validate an already-loaded YAML mapping."""
    return SemanticLayer.model_validate(raw)


def load_semantic_layer(path: Path) -> SemanticLayer:
    """Load and validate the governed semantic layer."""
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"semantic layer {path} must be a mapping")
    return parse_semantic_layer(raw)
