"""AI/BI SQL analyst: business question -> guarded SQL -> DuckDB over Gold -> executive reading.

Providers:

* ``mock``: deterministic planner that builds SQL from the metric and dimension definitions in
  ``semantic_layer/metrics.yaml`` (synonym matching). Needs no credentials.
* ``bedrock``: AWS Bedrock Runtime Converse API. The model receives the governed semantic layer
  and must return one SQL statement. Its interpretation must only cite numbers present in the
  returned table; otherwise the deterministic reading is used as fallback.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol

import boto3
import duckdb
import pyarrow as pa

from src.analyst.semantic import MetricSpec, SemanticLayer
from src.analyst.sql_guardrails import validate_sql
from src.config import Settings
from src.lakehouse import relation_to_arrow
from src.models import AnalystResponse, GuardrailViolationError

DEFAULT_LIMIT = 10
RANKING_DESC = ("mayor", "mayores", "mas", "top", "peor", "peores", "alta", "altas")
RANKING_ASC = ("menor", "menores", "menos", "bajo", "bajos", "baja", "bajas")


@dataclass(frozen=True)
class PlanResult:
    """A candidate SQL statement plus the metrics it is meant to answer."""

    sql: str
    metrics: tuple[str, ...]


class LLMProvider(Protocol):
    """Interface shared by the mock and Bedrock providers."""

    name: str

    def plan(self, question: str, layer: SemanticLayer) -> PlanResult | None:
        """Return SQL for the question, or None when it cannot be answered from Gold."""
        ...

    def interpret(
        self,
        question: str,
        sql: str,
        columns: list[str],
        rows: list[dict[str, Any]],
        layer: SemanticLayer,
    ) -> str:
        """Return an executive reading of the result table."""
        ...


def _normalize(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-z0-9]+", " ", ascii_text.lower()).strip()
    return f" {cleaned} "


def _position(padded: str, phrase: str) -> int:
    return padded.find(f" {_normalize(phrase).strip()} ")


def _first_position(padded: str, phrases: list[str]) -> int | None:
    found = [p for p in (_position(padded, ph) for ph in phrases) if p >= 0]
    return min(found) if found else None


class MockProvider:
    """Deterministic planner and interpreter. No external calls."""

    name = "mock"

    def __init__(self, row_limit: int = 1000) -> None:
        self._row_limit = row_limit

    def plan(self, question: str, layer: SemanticLayer) -> PlanResult | None:
        padded = _normalize(question)
        matches: list[tuple[int, MetricSpec]] = []
        for metric in layer.metrics:
            pos = _first_position(padded, [*metric.synonyms, metric.name.replace("_", " ")])
            if pos is not None:
                matches.append((pos, metric))
        if not matches:
            return None
        matches.sort(key=lambda item: item[0])
        table_name = matches[0][1].table
        metrics = [m for _, m in matches if m.table == table_name]
        table = layer.table(table_name)
        if table is None:
            return None

        dims: list[tuple[int, str, str]] = []
        for column in table.columns:
            if column.role != "dimension":
                continue
            phrases = [*column.synonyms, column.name.replace("_", " ")]
            pos = _first_position(padded, phrases)
            if pos is not None:
                dims.append((pos, column.name, column.type))
        dims.sort(key=lambda item: item[0])
        dims = dims[:2]

        date_dims = [name for _, name, typ in dims if typ == "DATE"]
        # Time series must not be truncated; rankings default to the top 10.
        limit = self._row_limit if date_dims else DEFAULT_LIMIT
        top = re.search(r"\b(?:top|primeros|primeras)\s+(\d+)\b", _normalize(question))
        if top:
            limit = min(int(top.group(1)), self._row_limit)

        select_parts = [name for _, name, _ in dims]
        select_parts.extend(f"{m.sql} AS {m.name}" for m in metrics)
        sql = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        if dims:
            sql += " GROUP BY " + ", ".join(name for _, name, _ in dims)
        if date_dims:
            sql += f" ORDER BY {date_dims[0]} ASC"
        else:
            descending = not any(f" {w} " in padded for w in RANKING_ASC)
            direction = "DESC" if descending else "ASC"
            sql += f" ORDER BY {metrics[0].name} {direction}"
        sql += f" LIMIT {limit}"
        return PlanResult(sql=sql, metrics=tuple(m.name for m in metrics))

    def interpret(
        self,
        question: str,
        sql: str,
        columns: list[str],
        rows: list[dict[str, Any]],
        layer: SemanticLayer,
    ) -> str:
        return mock_reading(columns, rows, layer)


def _format_value(value: Any, unit: str | None) -> str:
    if value is None:
        return "s/d"
    number = float(value) if isinstance(value, (int, float, Decimal)) else None
    if number is None:
        return str(value)
    if unit == "ratio":
        text = f"{number * 100:,.1f} %"
    elif unit == "currency":
        text = f"$ {number:,.0f}"
    elif unit == "count":
        text = f"{number:,.0f}"
    else:
        text = f"{number:,.2f}" if not float(number).is_integer() else f"{number:,.0f}"
    return text.replace(",", "§").replace(".", ",").replace("§", ".")


def mock_reading(columns: list[str], rows: list[dict[str, Any]], layer: SemanticLayer) -> str:
    """Factual reading built only from the returned rows."""
    if not rows:
        return "La consulta no devolvió filas; no hay evidencia numérica para interpretar."
    metric_units = {m.name: m.unit for m in layer.metrics}
    metric_cols = [c for c in columns if c in metric_units]
    dim_cols = [c for c in columns if c not in metric_units]
    lines = [f"La consulta devolvió {len(rows)} fila(s)."]

    def label(row: dict[str, Any]) -> str:
        return ", ".join(f"{d} = {row[d]}" for d in dim_cols) if dim_cols else "total"

    for metric in metric_cols:
        unit = metric_units[metric]
        values = [r[metric] for r in rows if r[metric] is not None]
        if not values:
            continue
        first, last = rows[0], rows[-1]
        if len(rows) == 1:
            lines.append(f"{metric} ({label(first)}) = {_format_value(first[metric], unit)}.")
        else:
            lines.append(
                f"{metric}: primera fila ({label(first)}) = "
                f"{_format_value(first[metric], unit)}; última fila ({label(last)}) = "
                f"{_format_value(last[metric], unit)}."
            )
        if unit in ("currency", "count") and len(rows) > 1:
            total = sum(float(v) for v in values)
            lines.append(f"Suma de {metric} en las filas devueltas = {_format_value(total, unit)}.")
        if unit == "ratio" and len(rows) > 1:
            lines.append(
                f"Rango de {metric} entre las filas devueltas: "
                f"mínimo {_format_value(min(float(v) for v in values), unit)}, "
                f"máximo {_format_value(max(float(v) for v in values), unit)}."
            )
    return " ".join(lines)


_NUMBER = re.compile(r"\d{1,3}(?:\.\d{3})+(?:,\d+)?|\d+(?:,\d+)?")


def _parse_es_number(token: str) -> float:
    return float(token.replace(".", "").replace(",", "."))


def is_grounded(text: str, columns: list[str], rows: list[dict[str, Any]]) -> bool:
    """True when every number in ``text`` matches a value in the returned table.

    Accepts the raw cell value, its percentage form, column sums and the row count.
    """
    cell_values: list[float] = [float(len(rows))]
    numeric_cols = [
        c for c in columns if rows and isinstance(rows[0].get(c), (int, float, Decimal))
    ]
    sums: dict[str, float] = {}
    for col in numeric_cols:
        sums[col] = sum(float(r[col]) for r in rows if r[col] is not None)
    for row in rows:
        for col in numeric_cols:
            if row[col] is not None:
                cell_values.append(float(row[col]))
    cell_values.extend(sums.values())
    candidates = cell_values + [v * 100 for v in cell_values]

    for token in _NUMBER.findall(text):
        value = _parse_es_number(token)
        decimals = len(token.split(",")[1]) if "," in token else 0
        tolerance = 0.5 * 10 ** (-decimals) + 1e-9
        if not any(abs(value - c) <= tolerance for c in candidates):
            return False
    return True


class BedrockProvider:
    """AWS Bedrock Runtime (Converse API). Requires AWS credentials or an injected client."""

    name = "bedrock"

    def __init__(self, settings: Settings, client: Any | None = None) -> None:
        self._model_id = settings.bedrock_model_id
        self._client: Any = client or boto3.client(
            "bedrock-runtime", region_name=settings.bedrock_region
        )

    def _converse(self, system: str, user: str, max_tokens: int) -> str:
        response = self._client.converse(
            modelId=self._model_id,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": user}]}],
            inferenceConfig={"maxTokens": max_tokens, "temperature": 0.0},
        )
        blocks = response["output"]["message"]["content"]
        return "".join(str(block.get("text", "")) for block in blocks).strip()

    def plan(self, question: str, layer: SemanticLayer) -> PlanResult | None:
        system = (
            "Eres un analista SQL para DuckDB. Responde con UNA sola sentencia SELECT o WITH, "
            "dentro de un bloque ```sql```. Usa únicamente las tablas y columnas de la capa "
            "semántica. No uses comentarios, ni DDL/DML, ni funciones que lean archivos. "
            "Si la pregunta no se puede responder con esta capa, responde UNANSWERABLE.\n\n"
            + semantic_context(layer)
        )
        text = self._converse(system, question, max_tokens=800)
        if text.strip().upper().startswith("UNANSWERABLE"):
            return None
        match = re.search(r"```sql\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
        sql = match.group(1).strip() if match else text.strip()
        return PlanResult(sql=sql, metrics=())

    def interpret(
        self,
        question: str,
        sql: str,
        columns: list[str],
        rows: list[dict[str, Any]],
        layer: SemanticLayer,
    ) -> str:
        table_md = markdown_table(columns, rows[:30], layer)
        system = (
            "Eres un analista de negocio para directivos de e-commerce en Latinoamérica. "
            "Escribe en español profesional, en un párrafo de como máximo 5 oraciones. "
            "Usa ÚNICAMENTE números que aparezcan literalmente en la tabla de resultados, "
            "con el mismo formato (miles con punto, decimales con coma, porcentajes con %). "
            "No inventes causas, no prometas acciones y no agregues cifras externas."
        )
        user = f"Pregunta: {question}\n\nSQL ejecutada:\n{sql}\n\nResultados:\n{table_md}"
        return self._converse(system, user, max_tokens=500)


def semantic_context(layer: SemanticLayer) -> str:
    """Render the governed layer as plain text for the LLM prompt."""
    lines: list[str] = ["Tablas autorizadas (solo Gold):"]
    for table in layer.authorized_tables:
        lines.append(f"- {table.name} (grano: {table.grain}): {table.description}")
        for col in table.columns:
            lines.append(f"    {col.name} [{col.type}, {col.role}]: {col.description}")
    lines.append("Métricas aprobadas:")
    for metric in layer.metrics:
        lines.append(f"- {metric.name} sobre {metric.table}: {metric.sql} ({metric.definition})")
    lines.append("Prohibido: Bronze, Silver, cuarentena, DDL, DML, COPY, ATTACH.")
    return "\n".join(lines)


def markdown_table(
    columns: list[str], rows: list[dict[str, Any]], layer: SemanticLayer | None = None
) -> str:
    """Render a result table as Markdown with Spanish number formatting."""
    if not columns:
        return "(sin columnas)"
    units = {m.name: m.unit for m in layer.metrics} if layer else {}
    header = "| " + " | ".join(columns) + " |"
    sep = "|" + "|".join("---" for _ in columns) + "|"
    body = [
        "| "
        + " | ".join(
            _format_value(row[c], units.get(c)) if units.get(c) else _cell(row[c]) for c in columns
        )
        + " |"
        for row in rows
    ]
    return "\n".join([header, sep, *body]) if body else "\n".join([header, sep, "| (sin filas) |"])


def _cell(value: Any) -> str:
    if value is None:
        return "s/d"
    if isinstance(value, bool):
        return "sí" if value else "no"
    if isinstance(value, (int, float, Decimal)):
        return _format_value(value, None)
    return str(value)


def provider_for(settings: Settings, name: str | None = None) -> LLMProvider:
    """Build the configured provider (``mock`` or ``bedrock``)."""
    choice = name or settings.llm_provider
    if choice == "bedrock":
        return BedrockProvider(settings)
    return MockProvider(row_limit=settings.sql_row_limit)


def execute_readonly(sql: str, gold: Mapping[str, pa.Table]) -> pa.Table:
    """Run a validated statement over in-memory Gold tables (external access disabled)."""
    con = duckdb.connect(config={"enable_external_access": False})
    try:
        for name, table in gold.items():
            con.register(name, table)
        return relation_to_arrow(con.sql(sql))
    finally:
        con.close()


class BiAnalyst:
    """Answers business questions with guardrails and an auditable response object."""

    def __init__(
        self,
        settings: Settings,
        provider: LLMProvider,
        layer: SemanticLayer,
        gold: Mapping[str, pa.Table],
    ) -> None:
        self._settings = settings
        self._provider = provider
        self._layer = layer
        self._gold = gold

    def answer(self, question: str) -> AnalystResponse:
        now = datetime.now(UTC)
        base: dict[str, Any] = {
            "question": question,
            "provider": self._provider.name,
            "generated_at": now,
        }
        try:
            plan = self._provider.plan(question, self._layer)
        except Exception as exc:  # provider/network errors are reported, not raised
            return AnalystResponse(status="error", error=f"proveedor LLM: {exc}", **base)
        if plan is None:
            return AnalystResponse(
                status="unanswerable",
                error="la pregunta no puede responderse con la capa semántica autorizada",
                **base,
            )
        try:
            guarded = validate_sql(plan.sql, self._layer, self._settings.sql_row_limit)
        except GuardrailViolationError as violation:
            return AnalystResponse(
                status="blocked",
                sql=plan.sql,
                metrics=list(plan.metrics),
                error=str(violation),
                **base,
            )
        try:
            table = execute_readonly(guarded.sql, self._gold)
        except duckdb.Error as exc:
            return AnalystResponse(
                status="error",
                sql=guarded.sql,
                metrics=list(plan.metrics),
                error=f"error de ejecución: {exc}",
                **base,
            )

        columns = list(table.column_names)
        rows = table.to_pylist()
        interpretation, source = self._read(question, guarded.sql, columns, rows)
        return AnalystResponse(
            status="ok",
            metrics=list(plan.metrics),
            sql=guarded.sql,
            tables=list(guarded.tables),
            columns=columns,
            rows=rows,
            row_count=len(rows),
            interpretation=interpretation,
            interpretation_source=source,
            **base,
        )

    def _read(
        self, question: str, sql: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> tuple[str, Literal["mock", "llm", "mock_fallback"]]:
        if self._provider.name == "mock":
            return self._provider.interpret(question, sql, columns, rows, self._layer), "mock"
        try:
            text = self._provider.interpret(question, sql, columns, rows, self._layer)
        except Exception:
            text = ""
        if text and is_grounded(text, columns, rows):
            return text, "llm"
        fallback = mock_reading(columns, rows, self._layer)
        return fallback, "mock_fallback"


SHOWCASE_STATEMENTS: tuple[tuple[str, str], ...] = (
    ("SELECT channel, net_margin_amount FROM mart_daily_unit_economics LIMIT 5", "lectura Gold"),
    ("SELECT order_id FROM mart_daily_unit_economics LIMIT 5", "columna fuera de la capa"),
    ("DROP TABLE mart_daily_unit_economics", "DDL"),
    ("DELETE FROM mart_sku_health WHERE sku = 'REM01-NEG-M'", "DML"),
    ("SELECT * FROM silver_orders LIMIT 5", "acceso directo a Silver"),
    ("SELECT * FROM bronze_orders LIMIT 5", "acceso directo a Bronze"),
    ("SELECT * FROM read_parquet('/etc/passwd')", "función de lectura de archivos"),
    ("SELECT 1; DROP TABLE mart_sku_health", "múltiples sentencias"),
    ("INSERT INTO mart_sku_health SELECT * FROM mart_sku_health", "escritura disfrazada"),
    ("WITH x AS (SELECT sku FROM mart_sku_health) DELETE FROM x", "DML con CTE"),
    ("SELECT sku FROM mart_sku_health -- comentario", "comentario SQL"),
)


def guardrail_showcase(layer: SemanticLayer, row_limit: int) -> list[dict[str, str]]:
    """Run adversarial statements through the validator and report each outcome."""
    results: list[dict[str, str]] = []
    for statement, label in SHOWCASE_STATEMENTS:
        try:
            validate_sql(statement, layer, row_limit)
            outcome, detail = "permitida", "pasó todos los controles"
        except GuardrailViolationError as violation:
            outcome, detail = "bloqueada", violation.code
        results.append(
            {"sentencia": statement, "motivo": label, "resultado": outcome, "código": detail}
        )
    return results
