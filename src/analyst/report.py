"""Renders the BI report (``output/bi_report.md`` and ``output/bi_report.json``) in Spanish."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from src.analyst.bi_agent import markdown_table
from src.analyst.semantic import SemanticLayer
from src.models import AnalystResponse

STATUS_ES = {
    "ok": "OK",
    "blocked": "Bloqueada por guardrail",
    "unanswerable": "No respondible con la capa semántica",
    "error": "Error",
}
SOURCE_ES = {
    "mock": "determinística (sin LLM)",
    "llm": "LLM (verificada contra la tabla)",
    "mock_fallback": "determinística (fallback: el LLM citó cifras no presentes en la tabla)",
}


def render_markdown(
    responses: Sequence[AnalystResponse],
    showcase: Sequence[dict[str, str]],
    layer: SemanticLayer,
    meta: dict[str, Any],
) -> str:
    """Build the Markdown report for a list of analyst responses."""
    lines: list[str] = [
        "# Reporte BI: Mini Lakehouse + AI/BI Analyst",
        "",
        f"- Generado: {meta['generated_at']}",
        f"- Proveedor LLM: `{meta['provider']}`",
        f"- Almacenamiento: `{meta['storage']}`",
        f"- Capa semántica: {layer.domain} (moneda {layer.currency})",
        "- Política: solo `SELECT`/`WITH` sobre tablas Gold autorizadas. "
        "Bronze, Silver y cuarentena están bloqueadas.",
        "",
    ]
    for index, response in enumerate(responses, start=1):
        lines.extend(_render_response(index, response, layer))
    lines.extend(_render_showcase(showcase))
    return "\n".join(lines).rstrip() + "\n"


def _render_response(index: int, response: AnalystResponse, layer: SemanticLayer) -> list[str]:
    lines = [
        f"## {index}. {response.question}",
        "",
        f"- Estado: **{STATUS_ES.get(response.status, response.status)}**",
    ]
    if response.metrics:
        lines.append(f"- Métricas: {', '.join(response.metrics)}")
    if response.tables:
        lines.append(f"- Tablas consultadas: {', '.join(response.tables)}")
    if response.status != "ok":
        lines.append(f"- Motivo: {response.error}")
        if response.sql:
            lines.extend(["", "SQL rechazada:", "", "```sql", response.sql, "```"])
        lines.append("")
        return lines

    lines.extend(
        [
            f"- Filas devueltas: {response.row_count}",
            "",
            "**SQL ejecutada**",
            "",
            "```sql",
            response.sql or "",
            "```",
            "",
            "**Tabla de resultados**",
            "",
            markdown_table(response.columns, response.rows[:50], layer),
            "",
        ]
    )
    if response.row_count > 50:
        lines.append(f"_Se muestran 50 de {response.row_count} filas._")
        lines.append("")
    source = SOURCE_ES.get(response.interpretation_source or "", "")
    lines.extend(
        [
            f"**Interpretación ejecutiva** (fuente: {source})",
            "",
            response.interpretation or "",
            "",
        ]
    )
    return lines


def _render_showcase(showcase: Sequence[dict[str, str]]) -> list[str]:
    lines = [
        "## Controles de seguridad (guardrails)",
        "",
        "Sentencias adversariales validadas antes de tocar el motor. "
        "Todas las que no sean SELECT sobre Gold deben quedar bloqueadas.",
        "",
        "| Sentencia | Motivo | Resultado | Código |",
        "|---|---|---|---|",
    ]
    for item in showcase:
        sentence = item["sentencia"].replace("|", "\\|")
        lines.append(
            f"| `{sentence}` | {item['motivo']} | {item['resultado']} | {item['código']} |"
        )
    lines.append("")
    return lines


def write_reports(
    output_dir: Path,
    responses: Sequence[AnalystResponse],
    showcase: Sequence[dict[str, str]],
    layer: SemanticLayer,
    meta: dict[str, Any],
) -> tuple[Path, Path]:
    """Write ``bi_report.md`` and ``bi_report.json`` into ``output_dir``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / "bi_report.md"
    json_path = output_dir / "bi_report.json"
    md_path.write_text(render_markdown(responses, showcase, layer, meta), encoding="utf-8")
    payload = {
        "meta": {k: str(v) for k, v in meta.items()},
        "responses": [r.model_dump(mode="json") for r in responses],
        "guardrail_showcase": list(showcase),
    }
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    return md_path, json_path


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
