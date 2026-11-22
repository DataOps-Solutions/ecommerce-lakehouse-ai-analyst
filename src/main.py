"""Command-line entry point.

Usage::

    python -m src.main run-pipeline           # Bronze -> Silver -> Gold (alias: pipeline)
    python -m src.main ask "¿Cuál fue el margen neto por canal?"
    python -m src.main demo                   # demo questions + guardrail showcase
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from src.analyst.bi_agent import BiAnalyst, guardrail_showcase, provider_for
from src.analyst.report import write_reports
from src.analyst.semantic import SemanticLayer, load_semantic_layer
from src.config import Settings, get_settings
from src.models import AnalystResponse
from src.pipeline import load_gold_tables, run_pipeline

DEMO_QUESTIONS: tuple[str, ...] = (
    "¿Cuál fue el margen neto por canal?",
    "¿Cuál es la tasa de conversión por medio de pago?",
    "¿Qué SKUs tienen la mayor tasa de devolución? top 10",
    "¿Cuántas alertas de stock hay por categoría?",
    "¿Cuál fue la venta bruta y los descuentos por día?",
    "¿Cuál es el clima de mañana en Buenos Aires?",
)


def _build_analyst(
    settings: Settings, provider_name: str | None
) -> tuple[BiAnalyst, SemanticLayer, str]:
    layer = load_semantic_layer(settings.resolve_path(settings.semantic_layer_path))
    gold = load_gold_tables(settings)
    provider = provider_for(settings, provider_name)
    return BiAnalyst(settings, provider, layer, gold), layer, provider.name


def _report_meta(settings: Settings, provider: str) -> dict[str, str]:
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "provider": provider,
        "storage": settings.lakehouse_storage,
    }


def cmd_run_pipeline(settings: Settings, _args: argparse.Namespace) -> int:
    report = run_pipeline(settings)
    print(f"Pipeline {report.run_id} ({report.storage}) finalizado.")
    print("Bronze: filas cargadas por batch:")
    for batch, rows in report.batches_loaded.items():
        print(f"  {batch}: {rows}")
    print("Silver / cuarentena:")
    header = f"  {'entidad':<16}{'bronze':>8}{'row-gate':>10}{'dupes':>8}"
    print(header + f"{'integr.':>9}{'silver':>9}{'cuarent.':>10}")
    for r in report.entities:
        print(
            f"  {r.entity:<16}{r.bronze_rows:>8}{r.row_rule_rejections:>10}"
            f"{r.duplicates_removed:>8}{r.integrity_rejections:>9}{r.silver_rows:>9}"
            f"{r.quarantine_rows:>10}"
        )
    print("Gold:")
    for name, rows in report.gold_rows.items():
        print(f"  {name}: {rows} filas")
    out = settings.resolve_path(settings.output_dir) / "pipeline_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    print(f"Reporte de pipeline: {out}")
    return 0


def _print_response(response: AnalystResponse) -> None:
    print(f"\nPregunta: {response.question}")
    print(f"Estado: {response.status}")
    if response.sql:
        print(f"SQL: {response.sql}")
    if response.error:
        print(f"Motivo: {response.error}")
    if response.interpretation:
        print(f"Interpretación: {response.interpretation}")


def cmd_ask(settings: Settings, args: argparse.Namespace) -> int:
    analyst, layer, provider_name = _build_analyst(settings, args.provider)
    response = analyst.answer(args.question)
    _print_response(response)
    out_dir = settings.resolve_path(settings.output_dir)
    write_reports(out_dir, [response], [], layer, _report_meta(settings, provider_name))
    print(f"Reporte: {out_dir / 'bi_report.md'}")
    return 0 if response.status == "ok" else 1


def cmd_demo(settings: Settings, args: argparse.Namespace) -> int:
    analyst, layer, provider_name = _build_analyst(settings, args.provider)
    responses = [analyst.answer(question) for question in DEMO_QUESTIONS]
    for response in responses:
        _print_response(response)
    showcase = guardrail_showcase(layer, settings.sql_row_limit)
    out_dir = settings.resolve_path(settings.output_dir)
    md_path, json_path = write_reports(
        out_dir, responses, showcase, layer, _report_meta(settings, provider_name)
    )
    print(f"\nReporte Markdown: {md_path}")
    print(f"Reporte JSON: {json_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ecommerce-lakehouse", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run-pipeline", aliases=["pipeline"], help="Bronze -> Silver -> Gold")
    run.set_defaults(handler=cmd_run_pipeline)
    ask = sub.add_parser("ask", help="Responde una pregunta de negocio")
    ask.add_argument("question", help="Pregunta en español")
    ask.add_argument("--provider", choices=["mock", "bedrock"], default=None)
    ask.set_defaults(handler=cmd_ask)
    demo = sub.add_parser("demo", help="Ejecuta las preguntas de demo y los guardrails")
    demo.add_argument("--provider", choices=["mock", "bedrock"], default=None)
    demo.set_defaults(handler=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    try:
        return int(args.handler(settings, args))
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
