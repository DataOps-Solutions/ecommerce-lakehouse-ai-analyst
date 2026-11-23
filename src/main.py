"""Command-line entry point.

Usage::

    python -m src.main run-pipeline           # synthetic data: Bronze -> Silver -> Gold
    python -m src.main ingest-csv             # real exports described by mappings/*.yaml
    python -m src.main ask "¿Cuál fue el margen neto por canal?"
    python -m src.main demo                   # demo questions + guardrail showcase
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

import pyarrow as pa

from src.analyst.bi_agent import BiAnalyst, guardrail_showcase, provider_for
from src.analyst.report import write_reports
from src.analyst.semantic import SemanticLayer, load_semantic_layer
from src.config import Settings, get_settings
from src.engine.runs import RunTracker
from src.ingestion.mapping import MappingError
from src.models import AnalystResponse
from src.pipeline import RunOutcome, load_gold_tables, run_csv_ingestion, run_pipeline
from src.reporting.client_summary import build_client_summary, write_client_summary

DEMO_QUESTIONS: tuple[str, ...] = (
    "¿Cuál fue el margen neto por canal?",
    "¿Cuál es la tasa de conversión por medio de pago?",
    "¿Qué SKUs tienen la mayor tasa de devolución? top 10",
    "¿Cuántas alertas de stock hay por categoría?",
    "¿Cuál fue la venta bruta y los descuentos por día?",
    "¿Cuál es el clima de mañana en Buenos Aires?",
)
DEFAULT_MAPPINGS: tuple[str, ...] = (
    "mappings/generic_store_orders.yaml",
    "mappings/costs_and_stock.yaml",
)
DEFAULT_CSV_INPUT = "data/samples_real"
DEFAULT_CSV_LAKEHOUSE = "data/lakehouse_csv"


def _build_analyst(
    settings: Settings, gold: dict[str, pa.Table], provider_name: str | None
) -> tuple[BiAnalyst, SemanticLayer, str]:
    layer = load_semantic_layer(settings.resolve_path(settings.semantic_layer_path))
    provider = provider_for(settings, provider_name)
    return BiAnalyst(settings, provider, layer, gold), layer, provider.name


def _report_meta(settings: Settings, provider: str) -> dict[str, str]:
    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "provider": provider,
        "storage": settings.lakehouse_storage,
    }


def _print_pipeline(outcome: RunOutcome) -> None:
    report = outcome.report
    print(f"Pipeline {report.run_id} ({report.storage}) finalizado.")
    print("Bronze: filas cargadas por batch:")
    for batch, rows in report.batches_loaded.items():
        print(f"  {batch}: {rows}")
    print("Silver / cuarentena:")
    print(f"  {'entidad':<16}{'bronze':>8}{'row-gate':>10}{'dupes':>8}", end="")
    print(f"{'integr.':>9}{'silver':>9}{'cuarent.':>10}")
    for r in report.entities:
        print(
            f"  {r.entity:<16}{r.bronze_rows:>8}{r.row_rule_rejections:>10}"
            f"{r.duplicates_removed:>8}{r.integrity_rejections:>9}{r.silver_rows:>9}"
            f"{r.quarantine_rows:>10}"
        )
    print("Gold:")
    for name, rows in report.gold_rows.items():
        print(f"  {name}: {rows} filas")


def _write_pipeline_report(settings: Settings, outcome: RunOutcome, workspace_id: str) -> Path:
    reports = settings.workspace(workspace_id).reports_dir(
        settings.resolve_path(settings.output_dir)
    )
    out = reports / "pipeline_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(outcome.report.model_dump_json(indent=2), encoding="utf-8")
    return out


def cmd_run_pipeline(settings: Settings, args: argparse.Namespace) -> int:
    outcome = run_pipeline(settings, args.workspace)
    _print_pipeline(outcome)
    print(f"Reporte de pipeline: {_write_pipeline_report(settings, outcome, args.workspace)}")
    print(f"Run registrado: {outcome.report.run_id} (workspace {args.workspace})")
    return 0


def cmd_runs(settings: Settings, args: argparse.Namespace) -> int:
    """Run history of a workspace: latest runs, or the full event trail of one run."""
    if args.lakehouse_dir:
        settings = settings.model_copy(update={"lakehouse_local_dir": Path(args.lakehouse_dir)})
    tracker = RunTracker(settings.workspace(args.workspace), settings.storage_options())
    if args.run_id:
        events = tracker.history(args.run_id)
        if not events:
            print(f"No hay runs con id {args.run_id} en el workspace {args.workspace}.")
            return 1
        for event in events:
            print(f"{event.event_at:%Y-%m-%d %H:%M:%S}  {event.status:<10} {event.pipeline_name}")
            if event.error_message:
                print(f"  error: {event.error_message}")
        print(json.dumps(events[-1].model_dump(mode="json"), ensure_ascii=False, indent=2))
        return 0
    runs = tracker.list_runs(limit=args.limit)
    if not runs:
        print(f"Sin runs registrados en el workspace {args.workspace}.")
        return 0
    header = f"{'run_id':<14}{'pipeline':<20}{'estado':<11}{'inicio':<20}"
    print(header + f"{'filas in':>10}{'filas out':>11}{'cuarent.':>10}{'costo est.%':>13}")
    for run in runs:
        m = run.metrics
        print(
            f"{run.run_id:<14}{run.pipeline_name:<20}{run.status:<11}"
            f"{run.started_at:%Y-%m-%d %H:%M}   "
            f"{_cell(m.rows_in):>10}{_cell(m.rows_out):>11}{_cell(m.quarantine_rows):>10}"
            f"{_cell(m.estimated_cost_pct):>13}"
        )
    return 0


def _cell(value: float | int | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return f"{value:,}"


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
    analyst, layer, provider_name = _build_analyst(
        settings, load_gold_tables(settings, args.workspace), args.provider
    )
    response = analyst.answer(args.question)
    _print_response(response)
    out_dir = settings.resolve_path(settings.output_dir)
    write_reports(out_dir, [response], [], layer, _report_meta(settings, provider_name))
    print(f"Reporte: {out_dir / 'bi_report.md'}")
    return 0 if response.status == "ok" else 1


def _run_demo_reports(
    settings: Settings, gold: dict[str, pa.Table], provider_name: str | None, out_dir: Path
) -> tuple[Path, Path]:
    analyst, layer, name = _build_analyst(settings, gold, provider_name)
    responses = [analyst.answer(question) for question in DEMO_QUESTIONS]
    for response in responses:
        _print_response(response)
    showcase = guardrail_showcase(layer, settings.sql_row_limit)
    return write_reports(out_dir, responses, showcase, layer, _report_meta(settings, name))


def cmd_demo(settings: Settings, args: argparse.Namespace) -> int:
    out_dir = settings.workspace(args.workspace).reports_dir(
        settings.resolve_path(settings.output_dir)
    )
    md_path, json_path = _run_demo_reports(
        settings, load_gold_tables(settings, args.workspace), args.provider, out_dir
    )
    print(f"\nReporte Markdown: {md_path}")
    print(f"Reporte JSON: {json_path}")
    return 0


def cmd_ingest_csv(settings: Settings, args: argparse.Namespace) -> int:
    input_dir = settings.resolve_path(Path(args.input_dir))
    lakehouse_dir = settings.resolve_path(Path(args.lakehouse_dir))
    mappings = [settings.resolve_path(Path(p)) for p in args.mapping]
    try:
        result = run_csv_ingestion(
            settings, input_dir, mappings, lakehouse_dir, workspace_id=args.workspace
        )
    except MappingError as exc:
        print(f"Error de configuración: {exc}", file=sys.stderr)
        return 2
    outcome = result.outcome
    _print_pipeline(outcome)
    _write_pipeline_report(settings, outcome, args.workspace)

    out_dir = settings.resolve_path(Path(args.output_dir))
    summary = build_client_summary(
        gold=outcome.gold,
        silver=outcome.silver,
        quarantine=outcome.quarantine,
        quality=outcome.report.entities,
        ingestion=result.ingestion,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    md_path, json_path = write_client_summary(out_dir, summary)
    bi_md, _ = _run_demo_reports(settings, outcome.gold, args.provider, out_dir)
    print(f"\nResumen para el cliente: {md_path}")
    print(f"Resumen (JSON): {json_path}")
    print(f"Reporte BI: {bi_md}")
    return 0


def _add_workspace(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workspace",
        default=None,
        help="Workspace (tenant). Por defecto, WORKSPACE_ID o 'default'.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ecommerce-lakehouse", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run-pipeline", aliases=["pipeline"], help="Bronze -> Silver -> Gold")
    _add_workspace(run)
    run.set_defaults(handler=cmd_run_pipeline)
    ask = sub.add_parser("ask", help="Responde una pregunta de negocio")
    ask.add_argument("question", help="Pregunta en español")
    ask.add_argument("--provider", choices=["mock", "bedrock"], default=None)
    _add_workspace(ask)
    ask.set_defaults(handler=cmd_ask)
    demo = sub.add_parser("demo", help="Ejecuta las preguntas de demo y los guardrails")
    demo.add_argument("--provider", choices=["mock", "bedrock"], default=None)
    _add_workspace(demo)
    demo.set_defaults(handler=cmd_demo)
    runs = sub.add_parser("runs", help="Historial de ejecuciones registradas del workspace")
    _add_workspace(runs)
    runs.add_argument("--limit", type=int, default=20, help="Cantidad de runs a listar")
    runs.add_argument("--run-id", default=None, help="Detalle y trazabilidad de un run")
    runs.add_argument(
        "--lakehouse-dir",
        default=None,
        help="Lakehouse a consultar, por ejemplo data/lakehouse_csv para cargas CSV.",
    )
    runs.set_defaults(handler=cmd_runs)
    csv_cmd = sub.add_parser(
        "ingest-csv", help="Carga exports reales (CSV) según mappings y genera el resumen"
    )
    csv_cmd.add_argument("--input-dir", default=DEFAULT_CSV_INPUT)
    csv_cmd.add_argument(
        "--mapping",
        action="append",
        default=None,
        help="Archivo YAML de mapeo (repetible). Por defecto, los de mappings/.",
    )
    csv_cmd.add_argument("--lakehouse-dir", default=DEFAULT_CSV_LAKEHOUSE)
    csv_cmd.add_argument("--output-dir", default="output")
    csv_cmd.add_argument("--provider", choices=["mock", "bedrock"], default=None)
    _add_workspace(csv_cmd)
    csv_cmd.set_defaults(handler=cmd_ingest_csv)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if getattr(args, "command", None) == "ingest-csv" and args.mapping is None:
        args.mapping = list(DEFAULT_MAPPINGS)
    settings = get_settings()
    if getattr(args, "workspace", None) is None:
        args.workspace = settings.workspace_id
    try:
        return int(args.handler(settings, args))
    except (RuntimeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


def _exit_now(code: int) -> NoReturn:
    """Exit without waiting for native deltalake worker threads to shut down.

    Observed in the development environment: after a Delta table is read into Python objects,
    the interpreter occasionally never finishes shutting down. Every file is closed before
    this point, so skipping interpreter teardown loses no data. Remove this once the
    upstream hang is confirmed fixed.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    _exit_now(main())
