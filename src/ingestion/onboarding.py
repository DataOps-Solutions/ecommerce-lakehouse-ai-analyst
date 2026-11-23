"""CSV onboarding: detect a file's layout and suggest a mapping to the canonical schema.

The suggestion is built from the templates in ``mappings/``. Headers that match a canonical
field keep that field. Every other header becomes a candidate ``custom_fields`` entry, named
in snake_case, and flagged as ``drop`` when its name looks personal. The suggested document is
validated with the same :class:`MappingFile` model used by ingestion. Nothing is persisted here.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from src.config import PROJECT_ROOT
from src.ingestion.csv_adapter import snake
from src.ingestion.mapping import (
    CUSTOM_FIELD_NAME,
    SOURCE_FIELDS,
    MappingFile,
    load_mapping,
    normalize_header,
)
from src.ingestion.privacy import looks_personal

TEMPLATE_DIR = PROJECT_ROOT / "mappings"
SAMPLE_ROWS = 5
DECIMAL_SCAN_ROWS = 200
_COMMA_DECIMAL = re.compile(r"^-?\d{1,3}(\.\d{3})*,\d{1,2}$|^-?\d+,\d{1,2}$")
_DOT_DECIMAL = re.compile(r"^-?\d{1,3}(,\d{3})*\.\d{1,2}$|^-?\d+\.\d{1,2}$")


@dataclass
class InspectResult:
    """Everything the onboarding preview shows about one uploaded file."""

    filename: str
    encoding: str
    delimiter: str
    decimal: str
    headers: list[str]
    sample_rows: list[dict[str, str]]
    detected_source: str | None = None
    template: str | None = None
    canonical_matches: dict[str, str] = field(default_factory=dict)
    missing_required: list[str] = field(default_factory=list)
    suggested_custom_fields: dict[str, list[str]] = field(default_factory=dict)
    suggested_sensitive: dict[str, str] = field(default_factory=dict)
    mapping_document: dict[str, Any] | None = None
    mapping_yaml: str | None = None
    mapping_errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def decode_csv(content: bytes) -> tuple[str, str]:
    """Text and encoding of an upload. UTF-8 (with or without BOM) first, then cp1252."""
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return content.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("el archivo no está en UTF-8 ni en cp1252")


def guess_delimiter(first_line: str) -> str:
    counts = {d: first_line.count(d) for d in (";", ",", "\t")}
    best = max(counts, key=lambda d: counts[d])
    return best if counts[best] > 0 else ","


def guess_decimal(values: list[str]) -> str:
    comma = sum(1 for v in values if _COMMA_DECIMAL.match(v.strip().replace("$", "").strip()))
    dot = sum(1 for v in values if _DOT_DECIMAL.match(v.strip().replace("$", "").strip()))
    return "," if comma > dot else "."


def _custom_name(header: str, taken: set[str], index: int) -> str:
    name = snake(header)[:40]
    if not CUSTOM_FIELD_NAME.match(name):
        name = f"campo_{index}"
    base, suffix = name, 2
    while name in taken:
        name = f"{base[:36]}_{suffix}"
        suffix += 1
    return name


@dataclass
class _Candidate:
    source: str
    template: str
    mapping: MappingFile
    matches: dict[str, str]
    missing: list[str]
    score: int


def _match_source(
    headers: list[str], mapping: MappingFile, source: str, template_name: str
) -> _Candidate:
    by_normalized: dict[str, str] = {}
    for header in headers:
        by_normalized.setdefault(normalize_header(header), header)
    matches: dict[str, str] = {}
    for name, aliases in mapping.sources[source].columns.items():
        for alias in aliases:
            actual = by_normalized.get(normalize_header(alias))
            if actual is not None:
                matches[name] = actual
                break
    missing = sorted(
        f for f, required in SOURCE_FIELDS[source].items() if required and f not in matches
    )
    return _Candidate(
        source, template_name, mapping, matches, missing, len(matches) - 10 * len(missing)
    )


def _best_candidate(headers: list[str], templates_dir: Path) -> _Candidate | None:
    best: _Candidate | None = None
    for path in sorted(templates_dir.glob("*.yaml")):
        mapping = load_mapping(path)
        for source in mapping.sources:
            candidate = _match_source(headers, mapping, source, path.name)
            if candidate.matches and (best is None or candidate.score > best.score):
                best = candidate
    return best


def inspect_csv(content: bytes, filename: str, templates_dir: Path = TEMPLATE_DIR) -> InspectResult:
    """Read the layout of one CSV file and suggest a mapping for it."""
    text, encoding = decode_csv(content)
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("el archivo está vacío")
    delimiter = guess_delimiter(lines[0])
    reader = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    headers = [h.strip() for h in reader[0]]
    if len(set(headers)) != len(headers):
        raise ValueError("hay encabezados repetidos; renómbrelos antes de cargar el archivo")
    body = reader[1:]
    sample_rows = [
        {h: (row[i].strip() if i < len(row) else "") for i, h in enumerate(headers)}
        for row in body[:SAMPLE_ROWS]
    ]
    values = [cell for row in body[:DECIMAL_SCAN_ROWS] for cell in row]
    decimal = guess_decimal(values)
    result = InspectResult(
        filename=filename,
        encoding=encoding,
        delimiter=delimiter,
        decimal=decimal,
        headers=headers,
        sample_rows=sample_rows,
    )
    best = _best_candidate(headers, templates_dir)
    if best is None:
        result.warnings.append(
            "No se reconoce ninguna fuente conocida en los encabezados. "
            "Revise que la primera fila contenga los nombres de columna."
        )
        return result

    source = best.source
    template = best.mapping
    result.detected_source = source
    result.template = best.template
    result.canonical_matches = dict(sorted(best.matches.items()))
    result.missing_required = best.missing
    used = set(best.matches.values())
    canonical_names = set(SOURCE_FIELDS[source])
    taken: set[str] = set(canonical_names)
    custom: dict[str, list[str]] = {}
    sensitive: dict[str, str] = {}
    free_headers = [h for h in headers if h and h not in used]
    for index, header in enumerate(free_headers, start=1):
        name = _custom_name(header, taken, index)
        taken.add(name)
        custom[name] = [header]
        if looks_personal(name):
            sensitive[name] = "drop"
    result.suggested_custom_fields = custom
    result.suggested_sensitive = sensitive

    document: dict[str, Any] = {
        "name": f"onboarding_{source}",
        "description": (
            f"Mapeo sugerido para {filename}. Generado por inspección: revisar antes de cargar."
        ),
        "currency": "ARS",
        "format": {
            "encoding": encoding,
            "delimiter": delimiter,
            "decimal": decimal,
            "date_format": template.format.date_format,
        },
        "estimation": template.estimation.model_dump(mode="json", exclude_none=True),
        "sources": {
            source: {
                "file": filename,
                "columns": {name: [header] for name, header in best.matches.items()},
                "custom_fields": custom,
                "sensitive": sensitive,
            }
        },
    }
    result.mapping_document = document
    result.mapping_yaml = yaml.safe_dump(document, allow_unicode=True, sort_keys=False)
    try:
        MappingFile.model_validate(document)
    except ValidationError as exc:
        result.mapping_errors = [err["msg"] for err in exc.errors()]
    except ValueError as exc:
        result.mapping_errors = [str(exc)]
    if best.missing:
        result.warnings.append(
            "Faltan columnas obligatorias para "
            f"{source}: {', '.join(best.missing)}. Agréguelas o asígnelas en el mapeo."
        )
    result.warnings.append(
        f"Formato de fecha copiado de la plantilla {best.template}: "
        f"verifique que coincida con su archivo ({template.format.date_format})."
    )
    return result
