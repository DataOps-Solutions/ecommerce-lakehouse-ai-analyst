"""Declarative column mappings for real store/marketplace CSV exports.

A mapping file (YAML) binds the exported column headers of a store to the canonical fields
the pipeline expects. Header aliases are matched after normalization (case, accents,
spaces and underscores are ignored), so ``"Número de pedido"`` and ``"numero_de_pedido"``
are the same column.

Canonical sources:

* ``orders``: one row per order line. Feeds ``orders``, ``order_items`` and ``payments``.
* ``products``: catalog and unit costs (optionally stock). Feeds ``products``.
* ``returns``: returns and refunds. Feeds ``returns``.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class MappingError(ValueError):
    """Raised when a mapping file or an input file does not match the expected contract."""


# canonical field -> required?
SOURCE_FIELDS: dict[str, dict[str, bool]] = {
    "orders": {
        "order_id": True,
        "order_date": True,
        "order_status": True,
        "channel": True,
        "sku": True,
        "quantity": True,
        "unit_price": True,
        "payment_method": True,
        "product_name": False,
        "line_discount": False,
        "shipping_cost": False,
        "order_discount": False,
        "payment_status": False,
        "amount_paid": False,
        "gateway_fee": False,
        "customer_id": False,
        "customer_name": False,
        "customer_email": False,
        "customer_phone": False,
        "customer_dni": False,
        "customer_address": False,
    },
    "products": {
        "sku": True,
        "product_name": False,
        "category": False,
        "size": False,
        "unit_cost": False,
        "list_price": False,
        "stock_on_hand": False,
    },
    "returns": {
        "order_id": True,
        "sku": True,
        "return_date": True,
        "quantity": True,
        "refund_amount": True,
        "return_id": False,
        "reason": False,
    },
}

PERSONAL_FIELDS = (
    "customer_name",
    "customer_email",
    "customer_phone",
    "customer_dni",
    "customer_address",
)
SENSITIVE_HINTS = (
    "mail",
    "telefono",
    "phone",
    "celular",
    "dni",
    "documento",
    "direccion",
    "address",
    "nombre",
    "name",
    "cliente",
    "customer",
    "cuit",
    "tarjeta",
    "card",
)


def normalize_header(value: str) -> str:
    """Case, accent, space and underscore insensitive form of a column header."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    collapsed = "".join(ch for ch in ascii_text.lower() if ch.isalnum())
    return collapsed


class CsvFormat(BaseModel):
    """How the file is encoded and how numbers and dates are written."""

    encoding: str = "utf-8-sig"
    delimiter: str = ","
    decimal: Literal[".", ","] = "."
    date_format: str | None = None

    @field_validator("delimiter")
    @classmethod
    def _single_char(cls, value: str) -> str:
        if len(value) != 1:
            raise ValueError("delimiter must be a single character")
        return value


CUSTOM_FIELD_NAME = re.compile(r"^[a-z][a-z0-9_]{1,39}$")


class SourceSpec(BaseModel):
    """One exported file and the mapping of its headers to canonical fields.

    ``custom_fields`` declares extra attributes (for example ``color`` or ``material``). They
    are kept as JSON in the ``custom_attributes`` column, never as canonical columns.
    """

    file: str
    columns: dict[str, list[str]]
    custom_fields: dict[str, list[str]] = Field(default_factory=dict)
    sensitive: dict[str, Literal["drop", "pseudonymize"]] = Field(default_factory=dict)


class Estimation(BaseModel):
    """Explicit assumptions used only when the export lacks an exact value.

    Every value filled with one of these assumptions is flagged as ``estimated``.
    """

    default_cost_ratio: float | None = Field(default=None, gt=0, lt=1)
    default_gateway_rate: float | None = Field(default=None, ge=0, lt=1)
    gateway_rates: dict[str, float] = Field(default_factory=dict)


class MappingFile(BaseModel):
    """A complete mapping document (one YAML file)."""

    name: str
    description: str
    currency: str = "ARS"
    format: CsvFormat = Field(default_factory=CsvFormat)
    estimation: Estimation = Field(default_factory=Estimation)
    sources: dict[str, SourceSpec]

    @field_validator("currency")
    @classmethod
    def _supported_currency(cls, value: str) -> str:
        if value.upper() != "ARS":
            raise ValueError(f"unsupported currency {value}; the pipeline only models ARS")
        return value.upper()

    @model_validator(mode="after")
    def _check_sources(self) -> MappingFile:
        for source, spec in self.sources.items():
            if source not in SOURCE_FIELDS:
                raise ValueError(f"unknown source '{source}' in mapping {self.name}")
            known = SOURCE_FIELDS[source]
            unknown = sorted(set(spec.columns) - set(known))
            if unknown:
                raise ValueError(f"unknown canonical fields for {source}: {unknown}")
            missing = sorted(f for f, req in known.items() if req and f not in spec.columns)
            if missing:
                raise ValueError(
                    f"mapping {self.name} lacks required fields for {source}: {missing}"
                )
            for name, aliases in spec.custom_fields.items():
                if not CUSTOM_FIELD_NAME.match(name):
                    raise ValueError(
                        f"invalid custom field name '{name}' in {source}: use lowercase "
                        "snake_case, 2-40 characters"
                    )
                if name in known:
                    raise ValueError(f"custom field '{name}' collides with a canonical field")
                if not aliases:
                    raise ValueError(f"custom field '{name}' in {source} has no column aliases")
            bad_sensitive = sorted(set(spec.sensitive) - set(known) - set(spec.custom_fields))
            if bad_sensitive:
                raise ValueError(f"unknown sensitive fields for {source}: {bad_sensitive}")
        return self


def load_mapping(path: Path) -> MappingFile:
    """Load and validate one mapping YAML file."""
    if not path.exists():
        raise MappingError(f"mapping file not found: {path}")
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise MappingError(f"mapping file {path} must contain a YAML mapping")
    try:
        return MappingFile.model_validate(raw)
    except ValueError as exc:
        raise MappingError(f"invalid mapping {path}: {exc}") from exc


def resolve_custom_columns(headers: list[str], spec: SourceSpec) -> dict[str, str]:
    """Custom attribute name -> actual header, for the custom fields present in the file.

    Missing custom columns are not an error: the attribute is simply absent for every row.
    """
    by_normalized: dict[str, str] = {}
    for header in headers:
        by_normalized.setdefault(normalize_header(header), header)
    resolved: dict[str, str] = {}
    for name, aliases in spec.custom_fields.items():
        for alias in aliases:
            actual = by_normalized.get(normalize_header(alias))
            if actual is not None:
                resolved[name] = actual
                break
    return resolved


def resolve_columns(headers: list[str], spec: SourceSpec, source: str) -> dict[str, str]:
    """Map canonical fields to the actual header names found in the file.

    Returns only the fields that were found. Raises :class:`MappingError` when a required
    canonical field has no matching header.
    """
    by_normalized: dict[str, str] = {}
    for header in headers:
        by_normalized.setdefault(normalize_header(header), header)
    resolved: dict[str, str] = {}
    for field, aliases in spec.columns.items():
        for alias in aliases:
            actual = by_normalized.get(normalize_header(alias))
            if actual is not None:
                resolved[field] = actual
                break
    missing = sorted(f for f, req in SOURCE_FIELDS[source].items() if req and f not in resolved)
    if missing:
        raise MappingError(
            f"file '{spec.file}' ({source}) lacks columns for required fields {missing}; "
            f"headers found: {headers}"
        )
    return resolved
