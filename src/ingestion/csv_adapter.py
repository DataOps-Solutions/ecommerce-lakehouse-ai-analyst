"""Adapter from exported store/marketplace CSV files to the pipeline's raw Bronze batch.

Design rules:

* Nothing is silently invented. Missing unit costs and gateway fees are estimated only with
  the explicit assumptions of the mapping file, and every such value is flagged
  (``unit_cost_source`` / ``gateway_fee_source`` = ``estimated``). Without an assumption the
  value stays blank and the Silver gates route the record to quarantine.
* Malformed numbers and dates are kept as raw text, so Silver ``TRY_CAST`` turns them into
  NULL and the quality gate reports them. The adapter never raises for bad rows.
* Personal data is filtered here, before Bronze: each sensitive cell is dropped or
  pseudonymized right after the file is read, so the original value never reaches a record.
* The batch id is a hash of the input bytes, the mappings and the salt. Re-running the same
  export is a no-op in Bronze.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from src.data_generator import RawBatch
from src.ingestion.mapping import (
    SENSITIVE_HINTS,
    MappingError,
    MappingFile,
    SourceSpec,
    load_mapping,
    normalize_header,
    resolve_columns,
    resolve_custom_columns,
)
from src.ingestion.privacy import FieldPrivacyStats, PrivacyPolicy
from src.medallion.silver import METHOD_MAP

CSV_SOURCE = "csv_export"
SOLD_STATUS_KEYS = {
    "paid",
    "pagado",
    "pagada",
    "shipped",
    "enviado",
    "enviada",
    "delivered",
    "entregado",
    "entregada",
}
CANCEL_STATUS_KEYS = {"cancelled", "canceled", "cancelado", "cancelada"}
PENDING_STATUS_KEYS = {"pending_payment", "pendiente_pago", "pendiente"}
DATE_FALLBACKS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%d-%m-%Y %H:%M",
    "%d-%m-%Y",
)
CANONICAL_COLUMNS: dict[str, tuple[str, ...]] = {
    "products": (
        "sku",
        "product_name",
        "category",
        "size",
        "unit_cost",
        "unit_cost_source",
        "list_price",
        "stock_on_hand",
    ),
    "orders": (
        "order_id",
        "customer_id",
        "order_ts",
        "order_status",
        "channel",
        "currency",
        "shipping_cost",
        "discount_amount",
    ),
    "order_items": (
        "order_item_id",
        "order_id",
        "sku",
        "quantity",
        "unit_price",
        "discount_amount",
    ),
    "payments": (
        "payment_id",
        "order_id",
        "payment_ts",
        "payment_method",
        "payment_status",
        "amount",
        "gateway_fee",
        "gateway_fee_source",
        "currency",
    ),
    "returns": ("return_id", "order_id", "sku", "return_ts", "quantity", "refund_amount", "reason"),
    "checkout_events": (
        "event_id",
        "session_id",
        "event_ts",
        "event_type",
        "channel",
        "payment_method",
        "order_id",
    ),
}
# Entities whose records carry the JSON column of custom attributes. Lines of the orders file
# hold their attributes on order_items; products and returns keep theirs on their own rows.
for _entity in ("products", "order_items", "returns"):
    CANONICAL_COLUMNS[_entity] = (*CANONICAL_COLUMNS[_entity], "custom_attributes")
CUSTOM_ATTRIBUTES_COLUMN = "custom_attributes"

ORDER_HEADER_FIELDS = (
    "order_date",
    "order_status",
    "channel",
    "shipping_cost",
    "order_discount",
    "payment_method",
    "payment_status",
    "amount_paid",
    "gateway_fee",
    "customer_id",
)
CONFLICT_CHECKED_FIELDS = (
    "order_date",
    "order_status",
    "channel",
    "shipping_cost",
    "order_discount",
    "payment_method",
)


class FileReport(BaseModel):
    """What was read from one input file."""

    source: str
    file: str
    rows_read: int
    mapped_fields: list[str]
    ignored_columns: list[str]


class EstimationStats(BaseModel):
    """Counts of values that were exact, estimated, derived or missing."""

    products_unit_cost_estimated: int = 0
    products_catalog_stubs: int = 0
    payments_gateway_fee_exact: int = 0
    payments_gateway_fee_estimated: int = 0
    payments_without_gateway_fee: int = 0
    payments_status_derived: int = 0
    orders_without_shipping: int = 0
    orders_header_conflicts: int = 0


class IngestionReport(BaseModel):
    """Machine-readable summary of one CSV ingestion."""

    batch_id: str
    mappings: list[str]
    files: list[FileReport]
    rows_by_entity: dict[str, int]
    estimation: EstimationStats = Field(default_factory=EstimationStats)
    privacy: list[FieldPrivacyStats] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


@dataclass
class IngestionResult:
    """Raw batch plus the report that explains how it was produced."""

    batch: RawBatch
    report: IngestionReport


def snake(value: str) -> str:
    """Lower-case snake form without accents: ``"Tarjeta Crédito"`` -> ``tarjeta_credito``."""
    decomposed = unicodedata.normalize("NFKD", value.strip())
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")


def canonical_method(value: str) -> str:
    """Payment method in the vocabulary shared with Silver (e.g. "Tarjeta de crédito")."""
    key = snake(value)
    return METHOD_MAP.get(key, key)


def sku_key(value: str) -> str:
    """Same SKU normalization the Silver layer applies."""
    return value.strip().upper().replace("_", "-")


def order_key(value: str) -> str:
    return value.strip().upper()


def _round_money(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}"


def parse_money(raw: str, decimal: str) -> str:
    """Normalize a money string to ``"1234.50"``.

    Currency symbols and thousands separators are removed according to ``decimal``.
    Unparseable values are returned stripped and unchanged so Silver can reject them.
    """
    text = raw.replace("\u00a0", " ").strip()
    if not text:
        return ""
    cleaned = text.upper().replace("ARS", "").replace("$", "").replace(" ", "")
    if decimal == ",":
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = cleaned.replace(",", "")
    try:
        return _round_money(Decimal(cleaned))
    except InvalidOperation:
        return text


def parse_quantity(raw: str, decimal: str) -> str:
    """Normalize a quantity or stock count to an integer string when it is integral."""
    money = parse_money(raw, decimal)
    try:
        value = Decimal(money)
    except InvalidOperation:
        return money
    if value == value.to_integral_value():
        return str(int(value))
    return money


def parse_timestamp(raw: str, date_format: str | None) -> str:
    """Normalize a timestamp to ISO 8601 (seconds). Returns the stripped raw value if unknown."""
    text = raw.strip()
    if not text:
        return ""
    candidates = ([date_format] if date_format else []) + list(DATE_FALLBACKS)
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt).isoformat(timespec="seconds")
        except ValueError:
            continue
    return text


def _is_decimal(text: str) -> bool:
    try:
        Decimal(text)
    except InvalidOperation:
        return False
    return True


@dataclass
class _Settings:
    decimal: str
    date_format: str | None
    cost_ratio: float | None
    gateway_rates: dict[str, float]
    default_gateway_rate: float | None


@dataclass
class _State:
    stats: EstimationStats = field(default_factory=EstimationStats)
    warnings: list[str] = field(default_factory=list)
    files: list[FileReport] = field(default_factory=list)
    resolved: dict[str, dict[str, str]] = field(default_factory=dict)
    custom_resolved: dict[str, dict[str, str]] = field(default_factory=dict)
    records: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: {k: [] for k in CANONICAL_COLUMNS}
    )
    catalog_skus: set[str] = field(default_factory=set)


def _read_rows(path: Path, mapping: MappingFile) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        raise MappingError(f"input file not found: {path}")
    with path.open(encoding=mapping.format.encoding, newline="") as handle:
        reader = csv.DictReader(handle, delimiter=mapping.format.delimiter)
        headers = list(reader.fieldnames or [])
        rows = [{k: (v or "") for k, v in row.items() if k is not None} for row in reader]
    return headers, rows


def _apply_privacy(
    policy: PrivacyPolicy, source: str, resolved: dict[str, str], rows: list[dict[str, str]]
) -> None:
    """Drop or pseudonymize sensitive cells in place. Each cell is processed exactly once."""
    for field_name, header in resolved.items():
        if not policy.is_sensitive(source, field_name):
            continue
        for row in rows:
            row[header] = policy.apply(source, field_name, row.get(header, "")) or ""


class _Builder:
    """Turns the (already privacy-filtered) rows into canonical Bronze records."""

    def __init__(self, settings: _Settings, state: _State) -> None:
        self.s = settings
        self.st = state

    def text(self, source: str, row: dict[str, str], field_name: str) -> str:
        header = self.st.resolved.get(source, {}).get(field_name)
        return row.get(header, "").strip() if header else ""

    def money(self, raw: str) -> str:
        return parse_money(raw, self.s.decimal)

    def quantity(self, raw: str) -> str:
        return parse_quantity(raw, self.s.decimal)

    def estimate_cost(self, list_price: str) -> tuple[str, str]:
        """Estimated unit cost from the mapping's ratio, or blank when no assumption exists."""
        if self.s.cost_ratio is None or not _is_decimal(list_price):
            return "", ""
        return _round_money(Decimal(list_price) * Decimal(str(self.s.cost_ratio))), "estimated"

    def custom_json(self, source: str, row: dict[str, str]) -> str | None:
        """JSON object of the custom attributes present in a row, or None when there are none.

        Values are read after the privacy pass, so dropped values never reach the JSON.
        """
        attributes: dict[str, str] = {}
        for name, header in self.st.custom_resolved.get(source, {}).items():
            value = row.get(header, "").strip()
            if value:
                attributes[name] = value
        if not attributes:
            return None
        return json.dumps(attributes, ensure_ascii=False, sort_keys=True)

    def products_file(self, rows: list[dict[str, str]]) -> None:
        for row in rows:
            sku = sku_key(self.text("products", row, "sku"))
            unit_cost = self.money(self.text("products", row, "unit_cost"))
            list_price = self.money(self.text("products", row, "list_price"))
            source = "exact" if unit_cost else ""
            if not unit_cost:
                unit_cost, source = self.estimate_cost(list_price)
                if source == "estimated":
                    self.st.stats.products_unit_cost_estimated += 1
            self.st.catalog_skus.add(sku)
            self.st.records["products"].append(
                {
                    "sku": sku,
                    "product_name": self.text("products", row, "product_name") or sku,
                    "category": snake(self.text("products", row, "category")) or "otros",
                    "size": self.text("products", row, "size").upper() or "UNICO",
                    "unit_cost": unit_cost,
                    "unit_cost_source": source,
                    "list_price": list_price,
                    "stock_on_hand": self.quantity(self.text("products", row, "stock_on_hand")),
                    CUSTOM_ATTRIBUTES_COLUMN: self.custom_json("products", row),
                }
            )

    def _catalog_stub(self, sku: str, product_name: str, unit_price: str) -> None:
        """A SKU sold but missing from the catalog file gets an estimated stub.

        Without it the sale line would be excluded and revenue silently understated.
        """
        if not sku or sku in self.st.catalog_skus:
            return
        self.st.catalog_skus.add(sku)
        unit_cost, source = self.estimate_cost(unit_price)
        self.st.stats.products_catalog_stubs += 1
        if source == "estimated":
            self.st.stats.products_unit_cost_estimated += 1
        self.st.records["products"].append(
            {
                "sku": sku,
                "product_name": product_name or sku,
                "category": "otros",
                "size": "UNICO",
                "unit_cost": unit_cost,
                "unit_cost_source": source,
                "list_price": unit_price,
                "stock_on_hand": "",
            }
        )

    def orders_file(self, rows: list[dict[str, str]]) -> None:
        groups: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            groups.setdefault(order_key(self.text("orders", row, "order_id")), []).append(row)
        for key, lines in groups.items():
            header, conflict = self._header(lines)
            if conflict:
                self.st.stats.orders_header_conflicts += 1
            # Header values are already normalized (see _normalize_header_value); normalizing
            # them again would misread "7890.00" as a thousands-separated number.
            shipping = header["shipping_cost"]
            if not shipping:
                self.st.stats.orders_without_shipping += 1
                shipping = "0.00"
            order_disc = header["order_discount"] or "0.00"
            order_ts = header["order_date"]
            self.st.records["orders"].append(
                {
                    "order_id": key,
                    "customer_id": header["customer_id"],
                    "order_ts": order_ts,
                    "order_status": header["order_status"],
                    "channel": header["channel"],
                    "currency": "ARS",
                    "shipping_cost": shipping,
                    "discount_amount": order_disc,
                }
            )
            line_totals, totals_ok = self._lines(key, lines)
            self._payment(key, header, order_ts, shipping, order_disc, line_totals, totals_ok)

    def _normalize_header_value(self, name: str, raw: str) -> str:
        """Canonical form of an order-level value, used both for output and conflict checks."""
        if name == "order_date":
            return parse_timestamp(raw, self.s.date_format)
        if name in ("shipping_cost", "order_discount"):
            return self.money(raw)
        if name in ("order_status", "channel", "payment_method", "payment_status"):
            return snake(raw)
        return raw

    def _header(self, lines: list[dict[str, str]]) -> tuple[dict[str, str], bool]:
        """First non-blank normalized value per order-level field, and whether lines disagree.

        Conflicts are compared on normalized values, so "03/09/2026" and "03/09/2026 00:00"
        or "1.000,00" and "1000.00" are not counted as disagreement.
        """
        header: dict[str, str] = {}
        conflict = False
        for name in ORDER_HEADER_FIELDS:
            values = [
                self._normalize_header_value(name, self.text("orders", row, name)) for row in lines
            ]
            non_blank = [v for v in values if v]
            header[name] = non_blank[0] if non_blank else ""
            if name in CONFLICT_CHECKED_FIELDS and len(set(non_blank)) > 1:
                conflict = True
        return header, conflict

    def _lines(self, key: str, lines: list[dict[str, str]]) -> tuple[list[Decimal], bool]:
        totals: list[Decimal] = []
        totals_ok = True
        for index, row in enumerate(lines, start=1):
            qty = self.quantity(self.text("orders", row, "quantity"))
            price = self.money(self.text("orders", row, "unit_price"))
            line_disc = self.money(self.text("orders", row, "line_discount")) or "0.00"
            sku = sku_key(self.text("orders", row, "sku"))
            self.st.records["order_items"].append(
                {
                    "order_item_id": f"{key}-L{index:03d}",
                    "order_id": key,
                    "sku": sku,
                    "quantity": qty,
                    "unit_price": price,
                    "discount_amount": line_disc,
                    CUSTOM_ATTRIBUTES_COLUMN: self.custom_json("orders", row),
                }
            )
            try:
                totals.append(Decimal(qty) * Decimal(price) - Decimal(line_disc))
            except InvalidOperation:
                totals_ok = False
            self._catalog_stub(sku, self.text("orders", row, "product_name"), price)
        return totals, totals_ok

    def _payment(
        self,
        key: str,
        header: dict[str, str],
        order_ts: str,
        shipping: str,
        order_disc: str,
        line_totals: list[Decimal],
        totals_ok: bool,
    ) -> None:
        stats = self.st.stats
        method = header["payment_method"]
        pay_status = header["payment_status"]
        if not pay_status:
            status = header["order_status"]
            if status in SOLD_STATUS_KEYS:
                pay_status = "approved"
            elif status in CANCEL_STATUS_KEYS:
                pay_status = "refunded"
            elif status in PENDING_STATUS_KEYS:
                pay_status = "pending"
            if pay_status:
                stats.payments_status_derived += 1

        amount = self.money(header["amount_paid"])
        if (
            not amount
            and totals_ok
            and line_totals
            and _is_decimal(order_disc)
            and _is_decimal(shipping)
        ):
            total = sum(line_totals, Decimal(0)) - Decimal(order_disc) + Decimal(shipping)
            amount = _round_money(total)

        fee, fee_source = "", ""
        fee_raw = self.money(header["gateway_fee"])
        if fee_raw and _is_decimal(fee_raw):
            fee, fee_source = fee_raw, "exact"
            stats.payments_gateway_fee_exact += 1
        elif amount and _is_decimal(amount):
            rate = self.s.gateway_rates.get(canonical_method(method), self.s.default_gateway_rate)
            if rate is not None:
                fee = _round_money(Decimal(amount) * Decimal(str(rate)))
                fee_source = "estimated"
                stats.payments_gateway_fee_estimated += 1
        if not fee:
            stats.payments_without_gateway_fee += 1

        self.st.records["payments"].append(
            {
                "payment_id": f"PAY-{key}",
                "order_id": key,
                "payment_ts": order_ts,
                "payment_method": method,
                "payment_status": pay_status,
                "amount": amount,
                "gateway_fee": fee,
                "gateway_fee_source": fee_source,
                "currency": "ARS",
            }
        )

    def returns_file(self, rows: list[dict[str, str]]) -> None:
        for number, row in enumerate(rows, start=1):
            self.st.records["returns"].append(
                {
                    "return_id": self.text("returns", row, "return_id") or f"RET-{number:06d}",
                    "order_id": order_key(self.text("returns", row, "order_id")),
                    "sku": sku_key(self.text("returns", row, "sku")),
                    "return_ts": parse_timestamp(
                        self.text("returns", row, "return_date"), self.s.date_format
                    ),
                    "quantity": self.quantity(self.text("returns", row, "quantity")),
                    "refund_amount": self.money(self.text("returns", row, "refund_amount")),
                    "reason": snake(self.text("returns", row, "reason")),
                    CUSTOM_ATTRIBUTES_COLUMN: self.custom_json("returns", row),
                }
            )


def _sensitive_warnings(source: str, ignored: list[str]) -> list[str]:
    hits = [h for h in ignored if any(hint in normalize_header(h) for hint in SENSITIVE_HINTS)]
    if not hits:
        return []
    return [
        f"{source}: columnas con posibles datos personales no mapeadas (no se persisten): "
        + ", ".join(hits)
    ]


def _combine_estimation(
    mappings: list[MappingFile],
) -> tuple[float | None, float | None, dict[str, float]]:
    ratios = {m.estimation.default_cost_ratio for m in mappings} - {None}
    defaults = {m.estimation.default_gateway_rate for m in mappings} - {None}
    if len(ratios) > 1 or len(defaults) > 1:
        raise MappingError("conflicting estimation assumptions across mappings")
    rates: dict[str, float] = {}
    for mapping in mappings:
        for method, rate in mapping.estimation.gateway_rates.items():
            key = canonical_method(method)
            if rates.get(key, rate) != rate:
                raise MappingError(f"conflicting gateway rate for {method}")
            rates[key] = rate
    return (
        next(iter(ratios), None),
        next(iter(defaults), None),
        rates,
    )


def ingest_files(
    input_dir: Path,
    mapping_paths: Sequence[Path],
    salt: str | None,
) -> IngestionResult:
    """Read the exported files described by ``mapping_paths`` and build one raw batch.

    Raises :class:`MappingError` for configuration problems: missing mapping or input file,
    missing required columns, unsupported currency, inconsistent mappings, or
    pseudonymization without a salt. Row-level problems never raise; they are kept as raw
    values and reported by the Silver quality gates.
    """
    mappings = [load_mapping(p) for p in mapping_paths]
    specs: dict[str, tuple[MappingFile, SourceSpec]] = {}
    for mapping in mappings:
        for source, spec in mapping.sources.items():
            if source in specs:
                raise MappingError(f"source '{source}' is defined by more than one mapping")
            specs[source] = (mapping, spec)
    if "orders" not in specs:
        raise MappingError("no mapping defines the required 'orders' source")
    if len({m.format.decimal for m in mappings}) > 1:
        raise MappingError("all mappings must use the same decimal separator")
    if len({m.format.date_format for m in mappings}) > 1:
        raise MappingError("all mappings must use the same date_format")

    cost_ratio, default_rate, gateway_rates = _combine_estimation(mappings)
    policy = PrivacyPolicy.from_mappings(
        {src: dict(spec.sensitive) for src, (_, spec) in specs.items()},
        {src: set(spec.columns) for src, (_, spec) in specs.items()},
        salt,
        custom_by_source={src: set(spec.custom_fields) for src, (_, spec) in specs.items()},
    )
    settings = _Settings(
        decimal=mappings[0].format.decimal,
        date_format=mappings[0].format.date_format,
        cost_ratio=cost_ratio,
        gateway_rates=gateway_rates,
        default_gateway_rate=default_rate,
    )
    state = _State()
    builder = _Builder(settings, state)
    hasher = hashlib.sha256()
    rows_by_source: dict[str, list[dict[str, str]]] = {}

    for source in ("products", "orders", "returns"):
        if source not in specs:
            rows_by_source[source] = []
            continue
        mapping, spec = specs[source]
        path = input_dir / spec.file
        headers, rows = _read_rows(path, mapping)
        resolved = resolve_columns(headers, spec, source)
        custom = resolve_custom_columns(headers, spec)
        state.resolved[source] = resolved
        state.custom_resolved[source] = custom
        used = set(resolved.values()) | set(custom.values())
        ignored = [h for h in headers if h not in used]
        state.files.append(
            FileReport(
                source=source,
                file=spec.file,
                rows_read=len(rows),
                mapped_fields=sorted(resolved),
                ignored_columns=ignored,
            )
        )
        state.warnings.extend(_sensitive_warnings(source, ignored))
        _apply_privacy(policy, source, {**resolved, **custom}, rows)
        hasher.update(path.read_bytes())
        rows_by_source[source] = rows

    for mapping in mappings:
        # exclude_defaults keeps batch ids of mappings without custom fields unchanged.
        hasher.update(mapping.model_dump_json(exclude_defaults=True).encode())
    hasher.update((salt or "").encode())
    batch_id = f"csv-{hasher.hexdigest()[:16]}"

    # Catalog first: order lines for SKUs missing from it create estimated stubs.
    builder.products_file(rows_by_source["products"])
    builder.orders_file(rows_by_source["orders"])
    builder.returns_file(rows_by_source["returns"])

    if "products" not in specs:
        state.warnings.append(
            "no hay archivo de costos y stock: todos los SKUs usan costo estimado "
            "y stock desconocido"
        )
    if state.stats.orders_without_shipping:
        state.warnings.append(
            f"Pedidos sin costo de envío informado: {state.stats.orders_without_shipping} "
            "(se asumió 0,00): el margen de esos pedidos puede estar sobreestimado"
        )
    if state.stats.orders_header_conflicts:
        state.warnings.append(
            "Pedidos con valores distintos entre sus líneas en campos de cabecera: "
            f"{state.stats.orders_header_conflicts}; se usó el primer valor no vacío"
        )

    records = state.records
    batch = RawBatch(
        batch_id=batch_id,
        sources={entity: list(rows) for entity, rows in records.items()},
        columns=dict(CANONICAL_COLUMNS),
        source=CSV_SOURCE,
    )
    report = IngestionReport(
        batch_id=batch_id,
        mappings=[m.name for m in mappings],
        files=state.files,
        rows_by_entity={entity: len(rows) for entity, rows in records.items()},
        estimation=state.stats,
        privacy=policy.stats(),
        warnings=state.warnings,
    )
    return IngestionResult(batch=batch, report=report)
