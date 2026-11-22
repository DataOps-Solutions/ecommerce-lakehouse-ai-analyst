"""Silver layer: normalization, deterministic deduplication and Data Quality Gates.

Processing per entity (in dependency order: products -> orders -> order_items -> payments
-> returns -> checkout_events):

1. **Normalize** raw strings: trim/case, status/channel/currency/method/SKU mapping, typed casts
   (``TRY_CAST``, so malformed values become NULL and are caught by the rules).
2. **Row-level gates** (positive prices, valid quantities, allowed enums, non-null keys).
   Rejected rows go to quarantine with every failing rule name.
3. **Deterministic dedupe** on the business key over the rows that passed gates: the latest
   batch wins, ties broken by ``_source_seq`` descending. A corrupt newer version therefore
   never hides a valid older one.
4. **Integrity gates** against the already-validated parents (order -> items/payments/returns,
   SKU -> catalog, returned item must have been sold in that order).

Everything runs in an in-memory DuckDB over Arrow inputs, so the layer is a pure function that
is easy to test.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

import duckdb
import pyarrow as pa

from src.lakehouse import relation_to_arrow
from src.models import (
    CURRENCY,
    Category,
    Channel,
    CheckoutEventType,
    EntityQualityReport,
    OrderStatus,
    PaymentMethod,
    PaymentStatus,
    Size,
)

QUARANTINE_COLUMNS_ORDER = "_quarantined_at"


@dataclass(frozen=True)
class Rule:
    """A named Data Quality rule. ``invalid_when`` is a SQL predicate that is TRUE for bad rows."""

    name: str
    invalid_when: str


@dataclass(frozen=True)
class EntitySpec:
    """Normalization projection, key and quality rules for one Silver entity."""

    name: str
    key: str
    projection: str
    row_rules: tuple[Rule, ...]
    integrity_rules: tuple[Rule, ...] = ()


@dataclass
class SilverResult:
    """Output of the Silver layer for one run."""

    tables: dict[str, pa.Table] = field(default_factory=dict)
    quarantine: dict[str, pa.Table] = field(default_factory=dict)
    reports: list[EntityQualityReport] = field(default_factory=list)


def _in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _case_map(expr: str, mapping: Mapping[str, str], default: str) -> str:
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in mapping.items())
    return f"CASE {expr} {whens} ELSE {default} END"


def _categorical(column: str) -> str:
    """Lower-case, trim and snake_case a categorical raw value (empty -> NULL)."""
    return f"NULLIF(replace(lower(trim(b.{column})), ' ', '_'), '')"


def _mapped(column: str, mapping: Mapping[str, str]) -> str:
    """Categorical column normalized through ``mapping``; unknown values pass through."""
    return _case_map(_categorical(column), mapping, _categorical(column))


ORDER_STATUS_MAP = {
    "pending_payment": OrderStatus.PENDING_PAYMENT.value,
    "pendiente_pago": OrderStatus.PENDING_PAYMENT.value,
    "pendiente": OrderStatus.PENDING_PAYMENT.value,
    "paid": OrderStatus.PAID.value,
    "pagado": OrderStatus.PAID.value,
    "shipped": OrderStatus.SHIPPED.value,
    "enviado": OrderStatus.SHIPPED.value,
    "delivered": OrderStatus.DELIVERED.value,
    "entregado": OrderStatus.DELIVERED.value,
    "cancelled": OrderStatus.CANCELLED.value,
    "canceled": OrderStatus.CANCELLED.value,
    "cancelado": OrderStatus.CANCELLED.value,
    "cancelada": OrderStatus.CANCELLED.value,
    "pagada": OrderStatus.PAID.value,
    "enviada": OrderStatus.SHIPPED.value,
    "entregada": OrderStatus.DELIVERED.value,
}
CHANNEL_MAP = {
    "web": Channel.WEB.value,
    "sitio_web": Channel.WEB.value,
    "app": Channel.APP.value,
    "aplicacion": Channel.APP.value,
    "marketplace": Channel.MARKETPLACE.value,
}
METHOD_MAP = {
    "tarjeta_credito": PaymentMethod.CREDIT_CARD.value,
    "tarjeta_de_credito": PaymentMethod.CREDIT_CARD.value,
    "credit_card": PaymentMethod.CREDIT_CARD.value,
    "tarjeta_debito": PaymentMethod.DEBIT_CARD.value,
    "tarjeta_de_debito": PaymentMethod.DEBIT_CARD.value,
    "debit_card": PaymentMethod.DEBIT_CARD.value,
    "billetera_virtual": PaymentMethod.WALLET.value,
    "wallet": PaymentMethod.WALLET.value,
    "transferencia": PaymentMethod.TRANSFER.value,
    "bank_transfer": PaymentMethod.TRANSFER.value,
}
PAYMENT_STATUS_MAP = {
    "approved": PaymentStatus.APPROVED.value,
    "aprobado": PaymentStatus.APPROVED.value,
    "accepted": PaymentStatus.APPROVED.value,
    "rejected": PaymentStatus.REJECTED.value,
    "rechazado": PaymentStatus.REJECTED.value,
    "declined": PaymentStatus.REJECTED.value,
    "pending": PaymentStatus.PENDING.value,
    "pendiente": PaymentStatus.PENDING.value,
    "refunded": PaymentStatus.REFUNDED.value,
    "reembolsado": PaymentStatus.REFUNDED.value,
    "aprobada": PaymentStatus.APPROVED.value,
    "rechazada": PaymentStatus.REJECTED.value,
    "pendiente_de_pago": PaymentStatus.PENDING.value,
    "reembolsada": PaymentStatus.REFUNDED.value,
}
EVENT_TYPE_MAP = {
    "cart_created": CheckoutEventType.CART_CREATED.value,
    "carrito_creado": CheckoutEventType.CART_CREATED.value,
    "checkout_started": CheckoutEventType.CHECKOUT_STARTED.value,
    "inicio_checkout": CheckoutEventType.CHECKOUT_STARTED.value,
    "payment_approved": CheckoutEventType.PAYMENT_APPROVED.value,
    "pago_aprobado": CheckoutEventType.PAYMENT_APPROVED.value,
    "payment_rejected": CheckoutEventType.PAYMENT_REJECTED.value,
    "pago_rechazado": CheckoutEventType.PAYMENT_REJECTED.value,
}
COST_SOURCES: tuple[str, ...] = ("exact", "estimated")
CURRENCY_MAP = {"ARS": CURRENCY, "$": CURRENCY, "AR$": CURRENCY}
CURRENCY_EXPR = _case_map("upper(trim(b.currency))", CURRENCY_MAP, "upper(trim(b.currency))")
CATEGORIES = tuple(c.value for c in Category)
SIZES = tuple(s.value for s in Size)
CHANNELS = tuple(c.value for c in Channel)
ORDER_STATUSES = tuple(s.value for s in OrderStatus)
PAYMENT_METHODS = tuple(m.value for m in PaymentMethod)
PAYMENT_STATUSES = tuple(s.value for s in PaymentStatus)
EVENT_TYPES = tuple(e.value for e in CheckoutEventType)


def _is_null_or(expr: str, predicate: str) -> str:
    return f"{expr} IS NULL OR NOT ({predicate})"


ENTITY_SPECS: tuple[EntitySpec, ...] = (
    EntitySpec(
        name="products",
        key="sku",
        projection=f"""
            NULLIF(upper(replace(trim(b.sku), '_', '-')), '') AS sku,
            NULLIF(trim(b.product_name), '') AS product_name,
            {_categorical("category")} AS category,
            NULLIF(upper(trim(b.size)), '') AS size,
            TRY_CAST(trim(b.unit_cost) AS DECIMAL(14,2)) AS unit_cost,
            TRY_CAST(trim(b.list_price) AS DECIMAL(14,2)) AS list_price,
            TRY_CAST(trim(b.stock_on_hand) AS INTEGER) AS stock_on_hand,
            NULLIF(trim(b.stock_on_hand), '') IS NOT NULL AS stock_reported,
            COALESCE(NULLIF(lower(trim(b.unit_cost_source)), ''), 'exact') AS unit_cost_source
        """,
        row_rules=(
            Rule("key_missing", "sku IS NULL"),
            Rule("missing_product_name", "product_name IS NULL"),
            Rule(
                "invalid_category", _is_null_or("category", f"category IN ({_in_list(CATEGORIES)})")
            ),
            Rule("invalid_size", _is_null_or("size", f"size IN ({_in_list(SIZES)})")),
            Rule("invalid_unit_cost", "unit_cost IS NULL OR unit_cost < 0"),
            Rule("invalid_list_price", "list_price IS NULL OR list_price <= 0"),
            Rule(
                "invalid_stock",
                "stock_on_hand < 0 OR (stock_reported AND stock_on_hand IS NULL)",
            ),
            Rule(
                "invalid_unit_cost_source",
                f"unit_cost_source NOT IN ({_in_list(COST_SOURCES)})",
            ),
        ),
    ),
    EntitySpec(
        name="orders",
        key="order_id",
        projection=f"""
            NULLIF(upper(trim(b.order_id)), '') AS order_id,
            NULLIF(upper(trim(b.customer_id)), '') AS customer_id,
            TRY_CAST(trim(b.order_ts) AS TIMESTAMP) AS order_ts,
            {_mapped("order_status", ORDER_STATUS_MAP)} AS order_status,
            {_mapped("channel", CHANNEL_MAP)} AS channel,
            {CURRENCY_EXPR} AS currency,
            TRY_CAST(trim(b.shipping_cost) AS DECIMAL(14,2)) AS shipping_cost,
            TRY_CAST(trim(b.discount_amount) AS DECIMAL(14,2)) AS discount_amount
        """,
        row_rules=(
            Rule("key_missing", "order_id IS NULL"),
            Rule("invalid_order_ts", "order_ts IS NULL"),
            Rule(
                "invalid_status",
                _is_null_or("order_status", f"order_status IN ({_in_list(ORDER_STATUSES)})"),
            ),
            Rule("invalid_channel", _is_null_or("channel", f"channel IN ({_in_list(CHANNELS)})")),
            Rule("invalid_currency", f"currency IS NULL OR currency <> '{CURRENCY}'"),
            Rule("invalid_shipping_cost", "shipping_cost IS NULL OR shipping_cost < 0"),
            Rule("invalid_discount", "discount_amount IS NULL OR discount_amount < 0"),
        ),
    ),
    EntitySpec(
        name="order_items",
        key="order_item_id",
        projection="""
            NULLIF(upper(trim(b.order_item_id)), '') AS order_item_id,
            NULLIF(upper(trim(b.order_id)), '') AS order_id,
            NULLIF(upper(replace(trim(b.sku), '_', '-')), '') AS sku,
            TRY_CAST(trim(b.quantity) AS INTEGER) AS quantity,
            TRY_CAST(trim(b.unit_price) AS DECIMAL(14,2)) AS unit_price,
            TRY_CAST(trim(b.discount_amount) AS DECIMAL(14,2)) AS discount_amount
        """,
        row_rules=(
            Rule("key_missing", "order_item_id IS NULL"),
            Rule("missing_order_id", "order_id IS NULL"),
            Rule("missing_sku", "sku IS NULL"),
            Rule("invalid_quantity", "quantity IS NULL OR quantity <= 0"),
            Rule("invalid_unit_price", "unit_price IS NULL OR unit_price <= 0"),
            Rule(
                "invalid_discount",
                "discount_amount IS NULL OR discount_amount < 0 "
                "OR discount_amount > unit_price * quantity",
            ),
        ),
        integrity_rules=(
            Rule("orphan_order_item", "order_id NOT IN (SELECT order_id FROM silver_orders)"),
            Rule("sku_not_in_catalog", "sku NOT IN (SELECT sku FROM silver_products)"),
        ),
    ),
    EntitySpec(
        name="payments",
        key="payment_id",
        projection=f"""
            NULLIF(upper(trim(b.payment_id)), '') AS payment_id,
            NULLIF(upper(trim(b.order_id)), '') AS order_id,
            TRY_CAST(trim(b.payment_ts) AS TIMESTAMP) AS payment_ts,
            {_mapped("payment_method", METHOD_MAP)} AS payment_method,
            {_mapped("payment_status", PAYMENT_STATUS_MAP)} AS payment_status,
            TRY_CAST(trim(b.amount) AS DECIMAL(14,2)) AS amount,
            TRY_CAST(trim(b.gateway_fee) AS DECIMAL(14,2)) AS gateway_fee,
            COALESCE(NULLIF(lower(trim(b.gateway_fee_source)), ''), 'exact') AS gateway_fee_source,
            {CURRENCY_EXPR} AS currency
        """,
        row_rules=(
            Rule("key_missing", "payment_id IS NULL"),
            Rule("missing_order_id", "order_id IS NULL"),
            Rule("invalid_payment_ts", "payment_ts IS NULL"),
            Rule(
                "invalid_payment_method",
                _is_null_or("payment_method", f"payment_method IN ({_in_list(PAYMENT_METHODS)})"),
            ),
            Rule(
                "invalid_payment_status",
                _is_null_or("payment_status", f"payment_status IN ({_in_list(PAYMENT_STATUSES)})"),
            ),
            Rule("invalid_amount", "amount IS NULL OR amount <= 0"),
            Rule(
                "invalid_gateway_fee",
                "gateway_fee IS NULL OR gateway_fee < 0 OR gateway_fee > amount",
            ),
            Rule("invalid_currency", f"currency IS NULL OR currency <> '{CURRENCY}'"),
            Rule(
                "invalid_fee_source",
                f"gateway_fee_source NOT IN ({_in_list(COST_SOURCES)})",
            ),
        ),
        integrity_rules=(
            Rule("orphan_payment", "order_id NOT IN (SELECT order_id FROM silver_orders)"),
        ),
    ),
    EntitySpec(
        name="returns",
        key="return_id",
        projection="""
            NULLIF(upper(trim(b.return_id)), '') AS return_id,
            NULLIF(upper(trim(b.order_id)), '') AS order_id,
            NULLIF(upper(replace(trim(b.sku), '_', '-')), '') AS sku,
            TRY_CAST(trim(b.return_ts) AS TIMESTAMP) AS return_ts,
            TRY_CAST(trim(b.quantity) AS INTEGER) AS quantity,
            TRY_CAST(trim(b.refund_amount) AS DECIMAL(14,2)) AS refund_amount,
            NULLIF(lower(trim(b.reason)), '') AS reason
        """,
        row_rules=(
            Rule("key_missing", "return_id IS NULL"),
            Rule("missing_order_id", "order_id IS NULL"),
            Rule("missing_sku", "sku IS NULL"),
            Rule("invalid_return_ts", "return_ts IS NULL"),
            Rule("invalid_quantity", "quantity IS NULL OR quantity <= 0"),
            Rule("invalid_refund", "refund_amount IS NULL OR refund_amount <= 0"),
        ),
        integrity_rules=(
            Rule("orphan_return", "order_id NOT IN (SELECT order_id FROM silver_orders)"),
            Rule(
                "return_item_not_sold",
                "(order_id || '|' || sku) NOT IN "
                "(SELECT order_id || '|' || sku FROM silver_order_items)",
            ),
        ),
    ),
    EntitySpec(
        name="checkout_events",
        key="event_id",
        projection=f"""
            NULLIF(upper(trim(b.event_id)), '') AS event_id,
            NULLIF(trim(b.session_id), '') AS session_id,
            TRY_CAST(trim(b.event_ts) AS TIMESTAMP) AS event_ts,
            {_mapped("event_type", EVENT_TYPE_MAP)} AS event_type,
            {_mapped("channel", CHANNEL_MAP)} AS channel,
            {_mapped("payment_method", METHOD_MAP)} AS payment_method,
            NULLIF(upper(trim(b.order_id)), '') AS order_id
        """,
        row_rules=(
            Rule("key_missing", "event_id IS NULL"),
            Rule("invalid_session", "session_id IS NULL"),
            Rule("invalid_event_ts", "event_ts IS NULL"),
            Rule(
                "invalid_event_type",
                _is_null_or("event_type", f"event_type IN ({_in_list(EVENT_TYPES)})"),
            ),
            Rule("invalid_channel", _is_null_or("channel", f"channel IN ({_in_list(CHANNELS)})")),
            Rule(
                "invalid_payment_method",
                "payment_method IS NOT NULL AND payment_method NOT IN "
                f"({_in_list(PAYMENT_METHODS)})",
            ),
        ),
    ),
)


def _reasons_expr(rules: tuple[Rule, ...]) -> str:
    if not rules:
        return "''"
    parts = [f"CASE WHEN ({r.invalid_when}) IS TRUE THEN '{r.name}' END" for r in rules]
    return f"COALESCE(array_to_string([{', '.join(parts)}], '; '), '')"


OPTIONAL_BRONZE_COLUMNS: dict[str, tuple[str, ...]] = {
    "products": ("unit_cost_source",),
    "payments": ("gateway_fee_source",),
}


def _with_optional_columns(table: pa.Table, names: tuple[str, ...]) -> pa.Table:
    """Add missing optional columns as NULL so older Bronze batches stay readable."""
    for name in names:
        if name not in table.column_names:
            table = table.append_column(name, pa.nulls(table.num_rows, type=pa.string()))
    return table


def _register_bronze(con: duckdb.DuckDBPyConnection, bronze: Mapping[str, pa.Table]) -> None:
    for entity, table in bronze.items():
        names = OPTIONAL_BRONZE_COLUMNS.get(entity, ())
        con.register(f"bronze_{entity}", _with_optional_columns(table, names))


def run_silver(bronze: Mapping[str, pa.Table], quarantined_at: datetime) -> SilverResult:
    """Apply Silver normalization, quality gates and dedupe to Bronze Arrow tables.

    Pure function: no I/O. ``bronze`` maps entity name to the full Bronze table (all batches).
    """
    con = duckdb.connect(config={"enable_external_access": False})
    _register_bronze(con, bronze)
    result = SilverResult()

    for spec in ENTITY_SPECS:
        e = spec.name
        con.execute(
            f"CREATE OR REPLACE TABLE {e}_norm AS "
            f"SELECT {spec.projection}, b._batch_id, b._source_seq FROM bronze_{e} b"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE {e}_pass1 AS "
            f"SELECT *, {_reasons_expr(spec.row_rules)} AS _dq_reasons FROM {e}_norm"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE {e}_valid1 AS SELECT * FROM {e}_pass1 WHERE _dq_reasons = ''"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE {e}_dedup AS "
            f"SELECT * EXCLUDE (_rn, _dq_reasons) FROM ("
            f"  SELECT *, row_number() OVER ("
            f"    PARTITION BY {spec.key} ORDER BY _batch_id DESC, _source_seq DESC) AS _rn"
            f"  FROM {e}_valid1) WHERE _rn = 1"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE {e}_final AS "
            f"SELECT *, {_reasons_expr(spec.integrity_rules)} AS _dq_reasons FROM {e}_dedup"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE silver_{e} AS "
            f"SELECT * EXCLUDE (_dq_reasons) FROM {e}_final WHERE _dq_reasons = '' "
            f"ORDER BY {spec.key}"
        )

        q1 = con.sql(f"SELECT *, 'row' AS _dq_stage FROM {e}_pass1 WHERE _dq_reasons <> ''")
        q2 = con.sql(f"SELECT *, 'integrity' AS _dq_stage FROM {e}_final WHERE _dq_reasons <> ''")
        quarantine = pa.concat_tables(
            [relation_to_arrow(q1), relation_to_arrow(q2)], promote_options="permissive"
        )
        quarantine = quarantine.append_column(
            QUARANTINE_COLUMNS_ORDER,
            pa.array([quarantined_at] * quarantine.num_rows, type=pa.timestamp("us")),
        )
        result.quarantine[e] = quarantine
        result.tables[e] = relation_to_arrow(con.sql(f"SELECT * FROM silver_{e}"))

        reasons: Counter[str] = Counter()
        for value in quarantine.column("_dq_reasons").to_pylist():
            for reason in str(value).split("; "):
                if reason:
                    reasons[reason] += 1

        def count(sql: str) -> int:
            row = con.execute(sql).fetchone()
            return int(row[0]) if row else 0

        result.reports.append(
            EntityQualityReport(
                entity=e,
                bronze_rows=count(f"SELECT count(*) FROM bronze_{e}"),
                row_rule_rejections=count(
                    f"SELECT count(*) FROM {e}_pass1 WHERE _dq_reasons <> ''"
                ),
                duplicates_removed=count(f"SELECT count(*) FROM {e}_valid1")
                - count(f"SELECT count(*) FROM {e}_dedup"),
                integrity_rejections=count(
                    f"SELECT count(*) FROM {e}_final WHERE _dq_reasons <> ''"
                ),
                silver_rows=count(f"SELECT count(*) FROM silver_{e}"),
                quarantine_rows=quarantine.num_rows,
                reasons=dict(sorted(reasons.items())),
            )
        )
        # Integrity rules of later entities read silver_<e>; nothing else to do here.
    con.close()
    return result
