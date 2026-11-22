"""Pydantic contracts: business enums, clean-row contracts, quality reports, analyst responses."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class OrderStatus(StrEnum):
    PENDING_PAYMENT = "pending_payment"
    PAID = "paid"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class Channel(StrEnum):
    WEB = "web"
    APP = "app"
    MARKETPLACE = "marketplace"


class PaymentMethod(StrEnum):
    CREDIT_CARD = "tarjeta_credito"
    DEBIT_CARD = "tarjeta_debito"
    WALLET = "billetera_virtual"
    TRANSFER = "transferencia"


class PaymentStatus(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    PENDING = "pending"
    REFUNDED = "refunded"


class CheckoutEventType(StrEnum):
    CART_CREATED = "cart_created"
    CHECKOUT_STARTED = "checkout_started"
    PAYMENT_APPROVED = "payment_approved"
    PAYMENT_REJECTED = "payment_rejected"


class Category(StrEnum):
    REMERA = "remera"
    PANTALON = "pantalon"
    BUZO = "buzo"
    CAMPERA = "campera"
    VESTIDO = "vestido"
    SHORT = "short"
    OTHER = "otros"


class Size(StrEnum):
    XS = "XS"
    S = "S"
    M = "M"
    L = "L"
    XL = "XL"
    UNIQUE = "UNICO"


CURRENCY = "ARS"
SOLD_ORDER_STATUSES: tuple[str, ...] = (
    OrderStatus.PAID.value,
    OrderStatus.SHIPPED.value,
    OrderStatus.DELIVERED.value,
)
RAW_ENTITIES: tuple[str, ...] = (
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
    "checkout_events",
)


class CleanOrder(BaseModel):
    """Contract for a row in ``silver/orders``."""

    model_config = ConfigDict(frozen=True)

    order_id: str = Field(pattern=r"^ORD-\d{7}$")
    customer_id: str = Field(pattern=r"^CUS-\d{5}$")
    order_ts: datetime
    order_status: OrderStatus
    channel: Channel
    currency: Literal["ARS"]
    shipping_cost: Decimal = Field(ge=0)
    discount_amount: Decimal = Field(ge=0)


class CleanOrderItem(BaseModel):
    """Contract for a row in ``silver/order_items``."""

    model_config = ConfigDict(frozen=True)

    order_item_id: str
    order_id: str
    sku: str
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)
    discount_amount: Decimal = Field(ge=0)


class QualityRuleHit(BaseModel):
    """Number of rows rejected by one named rule."""

    rule: str
    rows: int = Field(ge=0)


class EntityQualityReport(BaseModel):
    """Silver quality-gate summary for one entity."""

    entity: str
    bronze_rows: int
    row_rule_rejections: int
    duplicates_removed: int
    integrity_rejections: int
    silver_rows: int
    quarantine_rows: int
    reasons: dict[str, int] = Field(default_factory=dict)


class AuditRecord(BaseModel):
    """One audit event per Delta table write."""

    run_id: str
    layer: str
    table: str
    action: str
    rows: int
    created_at: datetime


class PipelineRunReport(BaseModel):
    """Summary of a full Bronze -> Silver -> Gold run."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    storage: str
    batches_loaded: dict[str, int]
    entities: list[EntityQualityReport]
    gold_rows: dict[str, int]


class GuardrailViolationError(ValueError):
    """Raised when a SQL statement violates the read-only policy."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message


class AnalystResponse(BaseModel):
    """Structured answer returned by the BI analyst for one question."""

    question: str
    provider: str
    status: Literal["ok", "blocked", "unanswerable", "error"]
    metrics: list[str] = Field(default_factory=list)
    sql: str | None = None
    tables: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    interpretation: str | None = None
    interpretation_source: Literal["mock", "llm", "mock_fallback"] | None = None
    error: str | None = None
    generated_at: datetime
