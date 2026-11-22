"""Deterministic generator of raw e-commerce (apparel retail) operational data.

The output mimics an ERP/e-commerce export: every value is a string, as it would be
in a raw landing zone. Two batches are produced:

* ``batch-001``: full historical load, including dirty records (bad statuses, currencies,
  non-positive prices, orphan items, unknown SKUs) and exact duplicates.
* ``batch-002``: incremental load with status progressions for existing orders, payment
  approvals that arrive late, stock updates and re-sent duplicates. Dedupe must keep the
  latest valid version.

Given the same seed, the output is byte-for-byte identical.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from src.models import CURRENCY, OrderStatus, PaymentMethod

START = datetime(2026, 8, 1)
DAYS = 92
N_SESSIONS = 9000
N_CUSTOMERS = 1500
SIZES = ("XS", "S", "M", "L", "XL")
COLORS = ("NEG", "BLA")
RAW_SOURCE = "erp_export_v1"

# code, category, name, list price (ARS)
STYLES: tuple[tuple[str, str, str, int], ...] = (
    ("REM01", "remera", "Remera básica algodón", 12900),
    ("REM02", "remera", "Remera oversize", 15900),
    ("REM03", "remera", "Remera estampada", 17500),
    ("REM04", "remera", "Remera rib", 14200),
    ("PAN01", "pantalon", "Jean recto", 34900),
    ("PAN02", "pantalon", "Jogger French", 28900),
    ("PAN03", "pantalon", "Pantalón de vestir", 39900),
    ("PAN04", "pantalon", "Pantalón cargo", 36500),
    ("BUZ01", "buzo", "Buzo canguro", 29900),
    ("BUZ02", "buzo", "Buzo sin capucha", 24900),
    ("BUZ03", "buzo", "Buzo polar", 33900),
    ("BUZ04", "buzo", "Buzo oversize", 27500),
    ("CAM01", "campera", "Campera rompeviento", 45900),
    ("CAM02", "campera", "Campera de jean", 52900),
    ("CAM03", "campera", "Campera acolchada", 68900),
    ("CAM04", "campera", "Campera bomber", 58900),
    ("VES01", "vestido", "Vestido midi", 42900),
    ("VES02", "vestido", "Vestido corto estampado", 31900),
    ("VES03", "vestido", "Vestido camisero", 38900),
    ("VES04", "vestido", "Vestido de punto", 35900),
    ("SHO01", "short", "Short de baño", 19900),
    ("SHO02", "short", "Short jean", 22900),
    ("SHO03", "short", "Short deportivo", 17900),
    ("SHO04", "short", "Short de lino", 21900),
)
CHANNEL_WEIGHTS = (("web", 0.55), ("app", 0.30), ("marketplace", 0.15))
METHOD_WEIGHTS = (
    (PaymentMethod.CREDIT_CARD.value, 0.45),
    (PaymentMethod.DEBIT_CARD.value, 0.20),
    (PaymentMethod.WALLET.value, 0.25),
    (PaymentMethod.TRANSFER.value, 0.10),
)
# Gateway fee rate per payment method (share of the charged amount).
FEE_RATE = {
    PaymentMethod.CREDIT_CARD.value: 0.039,
    PaymentMethod.DEBIT_CARD.value: 0.019,
    PaymentMethod.WALLET.value: 0.029,
    PaymentMethod.TRANSFER.value: 0.006,
}
RETURN_REASONS = ("talle_incorrecto", "no_satisfecho", "defecto", "otro")
SHIPPING_FEE = 5490.0
FREE_SHIPPING_FROM = 60000.0


@dataclass(frozen=True)
class RawBatch:
    """One raw landing batch: ``sources`` maps entity name to raw string records.

    ``columns`` optionally fixes the schema per entity (needed when a batch has no rows).
    ``source`` labels the origin system in Bronze metadata.
    """

    batch_id: str
    sources: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    source: str = RAW_SOURCE


def _money(value: float) -> str:
    return f"{round(value, 2):.2f}"


def _weighted(rng: random.Random, options: tuple[tuple[str, float], ...]) -> str:
    roll = rng.random()
    acc = 0.0
    for value, weight in options:
        acc += weight
        if roll < acc:
            return value
    return options[-1][0]


def _build_catalog(rng: random.Random) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for code, category, name, price in STYLES:
        cost_ratio = rng.uniform(0.38, 0.48)
        for color in COLORS:
            for size in SIZES:
                catalog.append(
                    {
                        "sku": f"{code}-{color}-{size}",
                        "product_name": name,
                        "category": category,
                        "size": size,
                        "unit_cost": _money(price * cost_ratio),
                        "list_price": _money(price),
                        "stock_on_hand": str(0 if rng.random() < 0.08 else rng.randint(1, 60)),
                    }
                )
    return catalog


def _ts(day: int, rng: random.Random) -> datetime:
    return START + timedelta(days=day, minutes=rng.randint(0, 1439))


def generate_batches(seed: int) -> list[RawBatch]:
    """Generate the two raw batches deterministically from ``seed``."""
    rng = random.Random(seed)
    catalog = _build_catalog(rng)
    price_by_sku = {p["sku"]: float(p["list_price"]) for p in catalog}
    skus = sorted(price_by_sku)

    events: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []
    returns: list[dict[str, Any]] = []
    order_seq = 0
    item_seq = 0
    event_seq = 0

    def add_event(
        session: str,
        event_type: str,
        ts: datetime,
        channel: str,
        method: str | None,
        order_id: str | None,
    ) -> None:
        nonlocal event_seq
        event_seq += 1
        events.append(
            {
                "event_id": f"EVT-{event_seq:08d}",
                "session_id": session,
                "event_ts": ts.isoformat(timespec="seconds"),
                "event_type": event_type,
                "channel": channel,
                "payment_method": method,
                "order_id": order_id,
            }
        )

    for i in range(N_SESSIONS):
        session = f"SES-{i + 1:07d}"
        cart_ts = _ts(rng.randrange(DAYS), rng)
        channel = _weighted(rng, CHANNEL_WEIGHTS)
        add_event(session, "cart_created", cart_ts, channel, None, None)
        if rng.random() >= 0.6:
            continue
        checkout_ts = cart_ts + timedelta(minutes=rng.randint(2, 30))
        method = _weighted(rng, METHOD_WEIGHTS)
        add_event(session, "checkout_started", checkout_ts, channel, method, None)
        roll = rng.random()
        if roll >= 0.82:
            if roll < 0.95:
                add_event(
                    session,
                    "payment_rejected",
                    checkout_ts + timedelta(minutes=2),
                    channel,
                    method,
                    None,
                )
            continue

        order_seq += 1
        order_id = f"ORD-{order_seq:07d}"
        pay_ts = checkout_ts + timedelta(minutes=rng.randint(1, 5))
        add_event(session, "payment_approved", pay_ts, channel, method, order_id)

        n_lines = rng.choices((1, 2, 3, 4), weights=(0.5, 0.3, 0.15, 0.05))[0]
        subtotal = 0.0
        order_items: list[dict[str, Any]] = []
        for _ in range(n_lines):
            item_seq += 1
            sku = rng.choice(skus)
            qty = 2 if rng.random() < 0.2 else 1
            unit = price_by_sku[sku]
            item_disc = round(unit * qty * 0.10, 2) if rng.random() < 0.2 else 0.0
            subtotal += unit * qty - item_disc
            line = {
                "order_item_id": f"OIT-{item_seq:08d}",
                "order_id": order_id,
                "sku": sku,
                "quantity": str(qty),
                "unit_price": _money(unit),
                "discount_amount": _money(item_disc),
                "_unit": unit,
                "_disc": item_disc,
            }
            order_items.append(line)
        order_disc = round(subtotal * 0.05, 2) if rng.random() < 0.10 else 0.0
        shipping = 0.0 if subtotal >= FREE_SHIPPING_FROM else SHIPPING_FEE
        amount = round(subtotal - order_disc + shipping, 2)

        roll_status = rng.random()
        if roll_status < 0.04:
            status = OrderStatus.PENDING_PAYMENT.value
            pay_status = "pending"
        elif roll_status < 0.10:
            status = OrderStatus.CANCELLED.value
            pay_status = "refunded"
        elif roll_status < 0.16:
            status = OrderStatus.PAID.value
            pay_status = "approved"
        elif roll_status < 0.26:
            status = OrderStatus.SHIPPED.value
            pay_status = "approved"
        else:
            status = OrderStatus.DELIVERED.value
            pay_status = "approved"

        orders.append(
            {
                "order_id": order_id,
                "customer_id": f"CUS-{rng.randint(1, N_CUSTOMERS):05d}",
                "order_ts": pay_ts.isoformat(timespec="seconds"),
                "order_status": status,
                "channel": channel,
                "currency": CURRENCY,
                "shipping_cost": _money(shipping),
                "discount_amount": _money(order_disc),
            }
        )
        payments.append(
            {
                "payment_id": f"PAY-{order_seq:07d}",
                "order_id": order_id,
                "payment_ts": pay_ts.isoformat(timespec="seconds"),
                "payment_method": method,
                "payment_status": pay_status,
                "amount": _money(amount),
                "gateway_fee": _money(amount * FEE_RATE[method]),
                "currency": CURRENCY,
            }
        )
        for line in order_items:
            items.append({k: v for k, v in line.items() if not k.startswith("_")})
            if status == OrderStatus.DELIVERED.value and rng.random() < 0.06:
                qty = int(line["quantity"])
                refund = float(line["_unit"]) * qty - float(line["_disc"])
                returns.append(
                    {
                        "return_id": f"RET-{len(returns) + 1:07d}",
                        "order_id": order_id,
                        "sku": line["sku"],
                        "return_ts": (pay_ts + timedelta(days=rng.randint(12, 30))).isoformat(
                            timespec="seconds"
                        ),
                        "quantity": str(qty),
                        "refund_amount": _money(refund),
                        "reason": rng.choice(RETURN_REASONS),
                    }
                )

    batch1 = _dirty_batch1(rng, catalog, orders, items, payments, events, returns)
    batch2 = _incremental_batch2(rng, catalog, orders, items, payments, events, skus)
    return [batch1, batch2]


def _dirty_batch1(
    rng: random.Random,
    catalog: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    items: list[dict[str, Any]],
    payments: list[dict[str, Any]],
    events: list[dict[str, Any]],
    returns: list[dict[str, Any]],
) -> RawBatch:
    """Full historical load with intentional defects for Silver quality gates."""
    products = [dict(p) for p in catalog]
    # Cosmetic dirt: normalizable values.
    for o in rng.sample(orders, int(len(orders) * 0.02)):
        o["order_status"] = o["order_status"].title() + " "
    for o in rng.sample(orders, int(len(orders) * 0.02)):
        o["channel"] = "Sitio Web" if o["channel"] == "web" else o["channel"].upper()
    for o in rng.sample(orders, int(len(orders) * 0.01)):
        o["currency"] = "$"
    for it in rng.sample(items, int(len(items) * 0.03)):
        it["sku"] = it["sku"].lower().replace("-", "_")

    # Hard defects: must be quarantined.
    for o in rng.sample(orders, 15):
        o["currency"] = "USD"
    for o in rng.sample(orders, 10):
        o["order_status"] = "EN PROCESO"
    for o in rng.sample(orders, 5):
        o["shipping_cost"] = "-5490.00"
    for it in rng.sample(items, 8):
        it["quantity"] = "0"
    for it in rng.sample(items, 4):
        it["quantity"] = "-1"
    for it in rng.sample(items, 6):
        it["unit_price"] = "0.00"
    for it in rng.sample(items, 4):
        it["unit_price"] = "-100.00"
    orphans = [dict(rng.choice(items)) for _ in range(6)]
    for n, it in enumerate(orphans, start=1):
        it["order_item_id"] = f"OIT-9{n:07d}"
        it["order_id"] = "ORD-9999999"
    unknown_sku = [dict(rng.choice(items)) for _ in range(5)]
    for it in unknown_sku:
        it["sku"] = "XXX99-NEG-M"
    for n, it in enumerate(unknown_sku, start=1):
        it["order_item_id"] = f"OIT-8{n:07d}"
    for p in rng.sample(payments, 5):
        p["amount"] = "0.00"
    for p in rng.sample(payments, 4):
        p["payment_method"] = "Credit Card"
    for e in rng.sample(events, 5):
        e["event_type"] = "cart_abandoned"
    for r in rng.sample(returns, min(4, len(returns))):
        r["refund_amount"] = "0.00"
    for r in rng.sample(returns, min(2, len(returns))):
        r["quantity"] = "0"
    for p in rng.sample(products, 5):
        p["category"] = p["category"].title() + " "
    products.append(
        {
            "sku": "PAN99-NEG-XXXL",
            "product_name": "Pantalón talle extra",
            "category": "pantalon",
            "size": "XXXL",
            "unit_cost": "9000.00",
            "list_price": "18000.00",
            "stock_on_hand": "3",
        }
    )

    # Exact duplicates (re-sent messages).
    items_dup = [dict(it) for it in rng.sample(items, int(len(items) * 0.01))]
    events_dup = [dict(e) for e in rng.sample(events, int(len(events) * 0.01))]

    return RawBatch(
        batch_id="batch-001",
        sources={
            "products": products,
            "orders": orders,
            "order_items": items + orphans + unknown_sku + items_dup,
            "payments": payments,
            "returns": returns,
            "checkout_events": events + events_dup,
        },
    )


def _incremental_batch2(
    rng: random.Random,
    catalog: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    items: list[dict[str, Any]],
    payments: list[dict[str, Any]],
    events: list[dict[str, Any]],
    skus: list[str],
) -> RawBatch:
    """Incremental load: status progressions, late payments, stock updates, re-sends."""
    updated_orders: list[dict[str, Any]] = []
    updated_payments: list[dict[str, Any]] = []
    pay_by_order = {p["order_id"]: p for p in payments}
    for o in orders:
        status = o["order_status"]
        if status == OrderStatus.PENDING_PAYMENT.value:
            new_status = OrderStatus.PAID.value
            pay = dict(pay_by_order[o["order_id"]])
            pay["payment_status"] = "approved"
            updated_payments.append(pay)
        elif status == OrderStatus.PAID.value and rng.random() < 0.5:
            new_status = OrderStatus.SHIPPED.value
        elif status == OrderStatus.SHIPPED.value and rng.random() < 0.5:
            new_status = OrderStatus.DELIVERED.value
        else:
            continue
        updated = dict(o)
        updated["order_status"] = new_status
        updated_orders.append(updated)

    stock_updates = []
    for p in rng.sample(catalog, 40):
        updated = dict(p)
        updated["stock_on_hand"] = str(rng.randint(0, 30))
        stock_updates.append(updated)
    # Corrupt newer version: must be quarantined, the valid batch-001 version must survive.
    corrupt = dict(rng.choice(catalog))
    corrupt["stock_on_hand"] = "-4"
    stock_updates.append(corrupt)

    resent_items = [dict(it) for it in rng.sample(items, 50)]
    resent_events = [dict(e) for e in rng.sample(events, 30)]
    _ = skus
    return RawBatch(
        batch_id="batch-002",
        sources={
            "products": stock_updates,
            "orders": updated_orders,
            "order_items": resent_items,
            "payments": updated_payments,
            "returns": [],
            "checkout_events": resent_events,
        },
    )
