"""Executive summary for the store owner (``output/client_summary.md`` and ``.json``).

The summary is computed from the Gold marts and the quality/ingestion reports. Every number
in it is either read from those tables or derived by plain sums and ratios of them. Wording
avoids lakehouse jargon: layers and tables are not mentioned, only what was analyzed, what
was excluded and why.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, Field

from src.ingestion.csv_adapter import IngestionReport
from src.ingestion.privacy import FieldPrivacyStats
from src.models import SOLD_ORDER_STATUSES, EntityQualityReport

MIN_RETURN_SAMPLE = 5
LOW_COVER_DAYS = 14
TOP_N = 5

REASON_ES: dict[str, str] = {
    "key_missing": "falta el identificador del registro",
    "missing_order_id": "falta el número de pedido",
    "missing_sku": "falta el SKU",
    "missing_product_name": "falta el nombre del producto",
    "invalid_order_ts": "fecha del pedido inválida",
    "invalid_status": "estado del pedido no reconocido",
    "invalid_channel": "canal no reconocido",
    "invalid_currency": "moneda distinta de pesos",
    "invalid_shipping_cost": "costo de envío inválido",
    "invalid_discount": "descuento inválido",
    "invalid_quantity": "cantidad inválida",
    "invalid_unit_price": "precio unitario inválido o cero",
    "orphan_order_item": "línea de un pedido excluido",
    "sku_not_in_catalog": "SKU sin ficha de producto",
    "invalid_payment_ts": "fecha de pago inválida",
    "invalid_payment_method": "medio de pago no reconocido",
    "invalid_payment_status": "estado de pago no reconocido",
    "invalid_amount": "monto de pago inválido o cero",
    "invalid_gateway_fee": "comisión inválida",
    "invalid_fee_source": "origen de comisión inválido",
    "orphan_payment": "pago de un pedido excluido",
    "invalid_return_ts": "fecha de devolución inválida",
    "invalid_refund": "reembolso inválido o cero",
    "orphan_return": "devolución de un pedido excluido",
    "return_item_not_sold": "devolución de un producto no vendido en ese pedido",
    "invalid_stock": "stock inválido",
    "invalid_unit_cost": "costo unitario faltante o inválido",
    "invalid_unit_cost_source": "origen de costo inválido",
    "invalid_list_price": "precio de lista inválido",
    "invalid_category": "categoría no reconocida",
    "invalid_size": "talle no reconocido",
}
ENTITY_ES: dict[str, str] = {
    "products": "Productos",
    "orders": "Pedidos",
    "order_items": "Líneas de venta",
    "payments": "Pagos",
    "returns": "Devoluciones",
    "checkout_events": "Eventos de checkout",
}
ACTION_ES = {
    "drop": "eliminado antes de guardar",
    "pseudonymize": "reemplazado por un código irreversible",
}
STOCK_ES = {
    "sin_stock": "sin stock",
    "reponer": "reponer (menos de 14 días de cobertura)",
    "sobrestock": "sobrestock (más de 180 días de cobertura)",
    "sin_rotacion": "sin ventas en el período",
    "sin_datos_stock": "sin dato de stock",
    "ok": "ok",
}


class SalesTotals(BaseModel):
    period_start: str
    period_end: str
    orders: int
    units: int
    gross_sales: str
    discounts: str
    refunds: str
    net_revenue: str
    gateway_fees: str
    shipping: str
    cogs: str
    net_margin: str
    margin_pct: float | None


class ChannelLine(BaseModel):
    channel: str
    orders: int
    units: int
    net_revenue: str
    net_margin: str
    margin_pct: float | None


class ReturnLine(BaseModel):
    sku: str
    product_name: str
    units_sold: int
    units_returned: int
    return_rate: float


class StockLine(BaseModel):
    sku: str
    product_name: str
    stock_on_hand: int | None
    days_of_cover: float | None
    stock_alert: str


class QualityLine(BaseModel):
    entity: str
    rows_read: int
    rows_used: int
    rows_excluded: int
    main_reasons: list[str]


class ClientSummary(BaseModel):
    generated_at: str
    batch_id: str
    mappings: list[str]
    totals: SalesTotals | None
    by_channel: list[ChannelLine]
    cost_exact_share: float | None
    fee_exact_share: float | None
    top_return_skus: list[ReturnLine]
    stock_counts: dict[str, int]
    reorder_items: list[StockLine]
    excluded_sales_lines: int
    excluded_sales_amount: str
    sold_orders_without_valid_payment: int
    quality: list[QualityLine]
    privacy: list[FieldPrivacyStats]
    ignored_columns: dict[str, list[str]]
    warnings: list[str] = Field(default_factory=list)
    findings: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def _money(value: Decimal | float | int) -> str:
    text = f"{float(value):,.0f}"
    return "$ " + text.replace(",", "§").replace(".", ",").replace("§", ".")


def _pct(value: float | None) -> str:
    if value is None:
        return "s/d"
    return f"{value * 100:,.1f} %".replace(",", "§").replace(".", ",").replace("§", ".")


def _ratio(num: float, den: float) -> float | None:
    return num / den if den else None


def _dec(value: Any) -> Decimal:
    return Decimal(str(value)) if value is not None else Decimal(0)


def _excluded_sales(quarantine_items: pa.Table) -> tuple[int, Decimal]:
    count = 0
    amount = Decimal(0)
    for row in _rows(quarantine_items):
        count += 1
        qty, price = row.get("quantity"), row.get("unit_price")
        if qty is None or price is None or qty <= 0 or price <= 0:
            continue
        discount = _dec(row.get("discount_amount"))
        amount += Decimal(qty) * _dec(price) - discount
    return count, amount


def _sold_orders_without_payment(silver: dict[str, pa.Table]) -> int:
    orders = {
        r["order_id"] for r in _rows(silver["orders"]) if r["order_status"] in SOLD_ORDER_STATUSES
    }
    with_fee = {
        r["order_id"]
        for r in _rows(silver["payments"])
        if r["payment_status"] in ("approved", "refunded")
    }
    return len(orders - with_fee)


def _reason_list(reasons: Counter[str], limit: int = 3) -> list[str]:
    ordered = sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
    return [f"{REASON_ES.get(name, name)} ({count})" for name, count in ordered]


def _quality_lines(quality: list[EntityQualityReport]) -> list[QualityLine]:
    lines: list[QualityLine] = []
    for item in quality:
        if item.bronze_rows == 0:
            continue  # source absent from this load: covered by a caveat instead
        reasons: Counter[str] = Counter()
        for name, count in item.reasons.items():
            reasons[name] += count
        lines.append(
            QualityLine(
                entity=ENTITY_ES.get(item.entity, item.entity),
                rows_read=item.bronze_rows,
                rows_used=item.silver_rows,
                rows_excluded=item.quarantine_rows,
                main_reasons=_reason_list(reasons),
            )
        )
    return lines


def build_client_summary(
    gold: dict[str, pa.Table],
    silver: dict[str, pa.Table],
    quarantine: dict[str, pa.Table],
    quality: list[EntityQualityReport],
    ingestion: IngestionReport,
    generated_at: str,
) -> ClientSummary:
    """Compute every figure and finding of the client summary."""
    econ = _rows(gold["mart_daily_unit_economics"])
    sku_rows = _rows(gold["mart_sku_health"])
    funnel_rows = _rows(gold["mart_checkout_funnel"])
    active = [r for r in econ if r["orders_count"] > 0]

    totals: SalesTotals | None = None
    by_channel: list[ChannelLine] = []
    cost_exact: float | None = None
    fee_exact: float | None = None
    if active:
        sums = {
            key: sum((_dec(r[key]) for r in active), Decimal(0))
            for key in (
                "gross_sales",
                "discounts_amount",
                "refunds_amount",
                "net_revenue",
                "gateway_fees_amount",
                "shipping_cost_amount",
                "cogs_amount",
                "net_margin_amount",
            )
        }
        orders = sum(r["orders_count"] for r in active)
        units = sum(r["units_sold"] for r in active)
        sold_lines = sum(r["sold_lines"] for r in active)
        est_lines = sum(r["estimated_cost_lines"] for r in active)
        fee_payments = sum(r["fee_payments"] for r in active)
        est_fees = sum(r["estimated_fee_payments"] for r in active)
        cost_exact = (sold_lines - est_lines) / sold_lines if sold_lines else None
        fee_exact = (fee_payments - est_fees) / fee_payments if fee_payments else None
        net_rev = float(sums["net_revenue"])
        totals = SalesTotals(
            period_start=min(str(r["order_date"]) for r in active),
            period_end=max(str(r["order_date"]) for r in active),
            orders=orders,
            units=units,
            gross_sales=_money(sums["gross_sales"]),
            discounts=_money(sums["discounts_amount"]),
            refunds=_money(sums["refunds_amount"]),
            net_revenue=_money(sums["net_revenue"]),
            gateway_fees=_money(sums["gateway_fees_amount"]),
            shipping=_money(sums["shipping_cost_amount"]),
            cogs=_money(sums["cogs_amount"]),
            net_margin=_money(sums["net_margin_amount"]),
            margin_pct=_ratio(float(sums["net_margin_amount"]), net_rev),
        )
        channels: dict[str, list[dict[str, Any]]] = {}
        for row in active:
            channels.setdefault(str(row["channel"]), []).append(row)
        for channel, rows in sorted(channels.items()):
            net = sum((_dec(r["net_revenue"]) for r in rows), Decimal(0))
            margin = sum((_dec(r["net_margin_amount"]) for r in rows), Decimal(0))
            by_channel.append(
                ChannelLine(
                    channel=channel,
                    orders=sum(r["orders_count"] for r in rows),
                    units=sum(r["units_sold"] for r in rows),
                    net_revenue=_money(net),
                    net_margin=_money(margin),
                    margin_pct=_ratio(float(margin), float(net)),
                )
            )

    top_returns = sorted(
        (
            r
            for r in sku_rows
            if r["units_sold"] >= MIN_RETURN_SAMPLE and r["return_rate"] is not None
        ),
        key=lambda r: (-float(r["return_rate"]), r["sku"]),
    )[:TOP_N]
    return_lines = [
        ReturnLine(
            sku=r["sku"],
            product_name=r["product_name"] or r["sku"],
            units_sold=r["units_sold"],
            units_returned=r["units_returned"],
            return_rate=float(r["return_rate"]),
        )
        for r in top_returns
    ]

    alerts = Counter(str(r["stock_alert"]) for r in sku_rows)
    reorder_rows = sorted(
        (r for r in sku_rows if r["stock_alert"] in ("sin_stock", "reponer")),
        key=lambda r: (r["stock_alert"] != "sin_stock", r["days_of_cover"] or 0, r["sku"]),
    )[:10]
    reorder = [
        StockLine(
            sku=r["sku"],
            product_name=r["product_name"] or r["sku"],
            stock_on_hand=r["stock_on_hand"],
            days_of_cover=float(r["days_of_cover"]) if r["days_of_cover"] is not None else None,
            stock_alert=r["stock_alert"],
        )
        for r in reorder_rows
    ]

    excluded_count, excluded_amount = _excluded_sales(quarantine["order_items"])
    unpaid_sold = _sold_orders_without_payment(silver)
    ignored = {f.source: f.ignored_columns for f in ingestion.files if f.ignored_columns}

    summary = ClientSummary(
        generated_at=generated_at,
        batch_id=ingestion.batch_id,
        mappings=ingestion.mappings,
        totals=totals,
        by_channel=by_channel,
        cost_exact_share=cost_exact,
        fee_exact_share=fee_exact,
        top_return_skus=return_lines,
        stock_counts=dict(sorted(alerts.items())),
        reorder_items=reorder,
        excluded_sales_lines=excluded_count,
        excluded_sales_amount=_money(excluded_amount),
        sold_orders_without_valid_payment=unpaid_sold,
        quality=_quality_lines(quality),
        privacy=ingestion.privacy,
        ignored_columns=ignored,
        warnings=list(ingestion.warnings),
    )
    summary.findings = _findings(summary, funnel_rows)
    summary.caveats = _caveats(summary, funnel_rows)
    summary.next_steps = _next_steps(summary)
    return summary


def _findings(s: ClientSummary, funnel_rows: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    if s.totals is not None:
        t = s.totals
        out.append(
            f"Se analizaron {t.orders} pedidos y {t.units} unidades entre {t.period_start} "
            f"y {t.period_end}. El ingreso neto (ventas menos descuentos y devoluciones) "
            f"fue {t.net_revenue}."
        )
        out.append(
            f"Después de comisiones de pasarela, envíos y costo de mercadería, el margen neto "
            f"fue {t.net_margin} ({_pct(t.margin_pct)} del ingreso neto)."
        )
        if s.by_channel:
            best = max(s.by_channel, key=lambda c: _money_to_float(c.net_margin))
            out.append(
                f"El canal con mayor margen neto fue {best.channel} ({best.net_margin}, "
                f"{_pct(best.margin_pct)} del ingreso neto de ese canal)."
            )
    if s.top_return_skus:
        top = s.top_return_skus[0]
        out.append(
            f"El producto con mayor tasa de devolución es {top.sku} ({top.product_name}): "
            f"{top.units_returned} de {top.units_sold} unidades vendidas "
            f"({_pct(top.return_rate)})."
        )
    reorder_now = s.stock_counts.get("sin_stock", 0)
    reorder_soon = s.stock_counts.get("reponer", 0)
    if reorder_now or reorder_soon:
        out.append(
            f"Hay {reorder_now} SKU sin stock y {reorder_soon} SKU con menos de "
            f"{LOW_COVER_DAYS} días de cobertura: conviene revisar la reposición."
        )
    if s.stock_counts.get("sin_datos_stock"):
        out.append(
            f"{s.stock_counts['sin_datos_stock']} SKU no tienen dato de stock en el archivo "
            "de costos: no se pudo evaluar su cobertura."
        )
    if funnel_rows and any(r["carts_created"] for r in funnel_rows):
        carts = sum(r["carts_created"] for r in funnel_rows)
        approved = sum(r["payments_approved"] for r in funnel_rows)
        out.append(
            f"En el embudo de compra, {approved} de {carts} carritos terminaron en pago "
            f"aprobado ({_pct(_ratio(approved, carts))})."
        )
    return out


def _caveats(s: ClientSummary, funnel_rows: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    if s.cost_exact_share is not None and s.cost_exact_share < 1:
        out.append(
            f"El {_pct(1 - s.cost_exact_share)} de las líneas vendidas usa un costo unitario "
            "estimado. El margen es provisional hasta cargar los costos reales."
        )
    if s.fee_exact_share is not None and s.fee_exact_share < 1:
        out.append(
            f"El {_pct(1 - s.fee_exact_share)} de los pagos usa una comisión estimada a partir "
            "de la tasa configurada, no del importe informado por la pasarela."
        )
    if s.excluded_sales_lines:
        out.append(
            f"{s.excluded_sales_lines} líneas de venta (por unos {s.excluded_sales_amount} "
            "a precio de venta informado) quedaron fuera de los números por datos inválidos."
        )
    if s.sold_orders_without_valid_payment:
        out.append(
            f"{s.sold_orders_without_valid_payment} pedidos vendidos no tienen un pago válido: "
            "sus comisiones no están incluidas en el margen."
        )
    if not funnel_rows or not any(r["carts_created"] for r in funnel_rows):
        out.append("Esta carga no incluye datos de carritos ni checkout: el embudo no se analiza.")
    out.extend(w if w.endswith(".") else w + "." for w in s.warnings)
    out.append(
        "Los pedidos se atribuyen a su fecha de pedido. Las devoluciones se restan del pedido "
        "original, no de la fecha en que se devolvieron."
    )
    return out


def _next_steps(s: ClientSummary) -> list[str]:
    out: list[str] = []
    if s.reorder_items:
        out.append("Revisar la reposición de los SKU listados en la sección de stock.")
    if s.top_return_skus:
        out.append("Revisar los motivos de devolución de los productos con mayor tasa.")
    if s.excluded_sales_lines or any(q.rows_excluded for q in s.quality):
        out.append(
            "Corregir en la planilla de origen las filas excluidas (ver calidad de datos) "
            "y volver a cargar el archivo."
        )
    if (s.cost_exact_share or 1) < 1 or (s.fee_exact_share or 1) < 1:
        out.append("Cargar costos unitarios reales y comisiones exactas para confirmar el margen.")
    out.append("Validar las cifras con el contador antes de usarlas para decisiones de precio.")
    return out


def _money_to_float(text: str) -> float:
    cleaned = text.replace("$", "").strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _privacy_lines(stats: list[FieldPrivacyStats]) -> list[str]:
    lines: list[str] = []
    for item in stats:
        if item.cells_affected == 0:
            continue
        lines.append(f"- {item.field}: {ACTION_ES[item.action]} ({item.cells_affected} celdas).")
    return lines


def render_client_markdown(s: ClientSummary) -> str:
    """Render the summary as a Spanish document for the store owner."""
    lines: list[str] = [
        "# Resumen de ventas y márgenes",
        "",
        f"- Generado: {s.generated_at}",
        f"- Archivos analizados: {', '.join(s.mappings) if s.mappings else 'sin datos'}",
        f"- Identificador de la carga: `{s.batch_id}`",
    ]
    if s.totals is not None:
        t = s.totals
        lines.append(f"- Período: {t.period_start} a {t.period_end} (fecha de pedido)")
    lines += ["", "## 1. Hallazgos principales", ""]
    lines += [f"- {f}" for f in s.findings] or ["- No hay ventas válidas para analizar."]

    if s.totals is not None:
        t = s.totals
        lines += [
            "",
            "## 2. Cifras del período",
            "",
            "| Concepto | Valor |",
            "|---|---|",
            f"| Pedidos | {t.orders} |",
            f"| Unidades vendidas | {t.units} |",
            f"| Ventas brutas | {t.gross_sales} |",
            f"| Descuentos | {t.discounts} |",
            f"| Devoluciones reembolsadas | {t.refunds} |",
            f"| Ingreso neto | {t.net_revenue} |",
            f"| Comisiones de pasarela | {t.gateway_fees} |",
            f"| Costo de envío | {t.shipping} |",
            f"| Costo de mercadería (neto de devueltas) | {t.cogs} |",
            f"| **Margen neto** | **{t.net_margin}** ({_pct(t.margin_pct)}) |",
        ]
    if s.by_channel:
        lines += [
            "",
            "## 3. Resultado por canal",
            "",
            "| Canal | Pedidos | Unidades | Ingreso neto | Margen neto | % margen |",
            "|---|---|---|---|---|---|",
        ]
        for c in s.by_channel:
            lines.append(
                f"| {c.channel} | {c.orders} | {c.units} | {c.net_revenue} | {c.net_margin} "
                f"| {_pct(c.margin_pct)} |"
            )
    if s.top_return_skus:
        lines += [
            "",
            "## 4. Devoluciones",
            "",
            f"Productos con al menos {MIN_RETURN_SAMPLE} unidades vendidas, ordenados por tasa "
            "de devolución:",
            "",
            "| SKU | Producto | Vendidas | Devueltas | Tasa |",
            "|---|---|---|---|---|",
        ]
        for r in s.top_return_skus:
            lines.append(
                f"| {r.sku} | {r.product_name} | {r.units_sold} | {r.units_returned} "
                f"| {_pct(r.return_rate)} |"
            )
    lines += ["", "## 5. Stock y reposición", ""]
    if s.stock_counts:
        lines += ["| Estado | SKU |", "|---|---|"]
        for alert, count in s.stock_counts.items():
            lines.append(f"| {STOCK_ES.get(alert, alert)} | {count} |")
    if s.reorder_items:
        lines += [
            "",
            "SKU para revisar primero:",
            "",
            "| SKU | Producto | Stock | Días de cobertura | Estado |",
            "|---|---|---|---|---|",
        ]
        for item in s.reorder_items:
            cover = (
                f"{item.days_of_cover:,.1f}".replace(".", ",")
                if item.days_of_cover is not None
                else "s/d"
            )
            stock = str(item.stock_on_hand) if item.stock_on_hand is not None else "s/d"
            status = STOCK_ES.get(item.stock_alert, item.stock_alert)
            lines.append(f"| {item.sku} | {item.product_name} | {stock} | {cover} | {status} |")
    lines += ["", "## 6. Calidad de los datos", ""]
    lines += [
        "| Información | Filas leídas | Filas usadas | Filas excluidas | Motivos principales |",
        "|---|---|---|---|---|",
    ]
    for q in s.quality:
        reasons = "; ".join(q.main_reasons) if q.main_reasons else "-"
        lines.append(
            f"| {q.entity} | {q.rows_read} | {q.rows_used} | {q.rows_excluded} | {reasons} |"
        )
    if s.excluded_sales_lines:
        lines += [
            "",
            f"Líneas de venta excluidas: {s.excluded_sales_lines}. Revise la planilla de origen "
            "para corregirlas.",
        ]
    lines += ["", "## 7. Datos personales", ""]
    privacy = _privacy_lines(s.privacy)
    if privacy:
        lines += ["Tratamiento aplicado antes de guardar los datos:", "", *privacy]
    else:
        lines.append("No se encontraron campos personales en los archivos.")
    if s.ignored_columns:
        lines += ["", "Columnas del archivo que no se usaron:"]
        for source, cols in s.ignored_columns.items():
            lines.append(f"- {source}: {', '.join(cols)}")
    lines += ["", "## 8. Advertencias y límites", ""]
    lines += [f"- {c}" for c in s.caveats] or ["- Sin advertencias."]
    lines += ["", "## 9. Próximos pasos", ""]
    lines += [f"{i}. {step}" for i, step in enumerate(s.next_steps, start=1)]
    lines.append("")
    return "\n".join(lines)


def write_client_summary(output_dir: Path, summary: ClientSummary) -> tuple[Path, Path]:
    """Write ``client_summary.md`` and ``client_summary.json`` into ``output_dir``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / "client_summary.md"
    json_path = output_dir / "client_summary.json"
    md_path.write_text(render_client_markdown(summary), encoding="utf-8")
    json_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
    return md_path, json_path
