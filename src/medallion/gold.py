"""Gold layer: business marts computed with DuckDB over Silver tables.

Marts:

* ``mart_daily_unit_economics``: grain ``order_date x channel``. Cohort by order date.
* ``mart_checkout_funnel``: grain ``session_date x channel x payment_method``. Sequential stages.
* ``mart_sku_health``: grain ``sku`` (with category and size) over the sold-order period.

Definitions are documented in ``semantic_layer/metrics.yaml``; keep both in sync.
"""

from __future__ import annotations

from collections.abc import Mapping

import duckdb
import pyarrow as pa

from src.lakehouse import relation_to_arrow
from src.models import SOLD_ORDER_STATUSES

LOW_COVER_DAYS = 14
OVERSTOCK_DAYS = 180
HIGH_RETURN_RATE = 0.25

GOLD_TABLES: tuple[str, ...] = (
    "mart_daily_unit_economics",
    "mart_checkout_funnel",
    "mart_sku_health",
)
SILVER_ENTITIES: tuple[str, ...] = (
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
    "checkout_events",
)


def _sold_list() -> str:
    return ", ".join(f"'{s}'" for s in SOLD_ORDER_STATUSES)


UNIT_ECONOMICS_SQL = f"""
WITH sold AS (
    SELECT order_id,
           CAST(order_ts AS DATE) AS order_date,
           channel,
           shipping_cost,
           discount_amount
    FROM silver_orders
    WHERE order_status IN ({_sold_list()})
),
lines AS (
    SELECT s.order_date, s.channel, i.quantity, i.unit_price, i.discount_amount AS item_discount,
           p.unit_cost
    FROM sold s
    JOIN silver_order_items i ON i.order_id = s.order_id
    JOIN silver_products p ON p.sku = i.sku
),
sales AS (
    SELECT order_date, channel,
           SUM(quantity) AS units_sold,
           SUM(quantity * unit_price) AS gross_sales,
           SUM(item_discount) AS item_discounts,
           SUM(quantity * unit_cost) AS cogs
    FROM lines GROUP BY 1, 2
),
orders_agg AS (
    SELECT order_date, channel,
           COUNT(*) AS orders_count,
           SUM(discount_amount) AS order_discounts,
           SUM(shipping_cost) AS shipping_cost
    FROM sold GROUP BY 1, 2
),
fees AS (
    SELECT s.order_date, s.channel, SUM(p.gateway_fee) AS gateway_fees
    FROM sold s
    JOIN silver_payments p ON p.order_id = s.order_id
    WHERE p.payment_status IN ('approved', 'refunded')
    GROUP BY 1, 2
),
returns_agg AS (
    SELECT s.order_date, s.channel,
           SUM(r.refund_amount) AS refunds,
           SUM(r.quantity * p.unit_cost) AS returned_cogs
    FROM silver_returns r
    JOIN sold s ON s.order_id = r.order_id
    JOIN silver_products p ON p.sku = r.sku
    GROUP BY 1, 2
),
grid AS (SELECT DISTINCT order_date, channel FROM sold)
SELECT g.order_date,
       g.channel,
       CAST(COALESCE(o.orders_count, 0) AS INTEGER) AS orders_count,
       CAST(COALESCE(s.units_sold, 0) AS INTEGER) AS units_sold,
       CAST(COALESCE(s.gross_sales, 0) AS DECIMAL(18,2)) AS gross_sales,
       CAST(COALESCE(s.item_discounts, 0) + COALESCE(o.order_discounts, 0)
            AS DECIMAL(18,2)) AS discounts_amount,
       CAST(COALESCE(s.gross_sales, 0) - COALESCE(s.item_discounts, 0)
            - COALESCE(o.order_discounts, 0) AS DECIMAL(18,2)) AS net_sales,
       CAST(COALESCE(r.refunds, 0) AS DECIMAL(18,2)) AS refunds_amount,
       CAST(COALESCE(s.gross_sales, 0) - COALESCE(s.item_discounts, 0)
            - COALESCE(o.order_discounts, 0) - COALESCE(r.refunds, 0)
            AS DECIMAL(18,2)) AS net_revenue,
       CAST(COALESCE(f.gateway_fees, 0) AS DECIMAL(18,2)) AS gateway_fees_amount,
       CAST(COALESCE(o.shipping_cost, 0) AS DECIMAL(18,2)) AS shipping_cost_amount,
       CAST(COALESCE(s.cogs, 0) - COALESCE(r.returned_cogs, 0) AS DECIMAL(18,2)) AS cogs_amount,
       CAST(
           (COALESCE(s.gross_sales, 0) - COALESCE(s.item_discounts, 0)
            - COALESCE(o.order_discounts, 0) - COALESCE(r.refunds, 0))
           - COALESCE(f.gateway_fees, 0)
           - COALESCE(o.shipping_cost, 0)
           - (COALESCE(s.cogs, 0) - COALESCE(r.returned_cogs, 0))
           AS DECIMAL(18,2)) AS net_margin_amount,
       CAST(
           ((COALESCE(s.gross_sales, 0) - COALESCE(s.item_discounts, 0)
             - COALESCE(o.order_discounts, 0) - COALESCE(r.refunds, 0))
            - COALESCE(f.gateway_fees, 0)
            - COALESCE(o.shipping_cost, 0)
            - (COALESCE(s.cogs, 0) - COALESCE(r.returned_cogs, 0)))
           / NULLIF(COALESCE(s.gross_sales, 0) - COALESCE(s.item_discounts, 0)
                    - COALESCE(o.order_discounts, 0) - COALESCE(r.refunds, 0), 0)
           AS DECIMAL(9,4)) AS net_margin_pct
FROM grid g
LEFT JOIN sales s USING (order_date, channel)
LEFT JOIN orders_agg o USING (order_date, channel)
LEFT JOIN fees f USING (order_date, channel)
LEFT JOIN returns_agg r USING (order_date, channel)
ORDER BY g.order_date, g.channel
"""

CHECKOUT_FUNNEL_SQL = """
WITH sessions AS (
    SELECT session_id,
           CAST(MIN(event_ts) AS DATE) AS session_date,
           first(channel ORDER BY event_ts, event_id) AS channel,
           MAX(CASE WHEN event_type = 'cart_created' THEN 1 ELSE 0 END) AS has_cart,
           MAX(CASE WHEN event_type = 'checkout_started' THEN 1 ELSE 0 END) AS has_checkout,
           MAX(CASE WHEN event_type = 'payment_approved' THEN 1 ELSE 0 END) AS has_payment,
           COALESCE(
               MAX(CASE WHEN event_type = 'payment_approved' THEN payment_method END),
               MAX(CASE WHEN event_type IN ('checkout_started', 'payment_rejected')
                        THEN payment_method END),
               'sin_seleccion') AS payment_method
    FROM silver_checkout_events
    GROUP BY session_id
),
staged AS (
    SELECT session_date, channel, payment_method,
           has_cart AS stage_cart,
           has_cart * has_checkout AS stage_checkout,
           has_cart * has_checkout * has_payment AS stage_payment
    FROM sessions
)
SELECT session_date,
       channel,
       payment_method,
       CAST(SUM(stage_cart) AS INTEGER) AS carts_created,
       CAST(SUM(stage_checkout) AS INTEGER) AS checkouts_started,
       CAST(SUM(stage_payment) AS INTEGER) AS payments_approved,
       CAST(SUM(stage_checkout) * 1.0 / NULLIF(SUM(stage_cart), 0) AS DECIMAL(9,4))
           AS checkout_rate,
       CAST(SUM(stage_payment) * 1.0 / NULLIF(SUM(stage_checkout), 0) AS DECIMAL(9,4))
           AS payment_rate_from_checkout,
       CAST(SUM(stage_payment) * 1.0 / NULLIF(SUM(stage_cart), 0) AS DECIMAL(9,4))
           AS conversion_rate
FROM staged
GROUP BY session_date, channel, payment_method
ORDER BY session_date, channel, payment_method
"""

SKU_HEALTH_SQL = f"""
WITH period AS (
    SELECT GREATEST(
               DATE_DIFF('day', MIN(CAST(order_ts AS DATE)), MAX(CAST(order_ts AS DATE))) + 1,
               1) AS period_days
    FROM silver_orders
    WHERE order_status IN ({_sold_list()})
),
sold_lines AS (
    SELECT i.sku, i.quantity, i.quantity * i.unit_price AS gross, i.discount_amount AS disc
    FROM silver_order_items i
    JOIN silver_orders o ON o.order_id = i.order_id
    WHERE o.order_status IN ({_sold_list()})
),
sales AS (
    SELECT sku,
           SUM(quantity) AS units_sold,
           SUM(gross) AS gross_sales,
           SUM(disc) AS item_discounts
    FROM sold_lines GROUP BY sku
),
returns_agg AS (
    SELECT r.sku,
           SUM(r.quantity) AS units_returned,
           SUM(r.refund_amount) AS refunds
    FROM silver_returns r
    JOIN silver_orders o ON o.order_id = r.order_id
    WHERE o.order_status IN ({_sold_list()})
    GROUP BY r.sku
),
base AS (
    SELECT p.sku,
           p.product_name,
           p.category,
           p.size,
           p.stock_on_hand,
           pe.period_days,
           COALESCE(s.units_sold, 0) AS units_sold,
           COALESCE(r.units_returned, 0) AS units_returned,
           COALESCE(s.gross_sales, 0) AS gross_sales,
           COALESCE(s.gross_sales, 0) - COALESCE(s.item_discounts, 0)
               - COALESCE(r.refunds, 0) AS net_revenue
    FROM silver_products p
    CROSS JOIN period pe
    LEFT JOIN sales s ON s.sku = p.sku
    LEFT JOIN returns_agg r ON r.sku = p.sku
),
metrics AS (
    SELECT *,
           units_returned * 1.0 / NULLIF(units_sold, 0) AS return_rate,
           units_sold * 1.0 / period_days AS avg_daily_units
    FROM base
),
enriched AS (
    SELECT *,
           stock_on_hand / NULLIF(avg_daily_units, 0) AS days_of_cover
    FROM metrics
)
SELECT sku,
       product_name,
       category,
       size,
       CAST(stock_on_hand AS INTEGER) AS stock_on_hand,
       CAST(units_sold AS INTEGER) AS units_sold,
       CAST(units_returned AS INTEGER) AS units_returned,
       CAST(gross_sales AS DECIMAL(18,2)) AS gross_sales,
       CAST(net_revenue AS DECIMAL(18,2)) AS net_revenue,
       CAST(return_rate AS DECIMAL(9,4)) AS return_rate,
       CAST(COALESCE(return_rate >= {HIGH_RETURN_RATE}, FALSE) AS BOOLEAN) AS high_return_flag,
       CAST(avg_daily_units AS DECIMAL(12,4)) AS avg_daily_units,
       CAST(days_of_cover AS DECIMAL(12,2)) AS days_of_cover,
       CASE
           WHEN stock_on_hand = 0 THEN 'sin_stock'
           WHEN units_sold = 0 THEN 'sin_rotacion'
           WHEN days_of_cover < {LOW_COVER_DAYS} THEN 'reponer'
           WHEN days_of_cover > {OVERSTOCK_DAYS} THEN 'sobrestock'
           ELSE 'ok'
       END AS stock_alert,
       CAST(period_days AS INTEGER) AS period_days
FROM enriched
ORDER BY sku
"""


def build_gold(silver: Mapping[str, pa.Table]) -> dict[str, pa.Table]:
    """Compute the three Gold marts from Silver Arrow tables. Pure function: no I/O."""
    con = duckdb.connect(config={"enable_external_access": False})
    for entity in SILVER_ENTITIES:
        con.register(f"silver_{entity}", silver[entity])
    marts = {
        "mart_daily_unit_economics": UNIT_ECONOMICS_SQL,
        "mart_checkout_funnel": CHECKOUT_FUNNEL_SQL,
        "mart_sku_health": SKU_HEALTH_SQL,
    }
    result = {name: relation_to_arrow(con.sql(sql)) for name, sql in marts.items()}
    con.close()
    return result
