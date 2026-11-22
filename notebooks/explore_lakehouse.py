# %% [markdown]
# # Explorar el Lakehouse
#
# Script reproducible (formato percent, compatible con Jupytext y VS Code).
# Requiere haber ejecutado `make pipeline`.
#
# Muestra: historial de versiones Delta, time travel, conteo de cuarentena y consultas Gold.
# Ejecutar con: `python notebooks/explore_lakehouse.py`

# %%
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb
from deltalake import DeltaTable

from src.config import get_settings
from src.lakehouse import read_table

settings = get_settings()
opts = settings.storage_options()
print(f"Almacenamiento: {settings.lakehouse_storage}")

# %% [markdown]
# ## 1. Historial de versiones Delta (auditoría y time travel)

# %%
for layer, table in [("bronze", "orders"), ("silver", "orders"), ("gold", "mart_sku_health")]:
    uri = settings.table_uri(layer, table)
    dt = DeltaTable(uri, storage_options=opts)
    print(f"\n{layer}/{table}: versión actual {dt.version()}")
    for entry in dt.history()[:4]:
        print(f"  v{entry.get('version')}  {entry.get('operation')}  {entry.get('timestamp')}")

# %% [markdown]
# ## 2. Time travel: primera versión de Bronze de pedidos
#
# Bronze es append-only: la versión 0 contiene solo el primer batch cargado.

# %%
bronze_uri = settings.table_uri("bronze", "orders")
v0 = DeltaTable(bronze_uri, version=0, storage_options=opts).to_pyarrow_table(columns=["_batch_id"])
current = read_table(bronze_uri, opts, columns=["_batch_id"])
print("Batches en v0:", sorted(set(v0.column("_batch_id").to_pylist())))
print("Filas v0:", v0.num_rows, "| filas actuales:", current.num_rows)

# %% [markdown]
# ## 3. Cuarentena de calidad

# %%
quarantine_rows = []
for entity in ("products", "orders", "order_items", "payments", "returns", "checkout_events"):
    q = read_table(settings.table_uri("quarantine", entity), opts)
    reasons: dict[str, int] = {}
    for value in q.column("_dq_reasons").to_pylist():
        for reason in str(value).split("; "):
            if reason:
                reasons[reason] = reasons.get(reason, 0) + 1
    quarantine_rows.append((entity, q.num_rows, reasons))

for entity, count, reasons in quarantine_rows:
    top = ", ".join(f"{k}={v}" for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])[:4])
    print(f"{entity:<16} cuarentena={count:<5} {top}")

# %% [markdown]
# ## 4. Consultas Gold (lectura directa con DuckDB sobre Arrow desde Delta)

# %%
gold = {
    name: read_table(settings.table_uri("gold", name), opts)
    for name in ("mart_daily_unit_economics", "mart_checkout_funnel", "mart_sku_health")
}
con = duckdb.connect(config={"enable_external_access": False})
for name, table in gold.items():
    con.register(name, table)

print(
    con.execute(
        """
        SELECT channel,
               SUM(net_sales) AS venta_neta,
               SUM(net_margin_amount) AS margen_neto,
               ROUND(SUM(net_margin_amount) / NULLIF(SUM(net_revenue), 0), 4) AS margen_pct
        FROM mart_daily_unit_economics
        GROUP BY channel ORDER BY margen_neto DESC
        """
    ).df()
)
print(
    con.execute(
        """
        SELECT payment_method,
               SUM(carts_created) AS carritos,
               SUM(payments_approved) AS pagos_aprobados,
               ROUND(SUM(payments_approved) * 1.0 / NULLIF(SUM(carts_created), 0), 4)
                   AS conversion
        FROM mart_checkout_funnel
        GROUP BY payment_method ORDER BY conversion DESC
        """
    ).df()
)
print(
    con.execute(
        """
        SELECT stock_alert, COUNT(*) AS skus
        FROM mart_sku_health GROUP BY stock_alert ORDER BY skus DESC
        """
    ).df()
)
con.close()
