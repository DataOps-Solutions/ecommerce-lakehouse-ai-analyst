# ecommerce-lakehouse-ai-analyst

Mini Lakehouse con arquitectura Medallion sobre Delta Lake y un analista BI con IA que responde preguntas de negocio en lenguaje natural mediante SQL de solo lectura sobre la capa Gold.

Está pensado para correr en local sin credenciales ni servicios externos, y es portable a AWS S3, Athena y Bedrock por configuración. Los datos son sintéticos y generados de forma determinística (seed fija): los números del reporte describen ese dataset de prueba, no un negocio real.

## Arquitectura

```
 raw batches (ERP / e-commerce export, strings)
        │
        ▼
 ┌──────────────┐   append-only, idempotente por batch_id
 │ BRONZE       │   + _batch_id, _source, _source_seq, _ingested_at
 └──────┬───────┘
        ▼   normalización · Data Quality Gates por fila · dedupe determinístico
        │   · integridad pedido-ítem / SKU-catálogo / devolución-ítem vendido
 ┌──────┴───────┐        ┌────────────────┐
 │ SILVER       │◄──────►│ quarantine/    │  registros rechazados con _dq_reasons
 └──────┬───────┘        └────────────────┘  (el pipeline nunca se rompe por datos sucios)
        ▼   marts de negocio (DuckDB sobre Delta)
 ┌──────────────────────────────────────────┐
 │ GOLD                                     │
 │  mart_daily_unit_economics               │
 │  mart_checkout_funnel                    │
 │  mart_sku_health                         │
 └──────┬───────────────────────────────────┘
        ▼
 semantic_layer/metrics.yaml  (tablas, columnas, métricas, sinónimos, reglas)
        ▼
 AI/BI SQL Analyst (src/analyst)
   pregunta → SQL (mock | bedrock) → guardrails AST → DuckDB (solo Gold)
   → tabla exacta + SQL ejecutada + interpretación verificada contra la tabla
```

Cada escritura Delta registra una auditoría en `audit/pipeline_runs`. Bronze conserva el historial completo, así que el time travel (`DeltaTable(uri, version=n)`) reconstruye cualquier estado previo.

### Capas

| Capa | Tablas | Contenido |
|---|---|---|
| Bronze | `bronze/{products, orders, order_items, payments, returns, checkout_events}` | Valores crudos como string, metadatos de carga, append-only. |
| Silver | `silver/<entidad>` | Tipado, normalización (estados, canales, medios de pago, monedas, SKUs), dedupe por clave de negocio (gana el batch más reciente) e integridad referencial. |
| Quarantine | `quarantine/<entidad>` | Filas rechazadas con `_dq_reasons` (reglas que fallaron) y `_dq_stage` (`row` o `integrity`). |
| Gold | `gold/mart_*` | Marts listos para BI. Definiciones en `semantic_layer/metrics.yaml`. |
| Audit | `audit/pipeline_runs` | Una fila por tabla escrita y por corrida. |

**Reglas de calidad Silver (ejemplos):** precio unitario mayor a cero, cantidades mayores a cero, descuento no mayor al importe de la línea, moneda ARS, estados y canales del catálogo permitido, pedido inexistente para un ítem o pago (`orphan_*`), SKU fuera del catálogo, devolución de un SKU que no se vendió en ese pedido.

**Orden de dedupe:** las reglas por fila se aplican antes del dedupe. Una versión nueva y corrupta de un registro (por ejemplo, stock negativo en el batch 2) va a cuarentena y no oculta la versión válida anterior.

### Gold: definiciones principales

- **`mart_daily_unit_economics`** (grano fecha de pedido × canal): venta bruta, descuentos (ítem + pedido), reembolsos, ingreso neto, comisiones de pasarela, costo de envío, costo de mercadería neto de unidades devueltas y reingresadas, y margen neto real. Solo cuentan los pedidos `paid`, `shipped` y `delivered`.
- **`mart_checkout_funnel`** (fecha × canal × medio de pago): carritos, checkouts iniciados y pagos aprobados por sesión. Las etapas son secuenciales: una sesión cuenta en una etapa solo si alcanzó las anteriores.
- **`mart_sku_health`** (SKU con categoría y talle): unidades, ingresos netos, tasa de devolución, días de cobertura y `stock_alert` (`sin_stock`, `reponer`, `sobrestock`, `sin_rotacion`, `ok`).

## Equivalencias con Databricks y AWS

| Concepto | Este proyecto | Databricks | AWS |
|---|---|---|---|
| Almacenamiento Delta | `deltalake` (delta-rs), local o `s3://` | Delta Lake en DBFS / Volumes | S3 |
| Motor SQL | DuckDB sobre Arrow leído desde Delta | Databricks SQL / Photon | Athena (Trino) |
| Gobierno y catálogo | `semantic_layer/metrics.yaml` + whitelist AST | Unity Catalog | Glue Catalog + Lake Formation |
| Analista en lenguaje natural | `src/analyst` (mock o Bedrock) | AI/BI Genie | Bedrock + Athena |
| Orquestación | `src/pipeline.py` (CLI) | Workflows / Jobs | Step Functions / MWAA |
| Calidad de datos | Gates Silver + cuarentena | Expectations (DLT) | Glue Data Quality |

## Cómo correrlo (3 comandos)

Requiere Python 3.11 o superior.

```bash
make setup      # crea .venv, instala requirements.txt y copia .env.example a .env
make pipeline   # Bronze -> Silver -> Gold (genera datos sintéticos con seed fija)
make demo       # LLM_PROVIDER=mock: preguntas de negocio y controles de seguridad
```

Salidas: `output/bi_report.md` (reporte legible en español), `output/bi_report.json` (respuestas estructuradas) y `output/pipeline_report.json` (resumen de calidad por entidad).

Otros comandos:

```bash
make test       # pytest (tests de Medallion, guardrails y analista)
make lint       # ruff check, ruff format --check y mypy --strict sobre src/
.venv/bin/python -m src.main ask "¿Cuál es la tasa de conversión por medio de pago?"
.venv/bin/python notebooks/explore_lakehouse.py   # historial Delta, time travel, cuarentena, Gold
```

### Proveedores del analista

- `LLM_PROVIDER=mock` (por defecto): planificador determinístico. Mapea sinónimos de la capa semántica a métricas, dimensiones y orden (ranking, serie temporal). No llama a ningún servicio. Responde las preguntas de demo y declara como no respondible lo que no está en la capa semántica.
- `LLM_PROVIDER=bedrock`: AWS Bedrock Runtime con la API Converse. El modelo recibe la capa semántica y debe devolver una sola sentencia. Su interpretación se acepta solo si cada número aparece en la tabla devuelta (incluidos porcentajes, sumas y cantidad de filas); si no, se usa la lectura determinística y el reporte lo indica.

### Almacenamiento S3 / MinIO

Por configuración, sin cambiar código:

```bash
docker compose up -d minio minio-init     # MinIO local en :9000 (consola :9001)
# en .env:
#   LAKEHOUSE_STORAGE=s3
#   S3_ENDPOINT_URL=http://localhost:9000
#   AWS_ACCESS_KEY_ID=minioadmin  AWS_SECRET_ACCESS_KEY=minioadmin
make pipeline
```

Para AWS real, deje `S3_ENDPOINT_URL` vacío y use credenciales de IAM con permisos sobre el bucket.

## Gobierno y seguridad del analista

El validador (`src/analyst/sql_guardrails.py`) aplica estas capas en orden. Cualquier violación bloquea la consulta antes de tocar el motor:

1. Sin comentarios SQL y sin múltiples sentencias.
2. Solo `SELECT` o `WITH`. Palabras clave de escritura o DDL (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `COPY`, `ATTACH`, `CREATE`, `TRUNCATE`, además de `PRAGMA`, `SET`, `LOAD`, `INSTALL`, etc.) se rechazan con búsqueda de tokens fuera de literales.
3. Análisis AST con sqlglot (dialecto DuckDB): nodo raíz `SELECT` o `UNION`, sin nodos DDL, DML ni comandos, sin funciones de lectura de archivos (`read_*`, `glob`, `query`), sin nombres de tabla calificados.
4. Whitelist de tablas: solo las declaradas en `authorized_tables`. Bronze, Silver y cuarentena se rechazan con código `restricted_layer`.
5. Whitelist de columnas: solo columnas de las tablas autorizadas, alias del SELECT o salidas de CTE.
6. Límite de filas (`SQL_ROW_LIMIT`, por defecto 1000) que se impone si falta o si excede el máximo.
7. La conexión DuckDB se abre con `enable_external_access=False`, y las tablas Gold se registran solo desde Arrow.

El reporte incluye una tabla de controles con sentencias adversariales (DROP, DELETE disfrazado de CTE, acceso directo a Silver, `read_parquet`, múltiples sentencias) y el código de bloqueo de cada una.

Otras decisiones:

- El SQL que se muestra y se ejecuta es la forma canónica que produjo el validador, no el texto original del LLM.
- Los montos se muestran en ARS con formato argentino. Los ratios se calculan sobre sumas, nunca promediando filas.
- Las métricas, columnas y sinónimos de la capa semántica se validan al cargar: una métrica que referencia una columna inexistente hace fallar el arranque.

## Estructura

```
config.py / models.py            configuración tipada y contratos Pydantic
lakehouse.py                     wrapper de deltalake (local y s3://)
data_generator.py                generador determinístico con defectos intencionales
medallion/bronze.py              ingesta append-only idempotente
medallion/silver.py              normalización, gates, dedupe, cuarentena
medallion/gold.py                marts en DuckDB
pipeline.py                      orquestación Bronze -> Silver -> Gold -> auditoría
analyst/semantic.py              carga y validación de semantic_layer/metrics.yaml
analyst/sql_guardrails.py        validador de solo lectura
analyst/bi_agent.py              proveedores mock/bedrock, ejecución y lectura
analyst/report.py                reportes Markdown y JSON
main.py                          CLI: run-pipeline | ask | demo
notebooks/explore_lakehouse.py   exploración reproducible
tests/                           pruebas de calidad, guardrails y analista
```

## Calidad y verificación

Lo que está verificado en este repositorio:

- `make pipeline`, `make demo`, `make test` (49 pruebas) y `make lint` (ruff + mypy strict sobre `src/`).
- Los números de Gold de un caso de prueba pequeño se comparan con valores calculados a mano (margen neto, reembolsos, comisiones).
- Las métricas de Gold del pipeline completo se recalculan de forma independiente desde Silver.
- Ejecución completa en almacenamiento S3 contra un servidor S3 simulado (`moto`). La segunda corrida no duplica Bronze (idempotencia).

Lo que no está verificado en este entorno:

- MinIO real con `docker compose` (no hay Docker disponible aquí). La configuración sigue el mismo camino de `s3://` que se probó con `moto`.
- Llamadas reales a AWS Bedrock. El proveedor se prueba con un cliente simulado que respeta la API Converse.

## Licencia

MIT.
