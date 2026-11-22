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

## Datos reales de tiendas y marketplaces (CSV)

Además del dataset sintético, el proyecto carga exportaciones reales (planillas CSV de una tienda o marketplace) mediante **mapeos declarativos** en `mappings/`. El flujo es independiente del sintético: usa su propio lakehouse (`data/lakehouse_csv/`) y genera el resumen para el cliente.

### Probarlo con los datos de ejemplo

Los archivos de `data/samples_real/` son ficticios. Incluyen filas sucias a propósito (fechas imposibles, cantidades cero o negativas, estados no reconocidos, un SKU fuera del catálogo, costos y stock faltantes, datos de contacto):

```bash
make ingest-sample
```

Salidas en `output/sample_csv/` (el demo sintético escribe en `output/`, así que los dos no se pisan):

| Archivo | Para quién | Contenido |
|---|---|---|
| `client_summary.md` / `.json` | Dueño o gerente de la tienda | Hallazgos, cifras, canales, devoluciones, reposición, calidad de los datos, datos personales tratados y próximos pasos. |
| `bi_report.md` / `.json` | Equipo técnico y analistas | Preguntas de negocio con la SQL ejecutada, la tabla exacta y la interpretación. |
| `pipeline_report.json` | Equipo técnico | Conteos por capa, filas excluidas y motivos. |

Para usar sus propias planillas:

```bash
python -m src.main ingest-csv --input-dir ruta/a/exports \
    --mapping mappings/generic_store_orders.yaml \
    --mapping mappings/costs_and_stock.yaml \
    --output-dir output/cliente_x
```

### Archivos que espera el mapeo

| Archivo (configurable en el YAML) | Obligatorio | Contenido |
|---|---|---|
| `pedidos.csv` (`generic_store_orders.yaml`) | Sí | Una fila por línea de venta: número de pedido, fecha, estado, canal, SKU, cantidad, precio unitario, medio de pago, estado de pago, total pagado, comisión de pasarela (opcional) y datos de contacto. |
| `costos_stock.csv` (`costs_and_stock.yaml`) | No | Costo unitario, precio de lista y stock por SKU. Sin este archivo, todos los costos son estimados. |
| `devoluciones.csv` (`costs_and_stock.yaml`) | No | Número de pedido, SKU, fecha, cantidad, monto reembolsado y motivo. |

### Cómo se configura un mapeo

Un mapeo es un YAML que liga las columnas de la exportación con los campos del pipeline. Los nombres de columna se reconocen por alias, sin distinguir mayúsculas, tildes, espacios ni guiones bajos. Extracto de `mappings/generic_store_orders.yaml`:

```yaml
format:
  delimiter: ";"       # separador de la exportación
  decimal: ","         # 12.900,00 -> decimal con coma
  date_format: "%d/%m/%Y %H:%M"
columns:
  order_id: ["Número de pedido", "Nro pedido", "order_id"]
  payment_method: ["Medio de pago", "Método de pago"]
sensitive:
  customer_email: drop          # nunca se guarda
  customer_id: pseudonymize     # se reemplaza por un código irreversible
```

Para una tienda nueva normalmente basta con copiar el YAML, agregar los alias de sus encabezados y ajustar `delimiter`, `decimal` y `date_format`. Si una columna obligatoria no aparece, la carga se detiene con un mensaje que indica qué falta y qué encabezados se encontraron. Los valores individuales mal formados no detienen la carga: quedan en cuarentena con su motivo.

### Exacto frente a estimado

El margen solo se presenta como firme cuando sus componentes son exactos. Cuando la planilla no trae un dato, el sistema puede estimarlo, pero con un supuesto declarado en el mapeo y marcado como tal:

| Dato | Si viene en la planilla | Si falta |
|---|---|---|
| Costo unitario | Se usa tal cual (`unit_cost_source = exact`). | Se estima como `default_cost_ratio` × precio de lista (`estimated`). Sin supuesto declarado, el SKU queda excluido y se informa. |
| Comisión de pasarela | Se usa el importe informado (`gateway_fee_source = exact`). | Se estima con la tasa del medio de pago en el mapeo (`estimated`). |
| Costo de envío | Se usa el importe informado. | Se asume 0,00 y se advierte que el margen puede estar sobreestimado. |
| Producto vendido sin ficha | — | Se crea una ficha provisoria marcada como estimada, para no perder la venta. |

En Gold, cada fila de `mart_daily_unit_economics` lleva `data_quality` (`exact` o `estimated`), el conteo de líneas y pagos estimados, y el resumen para el cliente lo informa como advertencia. Los supuestos del repositorio (`default_cost_ratio: 0.45` y las tasas de pasarela) son valores de referencia. Reemplácelos por los reales antes de usar cifras para decisiones.

## Datos personales

El tratamiento se aplica en el adaptador, antes de que cualquier valor llegue a Bronze. Los valores originales no se persisten en ninguna capa ni en los reportes.

- **Eliminar** (`drop`): nombre, email, teléfono, DNI y dirección del cliente. Estos campos no se extraen del archivo.
- **Seudonimizar** (`pseudonymize`): el identificador de cliente se reemplaza por `CUS-` más 12 caracteres hexadecimales. Es un hash SHA-256 de un secreto (`PSEUDONYMIZATION_SALT`) y el valor normalizado. Con el mismo secreto, un cliente recibe siempre el mismo código, lo que permite contar clientes recurrentes sin conocer su identidad. Cambiar el secreto desvincula los códigos.
- **Reporte**: el resumen indica cuántas celdas se eliminaron o seudonimizaron por campo, y lista las columnas no mapeadas que parecen personales (que tampoco se persisten).

Puntos a considerar antes de usarlo con datos reales de clientes:

- `PSEUDONYMIZATION_SALT` es un secreto. Guárdelo en un gestor de secretos o en el `.env` local, nunca en el repositorio. Si no está configurado y el mapeo pseudonimiza campos, la carga se detiene.
- Los archivos originales siguen en el disco del cliente. Esta herramienta no los cifra ni los borra. Conviene definir por separado cuánto tiempo se conservan.
- Esta herramienta reduce la exposición de datos personales. No reemplaza el análisis jurídico del tratamiento (base legal, información al titular, derechos ARCO, transferencias) ni constituye asesoramiento legal.

## Estructura

```
config.py / models.py            configuración tipada y contratos Pydantic
lakehouse.py                     wrapper de deltalake (local y s3://)
data_generator.py                generador sintético determinístico con defectos intencionales
pipeline.py                      orquestación: sintético y CSV, Silver -> Gold -> auditoría
medallion/bronze.py              ingesta append-only idempotente
medallion/silver.py              normalización, gates, dedupe, cuarentena
medallion/gold.py                marts en DuckDB
ingestion/mapping.py             mapeos YAML y resolución de alias de columnas
ingestion/csv_adapter.py         exports CSV -> lote Bronze, con estimaciones marcadas
ingestion/privacy.py             eliminación y seudonimización de datos personales
analyst/semantic.py              carga y validación de semantic_layer/metrics.yaml
analyst/sql_guardrails.py        validador de solo lectura
analyst/bi_agent.py              proveedores mock/bedrock, ejecución y lectura
analyst/report.py                reportes BI en Markdown y JSON
reporting/client_summary.py      resumen ejecutivo para el cliente
main.py                          CLI: run-pipeline | ingest-csv | ask | demo
mappings/                        mapeos declarativos de exports reales
data/samples_real/               exports ficticios de ejemplo (con filas sucias)
notebooks/explore_lakehouse.py   exploración reproducible
tests/                           pruebas de calidad, guardrails, analista, CSV y resumen
```

## Calidad y verificación

Lo que está verificado en este repositorio:

- `make pipeline`, `make demo`, `make ingest-sample`, `make test` y `make lint` (ruff + mypy strict sobre `src/`).
- Los números de Gold de un caso de prueba pequeño se comparan con valores calculados a mano (margen neto, reembolsos, comisiones).
- Las métricas de Gold del pipeline completo se recalculan de forma independiente desde Silver.
- Ejecución completa en almacenamiento S3 contra un servidor S3 simulado (`moto`). La segunda corrida no duplica Bronze (idempotencia).
- Ingesta CSV con datos ficticios: el margen con costos y comisiones exactos se calcula a mano y coincide; con costo estimado queda marcado como `estimated`; ningún dato personal de la muestra llega a Bronze, Silver, Gold ni al resumen; la segunda carga no duplica Bronze.

Lo que no está verificado en este entorno:

- MinIO real con `docker compose` (no hay Docker disponible aquí). La configuración sigue el mismo camino de `s3://` que se probó con `moto`.
- Llamadas reales a AWS Bedrock. El proveedor se prueba con un cliente simulado que respeta la API Converse.
- Exports reales de Tiendanube, Mercado Libre o Shopify. Los mapeos de `mappings/` están escritos a partir de un formato genérico y ficticio, no de una exportación real validada.

## Licencia

MIT.
