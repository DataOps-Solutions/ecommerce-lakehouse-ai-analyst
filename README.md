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

## Motor: workspaces, registro de ejecuciones y catálogo

Las capas Bronze, Silver, Gold y los reportes se resuelven desde un único módulo (`src/engine/workspace.py`). Cada cliente o espacio de trabajo tiene su propio `workspace_id`.

- **Workspace.** Por defecto es `default`, y usa exactamente las mismas rutas que antes (`data/lakehouse/<capa>/<tabla>`). Otro workspace vive en `<lakehouse>/workspaces/<id>/`, con sus tablas, su historial de runs y sus reportes (`output/workspaces/<id>/`). Se elige con `--workspace <id>` o con la variable `WORKSPACE_ID`. Los identificadores admiten minúsculas, dígitos, `-` y `_` (hasta 48 caracteres).
- **Registro de ejecuciones.** Cada corrida de `run-pipeline` o `ingest-csv` queda registrada con su `run_id`, el proceso, el estado (`running`, `succeeded` o `failed`), las fechas y las métricas: filas de entrada y salida, filas en cuarentena, porcentaje de costos y de comisiones estimados, y el mensaje de error si falla. Se guarda como tabla Delta en `_ops/run_events`, con un evento por cambio de estado.
- **Catálogo DuckDB.** `src/engine/catalog.py` registra las tablas Delta del workspace (`bronze_*`, `silver_*`, `quarantine_*` y los marts de Gold) y permite inspeccionar sus columnas, tipos, cantidad de filas y si tienen `custom_attributes`.

Consultas desde la línea de comandos:

```bash
python -m src.main runs                                  # últimas ejecuciones (sintéticas)
python -m src.main runs --lakehouse-dir data/lakehouse_csv   # ejecuciones de cargas CSV
python -m src.main runs --lakehouse-dir data/lakehouse_csv --run-id <run_id>   # trazabilidad
make runs                                                # equivalente al primer comando
```

Una ejecución que queda en `running` indica que el proceso se interrumpió: el registro no la marca como fallida por suposición.

## Campos personalizados (`custom_attributes`)

Además de las columnas canónicas, un mapeo puede declarar atributos extra de producto, venta o devolución. Se guardan como un objeto JSON en la columna `custom_attributes`, sin modificar ninguna columna canónica:

```yaml
sources:
  products:
    file: costos_stock.csv
    columns: { sku: ["SKU"], unit_cost: ["Costo unitario"], list_price: ["Precio de lista"] }
    custom_fields:
      color: ["Color"]
      material: ["Material", "Composición"]
```

- Los nombres de campo van en `snake_case` (2 a 40 caracteres) y no pueden repetir un campo canónico.
- Las líneas de pedidos guardan sus atributos en `order_items.custom_attributes`. Productos y devoluciones usan su propia tabla.
- Si una columna declarada no aparece en el archivo, el atributo queda vacío para todas las filas. No es un error.
- Silver valida que el JSON sea válido. Un valor mal formado va a cuarentena con el motivo `invalid_custom_attributes`.
- La privacidad aplica también a los campos extra. Un campo cuyo nombre sugiere dato personal (`email`, `telefono`, `dni`, `nombre`, `direccion`, etc.) se elimina por defecto antes de guardar. Si un nombre no personal contiene una de esas palabras, renómbrelo en el mapeo, o declárelo explícitamente con `sensitive:` para seudonimizarlo.

## API HTTP (FastAPI)

La API expone la plataforma por HTTP. No contiene lógica de negocio propia: usa `app/services.py`, `src/engine/` y `src/ingestion/onboarding.py`. Lo que carga por API se guarda en el lakehouse de plataforma (`data/lakehouse_csv`, o `PLATFORM_LAKEHOUSE_DIR`), el mismo que usa `ingest-csv`. Por eso lo cargado aparece en el catálogo, el historial y el resumen.

```bash
make api            # uvicorn src.api.main:app en http://127.0.0.1:8000
# documentación interactiva: http://127.0.0.1:8000/docs
```

Selección de workspace: las rutas con `{workspace_id}` lo llevan en la URL. Las rutas cortas (`/catalog`, `/runs`, `/summary`, `/analyst/query`) lo toman del parámetro `?workspace=` o del encabezado `X-Workspace-Id`, y por defecto usan `default`.

| Método y ruta | Qué hace |
|---|---|
| `GET /health` | Estado, versión y tipo de almacenamiento. |
| `GET /workspaces/{id}/runs?limit=` | Historial de ejecuciones del workspace. |
| `GET /workspaces/{id}/runs/{run_id}` | Detalle de un run con todos sus eventos (`running`, `succeeded`, `failed`). |
| `GET /workspaces/{id}/catalog` | Tablas registradas en DuckDB: capa, filas, columnas, tipos y si tienen `custom_attributes`. |
| `GET /workspaces/{id}/summary` | KPIs, margen, estado exacto o estimado del dato, calidad por entidad y advertencias. |
| `POST /workspaces/{id}/analyst/query` | Pregunta en lenguaje natural. Devuelve respuesta, SQL validada, tabla y bandera `exactness`. |
| `POST /workspaces/{id}/ingest/inspect-csv` | Sube un CSV y devuelve encabezados, 5 filas de muestra, la fuente detectada, las columnas canónicas encontradas, los `custom_fields` sugeridos y un mapeo YAML validado. |
| `POST /workspaces/{id}/ingest/csv` | Carga el mapeo (YAML o JSON, con `custom_fields`) y los CSV que indica su campo `file`. Crea un run y devuelve su `run_id`. |

Errores: todos responden `{"error": {"code": ..., "message": ...}}`.

| HTTP | Código | Cuándo |
|---|---|---|
| 400 | `invalid_workspace` | El identificador de workspace no es válido. |
| 404 | `workspace_not_found` | El workspace no tiene datos. |
| 404 | `run_not_found` | El `run_id` no existe en ese workspace. |
| 404 | `no_gold_tables` | El workspace tiene datos, pero aún no se calcularon las tablas Gold. |
| 422 | `empty_question` | La pregunta está vacía. |
| 422 | `sql_rejected` | Los guardrails rechazaron la SQL generada. |
| 422 | `mapping_invalid` | El mapeo no valida, faltan archivos o falta el salt de seudonimización. |
| 422 | `file_invalid` | El archivo está vacío, no se puede leer o supera 20 MB. |

Ejemplo de flujo de onboarding:

```bash
curl -F "file=@mis_ventas.csv" http://127.0.0.1:8000/workspaces/acme/ingest/inspect-csv
# revisar mapping_yaml, ajustar y luego cargar:
curl -F "mapping=@mapeo.yaml" -F "files=@mis_ventas.csv" \
     http://127.0.0.1:8000/workspaces/acme/ingest/csv
curl http://127.0.0.1:8000/workspaces/acme/runs
```

Para cargar datos reales hace falta `PSEUDONYMIZATION_SALT` en el entorno. Sin él, las cargas con identificadores de cliente se rechazan con `mapping_invalid`, y el intento queda registrado como run `failed`.

Un intento de carga que falla antes de terminar no devuelve su `run_id` en la respuesta de error. Se puede encontrar en `GET /workspaces/{id}/runs`.

## Interfaz web (Next.js)

La carpeta `web/` contiene la interfaz comercial: resumen, ingesta y mapeo, corridas, catálogo y analista BI, todos conectados a la API. Instrucciones completas en [`web/README.md`](web/README.md).

```bash
make api          # terminal 1: backend en http://127.0.0.1:8000
make web-install  # una sola vez
make web-dev      # terminal 2: interfaz en http://localhost:3000
```

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
engine/workspace.py              workspace (tenant) y resolución única de rutas
engine/runs.py                   registro de ejecuciones (run_events en Delta)
engine/catalog.py                catálogo DuckDB sobre las tablas Delta
ingestion/onboarding.py          detección de encabezados y mapeo sugerido para CSV nuevos
api/main.py                      aplicación FastAPI (rutas, CORS, ingesta por API)
api/schemas.py                   modelos de request y response
api/errors.py                    errores de dominio con códigos HTTP
web/                             interfaz Next.js + TypeScript + Tailwind (ver web/README.md)
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
- API: 22 pruebas con `TestClient` cubren aislamiento entre workspaces, el contrato de errores, la sugerencia de mapeo, la ingesta con campos personalizados y el registro de runs fallidos.
- Motor: el workspace por defecto conserva las rutas anteriores; un workspace distinto queda aislado; cada ejecución exitosa o fallida se registra con sus métricas; el catálogo expone los esquemas.
- Campos personalizados: los valores extra llegan a Silver como JSON, las columnas canónicas no cambian, el JSON inválido va a cuarentena y un campo de nombre personal no sale del adaptador.
- Ingesta CSV con datos ficticios: el margen con costos y comisiones exactos se calcula a mano y coincide; con costo estimado queda marcado como `estimated`; ningún dato personal de la muestra llega a Bronze, Silver, Gold ni al resumen; la segunda carga no duplica Bronze.

Lo que no está verificado en este entorno:

- MinIO real con `docker compose` (no hay Docker disponible aquí). La configuración sigue el mismo camino de `s3://` que se probó con `moto`.
- El registro de ejecuciones y el catálogo no se probaron contra S3. El pipeline en S3 se verificó con `moto` en la primera versión, antes de este cambio.
- Se observó, de forma intermitente en este entorno, que el intérprete no terminaba al salir después de leer tablas Delta con `to_pylist()`. La CLI termina con `os._exit` tras vaciar la salida (ver `src/main.py`). Es un workaround; no se verificó la causa en `deltalake`.
- Llamadas reales a AWS Bedrock. El proveedor se prueba con un cliente simulado que respeta la API Converse.
- Exports reales de Tiendanube, Mercado Libre o Shopify. Los mapeos de `mappings/` están escritos a partir de un formato genérico y ficticio, no de una exportación real validada.

## Licencia

MIT.
