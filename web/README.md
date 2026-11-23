# DataOps Solutions · Interfaz web

Interfaz en Next.js (App Router), TypeScript y Tailwind CSS sobre la API FastAPI del repositorio.
No contiene reglas de negocio: todas las cifras, validaciones y errores vienen de la API.

## Requisitos

- Node.js 20 o superior y npm.
- Backend en ejecución (ver la raíz del repositorio: `make api`).

## Instalación

```bash
cd web
npm install
cp .env.example .env.local     # opcional: solo si la API no está en http://127.0.0.1:8000
```

`NEXT_PUBLIC_API_URL` se incorpora al build. Si la cambia, reconstruya la aplicación.

## Desarrollo

Con dos terminales, desde la raíz del repositorio:

```bash
make api          # terminal 1: FastAPI en http://127.0.0.1:8000
make web-dev      # terminal 2: interfaz en http://localhost:3000
```

El backend permite por CORS los orígenes `http://localhost:3000` y `http://127.0.0.1:3000`. Si
usa otro puerto, agréguelo en `DEV_ORIGINS` (`src/api/main.py`).

## Producción

```bash
make web-build                # npm run build: compila TypeScript y genera la aplicación
cd web && npm run start -- -p 3000
```

## Verificación

```bash
make web-lint     # eslint + tsc --noEmit
make web-build
```

## Vistas

| Ruta | Endpoint | Contenido |
|---|---|---|
| `/overview` | `GET /workspaces/{id}/summary` | KPIs, margen neto, calidad exacto o estimado, calidad por entidad y advertencias. |
| `/onboarding` | `POST .../ingest/inspect-csv` y `POST .../ingest/csv` | Análisis del CSV, mapeo editable en YAML, confirmación y `run_id`. |
| `/runs` | `GET .../runs` | Historial de corridas y métricas. |
| `/runs/{run_id}` | `GET .../runs/{run_id}` | Tiempos, métricas, historial de eventos y error si falló. |
| `/catalog` | `GET .../catalog` | Tablas por capa, filas, columnas, tipos y badge `custom_attributes`. |
| `/analyst` | `POST .../analyst/query` | Respuesta, SQL validada, tabla y `exactness`. |

El workspace activo se elige en la barra lateral y se guarda en el navegador. Por defecto es `default`.

## Estructura

```
src/app/            rutas (App Router) y layout
src/components/     shell de navegación y componentes de interfaz
src/lib/api.ts      cliente HTTP tipado y errores {"error": {"code", "message"}}
src/lib/types.ts    contratos alineados con src/api/schemas.py
src/lib/use-api.ts  carga de datos con estados de carga, vacío y error
src/lib/workspace.ts workspace activo persistido en localStorage
src/lib/format.ts   formato de moneda, porcentajes y fechas en español (es-AR)
```

## Notas

- La exactitud se muestra con etiquetas en español (`Dato exacto`, `Dato estimado`) y, en el analista, como `exactness: exact | estimated`, igual que en el enunciado.
- El build no descarga fuentes externas: usa la pila de fuentes del sistema.
