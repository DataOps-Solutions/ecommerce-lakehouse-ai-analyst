"use client";

import { Badge, Card, EmptyState, ErrorPanel, PageHeader, Skeleton } from "@/components/ui";
import { api, type ApiError } from "@/lib/api";
import { count } from "@/lib/format";
import { useApi } from "@/lib/use-api";
import { useWorkspace } from "@/lib/workspace";
import type { TableInfo } from "@/lib/types";

const LAYER_ORDER = ["bronze", "silver", "quarantine", "gold"];
const LAYER_LABEL: Record<string, string> = {
  bronze: "Bronze · datos crudos",
  silver: "Silver · datos validados",
  quarantine: "Cuarentena · filas excluidas",
  gold: "Gold · tablas de negocio",
};

function layerRank(layer: string): number {
  const index = LAYER_ORDER.indexOf(layer);
  return index === -1 ? LAYER_ORDER.length : index;
}

function TableRow({ table }: { table: TableInfo }) {
  return (
    <details className="group border-t border-slate-100 first:border-t-0">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4 py-3 hover:bg-slate-50">
        <div className="flex min-w-0 items-center gap-3">
          <span className="text-slate-400 transition-transform group-open:rotate-90">▸</span>
          <span className="truncate font-mono text-sm font-medium text-slate-900">{table.name}</span>
          {table.has_custom_attributes && <Badge tone="violet">custom_attributes</Badge>}
        </div>
        <div className="flex shrink-0 items-center gap-6 text-sm text-slate-600">
          <span className="tabular-nums">{count(table.rows)} filas</span>
          <span className="tabular-nums">{table.columns.length} columnas</span>
        </div>
      </summary>
      <div className="pb-4 pl-7">
        <table className="min-w-full text-left text-xs">
          <thead className="text-slate-500">
            <tr>
              <th className="py-1.5 pr-6 font-medium">Columna</th>
              <th className="py-1.5 font-medium">Tipo DuckDB</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 font-mono text-slate-700">
            {table.columns.map((column) => (
              <tr key={column.name}>
                <td className="py-1.5 pr-6">{column.name}</td>
                <td className="py-1.5 text-slate-500">{column.type}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}

export default function CatalogPage() {
  const workspace = useWorkspace();
  const { state, reload } = useApi(`catalog:${workspace}`, () => api.catalog(workspace));

  const groups =
    state.status === "ok"
      ? [...new Set(state.data.tables.map((t) => t.layer))]
          .sort((a, b) => layerRank(a) - layerRank(b))
          .map((layer) => ({
            layer,
            tables: state.data.tables.filter((t) => t.layer === layer),
          }))
      : [];

  return (
    <>
      <PageHeader
        title="Catálogo"
        description="Tablas Delta registradas en DuckDB para el workspace activo, con su esquema y el estado de los atributos personalizados."
      />
      {state.status === "loading" && <Skeleton className="h-96" />}
      {state.status === "error" &&
        (state.error.code === "workspace_not_found" ? (
          <EmptyState
            title="Este workspace no tiene tablas"
            body="El catálogo se llena cuando se carga un archivo o se ejecuta un pipeline."
            action={{ href: "/onboarding", label: "Cargar un CSV" }}
          />
        ) : (
          <ErrorPanel error={state.error as ApiError} onRetry={reload} />
        ))}
      {state.status === "ok" &&
        (state.data.tables.length === 0 ? (
          <EmptyState title="El catálogo está vacío" body="No hay tablas registradas en este workspace." />
        ) : (
          groups.map((group) => (
            <Card key={group.layer} title={LAYER_LABEL[group.layer] ?? group.layer}>
              {group.tables.map((table) => (
                <TableRow key={table.name} table={table} />
              ))}
            </Card>
          ))
        ))}
    </>
  );
}
