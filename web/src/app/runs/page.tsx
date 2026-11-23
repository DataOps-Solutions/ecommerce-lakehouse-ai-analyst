"use client";

import Link from "next/link";

import { RunStatusBadge, EmptyState, ErrorPanel, PageHeader, Skeleton, Card } from "@/components/ui";
import { api, type ApiError } from "@/lib/api";
import { count, dateTimeText, points } from "@/lib/format";
import { useApi } from "@/lib/use-api";
import { useWorkspace } from "@/lib/workspace";

export default function RunsPage() {
  const workspace = useWorkspace();
  const { state, reload } = useApi(`runs:${workspace}`, () => api.runs(workspace));

  return (
    <>
      <PageHeader
        title="Corridas"
        description="Historial de ingestas y procesos del workspace. Cada corrida deja un registro con sus métricas y, si falla, el motivo."
      />
      {state.status === "loading" && <Skeleton className="h-64" />}
      {state.status === "error" &&
        (state.error.code === "workspace_not_found" ? (
          <EmptyState
            title="Este workspace no tiene corridas"
            body="Las corridas aparecen cuando se carga un CSV o se ejecuta un pipeline."
            action={{ href: "/onboarding", label: "Cargar un CSV" }}
          />
        ) : (
          <ErrorPanel error={state.error as ApiError} onRetry={reload} />
        ))}
      {state.status === "ok" &&
        (state.data.runs.length === 0 ? (
          <EmptyState
            title="Aún no hay corridas"
            body="Cargue un CSV desde Ingesta y mapeo para registrar la primera."
            action={{ href: "/onboarding", label: "Ir a ingesta" }}
          />
        ) : (
          <Card>
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-slate-500">
                  <tr>
                    <th className="py-2 pr-4 font-medium">Run</th>
                    <th className="py-2 pr-4 font-medium">Proceso</th>
                    <th className="py-2 pr-4 font-medium">Estado</th>
                    <th className="py-2 pr-4 font-medium">Inicio</th>
                    <th className="py-2 pr-4 text-right font-medium">Filas entrada</th>
                    <th className="py-2 pr-4 text-right font-medium">Cuarentena</th>
                    <th className="py-2 text-right font-medium">Costo estimado</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {state.data.runs.map((run) => (
                    <tr key={run.run_id} className="hover:bg-slate-50">
                      <td className="py-3 pr-4">
                        <Link
                          href={`/runs/${run.run_id}`}
                          className="font-mono text-xs font-medium text-brand-700 hover:underline"
                        >
                          {run.run_id}
                        </Link>
                      </td>
                      <td className="py-3 pr-4 text-slate-700">{run.pipeline_name}</td>
                      <td className="py-3 pr-4">
                        <RunStatusBadge status={run.status} />
                      </td>
                      <td className="py-3 pr-4 text-slate-600">{dateTimeText(run.started_at)}</td>
                      <td className="py-3 pr-4 text-right tabular-nums">{count(run.metrics.rows_in)}</td>
                      <td className="py-3 pr-4 text-right tabular-nums">
                        {count(run.metrics.quarantine_rows)}
                      </td>
                      <td className="py-3 text-right tabular-nums">
                        {points(run.metrics.estimated_cost_pct)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        ))}
    </>
  );
}
