"use client";

import Link from "next/link";
import { useParams } from "next/navigation";

import { Card, ErrorPanel, PageHeader, RunStatusBadge, Skeleton, Stat } from "@/components/ui";
import { api, type ApiError } from "@/lib/api";
import { count, dateTimeText, durationText, points } from "@/lib/format";
import { useApi } from "@/lib/use-api";
import { useWorkspace } from "@/lib/workspace";

export default function RunDetailPage() {
  const workspace = useWorkspace();
  const { runId } = useParams<{ runId: string }>();
  const { state, reload } = useApi(`run:${workspace}:${runId}`, () => api.run(workspace, runId));

  return (
    <>
      <div className="text-sm">
        <Link href="/runs" className="text-brand-700 hover:underline">
          ← Volver a corridas
        </Link>
      </div>
      {state.status === "loading" && <Skeleton className="h-96" />}
      {state.status === "error" && (
        <ErrorPanel error={state.error as ApiError} onRetry={reload} />
      )}
      {state.status === "ok" && (
        <>
          <PageHeader
            title={`Corrida ${state.data.run.run_id}`}
            description={`Proceso ${state.data.run.pipeline_name} en el workspace ${state.data.run.workspace_id}.`}
            actions={<RunStatusBadge status={state.data.run.status} />}
          />
          {state.data.run.status === "failed" && state.data.run.error_message && (
            <div role="alert" className="rounded-xl border border-red-200 bg-red-50 p-4">
              <p className="text-sm font-semibold text-red-800">La corrida falló</p>
              <p className="mt-1 whitespace-pre-wrap font-mono text-xs text-red-700">
                {state.data.run.error_message}
              </p>
            </div>
          )}
          <div className="grid gap-6 lg:grid-cols-2">
            <Card title="Tiempos">
              <dl className="grid grid-cols-2 gap-4">
                <Stat label="Inicio" value={dateTimeText(state.data.run.started_at)} />
                <Stat label="Fin" value={dateTimeText(state.data.run.finished_at)} />
                <Stat
                  label="Duración"
                  value={durationText(state.data.run.started_at, state.data.run.finished_at)}
                />
                <Stat label="Run ID" value={<span className="font-mono">{state.data.run.run_id}</span>} />
              </dl>
            </Card>
            <Card title="Métricas">
              <dl className="grid grid-cols-2 gap-4">
                <Stat label="Filas de entrada" value={count(state.data.run.metrics.rows_in)} />
                <Stat label="Filas de salida" value={count(state.data.run.metrics.rows_out)} />
                <Stat label="Filas en cuarentena" value={count(state.data.run.metrics.quarantine_rows)} />
                <Stat label="Costo estimado" value={points(state.data.run.metrics.estimated_cost_pct)} />
                <Stat label="Comisión estimada" value={points(state.data.run.metrics.estimated_fee_pct)} />
              </dl>
            </Card>
          </div>
          <Card title="Historial de eventos" subtitle="Cada cambio de estado queda registrado">
            <ol className="relative space-y-4 border-l border-slate-200 pl-6">
              {state.data.history.map((event, index) => (
                <li key={`${event.event_at}-${index}`}>
                  <span className="absolute -left-1.5 mt-1.5 h-3 w-3 rounded-full border-2 border-white bg-brand-500" />
                  <p className="text-sm font-medium capitalize text-slate-900">{event.status}</p>
                  <p className="text-xs text-slate-500">{dateTimeText(event.event_at)}</p>
                  {event.error_message && (
                    <p className="mt-1 font-mono text-xs text-red-700">{event.error_message}</p>
                  )}
                </li>
              ))}
            </ol>
          </Card>
        </>
      )}
    </>
  );
}
