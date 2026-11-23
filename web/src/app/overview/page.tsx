"use client";

import { ApiError, api } from "@/lib/api";
import { count, money, pct } from "@/lib/format";
import { useApi } from "@/lib/use-api";
import { useWorkspace } from "@/lib/workspace";
import { Card, EmptyState, ErrorPanel, Kpi, PageHeader, QualityBadge, Skeleton } from "@/components/ui";
import type { Summary } from "@/lib/types";

function ShareBar({ label, exact }: { label: string; exact: number | null }) {
  if (exact === null) {
    return (
      <div>
        <div className="flex justify-between text-xs text-slate-600">
          <span>{label}</span>
          <span>sin datos</span>
        </div>
      </div>
    );
  }
  const exactPct = Math.round(exact * 1000) / 10;
  return (
    <div>
      <div className="flex justify-between text-xs text-slate-600">
        <span>{label}</span>
        <span className="tabular-nums">
          exacto {pct(exact)} · estimado {pct(1 - exact)}
        </span>
      </div>
      <div className="mt-1.5 h-2.5 w-full overflow-hidden rounded-full bg-amber-200">
        <div className="h-full rounded-full bg-emerald-500" style={{ width: `${exactPct}%` }} />
      </div>
    </div>
  );
}

function SummaryView({ summary }: { summary: Summary }) {
  const k = summary.kpis;
  const period =
    summary.period_start && summary.period_end
      ? `${summary.period_start} a ${summary.period_end}`
      : "sin período";
  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3 text-sm text-slate-600">
        <span>Período por fecha de pedido: {period}</span>
        <QualityBadge quality={summary.data_quality} />
      </div>
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Kpi label="Ventas brutas" value={money(k.gross_sales)} hint={`${count(k.orders)} pedidos`} />
        <Kpi label="Ingreso neto" value={money(k.net_revenue)} hint="Después de descuentos y devoluciones" />
        <Kpi
          label="Margen neto real"
          value={money(k.net_margin)}
          hint={`${pct(k.margin_pct)} del ingreso neto`}
        />
        <Kpi label="Unidades vendidas" value={count(k.units)} />
      </div>
      <div className="grid gap-6 lg:grid-cols-3">
        <Card title="Calidad del dato" subtitle="Qué parte del margen usa valores informados" className="lg:col-span-1">
          <div className="space-y-5">
            <ShareBar label="Costo de mercadería" exact={summary.cost_exact_share} />
            <ShareBar label="Comisión de pasarela" exact={summary.fee_exact_share} />
            <p className="text-xs text-slate-500">
              Los valores estimados usan supuestos declarados en el mapeo. Reemplácelos por datos
              reales para cerrar el margen.
            </p>
          </div>
        </Card>
        <Card title="Calidad por entidad" subtitle="Filas usadas y excluidas en la carga" className="lg:col-span-2">
          {summary.quality.filter((q) => q.used + q.excluded > 0).length === 0 ? (
            <p className="text-sm text-slate-500">No hay entidades registradas.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-slate-500">
                  <tr>
                    <th className="py-2 pr-4 font-medium">Entidad</th>
                    <th className="py-2 pr-4 text-right font-medium">Usadas</th>
                    <th className="py-2 pr-4 text-right font-medium">Excluidas</th>
                    <th className="py-2 font-medium">Motivos principales</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {summary.quality
                    .filter((q) => q.used + q.excluded > 0)
                    .map((q) => (
                    <tr key={q.entity}>
                      <td className="py-2.5 pr-4 font-medium text-slate-900">{q.entity}</td>
                      <td className="py-2.5 pr-4 text-right tabular-nums">{count(q.used)}</td>
                      <td className="py-2.5 pr-4 text-right tabular-nums">
                        <span className={q.excluded > 0 ? "text-amber-700" : "text-slate-500"}>
                          {count(q.excluded)}
                        </span>
                      </td>
                      <td className="py-2.5 text-xs text-slate-600">
                        {q.top_reasons.length ? q.top_reasons.join(" · ") : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </div>
      {summary.caveats.length > 0 && (
        <Card title="Advertencias operativas">
          <ul className="list-disc space-y-1.5 pl-5 text-sm text-slate-700">
            {summary.caveats.map((caveat) => (
              <li key={caveat}>{caveat}</li>
            ))}
          </ul>
        </Card>
      )}
    </div>
  );
}

export default function OverviewPage() {
  const workspace = useWorkspace();
  const { state, reload } = useApi(`summary:${workspace}`, () => api.summary(workspace));

  return (
    <>
      <PageHeader
        title="Resumen"
        description="Ventas, margen neto y calidad del dato del workspace activo, calculados sobre las tablas Gold."
      />
      {state.status === "loading" && (
        <div className="space-y-6">
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {[0, 1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-28" />
            ))}
          </div>
          <Skeleton className="h-48" />
        </div>
      )}
      {state.status === "error" &&
        (state.error.code === "no_gold_tables" || state.error.code === "workspace_not_found" ? (
          <EmptyState
            title="Todavía no hay datos en este workspace"
            body="Cargue las exportaciones de la tienda para ver ventas, márgenes y calidad del dato."
            action={{ href: "/onboarding", label: "Cargar un CSV" }}
          />
        ) : (
          <ErrorPanel error={state.error as ApiError} onRetry={reload} />
        ))}
      {state.status === "ok" && <SummaryView summary={state.data} />}
    </>
  );
}
