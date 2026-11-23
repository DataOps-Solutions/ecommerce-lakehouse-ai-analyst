import Link from "next/link";
import type { ReactNode } from "react";

import { ApiError } from "@/lib/api";
import type { DataQuality, Exactness, RunStatus } from "@/lib/types";

export function Card({
  title,
  subtitle,
  actions,
  children,
  className = "",
}: {
  title?: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section
      className={`rounded-xl border border-slate-200 bg-white shadow-sm ${className}`}
    >
      {(title || actions) && (
        <header className="flex items-start justify-between gap-4 border-b border-slate-100 px-5 py-4">
          <div>
            {title && <h2 className="text-sm font-semibold text-slate-900">{title}</h2>}
            {subtitle && <p className="mt-0.5 text-xs text-slate-500">{subtitle}</p>}
          </div>
          {actions}
        </header>
      )}
      <div className="p-5">{children}</div>
    </section>
  );
}

export function PageHeader({
  title,
  description,
  actions,
}: {
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">{title}</h1>
        <p className="mt-1 max-w-2xl text-sm text-slate-600">{description}</p>
      </div>
      {actions}
    </div>
  );
}

export function Kpi({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
      <p className="mt-2 text-2xl font-semibold tabular-nums text-slate-900">{value}</p>
      {hint && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
    </div>
  );
}

type Tone = "neutral" | "green" | "amber" | "red" | "blue" | "violet";

const TONES: Record<Tone, string> = {
  neutral: "bg-slate-100 text-slate-700 ring-slate-200",
  green: "bg-emerald-50 text-emerald-700 ring-emerald-200",
  amber: "bg-amber-50 text-amber-800 ring-amber-200",
  red: "bg-red-50 text-red-700 ring-red-200",
  blue: "bg-blue-50 text-blue-700 ring-blue-200",
  violet: "bg-violet-50 text-violet-700 ring-violet-200",
};

export function Badge({ tone = "neutral", children }: { tone?: Tone; children: ReactNode }) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium ring-1 ring-inset ${TONES[tone]}`}
    >
      {children}
    </span>
  );
}

export function RunStatusBadge({ status }: { status: RunStatus }) {
  const map: Record<RunStatus, { tone: Tone; label: string }> = {
    succeeded: { tone: "green", label: "Exitosa" },
    failed: { tone: "red", label: "Fallida" },
    running: { tone: "amber", label: "En curso" },
  };
  return <Badge tone={map[status].tone}>{map[status].label}</Badge>;
}

const QUALITY_LABELS: Record<DataQuality, { tone: Tone; label: string }> = {
  exacto: { tone: "green", label: "Dato exacto" },
  estimado: { tone: "amber", label: "Dato estimado" },
  sin_datos: { tone: "neutral", label: "Sin datos" },
};

export function QualityBadge({ quality }: { quality: DataQuality }) {
  return <Badge tone={QUALITY_LABELS[quality].tone}>{QUALITY_LABELS[quality].label}</Badge>;
}

const EXACTNESS_LABELS: Record<Exactness, { tone: Tone; label: string }> = {
  exacto: { tone: "green", label: "exact" },
  estimado: { tone: "amber", label: "estimated" },
  no_aplica: { tone: "neutral", label: "no aplica" },
  sin_datos: { tone: "neutral", label: "sin datos" },
};

export function ExactnessBadge({ exactness }: { exactness: Exactness }) {
  const item = EXACTNESS_LABELS[exactness];
  return <Badge tone={item.tone}>exactness: {item.label}</Badge>;
}

export function Spinner({ label = "Cargando" }: { label?: string }) {
  return (
    <div className="flex items-center gap-3 text-sm text-slate-500" role="status">
      <span className="h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-brand-600" />
      {label}
    </div>
  );
}

export function Skeleton({ className = "h-24" }: { className?: string }) {
  return <div className={`animate-pulse rounded-xl bg-slate-200/70 ${className}`} />;
}

export function EmptyState({
  title,
  body,
  action,
}: {
  title: string;
  body: string;
  action?: { href: string; label: string };
}) {
  return (
    <div className="flex flex-col items-center rounded-xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center">
      <p className="text-sm font-semibold text-slate-900">{title}</p>
      <p className="mt-1 max-w-md text-sm text-slate-500">{body}</p>
      {action && (
        <Link
          href={action.href}
          className="mt-4 rounded-lg bg-brand-600 px-3.5 py-2 text-sm font-medium text-white hover:bg-brand-700"
        >
          {action.label}
        </Link>
      )}
    </div>
  );
}

export function ErrorPanel({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  return (
    <div role="alert" className="rounded-xl border border-red-200 bg-red-50 p-4">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm font-semibold text-red-800">
            {error.code === "network_error" ? "Sin conexión con la API" : "No se pudo completar la acción"}
          </p>
          <p className="mt-1 text-sm text-red-700">{error.message}</p>
          <p className="mt-2 font-mono text-xs text-red-600">
            {error.status ? `HTTP ${error.status} · ` : ""}código: {error.code}
          </p>
        </div>
        {onRetry && (
          <button
            type="button"
            onClick={onRetry}
            className="shrink-0 rounded-lg border border-red-300 bg-white px-3 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100"
          >
            Reintentar
          </button>
        )}
      </div>
    </div>
  );
}

export function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div>
      <dt className="text-xs text-slate-500">{label}</dt>
      <dd className="mt-0.5 break-words text-sm font-medium text-slate-900">{value}</dd>
    </div>
  );
}
