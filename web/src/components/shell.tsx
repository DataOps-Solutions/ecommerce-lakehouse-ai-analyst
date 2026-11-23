"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { type ReactNode, useEffect, useState } from "react";

import { api } from "@/lib/api";
import { isValidWorkspace, setWorkspace, useWorkspace } from "@/lib/workspace";

const NAV = [
  { href: "/overview", label: "Resumen" },
  { href: "/onboarding", label: "Ingesta y mapeo" },
  { href: "/runs", label: "Corridas" },
  { href: "/catalog", label: "Catálogo" },
  { href: "/analyst", label: "Analista BI" },
];

type Health = "checking" | "up" | "down";

function HealthIndicator() {
  const [health, setHealth] = useState<Health>("checking");

  useEffect(() => {
    let active = true;
    const check = () =>
      api.health().then(
        () => active && setHealth("up"),
        () => active && setHealth("down"),
      );
    check();
    const timer = window.setInterval(check, 30000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  const styles: Record<Health, string> = {
    checking: "bg-slate-400",
    up: "bg-emerald-400",
    down: "bg-red-400",
  };
  const labels: Record<Health, string> = {
    checking: "Verificando API",
    up: "API en línea",
    down: "API sin conexión",
  };
  return (
    <div className="flex items-center gap-2 text-xs text-slate-300" aria-live="polite">
      <span className={`h-2 w-2 rounded-full ${styles[health]}`} />
      {labels[health]}
    </div>
  );
}

function WorkspaceSelector({ workspace }: { workspace: string }) {
  const [draft, setDraft] = useState(workspace);
  const [error, setError] = useState<string | null>(null);

  const apply = (value: string) => {
    const next = value.trim();
    if (!isValidWorkspace(next)) {
      setError("Use minúsculas, números, - o _ (hasta 48 caracteres).");
      return;
    }
    setError(null);
    setWorkspace(next);
  };

  return (
    <form
      className="space-y-1.5"
      onSubmit={(event) => {
        event.preventDefault();
        apply(draft);
      }}
    >
      <label htmlFor="workspace" className="text-xs font-medium text-slate-400">
        Workspace
      </label>
      <div className="flex gap-2">
        <input
          id="workspace"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          className="min-w-0 flex-1 rounded-md border border-slate-700 bg-slate-800 px-2.5 py-1.5 text-sm text-white placeholder-slate-500 focus:border-brand-500 focus:outline-none"
          placeholder="default"
          spellCheck={false}
          autoComplete="off"
        />
        <button
          type="submit"
          className="rounded-md bg-slate-700 px-2.5 text-xs font-medium text-white hover:bg-slate-600"
        >
          Cambiar
        </button>
      </div>
      {error && <p className="text-xs text-amber-300">{error}</p>}
      <p className="text-xs text-slate-500">
        Activo: <span className="font-mono text-slate-300">{workspace}</span>
      </p>
    </form>
  );
}

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const workspace = useWorkspace();
  return (
    <div className="flex min-h-screen">
      <aside className="flex w-64 shrink-0 flex-col bg-slate-900 px-4 py-6 text-slate-100">
        <div className="px-2">
          <p className="text-base font-semibold tracking-tight text-white">DataOps Solutions</p>
          <p className="mt-0.5 text-xs text-slate-400">Plataforma lakehouse para e-commerce</p>
        </div>
        <div className="mt-8 border-t border-slate-800 pt-6">
          {/* Keyed by workspace so the input follows changes made elsewhere (e.g. storage events). */}
          <WorkspaceSelector key={workspace} workspace={workspace} />
        </div>
        <nav className="mt-8 space-y-1" aria-label="Principal">
          {NAV.map((item) => {
            const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
            return (
              <Link
                key={item.href}
                href={item.href}
                aria-current={active ? "page" : undefined}
                className={`block rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                  active ? "bg-brand-600 text-white" : "text-slate-300 hover:bg-slate-800 hover:text-white"
                }`}
              >
                {item.label}
              </Link>
            );
          })}
        </nav>
        <div className="mt-auto space-y-2 px-2 pt-8">
          <HealthIndicator />
          <p className="text-[11px] text-slate-500">Versión de la interfaz 0.1.0</p>
        </div>
      </aside>
      <main className="min-w-0 flex-1 px-8 py-8">
        <div className="mx-auto max-w-6xl space-y-6">{children}</div>
      </main>
    </div>
  );
}
