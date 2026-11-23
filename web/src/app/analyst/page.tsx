"use client";

import { useState, type FormEvent } from "react";

import { Card, EmptyState, ErrorPanel, ExactnessBadge, PageHeader, Spinner } from "@/components/ui";
import { api, toApiError, type ApiError } from "@/lib/api";
import { count } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import type { AnalystAnswer } from "@/lib/types";

const SUGGESTIONS = [
  "¿Cuál fue el margen neto por canal?",
  "¿Cuál es la tasa de conversión por medio de pago?",
  "¿Qué SKUs tienen la mayor tasa de devolución? top 10",
  "¿Cuántas alertas de stock hay por categoría?",
  "¿Cuál fue la venta bruta y los descuentos por día?",
];
const MAX_ROWS_SHOWN = 50;

function ResultTable({ answer }: { answer: AnalystAnswer }) {
  if (answer.columns.length === 0) return null;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            {answer.columns.map((column) => (
              <th key={column} className="whitespace-nowrap py-2 pr-6 font-medium">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {answer.rows.slice(0, MAX_ROWS_SHOWN).map((row, index) => (
            <tr key={index}>
              {answer.columns.map((column) => (
                <td key={column} className="whitespace-nowrap py-2 pr-6 tabular-nums text-slate-800">
                  {formatCell(row[column])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {answer.rows.length > MAX_ROWS_SHOWN && (
        <p className="mt-2 text-xs text-slate-500">
          Se muestran {MAX_ROWS_SHOWN} de {count(answer.rows.length)} filas.
        </p>
      )}
    </div>
  );
}

// The API serializes DECIMAL values as JSON strings ("586323.09"), so numeric strings are formatted too.
const NUMERIC_TEXT = /^-?\d+(\.\d+)?$/;

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "s/d";
  if (typeof value === "number" || (typeof value === "string" && NUMERIC_TEXT.test(value))) {
    const number = Number(value);
    return Number.isInteger(number)
      ? count(number)
      : number.toLocaleString("es-AR", { maximumFractionDigits: 4 });
  }
  return String(value);
}

export default function AnalystPage() {
  const workspace = useWorkspace();
  const [question, setQuestion] = useState("");
  const [pending, setPending] = useState(false);
  const [answer, setAnswer] = useState<AnalystAnswer | null>(null);
  const [error, setError] = useState<ApiError | null>(null);

  async function ask(text: string) {
    const clean = text.trim();
    setQuestion(clean);
    if (!clean) {
      setError(toApiError(new Error("Escriba una pregunta antes de consultar.")));
      return;
    }
    setPending(true);
    setError(null);
    try {
      setAnswer(await api.analyst(workspace, clean));
    } catch (err) {
      setAnswer(null);
      setError(toApiError(err));
    } finally {
      setPending(false);
    }
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    void ask(question);
  }

  return (
    <>
      <PageHeader
        title="Analista BI"
        description="Haga preguntas en lenguaje natural. Las respuestas salen solo de las tablas Gold autorizadas y siempre muestran la consulta que se ejecutó."
      />

      <Card>
        <form onSubmit={submit} className="flex flex-col gap-3 sm:flex-row">
          <input
            aria-label="Pregunta"
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="Por ejemplo: ¿Cuál fue el margen neto por canal?"
            maxLength={500}
            className="min-w-0 flex-1 rounded-lg border border-slate-300 px-3.5 py-2.5 text-sm focus:border-brand-500 focus:outline-none focus:ring-2 focus:ring-brand-500/20"
          />
          <button
            type="submit"
            disabled={pending}
            className="rounded-lg bg-brand-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-brand-700 disabled:bg-slate-300"
          >
            {pending ? "Consultando…" : "Consultar"}
          </button>
        </form>
        <div className="mt-4 flex flex-wrap gap-2">
          {SUGGESTIONS.map((suggestion) => (
            <button
              key={suggestion}
              type="button"
              disabled={pending}
              onClick={() => void ask(suggestion)}
              className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs text-slate-700 hover:border-brand-500 hover:text-brand-700 disabled:opacity-50"
            >
              {suggestion}
            </button>
          ))}
        </div>
      </Card>

      {pending && <Spinner label="Generando y validando la consulta" />}
      {error && <ErrorPanel error={error} />}

      {answer && answer.status === "unanswerable" && (
        <EmptyState
          title="Esta pregunta no se puede responder con los datos disponibles"
          body="El analista solo consulta las métricas y dimensiones de la capa semántica. Pruebe con una de las sugerencias."
        />
      )}

      {answer && answer.status === "ok" && (
        <Card
          title={answer.question}
          actions={<ExactnessBadge exactness={answer.exactness} />}
        >
          {answer.answer && (
            <p className="rounded-lg bg-brand-50 p-4 text-sm leading-6 text-slate-800">{answer.answer}</p>
          )}
          <div className="mt-5">
            <ResultTable answer={answer} />
          </div>
          <details className="mt-6 rounded-lg border border-slate-200 bg-slate-50">
            <summary className="cursor-pointer px-4 py-2.5 text-sm font-medium text-slate-700">
              Ver consulta SQL validada
            </summary>
            <div className="border-t border-slate-200 p-4">
              <p className="mb-2 text-xs text-slate-500">
                Tablas: {answer.tables.join(", ") || "—"} · proveedor: {answer.provider}
              </p>
              <pre className="overflow-x-auto whitespace-pre-wrap rounded-lg bg-slate-950 p-4 font-mono text-xs text-slate-100">
                {answer.sql}
              </pre>
            </div>
          </details>
        </Card>
      )}
    </>
  );
}
