"use client";

import Link from "next/link";
import { useRef, useState } from "react";

import { Badge, Card, ErrorPanel, PageHeader, Spinner, Stat } from "@/components/ui";
import { api, toApiError, type ApiError } from "@/lib/api";
import { count } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import type { IngestResult, InspectResult } from "@/lib/types";

type Step =
  | { kind: "idle" }
  | { kind: "inspecting" }
  | { kind: "inspected"; result: InspectResult }
  | { kind: "uploading"; result: InspectResult }
  | { kind: "done"; result: InspectResult; ingest: IngestResult }
  | { kind: "error"; error: ApiError; result: InspectResult | null };

function Detection({ result }: { result: InspectResult }) {
  const found = result.detected_source !== null;
  return (
    <Card title="Detección del archivo" subtitle={result.filename}>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Stat
          label="Fuente detectada"
          value={
            found ? (
              <Badge tone="green">{result.detected_source}</Badge>
            ) : (
              <Badge tone="red">no reconocida</Badge>
            )
          }
        />
        <Stat label="Plantilla de referencia" value={result.template ?? "—"} />
        <Stat
          label="Separador / decimal"
          value={`"${result.delimiter}" / "${result.decimal}" (${result.encoding})`}
        />
        <Stat label="Columnas en el archivo" value={count(result.headers.length)} />
      </div>
      {result.warnings.length > 0 && (
        <ul className="mt-5 space-y-1.5 rounded-lg bg-amber-50 p-4 text-sm text-amber-900">
          {result.warnings.map((warning) => (
            <li key={warning}>• {warning}</li>
          ))}
        </ul>
      )}
    </Card>
  );
}

function Mapping({ result }: { result: InspectResult }) {
  const canonical = Object.entries(result.canonical_matches);
  const custom = Object.entries(result.suggested_custom_fields);
  return (
    <div className="grid gap-6 lg:grid-cols-2">
      <Card title="Columnas canónicas reconocidas" subtitle="Encabezado del archivo → campo del modelo">
        {canonical.length === 0 ? (
          <p className="text-sm text-slate-500">Ninguna columna se reconoció automáticamente.</p>
        ) : (
          <table className="min-w-full text-left text-sm">
            <tbody className="divide-y divide-slate-100">
              {canonical.map(([field, header]) => (
                <tr key={field}>
                  <td className="py-1.5 pr-4 font-mono text-xs text-slate-700">{field}</td>
                  <td className="py-1.5 text-slate-500">← {header}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {result.missing_required.length > 0 && (
          <div className="mt-4 rounded-lg bg-red-50 p-3 text-sm text-red-800">
            Faltan campos obligatorios: {result.missing_required.join(", ")}. Asígnelos en el mapeo
            antes de confirmar.
          </div>
        )}
      </Card>
      <Card
        title="Campos personalizados sugeridos"
        subtitle="Columnas sin equivalente canónico. Se guardan como custom_attributes"
      >
        {custom.length === 0 ? (
          <p className="text-sm text-slate-500">No hay columnas adicionales.</p>
        ) : (
          <table className="min-w-full text-left text-sm">
            <tbody className="divide-y divide-slate-100">
              {custom.map(([name, headers]) => (
                <tr key={name}>
                  <td className="py-1.5 pr-4 font-mono text-xs text-slate-700">{name}</td>
                  <td className="py-1.5 pr-4 text-slate-500">← {headers.join(", ")}</td>
                  <td className="py-1.5 text-right">
                    {result.suggested_sensitive[name] ? (
                      <Badge tone="red">personal · se elimina</Badge>
                    ) : (
                      <Badge tone="violet">custom</Badge>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  );
}

function SampleRows({ result }: { result: InspectResult }) {
  if (result.sample_rows.length === 0) return null;
  return (
    <Card title="Muestra de datos" subtitle="Primeras filas tal como vienen en el archivo">
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-xs">
          <thead className="text-slate-500">
            <tr>
              {result.headers.map((header) => (
                <th key={header} className="whitespace-nowrap py-2 pr-4 font-medium">
                  {header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {result.sample_rows.map((row, index) => (
              <tr key={index}>
                {result.headers.map((header) => (
                  <td key={header} className="whitespace-nowrap py-2 pr-4 text-slate-700">
                    {row[header] || <span className="text-slate-300">vacío</span>}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function IngestSummary({ ingest }: { ingest: IngestResult }) {
  return (
    <Card title="Carga registrada" subtitle="El archivo se procesó y quedó asociado a una corrida">
      <div className="grid gap-4 sm:grid-cols-3">
        <Stat label="Run ID" value={<span className="font-mono">{ingest.run_id}</span>} />
        <Stat label="Filas en cuarentena" value={count(ingest.quarantine_rows)} />
        <Stat label="Workspace" value={ingest.workspace_id} />
      </div>
      <div className="mt-5 flex flex-wrap gap-2 text-sm">
        {Object.entries(ingest.rows_by_entity).map(([entity, rows]) => (
          <Badge key={entity}>
            {entity}: {count(rows)}
          </Badge>
        ))}
      </div>
      {ingest.privacy.length > 0 && (
        <div className="mt-5">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Datos personales tratados antes de guardar
          </p>
          <ul className="mt-2 space-y-1 text-sm text-slate-700">
            {ingest.privacy.map((item) => (
              <li key={`${item.source}.${item.field}`}>
                {item.field}:{" "}
                {item.action === "drop" ? "eliminado" : "seudonimizado"} ({count(item.cells_affected)}{" "}
                celdas)
              </li>
            ))}
          </ul>
        </div>
      )}
      {ingest.warnings.length > 0 && (
        <ul className="mt-5 list-disc space-y-1 pl-5 text-sm text-amber-800">
          {ingest.warnings.map((warning) => (
            <li key={warning}>{warning}</li>
          ))}
        </ul>
      )}
      <div className="mt-6 flex gap-3">
        <Link
          href={`/runs/${ingest.run_id}`}
          className="rounded-lg bg-brand-600 px-3.5 py-2 text-sm font-medium text-white hover:bg-brand-700"
        >
          Ver la corrida
        </Link>
        <Link
          href="/overview"
          className="rounded-lg border border-slate-300 px-3.5 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
        >
          Ver el resumen
        </Link>
      </div>
    </Card>
  );
}

export default function OnboardingPage() {
  const workspace = useWorkspace();
  const fileInput = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [mapping, setMapping] = useState("");
  const [step, setStep] = useState<Step>({ kind: "idle" });

  const inspected = step.kind === "inspected" || step.kind === "uploading" || step.kind === "done";
  const result = "result" in step ? step.result : null;

  async function analyze() {
    if (!file) return;
    setStep({ kind: "inspecting" });
    try {
      const inspect = await api.inspectCsv(workspace, file);
      setMapping(inspect.mapping_yaml ?? "");
      setStep({ kind: "inspected", result: inspect });
    } catch (error) {
      setStep({ kind: "error", error: toApiError(error), result: null });
    }
  }

  async function confirm() {
    if (!file || !result) return;
    setStep({ kind: "uploading", result });
    try {
      const ingest = await api.ingestCsv(workspace, mapping, file);
      setStep({ kind: "done", result, ingest });
    } catch (error) {
      setStep({ kind: "error", error: toApiError(error), result });
    }
  }

  function reset() {
    setFile(null);
    setMapping("");
    setStep({ kind: "idle" });
    if (fileInput.current) fileInput.current.value = "";
  }

  return (
    <>
      <PageHeader
        title="Ingesta y mapeo"
        description="Suba una exportación de su tienda o marketplace. Revisamos sus columnas, proponemos el mapeo y, cuando lo confirme, cargamos los datos."
        actions={
          step.kind !== "idle" && (
            <button
              type="button"
              onClick={reset}
              className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              Cargar otro archivo
            </button>
          )
        }
      />

      <Card title="1 · Seleccionar archivo" subtitle="CSV en UTF-8 o cp1252, hasta 20 MB">
        <div className="flex flex-wrap items-center gap-4">
          <input
            ref={fileInput}
            type="file"
            accept=".csv,text/csv"
            disabled={step.kind === "inspecting" || step.kind === "uploading"}
            onChange={(event) => {
              setFile(event.target.files?.[0] ?? null);
              setStep({ kind: "idle" });
            }}
            className="text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-100 file:px-4 file:py-2 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-200"
          />
          <button
            type="button"
            onClick={analyze}
            disabled={!file || step.kind === "inspecting" || step.kind === "uploading"}
            className="rounded-lg bg-brand-600 px-4 py-2 text-sm font-medium text-white hover:bg-brand-700 disabled:cursor-not-allowed disabled:bg-slate-300"
          >
            Analizar archivo
          </button>
        </div>
        {step.kind === "inspecting" && (
          <div className="mt-4">
            <Spinner label="Analizando encabezados y muestra" />
          </div>
        )}
      </Card>

      {step.kind === "error" && (
        <ErrorPanel error={step.error} onRetry={step.result ? confirm : analyze} />
      )}

      {result && (
        <>
          <Detection result={result} />
          <Mapping result={result} />
          <SampleRows result={result} />
        </>
      )}

      {inspected && result && (
        <Card
          title="2 · Ajustar el mapeo"
          subtitle="Edite el YAML si hace falta. El campo file debe coincidir con el nombre del archivo."
          actions={
            <button
              type="button"
              onClick={() => setMapping(result.mapping_yaml ?? "")}
              className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50"
            >
              Restaurar sugerencia
            </button>
          }
        >
          {result.mapping_errors.length > 0 && (
            <div className="mb-4 rounded-lg bg-red-50 p-3 text-sm text-red-800">
              {result.mapping_errors.map((message) => (
                <p key={message}>{message}</p>
              ))}
            </div>
          )}
          <textarea
            aria-label="Mapeo YAML"
            value={mapping}
            onChange={(event) => setMapping(event.target.value)}
            spellCheck={false}
            rows={20}
            className="w-full rounded-lg border border-slate-300 bg-slate-950 p-4 font-mono text-xs leading-5 text-slate-100 focus:border-brand-500 focus:outline-none focus:ring-2 focus:ring-brand-500/20"
          />
          <div className="mt-4 flex flex-wrap items-center justify-between gap-4">
            <p className="text-xs text-slate-500">
              La validación final la realiza el backend al confirmar. Si el mapeo no es válido, verá
              el motivo y nada se guarda.
            </p>
            <button
              type="button"
              onClick={confirm}
              disabled={step.kind === "uploading" || !mapping.trim()}
              className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:cursor-not-allowed disabled:bg-slate-300"
            >
              {step.kind === "uploading" ? "Cargando…" : "3 · Confirmar carga"}
            </button>
          </div>
          {step.kind === "uploading" && (
            <div className="mt-4">
              <Spinner label="Procesando el archivo: validación, privacidad, silver y gold" />
            </div>
          )}
        </Card>
      )}

      {step.kind === "done" && <IngestSummary ingest={step.ingest} />}
    </>
  );
}
