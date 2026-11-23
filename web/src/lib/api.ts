import type {
  AnalystAnswer,
  Catalog,
  Health,
  IngestResult,
  InspectResult,
  RunDetail,
  RunList,
  Summary,
} from "@/lib/types";

/** Base URL of the FastAPI backend. Inlined at build time from NEXT_PUBLIC_API_URL. */
export const API_URL = (process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8000").replace(
  /\/$/,
  "",
);

/** Error produced by the API (`{"error": {"code", "message"}}`) or by the network layer. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

export function toApiError(error: unknown): ApiError {
  if (error instanceof ApiError) return error;
  const message = error instanceof Error ? error.message : "Error inesperado";
  return new ApiError(0, "unexpected_error", message);
}

async function parseError(response: Response): Promise<ApiError> {
  let code = "http_error";
  let message = response.statusText || `Error HTTP ${response.status}`;
  try {
    const body: unknown = await response.json();
    if (body && typeof body === "object") {
      const record = body as Record<string, unknown>;
      const error = record.error as { code?: string; message?: string } | undefined;
      if (error?.code && error.message) {
        code = error.code;
        message = error.message;
      } else if (Array.isArray(record.detail)) {
        // FastAPI validation errors (e.g. a question that breaks the schema).
        code = "validation_error";
        message = record.detail
          .map((item: { msg?: string }) => item.msg ?? "dato inválido")
          .join("; ");
      }
    }
  } catch {
    // Non-JSON body: keep the HTTP status text.
  }
  return new ApiError(response.status, code, message);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_URL}${path}`, { cache: "no-store", ...init });
  } catch {
    throw new ApiError(
      0,
      "network_error",
      `No se pudo conectar con la API en ${API_URL}. Verifique que el backend esté en ejecución.`,
    );
  }
  if (!response.ok) throw await parseError(response);
  return (await response.json()) as T;
}

const ws = (workspaceId: string) => encodeURIComponent(workspaceId);

export const api = {
  health: () => request<Health>("/health"),
  summary: (workspaceId: string) => request<Summary>(`/workspaces/${ws(workspaceId)}/summary`),
  runs: (workspaceId: string, limit = 50) =>
    request<RunList>(`/workspaces/${ws(workspaceId)}/runs?limit=${limit}`),
  run: (workspaceId: string, runId: string) =>
    request<RunDetail>(`/workspaces/${ws(workspaceId)}/runs/${encodeURIComponent(runId)}`),
  catalog: (workspaceId: string) => request<Catalog>(`/workspaces/${ws(workspaceId)}/catalog`),
  analyst: (workspaceId: string, question: string) =>
    request<AnalystAnswer>(`/workspaces/${ws(workspaceId)}/analyst/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    }),
  inspectCsv: (workspaceId: string, file: File) => {
    const body = new FormData();
    body.append("file", file);
    return request<InspectResult>(`/workspaces/${ws(workspaceId)}/ingest/inspect-csv`, {
      method: "POST",
      body,
    });
  },
  ingestCsv: (workspaceId: string, mapping: string, file: File) => {
    const body = new FormData();
    body.append("mapping", mapping);
    body.append("files", file, file.name);
    return request<IngestResult>(`/workspaces/${ws(workspaceId)}/ingest/csv`, {
      method: "POST",
      body,
    });
  },
};
