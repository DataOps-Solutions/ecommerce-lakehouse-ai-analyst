// Mirrors src/api/schemas.py. Keep in sync when the API contract changes.

export type RunStatus = "running" | "succeeded" | "failed";
export type DataQuality = "exacto" | "estimado" | "sin_datos";
export type Exactness = "exacto" | "estimado" | "no_aplica" | "sin_datos";

export interface Health {
  status: "ok";
  version: string;
  storage: "local" | "s3";
}

export interface RunMetrics {
  rows_in: number | null;
  rows_out: number | null;
  quarantine_rows: number | null;
  estimated_cost_pct: number | null;
  estimated_fee_pct: number | null;
}

export interface Run {
  run_id: string;
  workspace_id: string;
  pipeline_name: string;
  status: RunStatus;
  started_at: string;
  finished_at: string | null;
  metrics: RunMetrics;
  error_message: string | null;
}

export interface RunList {
  workspace_id: string;
  runs: Run[];
}

export interface RunEvent {
  status: RunStatus;
  event_at: string;
  error_message: string | null;
}

export interface RunDetail {
  run: Run;
  history: RunEvent[];
}

export interface ColumnInfo {
  name: string;
  type: string;
}

export interface TableInfo {
  name: string;
  layer: string;
  rows: number;
  columns: ColumnInfo[];
  has_custom_attributes: boolean;
}

export interface Catalog {
  workspace_id: string;
  tables: TableInfo[];
}

export interface Kpis {
  gross_sales: number;
  net_revenue: number;
  net_margin: number;
  margin_pct: number | null;
  units: number;
  orders: number;
}

export interface EntityQuality {
  entity: string;
  used: number;
  excluded: number;
  top_reasons: string[];
}

export interface Summary {
  workspace_id: string;
  kpis: Kpis;
  period_start: string | null;
  period_end: string | null;
  cost_exact_share: number | null;
  fee_exact_share: number | null;
  data_quality: DataQuality;
  quality: EntityQuality[];
  caveats: string[];
}

export interface AnalystAnswer {
  workspace_id: string;
  status: "ok" | "unanswerable" | "error";
  question: string;
  answer: string | null;
  sql: string | null;
  tables: string[];
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  exactness: Exactness;
  provider: string;
  error: string | null;
}

export interface InspectResult {
  filename: string;
  encoding: string;
  delimiter: string;
  decimal: string;
  headers: string[];
  sample_rows: Record<string, string>[];
  detected_source: string | null;
  template: string | null;
  canonical_matches: Record<string, string>;
  missing_required: string[];
  suggested_custom_fields: Record<string, string[]>;
  suggested_sensitive: Record<string, string>;
  mapping_yaml: string | null;
  mapping_errors: string[];
  warnings: string[];
}

export interface PrivacyItem {
  source: string;
  field: string;
  action: "drop" | "pseudonymize";
  cells_affected: number;
}

export interface IngestResult {
  run_id: string;
  workspace_id: string;
  batch_id: string;
  rows_by_entity: Record<string, number>;
  quarantine_rows: number;
  privacy: PrivacyItem[];
  estimation: Record<string, number>;
  warnings: string[];
}
