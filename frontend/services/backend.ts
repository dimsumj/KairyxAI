type HttpMethod = "GET" | "POST" | "DELETE";

const backendBaseUrl =
  (import.meta as any).env?.VITE_BACKEND_URL ||
  `${window.location.protocol}//${window.location.hostname}:8000`;

async function request<T>(path: string, method: HttpMethod = "GET", body?: unknown): Promise<T> {
  const response = await fetch(`${backendBaseUrl}${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.detail || payload?.message || `Request failed (${response.status})`;
    throw new Error(message);
  }
  return payload as T;
}

export interface Connector {
  type: string;
  name: string;
  details: string;
}

export interface ConnectorFreshness {
  connector_name: string;
  connector_type: string;
  last_attempt_at?: string;
  last_success_at?: string;
  last_ingested_events?: number;
  last_error?: string | null;
}

export interface IdentityLink {
  source: string;
  source_user_id: string;
  canonical_user_id: string;
  confidence: number;
  method: string;
  first_seen_at: string;
  last_seen_at: string;
}

export interface ImportJob {
  name: string;
  status: string;
  timestamp: string;
  start_date: string;
  end_date: string;
  source_stats?: Array<{ source: string; type: string; ingested_events: number; status?: string; error?: string }>;
  processing_stats?: { raw_normalized_events: number; deduped_events: number; duplicates_removed: number };
}

export interface PredictionRow {
  user_id: string;
  ltv: number | string;
  session_count: number | string;
  event_count: number | string;
  predicted_churn_risk: string;
  churn_reason: string;
  suggested_action: string;
}

export interface ExperimentConfig {
  experiment_id: string;
  enabled: boolean;
  holdout_pct: number;
  b_variant_pct: number;
}

export interface ExperimentGroupSummary {
  n: number;
  engaged: number;
  returned: number;
  engagement_rate: number;
  return_rate: number;
  uplift_vs_holdout_return_rate?: number;
}

export interface ExperimentSummary {
  experiment_id: string;
  groups: Record<string, ExperimentGroupSummary>;
}

export const backendService = {
  baseUrl: backendBaseUrl,

  async health() {
    return request<{ status: string }>("/health");
  },

  async listConnectors() {
    return request<{ connectors: Connector[] }>("/connectors/list");
  },

  async listConfiguredSources() {
    return request<{ sources: Array<{ id: string; name: string }> }>("/list-configured-sources");
  },

  async connectorHealth(connectorName: string) {
    return request<{ connector: string; type: string; health: { ok: boolean; message?: string } }>(`/connector-health/${encodeURIComponent(connectorName)}`);
  },

  async connectorFreshness() {
    return request<{ connectors: Record<string, ConnectorFreshness> }>("/connector-freshness");
  },

  async getFieldMapping(connectorName: string) {
    return request<{ connector: string; mapping: Record<string, string> }>(`/field-mapping/${encodeURIComponent(connectorName)}`);
  },

  async saveFieldMapping(connectorName: string, mapping: Record<string, string>) {
    return request<{ message: string; connector: string; mapping: Record<string, string> }>(`/field-mapping/${encodeURIComponent(connectorName)}`, "POST", { mapping });
  },

  async previewFieldMapping(connectorName: string, sampleRecord: Record<string, any>) {
    return request<{ connector: string; mapping: Record<string, string>; preview: Record<string, any> }>("/field-mapping/preview", "POST", {
      connector_name: connectorName,
      sample_record: sampleRecord,
    });
  },

  async mappingCoverage(connectorName: string, sampleRecords: Record<string, any>[]) {
    return request<any>("/field-mapping/coverage", "POST", {
      connector_name: connectorName,
      sample_records: sampleRecords,
    });
  },

  async jobMappingCoverage(jobName: string, connectorName: string) {
    return request<any>(`/job/${encodeURIComponent(jobName)}/mapping-coverage?connector_name=${encodeURIComponent(connectorName)}`);
  },

  async listIdentityLinks(limit = 200) {
    return request<{ identity_links: IdentityLink[] }>(`/identity-links?limit=${limit}`);
  },

  async deleteConnector(connectorName: string) {
    return request<{ message: string }>(`/connector/${encodeURIComponent(connectorName)}`, "DELETE");
  },

  async configureAmplitude(apiKey: string, secretKey: string) {
    return request<{ message: string }>("/configure-amplitude-keys", "POST", {
      api_key: apiKey,
      secret_key: secretKey,
    });
  },

  async configureGoogle(apiKey: string, modelName?: string) {
    return request<{ message: string }>("/configure-google-key", "POST", {
      api_key: apiKey,
      model_name: modelName || undefined,
    });
  },

  async configureBigQuery(projectId: string) {
    return request<{ message: string }>("/configure-bigquery", "POST", { project_id: projectId });
  },

  async configureAdjust(apiToken: string, apiUrl?: string) {
    return request<{ message: string }>("/configure-adjust-credentials", "POST", { api_token: apiToken, api_url: apiUrl || undefined });
  },

  async configureAppsflyer(apiToken: string, appId: string, pullApiUrl?: string) {
    return request<{ message: string }>("/configure-appsflyer", "POST", { api_token: apiToken, app_id: appId, pull_api_url: pullApiUrl || undefined });
  },

  async configureSendgrid(apiKey: string) {
    return request<{ message: string }>("/configure-sendgrid-key", "POST", { api_key: apiKey });
  },

  async configureBraze(apiKey: string, restEndpoint: string) {
    return request<{ message: string }>("/configure-braze", "POST", { api_key: apiKey, rest_endpoint: restEndpoint });
  },

  async getExperimentConfig() {
    return request<{ experiment: ExperimentConfig }>("/experiments/config");
  },

  async updateExperimentConfig(payload: Partial<ExperimentConfig>) {
    return request<{ experiment: ExperimentConfig }>("/experiments/config", "POST", payload);
  },

  async getExperimentSummary(experimentId?: string) {
    const q = experimentId ? `?experiment_id=${encodeURIComponent(experimentId)}` : '';
    return request<ExperimentSummary>(`/experiments/summary${q}`);
  },

  async listImports() {
    return request<{ imports: ImportJob[] }>("/list-imports");
  },

  async startImport(startDate: string, endDate: string, source: string, continueOnSourceError = true, autoMapping = false) {
    return request<{ message: string }>("/ingest-and-process-data", "POST", {
      start_date: startDate,
      end_date: endDate,
      source,
      continue_on_source_error: continueOnSourceError,
      auto_mapping: autoMapping,
    });
  },

  async processAfterMapping(jobName: string) {
    return request<{ message: string; processing_stats: any }>(`/job/${encodeURIComponent(jobName)}/process-after-mapping`, "POST");
  },

  async predictForImport(jobName: string, forceRecalculate = true) {
    return request<{ predictions: PredictionRow[] }>("/predict-churn-for-import", "POST", {
      job_name: jobName,
      force_recalculate: forceRecalculate,
    });
  },
};

