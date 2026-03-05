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

  async startImport(startDate: string, endDate: string, source: string, continueOnSourceError = true) {
    return request<{ message: string }>("/ingest-and-process-data", "POST", {
      start_date: startDate,
      end_date: endDate,
      source,
      continue_on_source_error: continueOnSourceError,
    });
  },

  async predictForImport(jobName: string, forceRecalculate = true) {
    return request<{ predictions: PredictionRow[] }>("/predict-churn-for-import", "POST", {
      job_name: jobName,
      force_recalculate: forceRecalculate,
    });
  },
};

