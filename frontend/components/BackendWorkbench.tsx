import React, { useEffect, useMemo, useState } from 'react';
import { backendService, Connector, ImportJob, PredictionRow, ExperimentConfig, ExperimentSummary, ConnectorFreshness, IdentityLink } from '../services/backend.ts';

type ConnectorType = 'amplitude' | 'google' | 'bigquery' | 'adjust' | 'appsflyer' | 'sendgrid' | 'braze';

const modelOptions = ['gemini-2.5-flash', 'gemini-2.5-pro', 'gemini-flash-latest', 'gemini-pro-latest'];

const BackendWorkbench: React.FC = () => {
  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [sources, setSources] = useState<Array<{ id: string; name: string }>>([]);
  const [imports, setImports] = useState<ImportJob[]>([]);
  const [predictions, setPredictions] = useState<PredictionRow[]>([]);
  const [freshness, setFreshness] = useState<Record<string, ConnectorFreshness>>({});
  const [selectedMappingConnector, setSelectedMappingConnector] = useState('');
  const [mappingJson, setMappingJson] = useState('{}');
  const [previewJson, setPreviewJson] = useState('{"PID":"user_123","event_name":"install","timestamp":"2026-03-05T01:00:00Z"}');
  const [mappingPreviewResult, setMappingPreviewResult] = useState<any>(null);
  const [mappingCoverageResult, setMappingCoverageResult] = useState<any>(null);
  const [identityLinks, setIdentityLinks] = useState<IdentityLink[]>([]);
  const [rejectedEvents, setRejectedEvents] = useState<any[]>([]);
  const [conflicts, setConflicts] = useState<any[]>([]);
  const [cleanupJobFilter, setCleanupJobFilter] = useState('');
  const [cleanupSourceFilter, setCleanupSourceFilter] = useState('');
  const [selectedJob, setSelectedJob] = useState('');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [healthStatus, setHealthStatus] = useState('checking');

  const [connectorType, setConnectorType] = useState<ConnectorType>('amplitude');
  const [amplitudeApiKey, setAmplitudeApiKey] = useState('');
  const [amplitudeSecretKey, setAmplitudeSecretKey] = useState('');
  const [googleApiKey, setGoogleApiKey] = useState('');
  const [googleModel, setGoogleModel] = useState(modelOptions[0]);
  const [bigqueryProjectId, setBigqueryProjectId] = useState('');
  const [adjustApiToken, setAdjustApiToken] = useState('');
  const [adjustApiUrl, setAdjustApiUrl] = useState('');
  const [appsflyerApiToken, setAppsflyerApiToken] = useState('');
  const [appsflyerAppId, setAppsflyerAppId] = useState('');
  const [appsflyerPullApiUrl, setAppsflyerPullApiUrl] = useState('');
  const [sendgridApiKey, setSendgridApiKey] = useState('');
  const [brazeApiKey, setBrazeApiKey] = useState('');
  const [brazeEndpoint, setBrazeEndpoint] = useState('');

  const [experimentConfig, setExperimentConfig] = useState<ExperimentConfig>({
    experiment_id: 'churn_engagement_v1',
    enabled: true,
    holdout_pct: 0.1,
    b_variant_pct: 0.5,
  });
  const [experimentSummary, setExperimentSummary] = useState<ExperimentSummary | null>(null);
  const [churnInactiveDays, setChurnInactiveDays] = useState(14);

  const [importSource, setImportSource] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [continueOnSourceError, setContinueOnSourceError] = useState(true);
  const [autoMapping, setAutoMapping] = useState(false);

  const readyJobs = useMemo(() => imports.filter((job) => job.status === 'Ready to Use'), [imports]);
  const awaitingMappingJobs = useMemo(() => imports.filter((job) => job.status === 'Awaiting Mapping'), [imports]);

  const refreshAll = async () => {
    setError('');
    try {
      const [health, connectorsResp, sourcesResp, importsResp, expConfigResp, churnConfigResp, freshnessResp, identityResp, rejectedResp, conflictsResp] = await Promise.all([
        backendService.health(),
        backendService.listConnectors(),
        backendService.listConfiguredSources(),
        backendService.listImports(),
        backendService.getExperimentConfig(),
        backendService.getChurnConfig(),
        backendService.connectorFreshness(),
        backendService.listIdentityLinks(100),
        backendService.cleanupRejectedEvents({ limit: 50 }),
        backendService.cleanupConflicts({ limit: 50 }),
      ]);
      setHealthStatus(health.status);
      setConnectors(connectorsResp.connectors || []);
      setSources(sourcesResp.sources || []);
      setImports(importsResp.imports || []);
      setExperimentConfig(expConfigResp.experiment);
      setChurnInactiveDays(churnConfigResp.churn?.churn_inactive_days || 14);
      setFreshness(freshnessResp.connectors || {});
      setIdentityLinks(identityResp.identity_links || []);
      setRejectedEvents(rejectedResp.rejected_events || []);
      setConflicts(conflictsResp.conflicts || []);
      if (!selectedMappingConnector && connectorsResp.connectors?.length) {
        setSelectedMappingConnector(connectorsResp.connectors[0].name);
      }
      if (!importSource && sourcesResp.sources?.length) {
        setImportSource(sourcesResp.sources[0].id);
      }
      if (!selectedJob && importsResp.imports?.length) {
        const firstReady = importsResp.imports.find((j) => j.status === 'Ready to Use');
        if (firstReady) setSelectedJob(firstReady.name);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to refresh backend data.');
      setHealthStatus('down');
    }
  };

  useEffect(() => {
    refreshAll();
  }, []);

  useEffect(() => {
    if (!selectedJob && readyJobs.length) {
      setSelectedJob(readyJobs[0].name);
    }
  }, [readyJobs, selectedJob]);

  useEffect(() => {
    if (!awaitingMappingJobs.length) return;

    const topAwaiting = awaitingMappingJobs[0];
    const firstSource = topAwaiting.source_stats?.[0]?.source;
    if (firstSource && firstSource !== selectedMappingConnector) {
      setSelectedMappingConnector(firstSource);
      setMessage(`Job "${topAwaiting.name}" is awaiting mapping. Switched mapping panel to ${firstSource}.`);
    }
  }, [awaitingMappingJobs, selectedMappingConnector]);

  const runConnectorSave = async () => {
    setLoading(true);
    setError('');
    setMessage('');
    try {
      if (connectorType === 'amplitude') {
        await backendService.configureAmplitude(amplitudeApiKey, amplitudeSecretKey);
      } else if (connectorType === 'google') {
        await backendService.configureGoogle(googleApiKey, googleModel);
      } else if (connectorType === 'bigquery') {
        await backendService.configureBigQuery(bigqueryProjectId);
      } else if (connectorType === 'adjust') {
        await backendService.configureAdjust(adjustApiToken, adjustApiUrl);
      } else if (connectorType === 'appsflyer') {
        await backendService.configureAppsflyer(appsflyerApiToken, appsflyerAppId, appsflyerPullApiUrl);
      } else if (connectorType === 'sendgrid') {
        await backendService.configureSendgrid(sendgridApiKey);
      } else if (connectorType === 'braze') {
        await backendService.configureBraze(brazeApiKey, brazeEndpoint);
      }
      setMessage('Connector saved successfully.');
      await refreshAll();
    } catch (err: any) {
      setError(err.message || 'Failed to save connector.');
    } finally {
      setLoading(false);
    }
  };

  const runStartImport = async () => {
    if (!importSource || !startDate || !endDate) {
      setError('Please select source and date range.');
      return;
    }
    setLoading(true);
    setError('');
    setMessage('');
    try {
      await backendService.startImport(
        startDate.replaceAll('-', ''),
        endDate.replaceAll('-', ''),
        importSource,
        continueOnSourceError,
        autoMapping,
      );
      setMessage('Import job started.');
      await refreshAll();
    } catch (err: any) {
      setError(err.message || 'Failed to start import.');
    } finally {
      setLoading(false);
    }
  };

  const runPredict = async () => {
    if (!selectedJob) {
      setError('Please choose a ready import job.');
      return;
    }
    setLoading(true);
    setError('');
    setMessage('');
    try {
      const data = await backendService.predictForImport(selectedJob, true);
      setPredictions(data.predictions || []);
      setMessage(`Loaded ${data.predictions?.length || 0} prediction rows.`);
    } catch (err: any) {
      setError(err.message || 'Prediction failed.');
      setPredictions([]);
    } finally {
      setLoading(false);
    }
  };

  const deleteConnector = async (name: string) => {
    setLoading(true);
    setError('');
    setMessage('');
    try {
      await backendService.deleteConnector(name);
      setMessage(`Deleted connector "${name}".`);
      await refreshAll();
    } catch (err: any) {
      setError(err.message || 'Failed to delete connector.');
    } finally {
      setLoading(false);
    }
  };

  const processAfterMapping = async (jobName: string) => {
    setLoading(true);
    setError('');
    setMessage('');
    try {
      await backendService.processAfterMapping(jobName);
      setMessage(`Job "${jobName}" processed after manual mapping.`);
      await refreshAll();
    } catch (err: any) {
      setError(err.message || 'Failed to process after mapping.');
    } finally {
      setLoading(false);
    }
  };

  const saveExperimentConfig = async () => {
    setLoading(true);
    setError('');
    setMessage('');
    try {
      const resp = await backendService.updateExperimentConfig(experimentConfig);
      setExperimentConfig(resp.experiment);
      setMessage('Experiment config saved.');
    } catch (err: any) {
      setError(err.message || 'Failed to save experiment config.');
    } finally {
      setLoading(false);
    }
  };

  const refreshExperimentSummary = async () => {
    setLoading(true);
    setError('');
    try {
      const resp = await backendService.getExperimentSummary(experimentConfig.experiment_id);
      setExperimentSummary(resp);
    } catch (err: any) {
      setError(err.message || 'Failed to load experiment summary.');
    } finally {
      setLoading(false);
    }
  };

  const saveChurnConfig = async () => {
    setLoading(true);
    setError('');
    setMessage('');
    try {
      const resp = await backendService.updateChurnConfig(churnInactiveDays);
      setChurnInactiveDays(resp.churn.churn_inactive_days);
      setMessage('Churn config saved.');
    } catch (err: any) {
      setError(err.message || 'Failed to save churn config.');
    } finally {
      setLoading(false);
    }
  };

  const loadFieldMapping = async () => {
    if (!selectedMappingConnector) return;
    setLoading(true);
    setError('');
    try {
      const resp = await backendService.getFieldMapping(selectedMappingConnector);
      setMappingJson(JSON.stringify(resp.mapping || {}, null, 2));
      setMessage('Field mapping loaded.');
    } catch (err: any) {
      setError(err.message || 'Failed to load field mapping.');
    } finally {
      setLoading(false);
    }
  };

  const saveFieldMapping = async () => {
    if (!selectedMappingConnector) return;
    setLoading(true);
    setError('');
    try {
      const mappingObj = JSON.parse(mappingJson || '{}');
      await backendService.saveFieldMapping(selectedMappingConnector, mappingObj);
      setMessage('Field mapping saved.');
    } catch (err: any) {
      setError(err.message || 'Failed to save field mapping (check JSON format).');
    } finally {
      setLoading(false);
    }
  };

  const previewFieldMapping = async () => {
    if (!selectedMappingConnector) return;
    setLoading(true);
    setError('');
    try {
      const sample = JSON.parse(previewJson || '{}');
      const resp = await backendService.previewFieldMapping(selectedMappingConnector, sample);
      setMappingPreviewResult(resp.preview);
      setMessage('Mapping preview generated.');
    } catch (err: any) {
      setError(err.message || 'Failed to preview mapping (check sample JSON).');
    } finally {
      setLoading(false);
    }
  };

  const refreshCleanupObservability = async () => {
    setLoading(true);
    setError('');
    try {
      const [rejectedResp, conflictsResp] = await Promise.all([
        backendService.cleanupRejectedEvents({
          limit: 100,
          jobIdentifier: cleanupJobFilter || undefined,
          source: cleanupSourceFilter || undefined,
        }),
        backendService.cleanupConflicts({
          limit: 100,
          jobIdentifier: cleanupJobFilter || undefined,
          source: cleanupSourceFilter || undefined,
        }),
      ]);
      setRejectedEvents(rejectedResp.rejected_events || []);
      setConflicts(conflictsResp.conflicts || []);
      setMessage('Cleanup observability refreshed.');
    } catch (err: any) {
      setError(err.message || 'Failed to refresh cleanup observability.');
    } finally {
      setLoading(false);
    }
  };

  const loadMappingCoverage = async () => {
    if (!selectedMappingConnector) return;
    setLoading(true);
    setError('');
    try {
      const awaitingJob = awaitingMappingJobs[0];
      if (awaitingJob) {
        const resp = await backendService.jobMappingCoverage(awaitingJob.name, selectedMappingConnector);
        setMappingCoverageResult(resp);
      } else {
        const sample = JSON.parse(previewJson || '{}');
        const resp = await backendService.mappingCoverage(selectedMappingConnector, [sample]);
        setMappingCoverageResult(resp);
      }
      setMessage('Mapping coverage calculated.');
    } catch (err: any) {
      setError(err.message || 'Failed to calculate mapping coverage.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <header>
        <h2 className="text-2xl font-bold">Backend Test Workbench</h2>
        <p className="text-gray-400">Single React flow for connector setup, ingestion, and churn prediction.</p>
        <p className="text-gray-500 text-xs mt-2">Backend URL: {backendService.baseUrl}</p>
      </header>

      <div className="flex gap-3 items-center">
        <button
          className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm"
          onClick={refreshAll}
          disabled={loading}
        >
          Refresh
        </button>
        <span className={`text-sm ${healthStatus === 'ok' ? 'text-green-400' : 'text-red-400'}`}>
          Backend: {healthStatus}
        </span>
      </div>

      {message ? <div className="text-green-400 text-sm">{message}</div> : null}
      {error ? <div className="text-red-400 text-sm">{error}</div> : null}

      {awaitingMappingJobs.length > 0 ? (
        <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-4 text-sm space-y-2">
          <div className="font-semibold text-amber-300">Manual mapping required</div>
          <div className="text-amber-100/90">
            {awaitingMappingJobs.length} import job(s) are paused at <code>Awaiting Mapping</code>. Suggested flow: Load mapping → Preview → Save → Process After Mapping.
          </div>
          <div className="flex flex-wrap gap-2">
            <button className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs" onClick={loadFieldMapping} disabled={loading || !selectedMappingConnector}>Load Mapping</button>
            <button className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-xs" onClick={previewFieldMapping} disabled={loading || !selectedMappingConnector}>Preview Mapping</button>
            <button className="bg-indigo-600 hover:bg-indigo-500 rounded px-2 py-1 text-xs" onClick={() => processAfterMapping(awaitingMappingJobs[0].name)} disabled={loading}>Process First Awaiting Job</button>
          </div>
        </div>
      ) : null}

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Configure Connectors</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <select
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={connectorType}
            onChange={(e) => setConnectorType(e.target.value as ConnectorType)}
          >
            <option value="amplitude">Amplitude</option>
            <option value="google">Google Gemini</option>
            <option value="bigquery">BigQuery</option>
            <option value="adjust">Adjust</option>
            <option value="appsflyer">AppsFlyer</option>
            <option value="sendgrid">SendGrid</option>
            <option value="braze">Braze</option>
          </select>
          {connectorType === 'google' ? (
            <select
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={googleModel}
              onChange={(e) => setGoogleModel(e.target.value)}
            >
              {modelOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          ) : null}
        </div>

        {connectorType === 'amplitude' ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <input
              type="password"
              placeholder="Amplitude API Key"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={amplitudeApiKey}
              onChange={(e) => setAmplitudeApiKey(e.target.value)}
            />
            <input
              type="password"
              placeholder="Amplitude Secret Key"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={amplitudeSecretKey}
              onChange={(e) => setAmplitudeSecretKey(e.target.value)}
            />
          </div>
        ) : null}

        {connectorType === 'google' ? (
          <input
            type="password"
            placeholder="Google API Key"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 w-full"
            value={googleApiKey}
            onChange={(e) => setGoogleApiKey(e.target.value)}
          />
        ) : null}

        {connectorType === 'bigquery' ? (
          <input
            type="text"
            placeholder="BigQuery Project ID"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 w-full"
            value={bigqueryProjectId}
            onChange={(e) => setBigqueryProjectId(e.target.value)}
          />
        ) : null}

        {connectorType === 'adjust' ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <input
              type="password"
              placeholder="Adjust API Token"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={adjustApiToken}
              onChange={(e) => setAdjustApiToken(e.target.value)}
            />
            <input
              type="text"
              placeholder="Adjust API URL (optional for real mode)"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={adjustApiUrl}
              onChange={(e) => setAdjustApiUrl(e.target.value)}
            />
          </div>
        ) : null}

        {connectorType === 'appsflyer' ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <input
              type="password"
              placeholder="AppsFlyer API Token"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={appsflyerApiToken}
              onChange={(e) => setAppsflyerApiToken(e.target.value)}
            />
            <input
              type="text"
              placeholder="AppsFlyer App ID"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={appsflyerAppId}
              onChange={(e) => setAppsflyerAppId(e.target.value)}
            />
            <input
              type="text"
              placeholder="AppsFlyer Pull API URL (optional for real mode)"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={appsflyerPullApiUrl}
              onChange={(e) => setAppsflyerPullApiUrl(e.target.value)}
            />
          </div>
        ) : null}

        {connectorType === 'sendgrid' ? (
          <input
            type="password"
            placeholder="SendGrid API Key"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 w-full"
            value={sendgridApiKey}
            onChange={(e) => setSendgridApiKey(e.target.value)}
          />
        ) : null}

        {connectorType === 'braze' ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <input
              type="password"
              placeholder="Braze API Key"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={brazeApiKey}
              onChange={(e) => setBrazeApiKey(e.target.value)}
            />
            <input
              type="text"
              placeholder="Braze REST Endpoint (https://rest.iad-01.braze.com)"
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
              value={brazeEndpoint}
              onChange={(e) => setBrazeEndpoint(e.target.value)}
            />
          </div>
        ) : null}

        <button className="bg-indigo-600 hover:bg-indigo-500 rounded-lg px-4 py-2 text-sm" onClick={runConnectorSave} disabled={loading}>
          Save Connector
        </button>
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Saved Connectors</h3>
        {connectors.length === 0 ? <p className="text-gray-500 text-sm">No connectors configured yet.</p> : null}
        {connectors.map((connector) => {
          const fr = freshness[connector.name];
          return (
            <div key={connector.name} className="flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2">
              <div>
                <div className="font-medium">{connector.name}</div>
                <div className="text-xs text-gray-400">{connector.type}</div>
                {fr ? (
                  <div className="text-[11px] text-gray-400 mt-1">
                    <div>last_success_at: {fr.last_success_at ? new Date(fr.last_success_at).toLocaleString() : 'N/A'}</div>
                    <div>last_attempt_at: {fr.last_attempt_at ? new Date(fr.last_attempt_at).toLocaleString() : 'N/A'}</div>
                    <div>last_ingested_events: {fr.last_ingested_events ?? 0}</div>
                    {fr.last_error ? <div className="text-red-400">last_error: {fr.last_error}</div> : null}
                  </div>
                ) : null}
              </div>
              <button
                className="bg-red-600/20 border border-red-500/40 text-red-300 rounded px-3 py-1 text-xs"
                onClick={() => deleteConnector(connector.name)}
                disabled={loading}
              >
                Delete
              </button>
            </div>
          );
        })}
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Manual Field Mapping (Canonical Override)</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <select
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={selectedMappingConnector}
            onChange={(e) => setSelectedMappingConnector(e.target.value)}
          >
            <option value="">Select Connector</option>
            {connectors.map((c) => (
              <option key={c.name} value={c.name}>{c.name}</option>
            ))}
          </select>
          <div className="flex gap-2">
            <button className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm" onClick={loadFieldMapping} disabled={loading}>Load</button>
            <button className="bg-indigo-600 hover:bg-indigo-500 rounded-lg px-3 py-2 text-sm" onClick={saveFieldMapping} disabled={loading}>Save</button>
          </div>
        </div>
        <p className="text-xs text-gray-400">Example mapping keys: canonical_user_id, event_name, event_time, source_event_id, campaign, adset, media_source. Values are raw key paths like <code>event_properties.PID</code>.</p>
        <textarea
          className="w-full h-36 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 font-mono text-xs"
          value={mappingJson}
          onChange={(e) => setMappingJson(e.target.value)}
        />
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <textarea
            className="w-full h-28 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 font-mono text-xs"
            value={previewJson}
            onChange={(e) => setPreviewJson(e.target.value)}
          />
          <div className="bg-gray-800 border border-gray-700 rounded-lg p-3 text-xs font-mono overflow-auto">
            {mappingPreviewResult ? JSON.stringify(mappingPreviewResult, null, 2) : 'Preview result will appear here.'}
          </div>
        </div>
        <div className="flex gap-2">
          <button className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm" onClick={previewFieldMapping} disabled={loading}>Preview Mapping</button>
          <button className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm" onClick={loadMappingCoverage} disabled={loading}>Coverage</button>
        </div>

        {mappingCoverageResult ? (
          <div className="space-y-3 bg-gray-800/60 border border-gray-700 rounded-lg p-3">
            <div className="text-xs text-gray-300">Required Coverage Score: {(Number(mappingCoverageResult.required_coverage_score || 0) * 100).toFixed(1)}%</div>
            <div className="w-full bg-gray-700 rounded-full h-2.5">
              <div
                className={`h-2.5 rounded-full ${(Number(mappingCoverageResult.required_coverage_score || 0) >= 0.9) ? 'bg-green-500' : (Number(mappingCoverageResult.required_coverage_score || 0) >= 0.7) ? 'bg-amber-500' : 'bg-red-500'}`}
                style={{ width: `${Math.max(0, Math.min(100, Number(mappingCoverageResult.required_coverage_score || 0) * 100))}%` }}
              />
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs">
              {Object.entries(mappingCoverageResult.coverage || {}).map(([field, stat]: any) => (
                <div key={field} className="border border-gray-700 rounded px-2 py-1">
                  <div className="font-semibold text-gray-200">{field}</div>
                  <div className="text-gray-400">path: {stat.path || '-'}</div>
                  <div className="text-gray-300">hit: {(Number(stat.hit_rate || 0) * 100).toFixed(1)}% ({stat.hits || 0}/{stat.total || 0})</div>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Identity Links (Cross-Source User Matching)</h3>
        <div className="max-h-56 overflow-auto border border-gray-800 rounded-lg">
          <table className="w-full text-left text-xs">
            <thead className="bg-gray-800">
              <tr>
                <th className="px-3 py-2">Source</th>
                <th className="px-3 py-2">Source User ID</th>
                <th className="px-3 py-2">Canonical User ID</th>
                <th className="px-3 py-2">Method</th>
                <th className="px-3 py-2">Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {identityLinks.map((row, idx) => (
                <tr key={`${row.source}-${row.source_user_id}-${idx}`} className="border-t border-gray-800">
                  <td className="px-3 py-2">{row.source}</td>
                  <td className="px-3 py-2">{row.source_user_id}</td>
                  <td className="px-3 py-2">{row.canonical_user_id}</td>
                  <td className="px-3 py-2">{row.method}</td>
                  <td className="px-3 py-2">{new Date(row.last_seen_at).toLocaleString()}</td>
                </tr>
              ))}
              {identityLinks.length === 0 ? (
                <tr><td className="px-3 py-2 text-gray-500" colSpan={5}>No identity links yet.</td></tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Cleanup Observability (P2.5)</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <input
            type="text"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            placeholder="Filter by job_identifier"
            value={cleanupJobFilter}
            onChange={(e) => setCleanupJobFilter(e.target.value)}
          />
          <input
            type="text"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            placeholder="Filter by source"
            value={cleanupSourceFilter}
            onChange={(e) => setCleanupSourceFilter(e.target.value)}
          />
          <button className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm" onClick={refreshCleanupObservability} disabled={loading}>
            Refresh Cleanup Logs
          </button>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="border border-gray-800 rounded-lg overflow-auto max-h-64">
            <div className="bg-gray-800 px-3 py-2 text-sm font-semibold">Rejected Events ({rejectedEvents.length})</div>
            <table className="w-full text-left text-xs">
              <thead className="bg-gray-900/60">
                <tr>
                  <th className="px-3 py-2">Job</th>
                  <th className="px-3 py-2">Source</th>
                  <th className="px-3 py-2">Reason</th>
                </tr>
              </thead>
              <tbody>
                {rejectedEvents.map((r, idx) => (
                  <tr key={`rej-${idx}`} className="border-t border-gray-800">
                    <td className="px-3 py-2">{r.job_identifier}</td>
                    <td className="px-3 py-2">{r.event?.source || '-'}</td>
                    <td className="px-3 py-2">{r.event?.rejection_reason || '-'}</td>
                  </tr>
                ))}
                {rejectedEvents.length === 0 ? <tr><td className="px-3 py-2 text-gray-500" colSpan={3}>No rejected events.</td></tr> : null}
              </tbody>
            </table>
          </div>

          <div className="border border-gray-800 rounded-lg overflow-auto max-h-64">
            <div className="bg-gray-800 px-3 py-2 text-sm font-semibold">Conflicts ({conflicts.length})</div>
            <table className="w-full text-left text-xs">
              <thead className="bg-gray-900/60">
                <tr>
                  <th className="px-3 py-2">Job</th>
                  <th className="px-3 py-2">Field</th>
                  <th className="px-3 py-2">A→B</th>
                </tr>
              </thead>
              <tbody>
                {conflicts.map((c, idx) => (
                  <tr key={`conf-${idx}`} className="border-t border-gray-800">
                    <td className="px-3 py-2">{c.job_identifier}</td>
                    <td className="px-3 py-2">{c.field}</td>
                    <td className="px-3 py-2">{c.source_a}:{String(c.value_a)} → {c.source_b}:{String(c.value_b)}</td>
                  </tr>
                ))}
                {conflicts.length === 0 ? <tr><td className="px-3 py-2 text-gray-500" colSpan={3}>No conflicts.</td></tr> : null}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Import Data</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <input
            type="text"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            placeholder="Source name(s), comma-separated (e.g., Amplitude 1,AppsFlyer 1)"
            value={importSource}
            onChange={(e) => setImportSource(e.target.value)}
            list="configured-sources"
          />
          <datalist id="configured-sources">
            {sources.map((source) => (
              <option key={source.id} value={source.id} />
            ))}
          </datalist>
          <input
            type="date"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
          />
          <input
            type="date"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
          />
        </div>
        <label className="flex items-center gap-2 text-sm text-gray-300">
          <input
            type="checkbox"
            checked={continueOnSourceError}
            onChange={(e) => setContinueOnSourceError(e.target.checked)}
          />
          Continue if one source fails
        </label>
        <label className="flex items-center gap-2 text-sm text-gray-300">
          <input
            type="checkbox"
            checked={autoMapping}
            onChange={(e) => setAutoMapping(e.target.checked)}
          />
          Auto mapping (pause after pull for manual mapping)
        </label>
        <button className="bg-indigo-600 hover:bg-indigo-500 rounded-lg px-4 py-2 text-sm" onClick={runStartImport} disabled={loading}>
          Start Import
        </button>
        <div className="max-h-64 overflow-auto border border-gray-800 rounded-lg">
          <table className="w-full text-left text-sm">
            <thead className="bg-gray-800">
              <tr>
                <th className="px-3 py-2">Job</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">Date Range</th>
                <th className="px-3 py-2">Created</th>
                <th className="px-3 py-2">Source/Processing Stats</th>
              </tr>
            </thead>
            <tbody>
              {imports.map((job) => (
                <tr key={job.name} className="border-t border-gray-800 align-top">
                  <td className="px-3 py-2">{job.name}</td>
                  <td className="px-3 py-2">
                    <div className="space-y-1">
                      <div>{job.status}</div>
                      {job.status === 'Awaiting Mapping' ? (
                        <button
                          className="text-xs bg-indigo-600 hover:bg-indigo-500 rounded px-2 py-1"
                          onClick={() => processAfterMapping(job.name)}
                          disabled={loading}
                        >
                          Process After Mapping
                        </button>
                      ) : null}
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    {job.start_date} to {job.end_date}
                  </td>
                  <td className="px-3 py-2">{new Date(job.timestamp).toLocaleString()}</td>
                  <td className="px-3 py-2 text-xs text-gray-300">
                    {job.source_stats?.length ? (
                      <div className="space-y-1">
                        {job.source_stats.map((s, idx) => (
                          <div key={`${job.name}-s-${idx}`}>
                            {s.source} ({s.type}): {s.ingested_events}
                            {s.status === 'failed' ? <span className="text-red-400"> — failed: {s.error}</span> : null}
                          </div>
                        ))}
                        {job.processing_stats ? (
                          <div className="text-gray-400 mt-1 space-y-1">
                            <div>normalized={job.processing_stats.raw_normalized_events}, deduped={job.processing_stats.deduped_events}, removed={job.processing_stats.duplicates_removed}, rejected={job.processing_stats.rejected_events || 0}, conflicts={job.processing_stats.conflicts_logged || 0}</div>
                            {job.processing_stats.quality ? (
                              <div>
                                quality: clean={job.processing_stats.quality.rows_clean}, flagged={job.processing_stats.quality.rows_with_flags}
                                {Object.keys(job.processing_stats.quality.flag_counts || {}).length > 0 ? (
                                  <div className="text-[11px] text-amber-300">
                                    {Object.entries(job.processing_stats.quality.flag_counts).map(([k, v]) => `${k}:${v}`).join(' · ')}
                                  </div>
                                ) : null}
                              </div>
                            ) : null}
                          </div>
                        ) : null}
                      </div>
                    ) : <span className="text-gray-500">-</span>}
                  </td>
                </tr>
              ))}
              {imports.length === 0 ? (
                <tr>
                  <td className="px-3 py-2 text-gray-500" colSpan={5}>
                    No imports yet.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Experiment Control (A/B + Holdout)</h3>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <input
            type="text"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={experimentConfig.experiment_id}
            onChange={(e) => setExperimentConfig((prev) => ({ ...prev, experiment_id: e.target.value }))}
            placeholder="experiment_id"
          />
          <input
            type="number"
            step="0.01"
            min="0"
            max="0.9"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={experimentConfig.holdout_pct}
            onChange={(e) => setExperimentConfig((prev) => ({ ...prev, holdout_pct: Number(e.target.value) }))}
            placeholder="holdout_pct"
          />
          <input
            type="number"
            step="0.01"
            min="0"
            max="1"
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={experimentConfig.b_variant_pct}
            onChange={(e) => setExperimentConfig((prev) => ({ ...prev, b_variant_pct: Number(e.target.value) }))}
            placeholder="b_variant_pct"
          />
          <label className="flex items-center gap-2 text-sm bg-gray-800 border border-gray-700 rounded-lg px-3 py-2">
            <input
              type="checkbox"
              checked={experimentConfig.enabled}
              onChange={(e) => setExperimentConfig((prev) => ({ ...prev, enabled: e.target.checked }))}
            />
            Enabled
          </label>
        </div>
        <div className="flex gap-3">
          <button className="bg-indigo-600 hover:bg-indigo-500 rounded-lg px-4 py-2 text-sm" onClick={saveExperimentConfig} disabled={loading}>
            Save Experiment Config
          </button>
          <button className="bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-sm" onClick={refreshExperimentSummary} disabled={loading}>
            Refresh Summary
          </button>
        </div>

        {experimentSummary ? (
          <div className="overflow-auto border border-gray-800 rounded-lg">
            <table className="w-full text-left text-sm">
              <thead className="bg-gray-800">
                <tr>
                  <th className="px-3 py-2">Group</th>
                  <th className="px-3 py-2">N</th>
                  <th className="px-3 py-2">Engagement Rate</th>
                  <th className="px-3 py-2">Return Rate</th>
                  <th className="px-3 py-2">Uplift vs Holdout</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(experimentSummary.groups || {}).map(([group, stats]) => (
                  <tr key={group} className="border-t border-gray-800">
                    <td className="px-3 py-2">{group}</td>
                    <td className="px-3 py-2">{stats.n}</td>
                    <td className="px-3 py-2">{(stats.engagement_rate * 100).toFixed(2)}%</td>
                    <td className="px-3 py-2">{(stats.return_rate * 100).toFixed(2)}%</td>
                    <td className="px-3 py-2">{stats.uplift_vs_holdout_return_rate != null ? `${(stats.uplift_vs_holdout_return_rate * 100).toFixed(2)}%` : '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-gray-500 text-sm">No experiment summary loaded yet.</p>
        )}
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Churn Configuration</h3>
        <div className="flex items-center gap-3">
          <label className="text-sm text-gray-300">Inactive days threshold for "already churned"</label>
          <input
            type="number"
            min={1}
            className="w-28 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2"
            value={churnInactiveDays}
            onChange={(e) => setChurnInactiveDays(Number(e.target.value))}
          />
          <button className="bg-indigo-600 hover:bg-indigo-500 rounded-lg px-4 py-2 text-sm" onClick={saveChurnConfig} disabled={loading}>
            Save
          </button>
        </div>
      </section>

      <section className="bg-gray-900 border border-gray-800 rounded-2xl p-6 space-y-4">
        <h3 className="text-lg font-semibold">Predict Churn</h3>
        <div className="flex gap-3">
          <select
            className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 flex-1"
            value={selectedJob}
            onChange={(e) => setSelectedJob(e.target.value)}
          >
            <option value="">Select Ready Import Job</option>
            {readyJobs.map((job) => (
              <option key={job.name} value={job.name}>
                {job.name}
              </option>
            ))}
          </select>
          <button className="bg-indigo-600 hover:bg-indigo-500 rounded-lg px-4 py-2 text-sm" onClick={runPredict} disabled={loading}>
            Run Prediction
          </button>
        </div>
        <div className="max-h-72 overflow-auto border border-gray-800 rounded-lg">
          <table className="w-full text-left text-sm">
            <thead className="bg-gray-800">
              <tr>
                <th className="px-3 py-2">User ID</th>
                <th className="px-3 py-2">State</th>
                <th className="px-3 py-2">Inactive Days</th>
                <th className="px-3 py-2">LTV</th>
                <th className="px-3 py-2">Sessions</th>
                <th className="px-3 py-2">Events</th>
                <th className="px-3 py-2">Risk</th>
                <th className="px-3 py-2">Reason</th>
                <th className="px-3 py-2">Suggested Action</th>
              </tr>
            </thead>
            <tbody>
              {predictions.map((row, idx) => (
                <tr key={`${row.user_id}-${idx}`} className="border-t border-gray-800 align-top">
                  <td className="px-3 py-2">{row.user_id}</td>
                  <td className="px-3 py-2">{row.churn_state || '-'}</td>
                  <td className="px-3 py-2">{row.days_since_last_seen ?? '-'}</td>
                  <td className="px-3 py-2">{row.ltv}</td>
                  <td className="px-3 py-2">{row.session_count}</td>
                  <td className="px-3 py-2">{row.event_count}</td>
                  <td className="px-3 py-2">{row.predicted_churn_risk}</td>
                  <td className="px-3 py-2 text-xs text-gray-300">
                    <div>{row.churn_reason}</div>
                    {row.top_signals?.length ? (
                      <div className="text-[11px] text-gray-400 mt-1">
                        {row.top_signals.map((s) => `${s.signal}:${String(s.value)}`).join(' · ')}
                      </div>
                    ) : null}
                  </td>
                  <td className="px-3 py-2">{row.suggested_action}</td>
                </tr>
              ))}
              {predictions.length === 0 ? (
                <tr>
                  <td className="px-3 py-2 text-gray-500" colSpan={9}>
                    No predictions yet.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
};

export default BackendWorkbench;

