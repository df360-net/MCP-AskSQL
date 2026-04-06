import { useState, useEffect } from "react";
import { useFetch, apiPut, apiPost } from "../hooks/useApi.js";
import type { AISettings } from "../types.js";

export function AIProviderPage() {
  const { data: settings, loading, refetch } = useFetch<AISettings>("/api/ai");
  const [form, setForm] = useState({ baseUrl: "", model: "", maxTokens: "", temperature: "0.3", apiKey: "" });
  const [initialized, setInitialized] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testResult, setTestResult] = useState<{ reachable: boolean; error?: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Initialize form from settings on first load
  useEffect(() => {
    if (settings && !initialized) {
      setForm({
        baseUrl: settings.baseUrl,
        model: settings.model,
        maxTokens: String(settings.maxTokens ?? ""),
        temperature: String(settings.temperature ?? "0.3"),
        apiKey: "",
      });
      setInitialized(true);
    }
  }, [settings, initialized]);

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const body: Record<string, unknown> = {
        baseUrl: form.baseUrl,
        model: form.model,
      };
      if (form.maxTokens) body.maxTokens = parseInt(form.maxTokens, 10);
      if (form.temperature) body.temperature = parseFloat(form.temperature);
      if (form.apiKey) body.apiKey = form.apiKey;
      await apiPut("/api/ai", body);
      refetch();
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    setTestResult(null);
    try {
      const r = await apiPost<{ reachable: boolean; error?: string }>("/api/ai/test");
      setTestResult(r);
    } catch (err) {
      setTestResult({ reachable: false, error: err instanceof Error ? err.message : String(err) });
    }
  };

  if (loading || !settings) return <p>Loading...</p>;

  return (
    <div className="ai-page">
      <div className="ai-header">
        <h2>AI Provider Configuration</h2>
        <p className="ai-subtitle">Configure your AI model settings</p>
      </div>

      {error && <div className="error-msg">{error}</div>}

      <div className="ai-card">
        <div className="ai-field">
          <label className="ai-label">Base URL</label>
          <input
            className="ai-input"
            value={form.baseUrl}
            onChange={(e) => setForm({ ...form, baseUrl: e.target.value })}
            placeholder="https://api.openai.com/v1"
          />
        </div>

        <div className="ai-field">
          <label className="ai-label">API Key</label>
          <input
            className="ai-input"
            type="password"
            value={form.apiKey}
            onChange={(e) => setForm({ ...form, apiKey: e.target.value })}
            placeholder={settings.apiKeyMasked}
          />
          <p className="ai-hint">Leave blank to keep current key</p>
        </div>

        <div className="ai-field">
          <label className="ai-label">Model</label>
          <input
            className="ai-input"
            value={form.model}
            onChange={(e) => setForm({ ...form, model: e.target.value })}
            placeholder="deepseek-chat"
          />
        </div>

        <div className="ai-field">
          <label className="ai-label">Max Tokens</label>
          <input
            className="ai-input"
            type="number"
            value={form.maxTokens}
            onChange={(e) => setForm({ ...form, maxTokens: e.target.value })}
            placeholder="4096"
          />
        </div>

        <div className="ai-field">
          <label className="ai-label">
            Temperature: <span className="ai-temp-value">{form.temperature}</span>
          </label>
          <input
            className="ai-slider"
            type="range"
            min="0"
            max="2"
            step="0.1"
            value={form.temperature}
            onChange={(e) => setForm({ ...form, temperature: e.target.value })}
          />
          <div className="ai-slider-labels">
            <span>Precise</span>
            <span>Creative</span>
          </div>
        </div>

        <div className="ai-actions">
          <button className="conn-btn" onClick={handleTest}>
            Test Connection
          </button>
          {testResult && (
            <span className={`conn-status ${testResult.reachable ? "connected" : "disconnected"}`}>
              <span className="conn-status-dot" />
              {testResult.reachable ? "Reachable" : `Failed`}
            </span>
          )}
          <button className="ai-save-btn" onClick={handleSave} disabled={saving}>
            {saving ? "Saving..." : "Save Configuration"}
          </button>
        </div>
      </div>
    </div>
  );
}
