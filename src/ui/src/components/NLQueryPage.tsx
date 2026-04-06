import { useState } from "react";
import { useFetch } from "../hooks/useApi.js";
import type { ConnectorInfo, AskResult } from "../types.js";

export function NLQueryPage() {
  const [question, setQuestion] = useState("");
  const [connector, setConnector] = useState("");
  const [maxRows, setMaxRows] = useState(100);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AskResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<Array<{ id: string; question: string; connector: string; timestamp: string }>>([]);

  const { data: connectors } = useFetch<ConnectorInfo[]>("/api/connectors");

  const handleAsk = async () => {
    if (!question.trim()) return;
    setLoading(true);
    setResult(null);
    setError(null);

    try {
      const body: Record<string, unknown> = { question, maxRows };
      if (connector) body.connector = connector;

      const res = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);

      setResult(data as AskResult);
      setHistory((h) => [{ id: crypto.randomUUID(), question, connector: data.routedTo ?? connector ?? "default", timestamp: new Date().toISOString() }, ...h.slice(0, 19)]);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleAsk();
    }
  };

  return (
    <div className="nlq-page">
      <div className="nlq-header">
        <h2>Natural Language Query</h2>
        <p className="nlq-subtitle">Ask questions about your data in plain English</p>
      </div>

      <div className="nlq-input-card">
        <label className="nlq-label">Enter your question</label>
        <textarea
          className="nlq-textarea"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="e.g., Show me all users who signed up last month..."
          rows={2}
        />

        <div className="nlq-controls">
          <div className="nlq-controls-left">
            <select value={connector} onChange={(e) => setConnector(e.target.value)}>
              <option value="">Select Database</option>
              {connectors?.map((c) => (
                <option key={c.id} value={c.id}>{c.id} ({c.type})</option>
              ))}
            </select>
            <label className="nlq-maxrows">
              Max rows:
              <input type="number" value={maxRows} onChange={(e) => setMaxRows(parseInt(e.target.value, 10) || 100)} min={1} max={5000} />
            </label>
          </div>
          <button className="nlq-run-btn" onClick={handleAsk} disabled={loading || !question.trim()}>
            {loading ? "Running..." : "Run Query"}
          </button>
        </div>
      </div>

      {error && <div className="error-msg">{error}</div>}

      {!result && !error && (
        <div className="nlq-empty">
          <svg width="48" height="48" fill="none" stroke="#94a3b8" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          <p className="nlq-empty-title">No results yet</p>
          <p className="nlq-empty-sub">Run a query to see results here</p>
        </div>
      )}

      {result && (
        <div className="query-result">
          {result.routedTo && (
            <div className="route-info">
              Routed to <strong>{result.routedTo}</strong>
              <span className="badge">{result.routeMethod}</span>
              <span className="route-confidence">{result.routeConfidence}</span>
            </div>
          )}

          <div className={`result-status ${result.success ? "success" : "fail"}`}>
            {result.success ? "Success" : "Failed"}
            {result.success && <> &mdash; {result.rowCount} row(s) in {result.executionTimeMs}ms</>}
            {result.error && <> &mdash; {result.error}</>}
          </div>

          {result.sql && (
            <div className="result-sql">
              <div className="result-sql-header">
                <h4>Generated SQL</h4>
                <button
                  className="copy-btn"
                  title="Copy SQL"
                  onClick={() => { navigator.clipboard.writeText(result.sql!).catch(() => {}); }}
                >
                  <svg width="11" height="11" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} />
                    <path strokeWidth={2} d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                  </svg>
                </button>
              </div>
              <pre><code>{result.sql}</code></pre>
            </div>
          )}

          {result.explanation && (
            <div className="result-explanation">
              <h4>Explanation</h4>
              <p>{result.explanation}</p>
            </div>
          )}

          {result.rows.length > 0 && (
            <div className="result-data">
              <h4>Results ({result.rowCount} rows)</h4>
              <div className="result-table-wrapper">
                <table className="table result-table">
                  <thead>
                    <tr>
                      {Object.keys(result.rows[0]).map((col) => (
                        <th key={col}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, i) => (
                      <tr key={i}>
                        {Object.values(row).map((val, j) => (
                          <td key={j}>{val === null ? <span className="null-val">NULL</span> : String(val)}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {history.length > 0 && (
        <div className="query-history">
          <h4>Recent Questions</h4>
          {history.map((h) => (
            <div key={h.id} className="history-item" onClick={() => setQuestion(h.question)}>
              <span className="history-question">{h.question}</span>
              <span className="history-meta">{h.connector} &middot; {new Date(h.timestamp).toLocaleTimeString()}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
