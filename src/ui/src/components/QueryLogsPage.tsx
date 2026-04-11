import { useState, useRef } from "react";
import { useFetch, apiDelete, apiPost } from "../hooks/useApi.js";
import type { LogEntry, LogStats, ConnectorInfo, SQLExecuteResult } from "../types.js";

type RerunResult = SQLExecuteResult;

export function QueryLogsPage() {
  const [connector, setConnector] = useState("");
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(0);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [rerunning, setRerunning] = useState<string | null>(null);
  const [rerunResult, setRerunResult] = useState<{ entryId: string; sql: string; connector: string; data?: RerunResult; error?: string } | null>(null);
  const [promptLoading, setPromptLoading] = useState<string | null>(null);
  const [promptData, setPromptData] = useState<{ entryId: string; systemPrompt: string; userMessage: string } | null>(null);
  const [explanationData, setExplanationData] = useState<{ entryId: string; explanation: string } | null>(null);
  const rerunRef = useRef<HTMLDivElement>(null);
  const promptRef = useRef<HTMLDivElement>(null);
  const explanationRef = useRef<HTMLDivElement>(null);
  const pageSize = 10;

  const params = new URLSearchParams();
  if (connector) params.set("connector", connector);
  if (status) params.set("status", status);
  params.set("page", String(page));
  params.set("pageSize", String(pageSize));

  const { data: logs, refetch } = useFetch<{ rows: LogEntry[]; total: number }>(`/api/logs?${params}`, [connector, status, page]);
  const { data: stats, refetch: refetchStats } = useFetch<LogStats>("/api/logs/stats");
  const { data: connectors } = useFetch<ConnectorInfo[]>("/api/connectors");

  const handleClearOld = async () => {
    if (!confirm("Clear all query logs older than 3 days?")) return;
    await apiDelete("/api/logs/older-than/3");
    refetch();
    refetchStats();
    setRerunResult(null);
    setPromptData(null);
    setExplanationData(null);
  };

  const handleClear = async () => {
    if (!confirm("Clear ALL query logs? This cannot be undone.")) return;
    await apiDelete("/api/logs");
    refetch();
    refetchStats();
    setRerunResult(null);
    setPromptData(null);
    setExplanationData(null);
  };

  const handleRerun = async (entry: LogEntry) => {
    if (!entry.sql) return;
    setRerunning(entry.id);
    setRerunResult(null);
    try {
      const data = await apiPost<RerunResult>("/api/execute-sql", {
        sql: entry.sql,
        connector: entry.connector === "default" ? undefined : entry.connector,
        maxRows: 100,
      });
      setRerunResult({ entryId: entry.id, sql: entry.sql, connector: entry.connector, data });
      setTimeout(() => rerunRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
    } catch (err) {
      setRerunResult({ entryId: entry.id, sql: entry.sql, connector: entry.connector, error: err instanceof Error ? err.message : String(err) });
      setTimeout(() => rerunRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
    } finally {
      setRerunning(null);
    }
  };

  const handlePrompt = async (entry: LogEntry) => {
    setPromptLoading(entry.id);
    setPromptData(null);
    try {
      const connId = entry.connector === "default" ? undefined : entry.connector;
      const data = await apiPost<{ systemPrompt: string; userMessage: string }>(
        `/api/connectors/${connId}/prompt`,
        { question: entry.question ?? entry.sql ?? "" },
      );
      setPromptData({ entryId: entry.id, systemPrompt: data.systemPrompt, userMessage: entry.question ?? entry.sql ?? "" });
      setTimeout(() => promptRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
    } catch (err) {
      setPromptData({ entryId: entry.id, systemPrompt: `Error: ${err instanceof Error ? err.message : String(err)}`, userMessage: "" });
    } finally {
      setPromptLoading(null);
    }
  };

  const totalPages = logs ? Math.ceil(logs.total / pageSize) : 0;

  return (
    <div>
      <div className="page-header">
        <h2>Query Logs</h2>
        <div style={{ display: "flex", gap: 8 }}>
          <button className="btn danger" onClick={handleClearOld}>Clear Logs &gt; 3 Days</button>
          <button className="btn danger" onClick={handleClear}>Clear All Logs</button>
        </div>
      </div>

      {stats && (
        <div className="stats-bar">
          <span>Total: <strong>{stats.totalQueries}</strong></span>
          <span>Success: <strong className="green-text">{stats.successful}</strong></span>
          <span>Failed: <strong className="red-text">{stats.failed}</strong></span>
          <span>Avg Time: <strong>{stats.avgExecutionTimeMs}ms</strong></span>
        </div>
      )}

      <div className="filters">
        <select value={connector} onChange={(e) => { setConnector(e.target.value); setPage(0); }}>
          <option value="">All Connectors</option>
          {connectors?.map((c) => <option key={c.id} value={c.id}>{c.id}</option>)}
        </select>
        <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(0); }}>
          <option value="">All Status</option>
          <option value="success">Success</option>
          <option value="fail">Failed</option>
        </select>
      </div>

      <table className="table">
        <thead>
          <tr>
            <th>Time</th><th>Tool</th><th>Connector</th><th>Query</th><th>Status</th><th>Re-run</th><th>Time (ms)</th><th>Rows</th>
          </tr>
        </thead>
        {logs?.rows.map((entry) => (
          <tbody key={entry.id}>
            <tr className="clickable" title="Click on the row to see the SQL" onClick={() => setExpandedId(expandedId === entry.id ? null : entry.id)}>
              <td>{new Date(entry.timestamp).toLocaleString()}</td>
              <td>{entry.tool}</td>
              <td>{entry.connector}</td>
              <td className="truncate">{entry.question || entry.sql || "-"}</td>
              <td><span className={`badge ${entry.success ? "green" : "red"}`}>{entry.success ? "OK" : "FAIL"}</span></td>
              <td>
                {entry.sql && (
                  <button
                    className="rerun-btn"
                    title={`Re-run SQL on ${entry.connector}`}
                    disabled={rerunning === entry.id}
                    onClick={(e) => { e.stopPropagation(); handleRerun(entry); }}
                  >
                    {rerunning === entry.id ? "..." : "\u25B6"}
                  </button>
                )}
              </td>
              <td>{entry.executionTimeMs}</td>
              <td>{entry.rowCount ?? "-"}</td>
            </tr>
            {expandedId === entry.id && (
              <tr className="detail-row">
                <td colSpan={8}>
                  {entry.question && <div><strong>Question:</strong> {entry.question}</div>}
                  {entry.sql && <div><strong>SQL:</strong> <code>{entry.sql}</code></div>}
                  {entry.error && <div className="error-msg"><strong>Error:</strong> {entry.error}</div>}
                  <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
                    <button
                      className="prompt-reconstruct-btn"
                      disabled={promptLoading === entry.id}
                      onClick={(e) => { e.stopPropagation(); handlePrompt(entry); }}
                    >
                      {promptLoading === entry.id ? "Loading..." : "Reconstruct AI Prompt"}
                    </button>
                    {entry.explanation && (
                      <button
                        className="prompt-reconstruct-btn"
                        onClick={(e) => {
                          e.stopPropagation();
                          setExplanationData({ entryId: entry.id, explanation: entry.explanation! });
                          setTimeout(() => explanationRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
                        }}
                      >
                        Level 2 AI Explanation
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            )}
          </tbody>
        ))}
        {logs?.rows.length === 0 && (
          <tbody>
            <tr><td colSpan={8} className="no-logs-cell">No logs found</td></tr>
          </tbody>
        )}
      </table>

      {totalPages > 1 && (
        <div className="pagination">
          <button disabled={page === 0} onClick={() => setPage(0)}>&laquo;</button>
          <button disabled={page === 0} onClick={() => setPage(page - 1)}>Prev</button>
          <span>Page {page + 1} of {totalPages}</span>
          <button disabled={page >= totalPages - 1} onClick={() => setPage(page + 1)}>Next</button>
          <button disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)}>&raquo;</button>
        </div>
      )}

      {/* Re-run result section */}
      {rerunResult && (
        <div className="rerun-result" ref={rerunRef}>
          <div className="rerun-header">
            <h3>Re-run Result</h3>
            <button onClick={() => setRerunResult(null)}>Close</button>
          </div>
          <div className="rerun-meta">
            Connector: <strong>{rerunResult.connector}</strong> | SQL: <code>{rerunResult.sql}</code>
          </div>

          {rerunResult.error && (
            <div className="error-msg">{rerunResult.error}</div>
          )}

          {rerunResult.data && (
            <>
              <div className={`result-status success`}>
                {rerunResult.data.rowCount} row(s) in {rerunResult.data.executionTimeMs}ms
                {rerunResult.data.truncated && " (truncated)"}
              </div>
              {rerunResult.data.rows.length > 0 && (
                <div className="result-table-wrapper">
                  <table className="table result-table">
                    <thead>
                      <tr>
                        {Object.keys(rerunResult.data.rows[0]).map((col) => (
                          <th key={col}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rerunResult.data.rows.map((row, i) => (
                        <tr key={i}>
                          {Object.values(row).map((val, j) => (
                            <td key={j}>{val === null ? <span className="null-val">NULL</span> : String(val)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </div>
      )}
      {/* Reconstructed AI Prompt section */}
      {promptData && (
        <div className="rerun-result" ref={promptRef}>
          <div className="rerun-header">
            <h3>Reconstructed AI Prompt</h3>
            <button onClick={() => setPromptData(null)}>Close</button>
          </div>
          <div style={{ marginBottom: 12 }}>
            <strong>System Prompt:</strong>
            <div className="prompt-wrapper">
              <button
                className="prompt-copy-btn"
                title="Copy System Prompt"
                onClick={() => { navigator.clipboard.writeText(promptData.systemPrompt).catch(() => {}); }}
              >
                <svg width="14" height="14" fill="none" stroke="#fff" viewBox="0 0 24 24">
                  <rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} />
                  <path strokeWidth={2} d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                </svg>
              </button>
              <pre className="prompt-display">{promptData.systemPrompt}</pre>
            </div>
          </div>
          {promptData.userMessage && (
            <div>
              <strong>User Message:</strong>
              <pre className="prompt-display">{promptData.userMessage}</pre>
            </div>
          )}
        </div>
      )}
      {/* Level 2 AI Explanation section */}
      {explanationData && (
        <div className="rerun-result" ref={explanationRef}>
          <div className="rerun-header">
            <h3>Level 2 AI Explanation</h3>
            <button onClick={() => setExplanationData(null)}>Close</button>
          </div>
          <div style={{ marginBottom: 12 }}>
            <div className="prompt-wrapper">
              <button
                className="prompt-copy-btn"
                title="Copy Explanation"
                onClick={() => { navigator.clipboard.writeText(explanationData.explanation).catch(() => {}); }}
              >
                <svg width="14" height="14" fill="none" stroke="#fff" viewBox="0 0 24 24">
                  <rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} />
                  <path strokeWidth={2} d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                </svg>
              </button>
              <pre className="prompt-display">{explanationData.explanation}</pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
