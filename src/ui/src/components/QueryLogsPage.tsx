import { useState, useRef } from "react";
import { useFetch, apiDelete, apiPost } from "../hooks/useApi.js";
import type { LogEntry, LogStats, ConnectorInfo, SQLExecuteResult, Workflow } from "../types.js";

type RerunResult = SQLExecuteResult;

async function downloadPdf(markdown: string) {
  const res = await fetch("/api/render-pdf", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ markdown }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    alert((err as { error?: string }).error || "PDF generation failed");
    return;
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "explanation.pdf";
  a.click();
  URL.revokeObjectURL(url);
}

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
  const [toolCallsData, setToolCallsData] = useState<{ entryId: string; toolCalls: Array<{ turn: number; tool: string; input: Record<string, unknown>; output: string; durationMs: number }> } | null>(null);
  const [answerData, setAnswerData] = useState<{ entryId: string; answer: string } | null>(null);
  const [pdfBusy, setPdfBusy] = useState(false);
  const [saveWfEntry, setSaveWfEntry] = useState<LogEntry | null>(null);
  const [saveWfName, setSaveWfName] = useState("");
  const [saveWfDesc, setSaveWfDesc] = useState("");
  const [saveWfBusy, setSaveWfBusy] = useState(false);
  const rerunRef = useRef<HTMLDivElement>(null);
  const promptRef = useRef<HTMLDivElement>(null);
  const explanationRef = useRef<HTMLDivElement>(null);
  const toolCallsRef = useRef<HTMLDivElement>(null);
  const answerRef = useRef<HTMLDivElement>(null);
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
    setToolCallsData(null);
    setAnswerData(null);
  };

  const handleClear = async () => {
    if (!confirm("Clear ALL query logs? This cannot be undone.")) return;
    await apiDelete("/api/logs");
    refetch();
    refetchStats();
    setRerunResult(null);
    setPromptData(null);
    setExplanationData(null);
    setToolCallsData(null);
    setAnswerData(null);
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
                    {(entry.explanation || (entry.toolCalls && entry.toolCalls.length > 0)) && (
                      <button
                        className="prompt-reconstruct-btn"
                        onClick={(e) => {
                          e.stopPropagation();
                          setExplanationData({ entryId: entry.id, explanation: entry.explanation ?? "" });
                          setToolCallsData({ entryId: entry.id, toolCalls: entry.toolCalls ?? [] });
                          setTimeout(() => explanationRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
                        }}
                      >
                        Level1 &amp; Level2 AI Interactions
                      </button>
                    )}
                    {entry.answer && (
                      <button
                        className="prompt-reconstruct-btn"
                        onClick={(e) => {
                          e.stopPropagation();
                          setAnswerData({ entryId: entry.id, answer: entry.answer! });
                          setTimeout(() => answerRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
                        }}
                      >
                        Final Output
                      </button>
                    )}
                    <button
                      className="prompt-reconstruct-btn"
                      disabled={promptLoading === entry.id}
                      onClick={(e) => { e.stopPropagation(); handlePrompt(entry); }}
                    >
                      {promptLoading === entry.id ? "Loading..." : "Level 1 AI Prompt"}
                    </button>
                    {entry.toolCalls && entry.toolCalls.some((tc) => tc.sql && tc.sqlSuccess) && (
                      <button
                        className="prompt-reconstruct-btn"
                        style={{ background: "#059669" }}
                        onClick={(e) => {
                          e.stopPropagation();
                          setSaveWfEntry(entry);
                          setSaveWfName("");
                          setSaveWfDesc("");
                        }}
                      >
                        Save as Workflow
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
      {/* Level1 & Level2 AI Interactions (merged view) */}
      {explanationData && toolCallsData && (
        <div className="rerun-result" ref={explanationRef}>
          <div className="rerun-header">
            <h3>Level1 &amp; Level2 AI Interactions</h3>
            <button onClick={() => { setExplanationData(null); setToolCallsData(null); }}>Close</button>
          </div>
          {(() => {
            // Parse explanation into per-turn reasoning blocks
            const turnReasonings = new Map<number, string>();
            if (explanationData.explanation) {
              const parts = explanationData.explanation.split(/\*\*Turn \d+\*\*:\s*/);
              const turnNums = [...explanationData.explanation.matchAll(/\*\*Turn (\d+)\*\*/g)];
              for (let idx = 0; idx < turnNums.length; idx++) {
                const turnNum = parseInt(turnNums[idx][1], 10) - 1; // 0-based
                let text = (parts[idx + 1] ?? "").trim();
                // Remove the tool call line (-> ask_sql: ... ) from reasoning text
                text = text.replace(/\n\s*->.*$/s, "").trim();
                if (text) turnReasonings.set(turnNum, text);
              }
            }

            return toolCallsData.toolCalls.map((tc, i) => (
              <div key={i} style={{ marginBottom: 16, padding: "10px 14px", borderLeft: "3px solid #60a5fa", background: "rgba(96,165,250,0.05)" }}>
                <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 6 }}>
                  <span className="badge green" style={{ fontSize: 11 }}>Turn {tc.turn + 1}</span>
                  <strong>{tc.tool}</strong>
                  <span style={{ color: "#94a3b8", fontSize: 12 }}>{tc.durationMs}ms</span>
                </div>
                {turnReasonings.has(tc.turn) && (
                  <div style={{ margin: "6px 0", fontSize: 12, color: "#1a1a1a" }}>
                    <strong>Level2 AI Reasoning:</strong> {turnReasonings.get(tc.turn)}
                  </div>
                )}
                {tc.tool === "list_tables" && (
                  <div style={{ margin: "6px 0", fontSize: 12, color: "#1a1a1a" }}>
                    <strong>Internal Function Call:</strong> returning the connector schema definition to Level 2 AI.
                  </div>
                )}
                {tc.tool === "ask_sql" && tc.input.question && (
                  <div style={{ margin: "6px 0", fontSize: 12, color: "#1a1a1a" }}>
                    <strong>Level2 AI to Level1 AI question:</strong> {String(tc.input.question)}
                  </div>
                )}
                {tc.tool === "query" && tc.input.sql && (
                  <div style={{ margin: "6px 0", fontSize: 12, color: "#1a1a1a" }}>
                    <strong>Level2 AI direct SQL:</strong> <code style={{ fontSize: 11 }}>{String(tc.input.sql)}</code>
                  </div>
                )}
                {tc.sql && (
                  <div style={{ margin: "6px 0", fontSize: 12, color: "#1a1a1a" }}>
                    <strong>Level1 AI SQL:</strong> <code style={{ fontSize: 11 }}>{tc.sql}</code>
                  </div>
                )}
                {tc.sqlSuccess !== undefined && (
                  <div style={{ margin: "6px 0", fontSize: 12, color: "#1a1a1a" }}>
                    <strong>SQL Execution Status:</strong> {tc.sqlSuccess ? "Success" : "Failed"}
                  </div>
                )}
              </div>
            ));
          })()}
        </div>
      )}
      {/* Final Output section */}
      {answerData && (
        <div className="rerun-result" ref={answerRef}>
          <div className="rerun-header">
            <h3>Final Output</h3>
            <button onClick={() => setAnswerData(null)}>Close</button>
          </div>
          <div style={{ marginBottom: 12 }}>
            <div className="prompt-wrapper">
              <div style={{ display: "flex", gap: 4, position: "absolute", top: 8, right: 12 }}>
                <button
                  className="prompt-copy-btn"
                  title="Copy Final Output"
                  onClick={() => { navigator.clipboard.writeText(answerData.answer).catch(() => {}); }}
                >
                  <svg width="14" height="14" fill="none" stroke="#fff" viewBox="0 0 24 24">
                    <rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} />
                    <path strokeWidth={2} d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                  </svg>
                </button>
                <button
                  className="prompt-copy-btn"
                  title="Download as PDF"
                  disabled={pdfBusy}
                  onClick={async () => {
                    setPdfBusy(true);
                    try { await downloadPdf(answerData.answer); } finally { setPdfBusy(false); }
                  }}
                >
                  {pdfBusy ? (
                    <span style={{ fontSize: 11, color: "#fff" }}>...</span>
                  ) : (
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/>
                      <polyline points="14 2 14 8 20 8"/>
                      <line x1="16" y1="13" x2="8" y2="13"/>
                      <line x1="16" y1="17" x2="8" y2="17"/>
                      <line x1="10" y1="9" x2="8" y2="9"/>
                    </svg>
                  )}
                </button>
              </div>
              <pre className="prompt-display">{answerData.answer}</pre>
            </div>
          </div>
        </div>
      )}

      {/* Save as Workflow modal */}
      {saveWfEntry && (() => {
        const entry = saveWfEntry;
        const steps = (entry.toolCalls ?? [])
          .filter((tc) => tc.sql && tc.sqlSuccess)
          .map((tc, i) => ({
            order: i + 1,
            title: tc.tool === "ask_sql" ? String(tc.input.question ?? "") : tc.tool === "query" ? `Direct SQL query` : tc.tool,
            sql: tc.sql!,
          }));

        const handleSave = async () => {
          if (!saveWfName.trim()) { alert("Please enter a workflow name."); return; }
          setSaveWfBusy(true);
          try {
            await apiPost<Workflow>("/api/workflows", {
              name: saveWfName.trim(),
              description: saveWfDesc.trim() || undefined,
              connector: entry.connector,
              originalQuestion: entry.question ?? "",
              aiReasoning: entry.explanation ?? undefined,
              steps,
            });
            alert("Workflow saved! Go to the Workflows page to view and run it.");
            setSaveWfEntry(null);
          } catch (err) {
            alert(err instanceof Error ? err.message : String(err));
          } finally {
            setSaveWfBusy(false);
          }
        };

        return (
          <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
            <div style={{ background: "#fff", borderRadius: 8, padding: 24, width: 560, maxHeight: "80vh", overflow: "auto", boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}>
              <h3 style={{ margin: "0 0 16px", color: "#0f3460" }}>Save as Workflow</h3>

              <div style={{ marginBottom: 12 }}>
                <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Name *</label>
                <input
                  type="text"
                  value={saveWfName}
                  onChange={(e) => setSaveWfName(e.target.value)}
                  placeholder="e.g., TPCH Business Insights Report"
                  style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
                  autoFocus
                />
              </div>

              <div style={{ marginBottom: 12 }}>
                <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Description (optional)</label>
                <input
                  type="text"
                  value={saveWfDesc}
                  onChange={(e) => setSaveWfDesc(e.target.value)}
                  placeholder="Brief description of this workflow"
                  style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
                />
              </div>

              <div style={{ marginBottom: 12, fontSize: 13 }}>
                <strong>Connector:</strong> {entry.connector}
              </div>
              <div style={{ marginBottom: 12, fontSize: 13 }}>
                <strong>Original Question:</strong> {entry.question || "-"}
              </div>

              <div style={{ marginBottom: 12 }}>
                <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 6 }}>Steps ({steps.length}):</div>
                {steps.map((s) => (
                  <div key={s.order} style={{ marginBottom: 6, padding: "6px 10px", background: "#f8fafc", borderRadius: 4, border: "1px solid #e2e8f0" }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: "#334155" }}>Step {s.order}: {s.title}</div>
                    <code style={{ fontSize: 11, color: "#475569", wordBreak: "break-all" }}>{s.sql}</code>
                  </div>
                ))}
              </div>

              <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                <button className="btn" style={{ background: "#6b7280" }} onClick={() => setSaveWfEntry(null)}>Cancel</button>
                <button className="btn" style={{ background: "#059669" }} disabled={saveWfBusy} onClick={handleSave}>
                  {saveWfBusy ? "Saving..." : "Save Workflow"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
