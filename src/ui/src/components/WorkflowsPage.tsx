import { useState, useRef } from "react";
import { useFetch, apiDelete } from "../hooks/useApi.js";
import type { Workflow, WorkflowRunResult, ConnectorInfo } from "../types.js";

interface StepProgress {
  order: number;
  title: string;
  status: "pending" | "running" | "done";
  success?: boolean;
  error?: string;
  rowCount?: number;
  executionTimeMs?: number;
}

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
  a.download = "workflow-report.pdf";
  a.click();
  URL.revokeObjectURL(url);
}

export function WorkflowsPage() {
  const { data: workflows, refetch } = useFetch<Workflow[]>("/api/workflows");
  const { data: connectors } = useFetch<ConnectorInfo[]>("/api/connectors");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [running, setRunning] = useState<string | null>(null);
  const [stepProgress, setStepProgress] = useState<StepProgress[]>([]);
  const [summarizing, setSummarizing] = useState(false);
  const [runResult, setRunResult] = useState<WorkflowRunResult | null>(null);
  const [pdfBusy, setPdfBusy] = useState(false);
  const resultRef = useRef<HTMLDivElement>(null);

  // Group workflows by connector
  const grouped = new Map<string, Workflow[]>();
  if (workflows) {
    for (const wf of workflows) {
      const list = grouped.get(wf.connector) || [];
      list.push(wf);
      grouped.set(wf.connector, list);
    }
  }
  // Add empty connectors
  if (connectors) {
    for (const c of connectors) {
      if (!grouped.has(c.id)) grouped.set(c.id, []);
    }
  }

  const handleRun = async (wf: Workflow) => {
    setRunning(wf.id);
    setRunResult(null);
    setSummarizing(false);

    // Initialize progress for all steps
    setStepProgress(wf.steps.map((s) => ({ order: s.order, title: s.title, status: "pending" })));

    try {
      const response = await fetch(`/api/workflows/${wf.id}/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ summarize: true }),
      });

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event = JSON.parse(line.slice(6));

            if (event.type === "step-start") {
              setStepProgress((prev) => prev.map((s) =>
                s.order === event.order ? { ...s, status: "running" } : s
              ));
            } else if (event.type === "step-done") {
              setStepProgress((prev) => prev.map((s) =>
                s.order === event.order ? {
                  ...s,
                  status: "done",
                  success: event.success,
                  error: event.error,
                  rowCount: event.rowCount,
                  executionTimeMs: event.executionTimeMs,
                } : s
              ));
            } else if (event.type === "summarizing") {
              setSummarizing(true);
            } else if (event.type === "result") {
              setSummarizing(false);
              setRunResult(event as WorkflowRunResult);
              setTimeout(() => resultRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
            } else if (event.type === "error") {
              alert(event.error);
            }
          } catch { /* ignore parse errors */ }
        }
      }
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(null);
    }
  };

  const handleDelete = async (wf: Workflow) => {
    if (!confirm(`Delete workflow "${wf.name}"? This cannot be undone.`)) return;
    try {
      await apiDelete(`/api/workflows/${wf.id}`);
      refetch();
      if (expandedId === wf.id) setExpandedId(null);
      if (runResult?.workflowId === wf.id) setRunResult(null);
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <div>
      <div className="page-header">
        <h2>Workflows</h2>
      </div>

      {!workflows && <div style={{ padding: 20, color: "#94a3b8" }}>Loading...</div>}

      {workflows && workflows.length === 0 && (
        <div style={{ padding: 40, textAlign: "center", color: "#94a3b8" }}>
          <p style={{ fontSize: 16, marginBottom: 8 }}>No workflows saved yet.</p>
          <p style={{ fontSize: 13 }}>Go to <strong>Query Logs</strong> and click <strong>"Save as Workflow"</strong> on an agent query to create one.</p>
        </div>
      )}

      {[...grouped.entries()].map(([connector, wfs]) => (
        <div key={connector} style={{ marginBottom: 24 }}>
          <div style={{
            padding: "8px 14px",
            background: "#16213e",
            color: "#fff",
            borderRadius: "6px 6px 0 0",
            fontSize: 14,
            fontWeight: 600,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}>
            <span>{connector}</span>
            <span style={{ fontSize: 12, fontWeight: 400, color: "#94a3b8" }}>
              {wfs.length} workflow{wfs.length !== 1 ? "s" : ""}
            </span>
          </div>

          {wfs.length === 0 ? (
            <div style={{ padding: "14px 16px", background: "#f8fafc", borderRadius: "0 0 6px 6px", border: "1px solid #e2e8f0", borderTop: "none", color: "#94a3b8", fontSize: 13 }}>
              No workflows saved for this connector.
            </div>
          ) : (
            <div style={{ border: "1px solid #e2e8f0", borderTop: "none", borderRadius: "0 0 6px 6px", overflow: "hidden" }}>
              {wfs.map((wf) => (
                <div key={wf.id}>
                  {/* Workflow card */}
                  <div
                    style={{
                      padding: "12px 16px",
                      borderBottom: "1px solid #f1f5f9",
                      cursor: "pointer",
                      background: expandedId === wf.id ? "#f0f9ff" : "#fff",
                    }}
                    onClick={() => setExpandedId(expandedId === wf.id ? null : wf.id)}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontWeight: 600, fontSize: 14, color: "#0f3460", marginBottom: 4 }}>
                          {wf.name}
                        </div>
                        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 2 }}>
                          {wf.originalQuestion || "(no original question)"}
                        </div>
                        <div style={{ fontSize: 11, color: "#94a3b8" }}>
                          {wf.steps.length} step{wf.steps.length !== 1 ? "s" : ""} | Created: {new Date(wf.createdAt).toLocaleDateString()}
                        </div>
                      </div>
                      <div style={{ display: "flex", gap: 6 }} onClick={(e) => e.stopPropagation()}>
                        <button
                          className="btn"
                          style={{ fontSize: 12, padding: "4px 12px", background: "linear-gradient(135deg, #3b82f6, #2563eb)", color: "#fff" }}
                          disabled={running === wf.id}
                          onClick={() => handleRun(wf)}
                        >
                          {running === wf.id ? "Running..." : "Run"}
                        </button>
                        <button
                          className="btn danger"
                          style={{ fontSize: 12, padding: "4px 10px" }}
                          onClick={() => handleDelete(wf)}
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* Live progress (shown inline below the card while running) */}
                  {running === wf.id && stepProgress.length > 0 && (
                    <div style={{ padding: "12px 16px", background: "#f8fafc", borderBottom: "1px solid #e2e8f0" }}>
                      {stepProgress.map((sp) => (
                        <div key={sp.order} style={{ display: "flex", alignItems: "center", gap: 8, padding: "4px 0", fontSize: 13 }}>
                          {sp.status === "pending" && (
                            <span style={{ color: "#94a3b8", width: 18, textAlign: "center" }}>&#9675;</span>
                          )}
                          {sp.status === "running" && (
                            <span style={{ color: "#3b82f6", width: 18, textAlign: "center", animation: "pulse 1s infinite" }}>&#9679;</span>
                          )}
                          {sp.status === "done" && sp.success && (
                            <span className="badge green" style={{ fontSize: 10, minWidth: 18 }}>OK</span>
                          )}
                          {sp.status === "done" && !sp.success && (
                            <span className="badge red" style={{ fontSize: 10, minWidth: 18 }}>FAIL</span>
                          )}
                          <strong>Step {sp.order}:</strong>
                          <span style={{ flex: 1 }}>{sp.title}</span>
                          {sp.status === "running" && (
                            <span style={{ color: "#3b82f6", fontSize: 12, fontStyle: "italic" }}>Running...</span>
                          )}
                          {sp.status === "done" && (
                            <span style={{ color: "#94a3b8", fontSize: 12 }}>
                              {sp.rowCount} row(s) | {sp.executionTimeMs}ms
                            </span>
                          )}
                        </div>
                      ))}
                      {summarizing && (
                        <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 0 4px", fontSize: 13, color: "#3b82f6", fontStyle: "italic" }}>
                          <span style={{ width: 18, textAlign: "center", animation: "pulse 1s infinite" }}>&#9679;</span>
                          AI is summarizing the results...
                        </div>
                      )}
                    </div>
                  )}

                  {/* Expanded detail */}
                  {expandedId === wf.id && !running && (
                    <div style={{ padding: "12px 16px 16px", background: "#f8fafc", borderBottom: "1px solid #e2e8f0" }}>
                      {wf.description && (
                        <div style={{ marginBottom: 12, fontSize: 13, color: "#475569" }}>
                          <strong>Description:</strong> {wf.description}
                        </div>
                      )}
                      <div style={{ marginBottom: 12, fontSize: 13, color: "#475569" }}>
                        <strong>Original Question:</strong> {wf.originalQuestion || "-"}
                      </div>
                      {wf.aiReasoning && (
                        <details style={{ marginBottom: 12 }}>
                          <summary style={{ fontSize: 13, fontWeight: 600, color: "#0f3460", cursor: "pointer" }}>AI Reasoning</summary>
                          <pre style={{ marginTop: 6, fontSize: 11, whiteSpace: "pre-wrap", background: "#fff", padding: 10, borderRadius: 4, border: "1px solid #e2e8f0", maxHeight: 200, overflow: "auto" }}>
                            {wf.aiReasoning}
                          </pre>
                        </details>
                      )}
                      <div style={{ fontWeight: 600, fontSize: 13, color: "#0f3460", marginBottom: 8 }}>Steps:</div>
                      {wf.steps.map((step) => (
                        <div key={step.order} style={{ marginBottom: 10, padding: "8px 12px", background: "#fff", borderRadius: 4, border: "1px solid #e2e8f0" }}>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "#334155", marginBottom: 4 }}>
                            Step {step.order}: {step.title}
                          </div>
                          <code style={{ fontSize: 11, color: "#475569", wordBreak: "break-all" }}>{step.sql}</code>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      ))}

      {/* Run result section */}
      {runResult && !running && (
        <div className="rerun-result" ref={resultRef}>
          <div className="rerun-header">
            <h3>Workflow Run: {runResult.workflowName}</h3>
            <button onClick={() => setRunResult(null)}>Close</button>
          </div>
          <div className="rerun-meta">
            Connector: <strong>{runResult.connector}</strong> | Total time: <strong>{runResult.executionTimeMs}ms</strong> | Steps: <strong>{runResult.steps.length}</strong>
          </div>

          {/* Per-step results */}
          {runResult.steps.map((sr) => (
            <details key={sr.order} style={{ marginBottom: 8 }}>
              <summary style={{ cursor: "pointer", padding: "6px 0", fontSize: 13 }}>
                <span className={`badge ${sr.success ? "green" : "red"}`} style={{ fontSize: 10, marginRight: 6 }}>
                  {sr.success ? "OK" : "FAIL"}
                </span>
                <strong>Step {sr.order}:</strong> {sr.title}
                <span style={{ color: "#94a3b8", fontSize: 12, marginLeft: 8 }}>
                  {sr.rowCount} row(s) | {sr.executionTimeMs}ms
                </span>
              </summary>
              <div style={{ padding: "8px 16px" }}>
                <div style={{ fontSize: 12, marginBottom: 6 }}><strong>SQL:</strong> <code style={{ fontSize: 11 }}>{sr.sql}</code></div>
                {sr.error && <div className="error-msg" style={{ fontSize: 12 }}>{sr.error}</div>}
                {sr.success && sr.rows.length > 0 && (
                  <div className="result-table-wrapper">
                    <table className="table result-table">
                      <thead>
                        <tr>
                          {Object.keys(sr.rows[0]).map((col) => <th key={col}>{col}</th>)}
                        </tr>
                      </thead>
                      <tbody>
                        {sr.rows.slice(0, 50).map((row, i) => (
                          <tr key={i}>
                            {Object.values(row).map((val, j) => (
                              <td key={j}>{val === null ? <span className="null-val">NULL</span> : String(val)}</td>
                            ))}
                          </tr>
                        ))}
                        {sr.rows.length > 50 && (
                          <tr><td colSpan={Object.keys(sr.rows[0]).length} style={{ textAlign: "center", color: "#94a3b8" }}>... {sr.rows.length - 50} more rows</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </details>
          ))}

          {/* AI Summary */}
          {runResult.summary && (
            <div style={{ marginTop: 16 }}>
              <div style={{ fontWeight: 600, fontSize: 14, color: "#0f3460", marginBottom: 8 }}>AI Summary Report</div>
              <div className="prompt-wrapper">
                <div style={{ display: "flex", gap: 4, position: "absolute", top: 8, right: 12, zIndex: 1 }}>
                  <button
                    className="prompt-copy-btn"
                    style={{ position: "static" }}
                    title="Copy Summary"
                    onClick={() => { navigator.clipboard.writeText(runResult.summary!).catch(() => {}); }}
                  >
                    <svg width="14" height="14" fill="none" stroke="#fff" viewBox="0 0 24 24">
                      <rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} />
                      <path strokeWidth={2} d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                    </svg>
                  </button>
                  <button
                    className="prompt-copy-btn"
                    style={{ position: "static" }}
                    title="Download as PDF"
                    disabled={pdfBusy}
                    onClick={async () => {
                      setPdfBusy(true);
                      try { await downloadPdf(runResult.summary!); } finally { setPdfBusy(false); }
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
                <pre className="prompt-display">{runResult.summary}</pre>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
