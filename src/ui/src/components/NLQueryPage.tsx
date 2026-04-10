import { useState, useRef, useCallback } from "react";
import { useFetch } from "../hooks/useApi.js";
import type { ConnectorInfo, AskResult } from "../types.js";

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
  a.download = "report.pdf";
  a.click();
  URL.revokeObjectURL(url);
}

interface LiveTurn {
  turn: number;
  reasoning: string;
}

export function NLQueryPage() {
  const [question, setQuestion] = useState("");
  const [connector, setConnector] = useState("");
  const [maxRows, setMaxRows] = useState(100);
  const [maxRowsLoaded, setMaxRowsLoaded] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AskResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<Array<{ id: string; question: string; connector: string; timestamp: string }>>([]);

  const [pdfBusy, setPdfBusy] = useState(false);

  // Live streaming state
  const [liveTurns, setLiveTurns] = useState<LiveTurn[]>([]);
  const [routeInfo, setRouteInfo] = useState<{ routedTo: string; routeMethod: string; routeConfidence: string } | null>(null);
  const liveRef = useRef<HTMLDivElement>(null);

  const { data: connectors } = useFetch<ConnectorInfo[]>("/api/connectors");
  const { data: settings } = useFetch<{ maxRows: number }>("/api/settings");

  // Set maxRows from config on first load
  if (settings && !maxRowsLoaded) {
    setMaxRows(settings.maxRows);
    setMaxRowsLoaded(true);
  }

  const handleAsk = async () => {
    if (!question.trim()) return;
    setLoading(true);
    setResult(null);
    setError(null);
    setLiveTurns([]);
    setRouteInfo(null);

    try {
      const body: Record<string, unknown> = { question, maxRows };
      if (connector) body.connector = connector;

      const res = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      // Check if this is an SSE stream (agent loop) or regular JSON (single-shot)
      const contentType = res.headers.get("Content-Type") ?? "";
      if (contentType.includes("text/event-stream")) {
        // SSE streaming — read turns in real time
        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let finalResult: AskResult | null = null;

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          // Parse SSE events from buffer
          const lines = buffer.split("\n");
          buffer = lines.pop() ?? ""; // keep incomplete line in buffer

          for (const line of lines) {
            if (!line.startsWith("data: ")) continue;
            const json = line.slice(6);
            try {
              const event = JSON.parse(json);

              if (event.type === "route") {
                setRouteInfo({ routedTo: event.routedTo, routeMethod: event.routeMethod, routeConfidence: event.routeConfidence });
              } else if (event.done === false) {
                // Turn event — add to live turns
                setLiveTurns((prev) => [...prev, { turn: event.turn, reasoning: event.reasoning }]);
                // Auto-scroll the live panel
                setTimeout(() => liveRef.current?.scrollTo({ top: liveRef.current.scrollHeight, behavior: "smooth" }), 50);
              } else if (event.type === "result") {
                // Final result
                finalResult = {
                  success: event.success,
                  answer: event.answer,
                  explanation: event.explanation,
                  turns: event.turns,
                  toolCalls: event.toolCalls,
                  tokenUsage: event.tokenUsage,
                  executionTimeMs: event.executionTimeMs,
                  routedTo: event.routedTo,
                  routeMethod: event.routeMethod,
                  routeConfidence: event.routeConfidence,
                  // single-shot fields (not used)
                  sql: null,
                  rows: [],
                  rowCount: 0,
                };
              }
            } catch {
              // ignore malformed events
            }
          }
        }

        if (finalResult) {
          setResult(finalResult);
          setHistory((h) => [{ id: crypto.randomUUID(), question, connector: finalResult!.routedTo ?? connector ?? "default", timestamp: new Date().toISOString() }, ...h.slice(0, 19)]);
        }
      } else {
        // Regular JSON response (single-shot path)
        const data = await res.json();
        if (!res.ok) throw new Error(data.error ?? res.statusText);
        setResult(data as AskResult);
        setHistory((h) => [{ id: crypto.randomUUID(), question, connector: data.routedTo ?? connector ?? "default", timestamp: new Date().toISOString() }, ...h.slice(0, 19)]);
      }
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

      {/* ── Live streaming panel (visible while agent is running) ── */}
      {loading && liveTurns.length > 0 && (
        <div className="live-stream-panel">
          {routeInfo && (
            <div className="route-info">
              Routed to <strong>{routeInfo.routedTo}</strong>
              <span className="badge">{routeInfo.routeMethod}</span>
              <span className="route-confidence">{routeInfo.routeConfidence}</span>
            </div>
          )}
          <div className="live-stream-header">
            <span className="live-dot" />
            <h4>Agent Thinking — Turn {liveTurns.length}</h4>
          </div>
          <div className="live-stream-body" ref={liveRef}>
            {liveTurns.map((lt) => (
              <div key={lt.turn} className="live-turn">
                <pre>{lt.reasoning}</pre>
              </div>
            ))}
          </div>
        </div>
      )}

      {error && <div className="error-msg">{error}</div>}

      {!result && !error && !loading && (
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

          {/* ── Agent loop result (2-layer) ── */}
          {result.answer ? (
            <>
              <div className={`result-status ${result.success ? "success" : "fail"}`}>
                {result.success ? "Success" : "Failed"}
                {" "}&mdash; {result.turns} turn(s), {result.toolCalls?.length ?? 0} tool call(s) in {result.executionTimeMs}ms
                {result.tokenUsage && <> &middot; {result.tokenUsage.totalTokens} tokens (${result.tokenUsage.estimatedCost.toFixed(4)})</>}
              </div>

              {result.explanation && (
                <div className="result-explanation">
                  <h4>Explanation</h4>
                  <pre className="agent-explanation">{result.explanation}</pre>
                </div>
              )}

              <div className="result-answer">
                <div className="result-sql-header">
                  <h4>Analysis</h4>
                  <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                    <button
                      className="copy-btn"
                      title="Copy answer"
                      onClick={() => { navigator.clipboard.writeText(result.answer!).catch(() => {}); }}
                    >
                      <svg width="11" height="11" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} />
                        <path strokeWidth={2} d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                      </svg>
                    </button>
                    <button
                      className="copy-btn pdf-btn"
                      title="Download as PDF"
                      disabled={pdfBusy}
                      onClick={async () => {
                        setPdfBusy(true);
                        try { await downloadPdf(result.answer!); } finally { setPdfBusy(false); }
                      }}
                    >
                      {pdfBusy ? (
                        <span style={{ fontSize: 11 }}>...</span>
                      ) : (
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/>
                          <polyline points="14 2 14 8 20 8"/>
                          <line x1="16" y1="13" x2="8" y2="13"/>
                          <line x1="16" y1="17" x2="8" y2="17"/>
                          <line x1="10" y1="9" x2="8" y2="9"/>
                        </svg>
                      )}
                    </button>
                  </div>
                </div>
                <pre className="agent-answer">{result.answer}</pre>
              </div>

              {result.toolCalls && result.toolCalls.length > 0 && (
                <details className="result-tool-calls">
                  <summary>Tool Calls ({result.toolCalls.length})</summary>
                  {result.toolCalls.map((tc, i) => (
                    <div key={i} className="tool-call-entry">
                      <span className="tool-call-badge">Turn {tc.turn + 1}</span>
                      <strong>{tc.tool}</strong>
                      <span className="tool-call-time">{tc.durationMs}ms</span>
                      <pre className="tool-call-input">{JSON.stringify(tc.input, null, 2)}</pre>
                    </div>
                  ))}
                </details>
              )}
            </>
          ) : (
            /* ── Original single-shot result ── */
            <>
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

              {result.rows && result.rows.length > 0 && (
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
            </>
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
