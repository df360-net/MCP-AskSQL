import { useState, useEffect, useRef } from "react";
import { useFetch, apiPost, apiPut, apiDelete } from "../hooks/useApi.js";
import type { ScheduledJob, JobRun, Workflow } from "../types.js";

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
  a.download = "scheduler-report.pdf";
  a.click();
  URL.revokeObjectURL(url);
}

function formatSchedule(job: ScheduledJob): string {
  if (job.scheduleType === "daily" && job.dailyRunTime) {
    return `Daily at ${job.dailyRunTime}`;
  }
  const s = job.intervalSeconds ?? 3600;
  if (s < 60) return `Every ${s}s`;
  if (s < 3600) return `Every ${Math.round(s / 60)}m`;
  if (s < 86400) return `Every ${Math.round(s / 3600)}h`;
  return `Every ${Math.round(s / 86400)}d`;
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function statusColor(status: string): string {
  switch (status) {
    case "COMPLETED": return "green";
    case "RUNNING": return "blue";
    case "FAILED": return "red";
    case "TIMED_OUT": return "red";
    default: return "gray";
  }
}

export function SchedulerPage() {
  const { data: jobs, refetch } = useFetch<ScheduledJob[]>("/api/scheduler/jobs");
  const { data: workflows } = useFetch<Workflow[]>("/api/workflows");
  const [showCreate, setShowCreate] = useState(false);
  const [createWorkflowId, setCreateWorkflowId] = useState("");
  const [createName, setCreateName] = useState("");
  const [createType, setCreateType] = useState<"interval" | "daily">("interval");
  const [createInterval, setCreateInterval] = useState("3600");
  const [createDailyTime, setCreateDailyTime] = useState("07:00");
  const [createTimeout, setCreateTimeout] = useState("300");
  const [createEmailRecipients, setCreateEmailRecipients] = useState("");
  const [creating, setCreating] = useState(false);
  const [editJob, setEditJob] = useState<ScheduledJob | null>(null);
  const [editName, setEditName] = useState("");
  const [editType, setEditType] = useState<"interval" | "daily">("interval");
  const [editInterval, setEditInterval] = useState("3600");
  const [editDailyTime, setEditDailyTime] = useState("07:00");
  const [editTimeout, setEditTimeout] = useState("300");
  const [editEmailRecipients, setEditEmailRecipients] = useState("");
  const [saving, setSaving] = useState(false);
  const [triggering, setTriggering] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [runs, setRuns] = useState<{ jobId: string; runs: JobRun[] } | null>(null);
  const [viewReportId, setViewReportId] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Auto-poll while any job has RUNNING status
  const hasRunning = jobs?.some((j) => j.lastRun?.status === "RUNNING");
  useEffect(() => {
    if (hasRunning) {
      pollRef.current = setInterval(() => {
        refetch();
        // Also refresh run history if expanded
        if (expandedId) {
          fetch(`/api/scheduler/jobs/${expandedId}/runs?limit=10`)
            .then((r) => r.json())
            .then((data) => setRuns({ jobId: expandedId, runs: data as JobRun[] }))
            .catch(() => {});
        }
      }, 3000);
    } else if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [hasRunning, refetch, expandedId]);

  const handleCreate = async () => {
    if (!createWorkflowId) { alert("Please select a workflow."); return; }
    setCreating(true);
    try {
      const recipients = createEmailRecipients.split(";").map((s) => s.trim()).filter(Boolean);
      await apiPost("/api/scheduler/jobs", {
        workflowId: createWorkflowId,
        name: createName || "Scheduled Workflow",
        scheduleType: createType,
        intervalSeconds: createType === "interval" ? parseInt(createInterval, 10) : undefined,
        dailyRunTime: createType === "daily" ? createDailyTime : undefined,
        timeoutSeconds: parseInt(createTimeout, 10),
        emailRecipients: recipients.length > 0 ? recipients : undefined,
      });
      setShowCreate(false);
      setCreateWorkflowId("");
      setCreateName("");
      setCreateEmailRecipients("");
      refetch();
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    } finally {
      setCreating(false);
    }
  };

  const handleTrigger = async (jobId: string) => {
    setTriggering(jobId);
    try {
      await apiPost(`/api/scheduler/jobs/${jobId}/trigger`, {});
      // Poll for update after a short delay
      setTimeout(() => refetch(), 2000);
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    } finally {
      setTriggering(null);
    }
  };

  const handleToggle = async (job: ScheduledJob) => {
    try {
      await apiPut(`/api/scheduler/jobs/${job.id}`, { isEnabled: !job.isEnabled });
      refetch();
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    }
  };

  const handleDelete = async (job: ScheduledJob) => {
    if (!confirm(`Delete scheduled job "${job.name}"?`)) return;
    try {
      await apiDelete(`/api/scheduler/jobs/${job.id}`);
      refetch();
      if (expandedId === job.id) setExpandedId(null);
    } catch (err) {
      alert(err instanceof Error ? err.message : String(err));
    }
  };

  const loadRuns = async (jobId: string) => {
    if (expandedId === jobId) {
      setExpandedId(null);
      setRuns(null);
      setViewReportId(null);
      return;
    }
    setExpandedId(jobId);
    try {
      const res = await fetch(`/api/scheduler/jobs/${jobId}/runs?limit=10`);
      const data = await res.json();
      setRuns({ jobId, runs: data as JobRun[] });
    } catch { /* ignore */ }
  };

  return (
    <div>
      <div className="page-header">
        <h2>Scheduler</h2>
        <button className="btn" style={{ background: "linear-gradient(135deg, #3b82f6, #2563eb)", color: "#fff" }} onClick={() => setShowCreate(true)}>
          + Schedule Workflow
        </button>
      </div>

      {!jobs && <div style={{ padding: 20, color: "#94a3b8" }}>Loading...</div>}

      {jobs && jobs.length === 0 && (
        <div style={{ padding: 40, textAlign: "center", color: "#94a3b8" }}>
          <p style={{ fontSize: 16, marginBottom: 8 }}>No scheduled workflows yet.</p>
          <p style={{ fontSize: 13 }}>Click <strong>"+ Schedule Workflow"</strong> to set up automatic workflow execution.</p>
        </div>
      )}

      {jobs && jobs.length > 0 && (
        <>
        <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 8, fontStyle: "italic" }}>
          Click on a scheduled workflow to see the job run history.
        </div>
        <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, overflow: "hidden" }}>
          {jobs.map((job) => (
            <div key={job.id}>
              <div
                title="Click to see job run history"
                style={{
                  padding: "12px 16px",
                  borderBottom: "1px solid #f1f5f9",
                  cursor: "pointer",
                  background: expandedId === job.id ? "#f0f9ff" : "#fff",
                }}
                onClick={() => loadRuns(job.id)}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                      <span style={{ fontWeight: 600, fontSize: 14, color: "#0f3460" }}>{job.name}</span>
                      <span className={`badge ${job.isEnabled ? "green" : ""}`} style={{ fontSize: 10 }}>
                        {job.isEnabled ? "ON" : "OFF"}
                      </span>
                      <span style={{ fontSize: 12, color: "#64748b", background: "#f1f5f9", padding: "1px 8px", borderRadius: 4 }}>
                        {formatSchedule(job)}
                      </span>
                    </div>
                    <div style={{ fontSize: 12, color: "#64748b", marginBottom: 2 }}>
                      Next run: {job.isEnabled ? new Date(job.nextRunAt).toLocaleString() : "(disabled)"}
                    </div>
                    {job.emailRecipients && job.emailRecipients.length > 0 && (
                      <div style={{ fontSize: 11, color: "#64748b", marginBottom: 2 }}>
                        <span style={{ color: "#94a3b8" }}>Email:</span> {job.emailRecipients.join("; ")}
                      </div>
                    )}
                    {job.lastRun && (
                      <div style={{ fontSize: 11, color: "#94a3b8", display: "flex", alignItems: "center", gap: 6 }}>
                        Last: <span className={`badge ${statusColor(job.lastRun.status)}`} style={{ fontSize: 9 }}>{job.lastRun.status}</span>
                        {job.lastRun.durationMs && <span>{Math.round(job.lastRun.durationMs / 1000)}s</span>}
                        <span>{timeAgo(job.lastRun.startedAt)}</span>
                        {job.lastRun.triggeredBy === "MANUAL" && <span style={{ color: "#3b82f6" }}>(manual)</span>}
                      </div>
                    )}
                  </div>
                  <div style={{ display: "flex", gap: 6 }} onClick={(e) => e.stopPropagation()}>
                    <button
                      className="btn"
                      style={{ fontSize: 12, padding: "4px 10px", background: "linear-gradient(135deg, #3b82f6, #2563eb)", color: "#fff" }}
                      disabled={triggering === job.id}
                      onClick={() => handleTrigger(job.id)}
                      title="Trigger run now"
                    >
                      {triggering === job.id ? "..." : "Trigger"}
                    </button>
                    <button
                      className="btn"
                      style={{ fontSize: 12, padding: "4px 10px" }}
                      onClick={() => {
                        setEditJob(job);
                        setEditName(job.name);
                        setEditType(job.scheduleType);
                        setEditInterval(String(job.intervalSeconds ?? 3600));
                        setEditDailyTime(job.dailyRunTime ?? "07:00");
                        setEditTimeout(String(job.timeoutSeconds));
                        setEditEmailRecipients((job.emailRecipients ?? []).join("; "));
                      }}
                    >
                      Edit
                    </button>
                    <button
                      className="btn"
                      style={{ fontSize: 12, padding: "4px 10px" }}
                      onClick={() => handleToggle(job)}
                      title={job.isEnabled ? "Disable" : "Enable"}
                    >
                      {job.isEnabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      className="btn danger"
                      style={{ fontSize: 12, padding: "4px 10px" }}
                      onClick={() => handleDelete(job)}
                    >
                      Delete
                    </button>
                  </div>
                </div>
              </div>

              {/* Run history */}
              {expandedId === job.id && runs && runs.jobId === job.id && (
                <div style={{ padding: "12px 16px", background: "#f8fafc", borderBottom: "1px solid #e2e8f0" }}>
                  <div style={{ fontWeight: 600, fontSize: 13, color: "#0f3460", marginBottom: 8 }}>Run History (last 10)</div>
                  {runs.runs.length === 0 ? (
                    <div style={{ fontSize: 13, color: "#94a3b8" }}>No runs yet.</div>
                  ) : (
                    <>
                    <table className="table" style={{ fontSize: 12 }}>
                      <thead>
                        <tr>
                          <th>Started</th><th>Status</th><th>Duration</th><th>Steps</th><th>Triggered By</th><th>Report</th>{job.emailRecipients && job.emailRecipients.length > 0 && <th>Email</th>}<th>Error</th>
                        </tr>
                      </thead>
                      <tbody>
                        {runs.runs.map((r) => (
                          <tr key={r.id}>
                            <td>{new Date(r.startedAt).toLocaleString()}</td>
                            <td><span className={`badge ${statusColor(r.status)}`} style={{ fontSize: 10 }}>{r.status}</span></td>
                            <td>{r.durationMs ? `${Math.round(r.durationMs / 1000)}s` : "-"}</td>
                            <td>{r.stepsCompleted}/{r.stepsTotal}</td>
                            <td>{r.triggeredBy}</td>
                            <td>
                              {r.summary ? (
                                <button
                                  className="btn"
                                  style={{ fontSize: 11, padding: "2px 8px", background: "linear-gradient(135deg, #3b82f6, #2563eb)", color: "#fff" }}
                                  onClick={(e) => { e.stopPropagation(); setViewReportId(viewReportId === r.id ? null : r.id); }}
                                >
                                  {viewReportId === r.id ? "Hide" : "View"}
                                </button>
                              ) : r.status === "RUNNING" ? (
                                <span style={{ color: "#94a3b8", fontStyle: "italic" }}>pending...</span>
                              ) : "-"}
                            </td>
                            {job.emailRecipients && job.emailRecipients.length > 0 && (
                              <td style={{ textAlign: "center" }}>
                                {r.emailSent ? (
                                  <svg width="18" height="18" fill="none" viewBox="0 0 24 24" style={{ verticalAlign: "middle" }}>
                                    <circle cx="12" cy="12" r="10" fill="#22c55e" />
                                    <path d="M8 12l2.5 2.5L16 9.5" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                                  </svg>
                                ) : r.emailSent === false ? (
                                  <svg width="18" height="18" fill="none" viewBox="0 0 24 24" style={{ verticalAlign: "middle" }}>
                                    <circle cx="12" cy="12" r="10" fill="#ef4444" />
                                    <path d="M15 9l-6 6M9 9l6 6" stroke="#fff" strokeWidth="2" strokeLinecap="round" />
                                  </svg>
                                ) : <span style={{ color: "#94a3b8" }}>-</span>}
                              </td>
                            )}
                            <td className="truncate" style={{ maxWidth: 200 }}>{r.error || "-"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>

                    {/* Expanded report view */}
                    {viewReportId && runs.runs.find((r) => r.id === viewReportId)?.summary && (
                      <div style={{ marginTop: 12, border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff" }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 12px", borderBottom: "1px solid #f1f5f9", background: "#f8fafc" }}>
                          <span style={{ fontWeight: 600, fontSize: 13, color: "#0f3460" }}>
                            AI Summary Report — {new Date(runs.runs.find((r) => r.id === viewReportId)!.startedAt).toLocaleString()}
                          </span>
                          <div style={{ display: "flex", gap: 6 }}>
                            <button
                              className="prompt-copy-btn"
                              style={{ position: "static" }}
                              title="Copy report"
                              onClick={() => {
                                const text = runs.runs.find((r) => r.id === viewReportId)?.summary ?? "";
                                navigator.clipboard.writeText(text);
                              }}
                            >
                              <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24"><rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" strokeWidth={2} /></svg>
                            </button>
                            <button
                              className="prompt-copy-btn"
                              style={{ position: "static" }}
                              title="Download PDF"
                              onClick={() => {
                                const text = runs.runs.find((r) => r.id === viewReportId)?.summary ?? "";
                                downloadPdf(text);
                              }}
                            >
                              <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>
                            </button>
                          </div>
                        </div>
                        <div
                          className="markdown-body"
                          style={{ padding: "12px 16px", fontSize: 13, lineHeight: 1.6, maxHeight: 500, overflow: "auto", whiteSpace: "pre-wrap" }}
                        >
                          {runs.runs.find((r) => r.id === viewReportId)?.summary}
                        </div>
                      </div>
                    )}
                    </>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
        </>
      )}

      {/* Create schedule modal */}
      {showCreate && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
          <div style={{ background: "#fff", borderRadius: 8, padding: 24, width: 480, maxHeight: "80vh", overflow: "auto", boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}>
            <h3 style={{ margin: "0 0 16px", color: "#0f3460" }}>Schedule a Workflow</h3>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Workflow *</label>
              <select
                value={createWorkflowId}
                onChange={(e) => {
                  setCreateWorkflowId(e.target.value);
                  const wf = workflows?.find((w) => w.id === e.target.value);
                  if (wf && !createName) setCreateName(wf.name);
                }}
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              >
                <option value="">Select a workflow...</option>
                {workflows?.map((wf) => (
                  <option key={wf.id} value={wf.id}>{wf.name} ({wf.connector})</option>
                ))}
              </select>
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Schedule Name</label>
              <input
                type="text"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                placeholder="e.g., Daily TPCH Report"
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Schedule Type</label>
              <select
                value={createType}
                onChange={(e) => setCreateType(e.target.value as "interval" | "daily")}
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              >
                <option value="interval">Interval (every N seconds)</option>
                <option value="daily">Daily at fixed time</option>
              </select>
            </div>

            {createType === "interval" && (
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Interval (seconds)</label>
                <input
                  type="number"
                  value={createInterval}
                  onChange={(e) => setCreateInterval(e.target.value)}
                  min="60"
                  style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
                />
                <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
                  = {parseInt(createInterval) >= 3600 ? `${Math.round(parseInt(createInterval) / 3600)}h` : `${Math.round(parseInt(createInterval) / 60)}m`}
                </div>
              </div>
            )}

            {createType === "daily" && (
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Daily Run Time</label>
                <input
                  type="time"
                  value={createDailyTime}
                  onChange={(e) => setCreateDailyTime(e.target.value)}
                  style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
                />
              </div>
            )}

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Timeout (seconds)</label>
              <input
                type="number"
                value={createTimeout}
                onChange={(e) => setCreateTimeout(e.target.value)}
                min="30"
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Email Recipients</label>
              <input
                type="text"
                value={createEmailRecipients}
                onChange={(e) => setCreateEmailRecipients(e.target.value)}
                placeholder="e.g., alice@example.com; bob@example.com"
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              />
              <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
                Separate multiple emails with semicolons (;). Leave empty to skip email notifications.
              </div>
            </div>

            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button className="btn" style={{ background: "#6b7280" }} onClick={() => setShowCreate(false)}>Cancel</button>
              <button className="btn" style={{ background: "linear-gradient(135deg, #3b82f6, #2563eb)", color: "#fff" }} disabled={creating} onClick={handleCreate}>
                {creating ? "Creating..." : "Create Schedule"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit schedule modal */}
      {editJob && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
          <div style={{ background: "#fff", borderRadius: 8, padding: 24, width: 480, maxHeight: "80vh", overflow: "auto", boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}>
            <h3 style={{ margin: "0 0 16px", color: "#0f3460" }}>Edit Schedule</h3>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Schedule Name</label>
              <input
                type="text"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Schedule Type</label>
              <select
                value={editType}
                onChange={(e) => setEditType(e.target.value as "interval" | "daily")}
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              >
                <option value="interval">Interval (every N seconds)</option>
                <option value="daily">Daily at fixed time</option>
              </select>
            </div>

            {editType === "interval" && (
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Interval (seconds)</label>
                <input
                  type="number"
                  value={editInterval}
                  onChange={(e) => setEditInterval(e.target.value)}
                  min="60"
                  style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
                />
                <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
                  = {parseInt(editInterval) >= 3600 ? `${Math.round(parseInt(editInterval) / 3600)}h` : `${Math.round(parseInt(editInterval) / 60)}m`}
                </div>
              </div>
            )}

            {editType === "daily" && (
              <div style={{ marginBottom: 12 }}>
                <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Daily Run Time</label>
                <input
                  type="time"
                  value={editDailyTime}
                  onChange={(e) => setEditDailyTime(e.target.value)}
                  style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
                />
              </div>
            )}

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Timeout (seconds)</label>
              <input
                type="number"
                value={editTimeout}
                onChange={(e) => setEditTimeout(e.target.value)}
                min="30"
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              />
            </div>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Email Recipients</label>
              <input
                type="text"
                value={editEmailRecipients}
                onChange={(e) => setEditEmailRecipients(e.target.value)}
                placeholder="e.g., alice@example.com; bob@example.com"
                style={{ width: "100%", padding: "8px 10px", border: "1px solid #d1d5db", borderRadius: 4, fontSize: 13, boxSizing: "border-box" }}
              />
              <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
                Separate multiple emails with semicolons (;). Leave empty to skip email notifications.
              </div>
            </div>

            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button className="btn" style={{ background: "#6b7280" }} onClick={() => setEditJob(null)}>Cancel</button>
              <button
                className="btn"
                style={{ background: "linear-gradient(135deg, #3b82f6, #2563eb)", color: "#fff" }}
                disabled={saving}
                onClick={async () => {
                  setSaving(true);
                  const editRecipients = editEmailRecipients.split(";").map((s) => s.trim()).filter(Boolean);
                  try {
                    await apiPut(`/api/scheduler/jobs/${editJob.id}`, {
                      name: editName,
                      scheduleType: editType,
                      intervalSeconds: editType === "interval" ? parseInt(editInterval, 10) : undefined,
                      dailyRunTime: editType === "daily" ? editDailyTime : undefined,
                      timeoutSeconds: parseInt(editTimeout, 10),
                      emailRecipients: editRecipients.length > 0 ? editRecipients : [],
                    });
                    setEditJob(null);
                    refetch();
                  } catch (err) {
                    alert(err instanceof Error ? err.message : String(err));
                  } finally {
                    setSaving(false);
                  }
                }}
              >
                {saving ? "Saving..." : "Save Changes"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
