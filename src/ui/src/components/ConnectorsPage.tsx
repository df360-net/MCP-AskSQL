import { useState } from "react";
import { useFetch, apiPost, apiDelete } from "../hooks/useApi.js";
import type { ConnectorInfo, HealthResult } from "../types.js";
import { ConnectorFormModal } from "./ConnectorFormModal.js";
import { SchemaDetailPanel } from "./SchemaDetailPanel.js";

export function ConnectorsPage() {
  const { data: connectors, loading, refetch } = useFetch<ConnectorInfo[]>("/api/connectors");
  const [showForm, setShowForm] = useState(false);
  const [editId, setEditId] = useState<string | null>(null);
  const [schemaConnectorId, setSchemaConnectorId] = useState<string | null>(null);
  const [healthResults, setHealthResults] = useState<Record<string, { status: string; detail?: string }>>({});
  const [actionLoading, setActionLoading] = useState<Record<string, boolean>>({});

  const handleHealth = async (id: string) => {
    setActionLoading((p) => ({ ...p, [id]: true }));
    try {
      const r = await apiPost<HealthResult>(`/api/connectors/${id}/health`);
      setHealthResults((p) => ({ ...p, [id]: { status: r.database.connected ? "connected" : "failed", detail: r.database.version } }));
    } catch (err) {
      setHealthResults((p) => ({ ...p, [id]: { status: "error", detail: err instanceof Error ? err.message : String(err) } }));
    } finally {
      setActionLoading((p) => ({ ...p, [id]: false }));
    }
  };

  const handleRefresh = async (id: string) => {
    setActionLoading((p) => ({ ...p, [`refresh-${id}`]: true }));
    try {
      await apiPost(`/api/connectors/${id}/refresh-schema`);
      setHealthResults((p) => ({ ...p, [id]: { status: "connected" } }));
      refetch();
    } catch (err) {
      setHealthResults((p) => ({ ...p, [id]: { status: "failed", detail: err instanceof Error ? err.message : String(err) } }));
    } finally {
      setActionLoading((p) => ({ ...p, [`refresh-${id}`]: false }));
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm(`Delete connector "${id}"? This cannot be undone.`)) return;
    try {
      await apiDelete(`/api/connectors/${id}`);
      refetch();
    } catch (err) {
      alert(`Delete failed: ${err instanceof Error ? err.message : err}`);
    }
  };

  const handleSchemaInfo = (id: string) => {
    setSchemaConnectorId(id);
  };

  if (loading) return <p>Loading...</p>;

  return (
    <div className="conn-page">
      <div className="conn-header">
        <div>
          <h2>Database Connectors</h2>
          <p className="conn-subtitle">Manage your database connections</p>
        </div>
        <button className="conn-add-btn" onClick={() => { setEditId(null); setShowForm(true); }}>
          <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          Add Connector
        </button>
      </div>

      <div className="conn-list">
        {connectors?.map((c) => (
          <div key={c.id} className="conn-card">
            <div className="conn-card-left">
              <div className="conn-icon">
                <svg width="20" height="20" fill="none" stroke="#3b82f6" viewBox="0 0 24 24">
                  <rect x="3" y="3" width="18" height="18" rx="3" strokeWidth={1.5} />
                  <path strokeLinecap="round" strokeWidth={1.5} d="M7 8h10M7 12h10M7 16h6" />
                </svg>
              </div>
              <div className="conn-info">
                <div className="conn-name">
                  {c.id}
                  {c.isDefault && <span className="conn-default-badge">default</span>}
                </div>
                <div className="conn-meta">
                  <span className="conn-type-badge">{c.type}</span>
                  <span className="conn-dot">&middot;</span>
                  <span>{c.schemas.join(", ")}</span>
                  {c.cached && (
                    <>
                      <span className="conn-dot">&middot;</span>
                      <span>cached {c.cacheAgeHours !== null ? `${c.cacheAgeHours.toFixed(1)}h ago` : ""}</span>
                    </>
                  )}
                </div>
              </div>
            </div>

            <div className="conn-card-right">
              {healthResults[c.id] ? (
                <span className={`conn-status ${healthResults[c.id].status === "connected" ? "connected" : "disconnected"}`}>
                  <span className="conn-status-dot" />
                  {healthResults[c.id].status === "connected" ? "Connected" : "Disconnected"}
                </span>
              ) : (
                <span className="conn-status neutral">
                  <span className="conn-status-dot" />
                  Unknown
                </span>
              )}

              <div className="conn-actions">
                <button className="conn-btn" disabled={actionLoading[c.id]} onClick={() => handleHealth(c.id)}>
                  {actionLoading[c.id] ? "..." : "Test"}
                </button>
                <button className="conn-btn" disabled={actionLoading[`refresh-${c.id}`]} onClick={() => handleRefresh(c.id)}>
                  {actionLoading[`refresh-${c.id}`] ? "..." : "Refresh"}
                </button>
                <button className="conn-btn" onClick={() => handleSchemaInfo(c.id)}>Schema</button>
                <button className="conn-btn" onClick={() => { setEditId(c.id); setShowForm(true); }}>Edit</button>
                <button className="conn-btn remove" onClick={() => handleDelete(c.id)}>Remove</button>
              </div>
            </div>
          </div>
        ))}
      </div>

      {showForm && (
        <ConnectorFormModal
          editId={editId}
          onClose={() => setShowForm(false)}
          onSaved={() => { setShowForm(false); refetch(); }}
        />
      )}

      {schemaConnectorId && (
        <SchemaDetailPanel
          connectorId={schemaConnectorId}
          onClose={() => setSchemaConnectorId(null)}
        />
      )}
    </div>
  );
}
