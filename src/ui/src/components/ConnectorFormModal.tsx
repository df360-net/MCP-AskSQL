import { useState, useEffect } from "react";
import { apiPost, apiPut } from "../hooks/useApi.js";
import type { ConnectorDetail } from "../types.js";

interface Props {
  editId: string | null;
  onClose: () => void;
  onSaved: () => void;
}

export function ConnectorFormModal({ editId, onClose, onSaved }: Props) {
  const [id, setId] = useState("");
  const [connectionString, setConnectionString] = useState("");
  const [schemas, setSchemas] = useState("public");
  const [schemaPrefix, setSchemaPrefix] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (editId) {
      fetch(`/api/connectors/${editId}`)
        .then((r) => {
          if (!r.ok) throw new Error(`Failed to load connector: ${r.statusText}`);
          return r.json();
        })
        .then((d: ConnectorDetail) => {
          setId(d.id);
          setConnectionString(d.connectionString);
          setSchemas(d.schemas?.join(", ") ?? "public");
          setSchemaPrefix(d.schemaPrefix ?? "");
        })
        .catch((err) => setError(err instanceof Error ? err.message : String(err)));
    }
  }, [editId]);

  const handleSave = async () => {
    setSaving(true);
    setError(null);
    try {
      const body = {
        id,
        connectionString,
        schemas: schemas.split(",").map((s) => s.trim()).filter(Boolean),
        schemaPrefix: schemaPrefix || undefined,
      };
      if (editId) {
        await apiPut(`/api/connectors/${editId}`, body);
      } else {
        await apiPost("/api/connectors", body);
      }
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  };

  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  return (
    <div className="modal-overlay" onClick={onClose} role="dialog" aria-modal="true" aria-labelledby="connector-modal-title">
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3 id="connector-modal-title">{editId ? `Edit: ${editId}` : "Add Connector"}</h3>

        {error && <div className="error-msg">{error}</div>}

        <label>ID</label>
        <input value={id} onChange={(e) => setId(e.target.value)} disabled={!!editId} placeholder="e.g. mydb" />

        <label>Connection String</label>
        <input value={connectionString} onChange={(e) => setConnectionString(e.target.value)} placeholder="postgres://user:pass@host:5432/db" />

        <label>Schemas (comma-separated)</label>
        <input value={schemas} onChange={(e) => setSchemas(e.target.value)} placeholder="public" />

        <label>Schema Prefix (optional)</label>
        <input value={schemaPrefix} onChange={(e) => setSchemaPrefix(e.target.value)} placeholder="" />

        <div className="modal-actions">
          <button onClick={onClose}>Cancel</button>
          <button className="btn primary" onClick={handleSave} disabled={saving || !id || !connectionString}>
            {saving ? "Saving..." : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}
