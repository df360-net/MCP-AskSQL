import { useState, useEffect } from "react";

interface Column {
  columnName: string;
  ordinalPosition: number;
  dataType: string;
  fullDataType: string;
  isNullable: boolean;
  columnDefault?: string;
  isPrimaryKey: boolean;
  isAutoIncrement: boolean;
  columnComment?: string;
}

interface PrimaryKey {
  constraintName: string;
  columns: string[];
}

interface ForeignKey {
  constraintName: string;
  columns: string[];
  referencedSchema: string;
  referencedTable: string;
  referencedColumns: string[];
  onDelete?: string;
  onUpdate?: string;
}

interface Index {
  indexName: string;
  columns: string[];
  isUnique: boolean;
}

interface Table {
  tableName: string;
  tableType: string;
  estimatedRowCount?: number;
  tableComment?: string;
  columns: Column[];
  primaryKey?: PrimaryKey;
  foreignKeys: ForeignKey[];
  indexes: Index[];
}

interface Schema {
  schemaName: string;
  tables: Table[];
}

interface SchemaDetail {
  databaseName: string;
  serverVersion: string;
  databaseType: string;
  schemas: Schema[];
  discoveredAt: string;
  durationMs: number;
}

interface Props {
  connectorId: string;
  onClose: () => void;
}

export function SchemaDetailPanel({ connectorId, onClose }: Props) {
  const [data, setData] = useState<SchemaDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    fetch(`/api/connectors/${connectorId}/schema-detail`, { signal: controller.signal })
      .then((r) => r.json())
      .then((d) => setData(d as SchemaDetail))
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setData(null);
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [connectorId]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  if (loading) return <div className="modal-overlay"><div className="modal"><p>Loading schema...</p></div></div>;
  if (!data) return <div className="modal-overlay"><div className="modal"><p>No schema cache found.</p><button onClick={onClose}>Close</button></div></div>;

  const allTables = data.schemas.flatMap((s) =>
    s.tables.map((t) => ({ ...t, schemaName: s.schemaName, fullName: `${s.schemaName}.${t.tableName}` }))
  );

  const totalColumns = allTables.reduce((sum, t) => sum + t.columns.length, 0);
  const totalFKs = allTables.reduce((sum, t) => sum + t.foreignKeys.length, 0);
  const totalIndexes = allTables.reduce((sum, t) => sum + t.indexes.length, 0);

  const filtered = search
    ? allTables.filter((t) => t.fullName.toLowerCase().includes(search.toLowerCase()) ||
        t.columns.some((c) => c.columnName.toLowerCase().includes(search.toLowerCase())))
    : allTables;

  return (
    <div className="modal-overlay" onClick={onClose} role="dialog" aria-modal="true" aria-labelledby="schema-panel-title">
      <div className="schema-panel" onClick={(e) => e.stopPropagation()}>
        <div className="schema-header">
          <div>
            <h3 id="schema-panel-title">Schema: {connectorId}</h3>
            <div className="schema-meta">
              {data.databaseType} | {data.databaseName} | {data.serverVersion}
              <br />
              Discovered: {new Date(data.discoveredAt).toLocaleString()} ({data.durationMs}ms)
            </div>
          </div>
          <button onClick={onClose}>Close</button>
        </div>

        <div className="schema-stats">
          <div className="stat"><span className="stat-value">{allTables.length}</span><span className="stat-label">Tables</span></div>
          <div className="stat"><span className="stat-value">{totalColumns}</span><span className="stat-label">Columns</span></div>
          <div className="stat"><span className="stat-value">{totalFKs}</span><span className="stat-label">Foreign Keys</span></div>
          <div className="stat"><span className="stat-value">{totalIndexes}</span><span className="stat-label">Indexes</span></div>
        </div>

        <input
          className="schema-search"
          placeholder="Search tables or columns..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />

        <div className="schema-table-list">
          {filtered.map((t) => {
            const isExpanded = expandedTable === t.fullName;
            const pkCols = new Set(t.primaryKey?.columns ?? []);

            return (
              <div key={t.fullName} className="schema-table-item">
                <div
                  className="schema-table-header"
                  onClick={() => setExpandedTable(isExpanded ? null : t.fullName)}
                >
                  <span className="table-icon">{t.tableType === "VIEW" ? "V" : "T"}</span>
                  <strong>{t.fullName}</strong>
                  <span className="table-meta">
                    {t.columns.length} cols
                    {t.foreignKeys.length > 0 && ` | ${t.foreignKeys.length} FKs`}
                    {t.estimatedRowCount !== undefined && ` | ~${t.estimatedRowCount.toLocaleString()} rows`}
                  </span>
                  {t.tableComment && <span className="table-comment">{t.tableComment}</span>}
                  <span className="expand-icon">{isExpanded ? "\u25BC" : "\u25B6"}</span>
                </div>

                {isExpanded && (
                  <div className="schema-table-detail">
                    {/* Columns */}
                    <table className="schema-columns-table">
                      <thead>
                        <tr>
                          <th>#</th><th>Column</th><th>Type</th><th>Nullable</th><th>Key</th><th>Default</th><th>Comment</th>
                        </tr>
                      </thead>
                      <tbody>
                        {t.columns.map((c) => (
                          <tr key={c.columnName} className={pkCols.has(c.columnName) ? "pk-row" : ""}>
                            <td className="col-ordinal">{c.ordinalPosition}</td>
                            <td>
                              <code>{c.columnName}</code>
                              {c.isAutoIncrement && <span className="badge-mini">auto</span>}
                            </td>
                            <td><code className="type-code">{c.fullDataType}</code></td>
                            <td>{c.isNullable ? "YES" : "NO"}</td>
                            <td>
                              {pkCols.has(c.columnName) && <span className="badge-mini pk">PK</span>}
                              {t.foreignKeys.some((fk) => fk.columns.includes(c.columnName)) && <span className="badge-mini fk">FK</span>}
                            </td>
                            <td className="default-val">{c.columnDefault ? <code>{c.columnDefault}</code> : "-"}</td>
                            <td className="comment-val">{c.columnComment || "-"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>

                    {/* Primary Key */}
                    {t.primaryKey && (
                      <div className="constraint-section">
                        <h4>Primary Key</h4>
                        <div className="constraint-item">
                          <code>{t.primaryKey.constraintName}</code>: ({t.primaryKey.columns.join(", ")})
                        </div>
                      </div>
                    )}

                    {/* Foreign Keys */}
                    {t.foreignKeys.length > 0 && (
                      <div className="constraint-section">
                        <h4>Foreign Keys ({t.foreignKeys.length})</h4>
                        {t.foreignKeys.map((fk) => (
                          <div key={fk.constraintName} className="constraint-item">
                            <code>{fk.constraintName}</code>
                            <br />
                            ({fk.columns.join(", ")}) &rarr; <strong>{fk.referencedSchema}.{fk.referencedTable}</strong>({fk.referencedColumns.join(", ")})
                            {(fk.onDelete || fk.onUpdate) && (
                              <span className="fk-actions">
                                {fk.onDelete && ` ON DELETE ${fk.onDelete}`}
                                {fk.onUpdate && ` ON UPDATE ${fk.onUpdate}`}
                              </span>
                            )}
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Indexes */}
                    {t.indexes.length > 0 && (
                      <div className="constraint-section">
                        <h4>Indexes ({t.indexes.length})</h4>
                        {t.indexes.map((idx) => (
                          <div key={idx.indexName} className="constraint-item">
                            <code>{idx.indexName}</code>
                            {idx.isUnique && <span className="badge-mini">unique</span>}
                            : ({idx.columns.join(", ")})
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
