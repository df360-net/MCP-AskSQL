/**
 * Dremio Connector — Layer 2
 *
 * Dual-mode connectivity:
 *   - Dremio Cloud: REST API (api.dremio.cloud) — zero dependencies
 *   - Dremio Software: PostgreSQL wire protocol (port 31010) — reuses postgres.js
 *
 * Connection strings:
 *   Cloud:  dremio://token:PAT@api.dremio.cloud/source?projectId=xxx
 *   Local:  dremio://user:password@host:31010[/catalog]
 *
 * Dremio hierarchy: Source → Space → Folder → Dataset
 * INFORMATION_SCHEMA maps to: TABLE_SCHEMA → TABLE_NAME
 */

import postgres from "postgres";
import type {
  AskSQLConnector,
  ConnectorType,
  SQLDialect,
  SQLDialectHints,
  DiscoveryOptions,
  RawQueryResult,
  ExecuteOptions,
  ConnectionTestResult,
} from "../../core/index.js";

import type {
  DiscoveredDatabase,
  DiscoveredSchema,
  DiscoveredTable,
  DiscoveredColumn,
  SampleCollectionRequest,
  SampleCollectionResult,
  DiscoveredColumnSample,
} from "../../core/connector/discovery-types.js";

const DREMIO_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: '"',
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "CURRENT_TIMESTAMP",
  dateDiffHint: "Use TIMESTAMPDIFF(DAY, date1, date2) or DATE_SUB(CURRENT_DATE, INTERVAL '1' DAY) for date arithmetic",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "Dremio has NO ILIKE — use LOWER(col) LIKE LOWER('%pattern%') for case-insensitive matching",
    'Use double-quotes for identifiers: "schema"."table"."column"',
    "Use CAST(col AS VARCHAR) for type conversions to string",
    "Dremio is a query engine over data lakes — it does NOT enforce PK/FK constraints",
    "Use EXTRACT(field FROM date), DATE_TRUNC('day', ts) for date operations",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "INFORMATION_SCHEMA",
  "information_schema",
  "sys",
  "$scratch",
]);

// ---------------------------------------------------------------------------
// Dremio Cloud REST API client
// ---------------------------------------------------------------------------

interface DremioCloudConfig {
  mode: "cloud";
  apiHost: string;
  projectId: string;
  token: string;
  catalog: string;
}

interface DremioLocalConfig {
  mode: "local";
  pgUrl: string;
  catalog: string;
}

type DremioConfig = DremioCloudConfig | DremioLocalConfig;

export class DremioConnector implements AskSQLConnector {
  readonly type: ConnectorType = "dremio";
  readonly displayName = "Dremio";
  readonly dialect: SQLDialect = "postgresql";
  readonly dialectHints = DREMIO_DIALECT_HINTS;

  private config: DremioConfig;
  private sql: postgres.Sql | null = null; // Only used in local mode
  private jobTimeoutMs: number;
  private jobPollIntervalMs: number;
  private maxSampleValues: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("DremioConnector requires a connectionString");
    }

    this.config = this.parseConnectionString(
      connectionString,
      config.catalog as string | undefined,
      config.projectId as string | undefined,
    );

    // Initialize PG wire connection for local mode
    if (this.config.mode === "local") {
      this.sql = postgres(this.config.pgUrl, {
        max: (config.poolSize as number) ?? 3,
        idle_timeout: ((config.idleTimeoutMs as number) ?? 20000) / 1000,
        connect_timeout: ((config.connectTimeoutMs as number) ?? 30000) / 1000,
        prepare: false, // Dremio doesn't support prepared statements
      });
    }

    this.jobTimeoutMs = (config.connectTimeoutMs as number) ?? 120000;
    this.jobPollIntervalMs = (config.jobPollIntervalMs as number) ?? 500;
    this.maxSampleValues = (config.maxSampleValues as number) ?? 10;
  }

  /**
   * Parse connection string — auto-detect cloud vs local.
   *
   * Cloud:  dremio://token:PAT@api.dremio.cloud/source?projectId=xxx
   * Local:  dremio://user:password@host[:port][/catalog]
   */
  private parseConnectionString(
    url: string,
    catalogOverride?: string,
    projectIdOverride?: string,
  ): DremioConfig {
    const match = url.match(/^dremio:\/\/([^:]+):([^@]+)@([^:/]+)(?::(\d+))?(?:\/([^?]*))?(?:\?(.+))?$/i);
    if (!match) {
      throw new Error(
        "Invalid Dremio connection string. Expected: dremio://user:password@host[:port][/catalog][?projectId=xxx]"
      );
    }

    const user = match[1];
    const pass = decodeURIComponent(match[2]);
    const host = match[3];
    const port = match[4];
    const catalog = catalogOverride ?? (match[5] ?? "");
    const params = new URLSearchParams(match[6] ?? "");
    const projectId = projectIdOverride ?? params.get("projectId") ?? "";

    // Detect cloud mode by hostname
    const isCloud = host.includes("dremio.cloud");

    if (isCloud) {
      if (!projectId) {
        throw new Error("Dremio Cloud requires a projectId. Add ?projectId=xxx to the connection string or set projectId in config.");
      }
      return {
        mode: "cloud",
        apiHost: `https://${host}`,
        projectId,
        token: pass, // PAT token
        catalog,
      };
    }

    // Local mode — rewrite to postgres://
    const pgPort = port ?? "31010";
    const pgUrl = `postgres://${user}:${encodeURIComponent(pass)}@${host}:${pgPort}/dremio`;

    return { mode: "local", pgUrl, catalog };
  }

  // ═══════════════════════════════════════════════════════════════
  // UNIFIED QUERY INTERFACE
  // ═══════════════════════════════════════════════════════════════

  /**
   * Execute SQL against Dremio — routes to REST API (cloud) or PG wire (local).
   */
  private async query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    if (this.config.mode === "cloud") {
      return this.cloudQuery<T>(sqlText);
    }
    // Local mode — use postgres.js
    const rows = await this.sql!.unsafe(sqlText);
    return [...rows] as T[];
  }

  /**
   * Dremio Cloud REST API — submit SQL job, poll for completion, fetch results.
   */
  private async cloudQuery<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    const cfg = this.config as DremioCloudConfig;
    const headers = {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${cfg.token}`,
    };

    // 1. Submit SQL job (Dremio Cloud uses project-scoped endpoint)
    const submitUrl = `${cfg.apiHost}/v0/projects/${cfg.projectId}/sql`;
    const submitRes = await fetch(submitUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({
        sql: sqlText,
        context: cfg.catalog ? [cfg.catalog] : undefined,
      }),
    });

    if (!submitRes.ok) {
      const errorBody = await submitRes.text();
      throw new Error(`Dremio API error (${submitRes.status}): ${errorBody}`);
    }

    const submitData = await submitRes.json() as { id: string };
    const jobId = submitData.id;

    // 2. Poll for job completion
    const maxWaitMs = this.jobTimeoutMs;
    const pollIntervalMs = this.jobPollIntervalMs;
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const statusUrl = `${cfg.apiHost}/v0/projects/${cfg.projectId}/job/${jobId}`;
      const statusRes = await fetch(statusUrl, { headers });
      if (!statusRes.ok) throw new Error(`Dremio job status error: ${statusRes.status}`);

      const statusData = await statusRes.json() as Record<string, unknown>;
      const jobState = statusData.jobState as string ?? statusData.state as string;

      if (jobState === "COMPLETED") {
        // 3. Fetch results
        const resultsUrl = `${cfg.apiHost}/v0/projects/${cfg.projectId}/job/${jobId}/results?offset=0&limit=500`;
        const resultsRes = await fetch(resultsUrl, { headers });
        if (!resultsRes.ok) {
          const errBody = await resultsRes.text();
          throw new Error(`Dremio results error (${resultsRes.status}): ${errBody}`);
        }

        const resultsData = await resultsRes.json() as Record<string, unknown>;

        const columns = (resultsData.columns ?? resultsData.schema) as Array<{ name: string }> | undefined;
        const rows = (resultsData.rows ?? []) as Array<Record<string, unknown>>;

        // Dremio Cloud v0 API returns rows as plain objects with lowercase keys — no transformation needed
        if (!columns || columns.length === 0) {
          return rows as T[];
        }

        // Normalize column names to lowercase (Dremio may return uppercase from INFORMATION_SCHEMA)
        return rows.map((row) => {
          const out: Record<string, unknown> = {};
          for (const col of columns) {
            out[col.name.toLowerCase()] = row[col.name] ?? null;
          }
          return out as T;
        });
      }

      if (jobState === "FAILED" || jobState === "CANCELED") {
        throw new Error((statusData.errorMessage as string) ?? `Dremio job ${jobState}`);
      }

      // Still running — wait and poll again
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }

    throw new Error("Dremio query timed out after 2 minutes");
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      await this.query("SELECT 1 AS connected");
      return {
        success: true,
        serverVersion: `Dremio ${this.config.mode === "cloud" ? "Cloud" : "Software"}`,
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^dremio:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.sql) {
      await this.sql.end();
      this.sql = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    const schemaNames = await this.discoverSchemaNames(options?.schemas);

    const schemas: DiscoveredSchema[] = [];
    for (const schemaName of schemaNames) {
      const tables = await this.discoverTablesAndViews(schemaName);

      const includeRe = options?.includeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const excludeRe = options?.excludeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const filtered = tables.filter((t) => {
        if (includeRe?.length) return includeRe.some((r) => r.test(t.tableName));
        if (excludeRe?.length) return !excludeRe.some((r) => r.test(t.tableName));
        return true;
      });

      const columnsByTable = await this.discoverAllColumns(schemaName);
      for (const table of filtered) {
        table.columns = columnsByTable.get(table.tableName) ?? [];
        table.primaryKey = undefined;
        table.foreignKeys = [];
        table.indexes = [];
      }

      schemas.push({ schemaName, tables: filtered });
    }

    return {
      databaseName: this.config.catalog || "dremio",
      serverVersion: `Dremio ${this.config.mode === "cloud" ? "Cloud" : "Software"}`,
      databaseType: "DREMIO",
      schemas,
      discoveredAt: new Date(),
      durationMs: Date.now() - start,
    };
  }

  // ═══════════════════════════════════════════════════════════════
  // SAMPLE COLLECTION
  // ═══════════════════════════════════════════════════════════════

  async collectSamples(requests: SampleCollectionRequest[]): Promise<SampleCollectionResult[]> {
    const results: SampleCollectionResult[] = [];

    const ident = (name: string) => `"${name.replace(/"/g, '""')}"`;

    for (const req of requests) {
      const samples: DiscoveredColumnSample[] = [];

      for (const colName of req.columns) {
        try {
          const maxDist = req.maxDistinctValues ?? this.maxSampleValues;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          const distinctRows = await this.query<{ value: string }>(
            `SELECT DISTINCT CAST(${col} AS VARCHAR) AS value FROM ${qualified}
             WHERE ${col} IS NOT NULL ORDER BY 1 LIMIT ${maxDist + 1}`
          );

          const distinctCount = distinctRows.length;
          if (distinctCount > maxDist) {
            samples.push({ columnName: colName, distinctCount, sampleValues: [] });
            continue;
          }

          const statsRows = await this.query<Record<string, unknown>>(
            `SELECT
              COUNT(DISTINCT ${col}) AS distinct_count,
              CAST(SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / GREATEST(COUNT(*), 1) AS null_fraction,
              MIN(CAST(${col} AS VARCHAR)) AS min_value,
              MAX(CAST(${col} AS VARCHAR)) AS max_value,
              CAST(AVG(CHAR_LENGTH(CAST(${col} AS VARCHAR))) AS INTEGER) AS avg_length
            FROM ${qualified}`
          );
          const stats = statsRows[0] ?? {};

          samples.push({
            columnName: colName,
            distinctCount: Number(stats.distinct_count ?? distinctCount),
            nullFraction: stats.null_fraction != null ? Number(stats.null_fraction) : undefined,
            sampleValues: distinctRows.map((r) => String(r.value)),
            minValue: stats.min_value != null ? String(stats.min_value) : undefined,
            maxValue: stats.max_value != null ? String(stats.max_value) : undefined,
            avgLength: stats.avg_length != null ? Number(stats.avg_length) : undefined,
          });
        } catch (err: unknown) {
          console.warn(`[SAMPLE] Skip ${req.schemaName}.${req.tableName}.${colName}: ${(err instanceof Error ? err.message.substring(0, 80) : String(err))}`);
        }
      }

      results.push({ schemaName: req.schemaName, tableName: req.tableName, samples });
    }

    return results;
  }

  // ═══════════════════════════════════════════════════════════════
  // QUERY EXECUTION
  // ═══════════════════════════════════════════════════════════════

  async executeQuery(sqlQuery: string, options?: ExecuteOptions): Promise<RawQueryResult> {
    const maxRows = options?.maxRows ?? 5000;
    const timeoutMs = options?.timeoutMs ?? 30000;

    const upperSql = sqlQuery.toUpperCase();
    const hasLimit = upperSql.includes("LIMIT");
    const safeSql = hasLimit
      ? sqlQuery
      : `${sqlQuery.replace(/;?\s*$/, "")} LIMIT ${maxRows + 1}`;

    const start = Date.now();
    try {
      const rows = await Promise.race([
        this.query<Record<string, unknown>>(safeSql),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Query timed out after ${timeoutMs}ms`)), timeoutMs),
        ),
      ]);

      const truncated = rows.length > maxRows;
      const resultRows = truncated ? rows.slice(0, maxRows) : rows;

      const columns = resultRows.length > 0
        ? Object.keys(resultRows[0]).map((name) => ({
            name,
            type: typeof resultRows[0][name] === "number" ? "number"
              : resultRows[0][name] instanceof Date ? "date"
              : "string",
          }))
        : [];

      return { rows: resultRows, columns, rowCount: resultRows.length, truncated, executionTimeMs: Date.now() - start };
    } catch (err: unknown) {
      throw new Error(err instanceof Error ? err.message : "Query execution failed");
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (via INFORMATION_SCHEMA)
  // ═══════════════════════════════════════════════════════════════

  private async discoverSchemaNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) return configSchemas;

    const rows = await this.query<{ schema_name: string }>(
      `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME`
    );

    return rows
      .map((r) => r.schema_name)
      .filter((s) => !EXCLUDED_SCHEMAS.has(s));
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    const safe = schemaName.replace(/'/g, "''");
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, TABLE_TYPE
       FROM INFORMATION_SCHEMA."TABLES"
       WHERE TABLE_SCHEMA = '${safe}'
       ORDER BY TABLE_NAME`
    );

    return rows.map((r) => ({
      tableName: r.table_name as string,
      tableType: ((r.table_type as string)?.toUpperCase().includes("VIEW") ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
      detailedTableType: r.table_type as string,
      columns: [],
      foreignKeys: [],
      indexes: [],
    }));
  }

  private async discoverAllColumns(schemaName: string): Promise<Map<string, DiscoveredColumn[]>> {
    const safe = schemaName.replace(/'/g, "''");
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE,
              IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH,
              NUMERIC_PRECISION, NUMERIC_SCALE
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = '${safe}'
       ORDER BY TABLE_NAME, ORDINAL_POSITION`
    );

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = r.table_name as string;
      if (!map.has(tableName)) map.set(tableName, []);

      const dataType = (r.data_type as string) ?? "VARCHAR";
      const charMaxLen = r.character_maximum_length != null ? Number(r.character_maximum_length) : undefined;
      const numPrec = r.numeric_precision != null ? Number(r.numeric_precision) : undefined;
      const numScale = r.numeric_scale != null ? Number(r.numeric_scale) : undefined;

      let fullDataType = dataType;
      if (numPrec != null && numScale != null && (dataType === "DECIMAL" || dataType === "NUMERIC")) {
        fullDataType = `${dataType}(${numPrec},${numScale})`;
      } else if (charMaxLen != null && dataType === "VARCHAR") {
        fullDataType = `VARCHAR(${charMaxLen})`;
      }

      map.get(tableName)!.push({
        columnName: r.column_name as string,
        ordinalPosition: Number(r.ordinal_position ?? 0),
        dataType,
        fullDataType,
        isNullable: (r.is_nullable as string)?.toUpperCase() !== "NO",
        columnDefault: (r.column_default as string) || undefined,
        characterMaxLength: charMaxLen,
        numericPrecision: numPrec,
        numericScale: numScale,
        isPrimaryKey: false,
        isAutoIncrement: false,
      });
    }
    return map;
  }
}
