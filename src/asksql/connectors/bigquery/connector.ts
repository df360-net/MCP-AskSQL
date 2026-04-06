/**
 * BigQuery Connector — Layer 2
 *
 * Reads metadata from BigQuery via INFORMATION_SCHEMA (free — 0 bytes billed).
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 * Also handles query execution and sample collection.
 *
 * Uses @google-cloud/bigquery (official SDK, stateless REST/gRPC).
 * Connection string: bigquery://project-id?keyFile=/path/to/key.json&location=US
 *
 * BigQuery hierarchy: Project → Dataset → Table (dataset = schema in AskSQL terms)
 */

import { BigQuery } from "@google-cloud/bigquery";
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
  DiscoveredPrimaryKey,
  DiscoveredForeignKey,
  SampleCollectionRequest,
  SampleCollectionResult,
  DiscoveredColumnSample,
} from "../../core/connector/discovery-types.js";

const BIGQUERY_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: "`",
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "CURRENT_TIMESTAMP()",
  dateDiffHint: "Use DATE_DIFF(date1, date2, DAY) or DATE_SUB(CURRENT_DATE(), INTERVAL N DAY) for date arithmetic. Note: DATE_DIFF arg order is end, start, granularity.",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "CRITICAL: In BigQuery, backtick each identifier SEPARATELY. Correct: `analytics`.`MY_TABLE` — Wrong: `analytics.MY_TABLE`. The dot must be OUTSIDE the backticks.",
    "BigQuery is CASE-SENSITIVE for table and column names. Use EXACT case from the schema — do not lowercase.",
    "BigQuery has no ILIKE — use LOWER(col) LIKE LOWER('%pattern%') for case-insensitive matching",
    "Use SAFE_CAST(col AS type) for safe casting that returns NULL on failure",
    "Use UNNEST() to flatten ARRAY columns into rows",
    "BigQuery uses STRUCT and ARRAY types — access nested fields with dot notation: col.field",
    "When querying partitioned tables, include a WHERE filter on the partition column to reduce bytes scanned",
    "Use QUALIFY with window functions to filter without subquery",
  ],
};

const EXCLUDED_DATASETS = new Set([
  "INFORMATION_SCHEMA",
]);

// Skip these complex types during sample collection
const UNSAMPLEABLE_TYPES = new Set([
  "STRUCT", "ARRAY", "GEOGRAPHY", "JSON", "BYTES", "RECORD",
]);

export class BigQueryConnector implements AskSQLConnector {
  readonly type: ConnectorType = "bigquery";
  readonly displayName = "Google BigQuery";
  readonly dialect: SQLDialect = "bigquery";
  readonly dialectHints = BIGQUERY_DIALECT_HINTS;

  private client: BigQuery | null = null;
  private projectId: string;
  private keyFilename: string | undefined;
  private location: string;
  private maxBytesBilled: string;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("BigQueryConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.projectId = (config.catalog as string) ?? parsed.projectId;
    this.keyFilename = parsed.keyFilename;
    this.location = parsed.location;
    // Default 1 GB safety cap on bytes billed per query
    this.maxBytesBilled = (config.maxBytesBilled as string) ?? "1073741824";
  }

  /**
   * Parse: bigquery://project-id[?keyFile=/path/to/key.json&location=US]
   */
  private parseConnectionString(url: string): {
    projectId: string; keyFilename: string | undefined; location: string;
  } {
    const match = url.match(/^bigquery:\/\/([^?/]+)(.*)$/i);
    if (!match) {
      throw new Error(
        "Invalid BigQuery connection string. Expected: bigquery://project-id[?keyFile=/path/to/key.json&location=US]"
      );
    }

    const projectId = match[1];
    const queryString = match[2]?.startsWith("?") ? match[2].slice(1) : "";
    const params = new URLSearchParams(queryString);

    return {
      projectId,
      keyFilename: params.get("keyFile") || undefined,
      location: params.get("location") || "US",
    };
  }

  private getClient(): BigQuery {
    if (!this.client) {
      const opts: Record<string, unknown> = {
        projectId: this.projectId,
        location: this.location,
      };
      if (this.keyFilename) {
        opts.keyFilename = this.keyFilename;
      }
      this.client = new BigQuery(opts);
    }
    return this.client;
  }

  private async query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    const [rows] = await this.getClient().query({
      query: sqlText,
      location: this.location,
    });
    return rows as T[];
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
        serverVersion: "Google BigQuery",
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^bigquery:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    // BigQuery is stateless (REST API) — no connection to close
    this.client = null;
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    // Determine which datasets to crawl
    const datasetNames = await this.discoverDatasetNames(options?.schemas);

    const schemas: DiscoveredSchema[] = [];
    for (const dataset of datasetNames) {
      const tables = await this.discoverTablesAndViews(dataset);

      // Apply include/exclude filters
      const includeRe = options?.includeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const excludeRe = options?.excludeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const filtered = tables.filter((t) => {
        if (includeRe?.length) return includeRe.some((r) => r.test(t.tableName));
        if (excludeRe?.length) return !excludeRe.some((r) => r.test(t.tableName));
        return true;
      });

      // Batch-load all metadata for this dataset
      const columnsByTable = await this.discoverAllColumns(dataset);
      const descriptionsByTable = await this.discoverTableDescriptions(dataset);
      const columnDescriptions = await this.discoverColumnDescriptions(dataset);
      const pksByTable = await this.discoverPrimaryKeys(dataset);
      const fksByTable = await this.discoverForeignKeys(dataset);
      const partitionInfo = await this.discoverPartitioning(dataset);

      for (const table of filtered) {
        table.columns = columnsByTable.get(table.tableName) ?? [];
        table.tableComment = descriptionsByTable.get(table.tableName) ?? table.tableComment;

        // Apply column descriptions
        const colDescs = columnDescriptions.get(table.tableName);
        if (colDescs) {
          for (const col of table.columns) {
            const desc = colDescs.get(col.columnName);
            if (desc) col.columnComment = desc;
          }
        }

        // PK
        const pk = pksByTable.get(table.tableName);
        if (pk) {
          table.primaryKey = pk;
          for (const col of table.columns) {
            if (pk.columns.includes(col.columnName)) col.isPrimaryKey = true;
          }
        }

        table.foreignKeys = fksByTable.get(table.tableName) ?? [];
        table.indexes = []; // BigQuery has no traditional indexes

        // Partitioning
        const partInfo = partitionInfo.get(table.tableName);
        if (partInfo) {
          table.partitioning = partInfo.partitioning;
          // Mark partition columns
          for (const col of table.columns) {
            if (col.columnName === partInfo.partitioning.field) {
              col.isPartitionColumn = true;
            }
          }
        }
      }

      schemas.push({ schemaName: dataset, tables: filtered });
    }

    return {
      databaseName: this.projectId,
      serverVersion: "Google BigQuery",
      databaseType: "BIGQUERY",
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

    const ident = (name: string) => `\`${name.replace(/`/g, "")}\``;

    for (const req of requests) {
      const samples: DiscoveredColumnSample[] = [];

      for (const colName of req.columns) {
        try {
          const maxDist = req.maxDistinctValues ?? 10;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          // Get distinct values
          const distinctRows = await this.query<{ value: string }>(
            `SELECT DISTINCT CAST(${col} AS STRING) AS value FROM ${qualified}
             WHERE ${col} IS NOT NULL ORDER BY 1 LIMIT ${maxDist + 1}`
          );

          const distinctCount = distinctRows.length;
          if (distinctCount > maxDist) {
            samples.push({ columnName: colName, distinctCount, sampleValues: [] });
            continue;
          }

          // Get stats
          const statsRows = await this.query<Record<string, unknown>>(
            `SELECT
              COUNT(DISTINCT ${col}) AS distinct_count,
              SAFE_DIVIDE(COUNTIF(${col} IS NULL), COUNT(*)) AS null_fraction,
              MIN(CAST(${col} AS STRING)) AS min_value,
              MAX(CAST(${col} AS STRING)) AS max_value,
              CAST(AVG(LENGTH(CAST(${col} AS STRING))) AS INT64) AS avg_length
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

      results.push({
        schemaName: req.schemaName,
        tableName: req.tableName,
        samples,
      });
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
      const [rows] = await this.getClient().query({
        query: safeSql,
        location: this.location,
        maximumBytesBilled: this.maxBytesBilled,
        jobTimeoutMs: timeoutMs,
      });

      const truncated = rows.length > maxRows;
      const rawRows = truncated ? rows.slice(0, maxRows) : rows;

      // BigQuery returns DATE/TIMESTAMP/DATETIME as objects with a `value` property.
      // Serialize them to plain strings so the UI can render them.
      const resultRows = rawRows.map((row: Record<string, unknown>) => {
        const out: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(row)) {
          out[key] = val != null && typeof val === "object" && "value" in (val as Record<string, unknown>)
            ? (val as Record<string, unknown>).value
            : val;
        }
        return out;
      });

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
      throw new Error(
        (err instanceof Error && (err.message.includes("timeout") || err.message.includes("Timeout")))
          ? `Query timed out after ${timeoutMs}ms`
          : (err instanceof Error ? err.message : "Query execution failed")
      );
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (via INFORMATION_SCHEMA — free)
  // ═══════════════════════════════════════════════════════════════

  private async discoverDatasetNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) return configSchemas;

    const rows = await this.query<{ schema_name: string }>(
      `SELECT schema_name FROM \`${this.projectId}\`.INFORMATION_SCHEMA.SCHEMATA ORDER BY schema_name`
    );

    return rows
      .map((r) => r.schema_name)
      .filter((s) => !EXCLUDED_DATASETS.has(s));
  }

  private async discoverTablesAndViews(dataset: string): Promise<DiscoveredTable[]> {
    // Tables metadata
    const tableRows = await this.query<Record<string, unknown>>(
      `SELECT table_name, table_type
       FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.TABLES
       WHERE table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW', 'EXTERNAL')
       ORDER BY table_name`
    );

    // Row counts and sizes from __TABLES__ (also free)
    let statsMap = new Map<string, { rowCount: number; sizeBytes: number }>();
    try {
      const statsRows = await this.query<Record<string, unknown>>(
        `SELECT table_id, row_count, size_bytes FROM \`${this.projectId}.${dataset}.__TABLES__\``
      );
      for (const r of statsRows) {
        statsMap.set(r.table_id as string, {
          rowCount: Number(r.row_count ?? 0),
          sizeBytes: Number(r.size_bytes ?? 0),
        });
      }
    } catch { /* __TABLES__ may not be available */ }

    return tableRows.map((r) => {
      const tableName = r.table_name as string;
      const tableType = r.table_type as string;
      const stats = statsMap.get(tableName);

      return {
        tableName,
        tableType: (tableType.includes("VIEW") ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
        detailedTableType: tableType,
        estimatedRowCount: stats?.rowCount,
        sizeBytes: stats?.sizeBytes,
        tableComment: undefined,
        columns: [],
        foreignKeys: [],
        indexes: [],
      };
    });
  }

  /**
   * Batch-load ALL columns for a dataset in 1 query.
   */
  private async discoverAllColumns(dataset: string): Promise<Map<string, DiscoveredColumn[]>> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT table_name, column_name, ordinal_position, data_type,
              is_nullable, column_default, is_partitioning_column
       FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.COLUMNS
       ORDER BY table_name, ordinal_position`
    );

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = r.table_name as string;
      if (!map.has(tableName)) map.set(tableName, []);

      const dataType = (r.data_type as string) ?? "STRING";
      const columnName = r.column_name as string;

      map.get(tableName)!.push({
        columnName,
        ordinalPosition: Number(r.ordinal_position ?? 0),
        dataType,
        fullDataType: dataType,
        isNullable: (r.is_nullable as string)?.toUpperCase() !== "NO",
        columnDefault: (r.column_default as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement: false,
        isPartitionColumn: (r.is_partitioning_column as string)?.toUpperCase() === "YES",
        // STRUCT fields appear with dot notation (e.g., "address.city")
        fieldPath: columnName.includes(".") ? columnName : undefined,
      });
    }
    return map;
  }

  /**
   * Batch-load table descriptions from TABLE_OPTIONS.
   */
  private async discoverTableDescriptions(dataset: string): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    try {
      const rows = await this.query<Record<string, unknown>>(
        `SELECT table_name, option_value
         FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.TABLE_OPTIONS
         WHERE option_name = 'description'
           AND option_value IS NOT NULL
           AND option_value != ''`
      );
      for (const r of rows) {
        map.set(r.table_name as string, r.option_value as string);
      }
    } catch { /* non-fatal */ }
    return map;
  }

  /**
   * Batch-load column descriptions from COLUMN_FIELD_PATHS.
   */
  private async discoverColumnDescriptions(dataset: string): Promise<Map<string, Map<string, string>>> {
    const map = new Map<string, Map<string, string>>();
    try {
      const rows = await this.query<Record<string, unknown>>(
        `SELECT table_name, column_name, description
         FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
         WHERE description IS NOT NULL AND description != ''`
      );
      for (const r of rows) {
        const tableName = r.table_name as string;
        if (!map.has(tableName)) map.set(tableName, new Map());
        map.get(tableName)!.set(r.column_name as string, r.description as string);
      }
    } catch { /* non-fatal */ }
    return map;
  }

  /**
   * Batch-load primary keys for all tables in a dataset.
   */
  private async discoverPrimaryKeys(dataset: string): Promise<Map<string, DiscoveredPrimaryKey>> {
    const map = new Map<string, DiscoveredPrimaryKey>();
    try {
      const rows = await this.query<Record<string, unknown>>(
        `SELECT tc.table_name, tc.constraint_name, kcu.column_name, kcu.ordinal_position
         FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         JOIN \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
           ON kcu.constraint_name = tc.constraint_name
           AND kcu.table_schema = tc.table_schema
           AND kcu.table_catalog = tc.table_catalog
         WHERE tc.constraint_type = 'PRIMARY KEY'
         ORDER BY tc.table_name, kcu.ordinal_position`
      );
      for (const r of rows) {
        const tableName = r.table_name as string;
        if (!map.has(tableName)) {
          map.set(tableName, { constraintName: r.constraint_name as string, columns: [] });
        }
        map.get(tableName)!.columns.push(r.column_name as string);
      }
    } catch { /* PK constraints may not be available */ }
    return map;
  }

  /**
   * Batch-load foreign keys for all tables in a dataset.
   */
  private async discoverForeignKeys(dataset: string): Promise<Map<string, DiscoveredForeignKey[]>> {
    const map = new Map<string, DiscoveredForeignKey[]>();
    try {
      const rows = await this.query<Record<string, unknown>>(
        `SELECT
           tc.table_name,
           tc.constraint_name,
           kcu.column_name,
           kcu.ordinal_position,
           ctu.table_schema AS ref_dataset,
           ctu.table_name AS ref_table_name
         FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         JOIN \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
           ON kcu.constraint_name = tc.constraint_name
           AND kcu.table_schema = tc.table_schema
           AND kcu.table_catalog = tc.table_catalog
         JOIN \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu
           ON ctu.constraint_name = tc.constraint_name
           AND ctu.table_schema = tc.table_schema
           AND ctu.table_name != tc.table_name
         WHERE tc.constraint_type = 'FOREIGN KEY'
         ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position`
      );

      // Group by table + constraint
      const fkMap = new Map<string, {
        tableName: string; constraintName: string;
        columns: string[]; refDataset: string; refTable: string;
      }>();

      for (const r of rows) {
        const tableName = r.table_name as string;
        const constraintName = r.constraint_name as string;
        const key = `${tableName}.${constraintName}`;

        if (!fkMap.has(key)) {
          fkMap.set(key, {
            tableName,
            constraintName,
            columns: [],
            refDataset: (r.ref_dataset as string) ?? dataset,
            refTable: r.ref_table_name as string,
          });
        }
        fkMap.get(key)!.columns.push(r.column_name as string);
      }

      for (const fk of fkMap.values()) {
        if (!map.has(fk.tableName)) map.set(fk.tableName, []);
        map.get(fk.tableName)!.push({
          constraintName: fk.constraintName,
          columns: fk.columns,
          referencedSchema: fk.refDataset,
          referencedTable: fk.refTable,
          referencedColumns: [], // BigQuery CONSTRAINT_TABLE_USAGE doesn't expose ref column names
        });
      }
    } catch { /* FK constraints may not be available */ }
    return map;
  }

  /**
   * Discover partitioning and clustering info from TABLE_OPTIONS.
   */
  private async discoverPartitioning(dataset: string): Promise<Map<string, {
    partitioning: { field: string; type: string; expirationMs?: number; requirePartitionFilter?: boolean };
  }>> {
    const map = new Map<string, {
      partitioning: { field: string; type: string; expirationMs?: number; requirePartitionFilter?: boolean };
    }>();

    try {
      // Get partition columns
      const partCols = await this.query<Record<string, unknown>>(
        `SELECT table_name, column_name
         FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.COLUMNS
         WHERE is_partitioning_column = 'YES'`
      );
      const partColByTable = new Map<string, string>();
      for (const r of partCols) {
        partColByTable.set(r.table_name as string, r.column_name as string);
      }

      // Get partition options
      const optRows = await this.query<Record<string, unknown>>(
        `SELECT table_name, option_name, option_value
         FROM \`${this.projectId}.${dataset}\`.INFORMATION_SCHEMA.TABLE_OPTIONS
         WHERE option_name IN ('time_partitioning_type', 'time_partitioning_expiration_ms', 'require_partition_filter')`
      );

      const optsByTable = new Map<string, Map<string, string>>();
      for (const r of optRows) {
        const tableName = r.table_name as string;
        if (!optsByTable.has(tableName)) optsByTable.set(tableName, new Map());
        optsByTable.get(tableName)!.set(r.option_name as string, r.option_value as string);
      }

      // Combine
      for (const [tableName, colName] of partColByTable) {
        const opts = optsByTable.get(tableName);
        const partType = opts?.get("time_partitioning_type")?.replace(/"/g, "") ?? "DAY";
        const expMs = opts?.get("time_partitioning_expiration_ms");
        const requireFilter = opts?.get("require_partition_filter");

        map.set(tableName, {
          partitioning: {
            field: colName,
            type: partType,
            expirationMs: expMs ? Number(expMs) : undefined,
            requirePartitionFilter: requireFilter === "true",
          },
        });
      }
    } catch { /* non-fatal */ }
    return map;
  }
}
