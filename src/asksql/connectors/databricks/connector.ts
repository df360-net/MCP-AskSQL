/**
 * Databricks Connector — Layer 2
 *
 * Reads metadata from Databricks Unity Catalog via INFORMATION_SCHEMA.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 * Also handles query execution and sample collection.
 *
 * Uses @databricks/sql (Thrift-based driver).
 * Connection string: databricks://token:dapi_xxx@hostname:443/http_path
 *
 * Also handles query execution and sample collection.
 */

import { DBSQLClient } from "@databricks/sql";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type IDBSQLSession = any;
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
  DiscoveredIndex,
  SampleCollectionRequest,
  SampleCollectionResult,
  DiscoveredColumnSample,
} from "../../core/connector/discovery-types.js";

const DATABRICKS_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: "`",
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "CURRENT_TIMESTAMP()",
  dateDiffHint: "Use DATEDIFF(day, date1, date2) or DATE_SUB(CURRENT_DATE(), N) for date arithmetic",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "Use backticks for identifiers: `schema`.`table`.`column`",
    "Use CAST(col AS STRING) for type conversions",
    "Use length() instead of LEN() for string length",
    "Use upper()/lower() for case conversions",
    "Databricks has native BOOLEAN type — use TRUE/FALSE",
    "Use LIMIT for row limiting (standard SQL syntax)",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "information_schema",
  "default",
]);

export class DatabricksConnector implements AskSQLConnector {
  readonly type: ConnectorType = "databricks";
  readonly displayName = "Databricks";
  readonly dialect: SQLDialect = "databricks-sql";
  readonly dialectHints = DATABRICKS_DIALECT_HINTS;

  private client: DBSQLClient;
  private session: IDBSQLSession | null = null;
  private hostname: string;
  private httpPath: string;
  private token: string;
  private catalog: string;
  private maxSampleValues: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("DatabricksConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.hostname = parsed.hostname;
    this.httpPath = parsed.httpPath;
    this.token = parsed.token;
    this.catalog = (config.catalog as string) ?? parsed.catalog;
    this.client = new DBSQLClient();
    this.maxSampleValues = (config.maxSampleValues as number) ?? 20;
  }

  /**
   * Parse: databricks://token:dapi_xxx@hostname:443/sql/1.0/warehouses/warehouse_id
   * or:   databricks://token:dapi_xxx@hostname/sql/1.0/warehouses/warehouse_id
   * Catalog can be passed as config.schemas[0] or defaults to "main"
   */
  private parseConnectionString(url: string): {
    token: string; hostname: string; httpPath: string; catalog: string;
  } {
    // databricks://token:dapi_xxx@hostname[:port]/http/path
    const match = url.match(/^databricks:\/\/([^:]+):([^@]+)@([^:/]+)(?::\d+)?\/(.+)$/i);
    if (!match) {
      throw new Error(
        "Invalid Databricks connection string. Expected: databricks://token:dapi_xxx@hostname/sql/1.0/warehouses/warehouse_id"
      );
    }
    return {
      token: decodeURIComponent(match[2]),
      hostname: match[3],
      httpPath: `/${match[4]}`,
      catalog: "main", // Override via schemas config
    };
  }

  private async getSession(): Promise<IDBSQLSession> {
    if (!this.session) {
      await this.client.connect({
        host: this.hostname,
        path: this.httpPath,
        token: this.token,
      });
      this.session = await this.client.openSession({
        initialCatalog: this.catalog,
      });
    }
    return this.session;
  }

  private async query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    const session = await this.getSession();
    const operation = await session.executeStatement(sqlText);
    const result = await operation.fetchAll();
    await operation.close();
    return result as T[];
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const rows = await this.query<Record<string, unknown>>(
        "SELECT current_catalog() AS catalog_name, current_version() AS version"
      );
      const row = rows[0];
      return {
        success: true,
        serverVersion: `Databricks Runtime ${row?.version ?? "unknown"}`,
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^databricks:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.session) {
      await this.session.close();
      this.session = null;
    }
    await this.client.close();
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    // Get version
    let serverVersion = "Databricks";
    try {
      const vRows = await this.query<Record<string, unknown>>(
        "SELECT current_version() AS version"
      );
      serverVersion = `Databricks Runtime ${vRows[0]?.version ?? ""}`;
    } catch { /* use default */ }

    // Determine which schemas to crawl
    const schemaNames = await this.discoverSchemaNames(options?.schemas);

    const schemas: DiscoveredSchema[] = [];
    for (const schemaName of schemaNames) {
      const tables = await this.discoverTablesAndViews(schemaName);

      // Apply include/exclude filters
      const includeRe = options?.includeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const excludeRe = options?.excludeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const filtered = tables.filter((t) => {
        if (includeRe?.length) return includeRe.some((r) => r.test(t.tableName));
        if (excludeRe?.length) return !excludeRe.some((r) => r.test(t.tableName));
        return true;
      });

      // Batch-load ALL columns for this schema in 1 query (instead of N per-table queries)
      const columnsByTable = await this.discoverAllColumns(schemaName);
      for (const table of filtered) {
        table.columns = columnsByTable.get(table.tableName) ?? [];
        // Databricks doesn't enforce PK/FK/Index constraints
        table.primaryKey = undefined;
        table.foreignKeys = [];
        table.indexes = [];
      }

      schemas.push({ schemaName, tables: filtered });
    }

    return {
      databaseName: this.catalog,
      serverVersion,
      databaseType: "DATABRICKS",
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

    const ident = (name: string) => `\`${name.replace(/`/g, "``")}\``;

    for (const req of requests) {
      const samples: DiscoveredColumnSample[] = [];

      for (const colName of req.columns) {
        try {
          const maxDist = req.maxDistinctValues ?? this.maxSampleValues;
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
              CAST(SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / GREATEST(COUNT(*), 1) AS null_fraction,
              MIN(CAST(${col} AS STRING)) AS min_value,
              MAX(CAST(${col} AS STRING)) AS max_value,
              AVG(length(CAST(${col} AS STRING))) AS avg_length
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
      const result = await Promise.race([
        this.query<Record<string, unknown>>(safeSql),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error(`Query timed out after ${timeoutMs}ms`)), timeoutMs),
        ),
      ]);

      const truncated = result.length > maxRows;
      const rows = truncated ? result.slice(0, maxRows) : result;

      const columns = rows.length > 0
        ? Object.keys(rows[0]).map((name) => ({
            name,
            type: typeof rows[0][name] === "number" ? "number"
              : rows[0][name] instanceof Date ? "date"
              : "string",
          }))
        : [];

      return { rows, columns, rowCount: rows.length, truncated, executionTimeMs: Date.now() - start };
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
      `SELECT schema_name FROM ${this.catalog}.information_schema.schemata ORDER BY schema_name`
    );

    return rows
      .map((r) => r.schema_name)
      .filter((s) => !EXCLUDED_SCHEMAS.has(s));
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT table_name, table_type, comment
       FROM ${this.catalog}.information_schema.tables
       WHERE table_schema = '${schemaName}'
         AND table_catalog = '${this.catalog}'
       ORDER BY table_name`
    );

    return rows.map((r) => ({
      tableName: r.table_name as string,
      tableType: ((r.table_type as string)?.toUpperCase().includes("VIEW") ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
      detailedTableType: r.table_type as string,
      tableComment: (r.comment as string) || undefined,
      columns: [],
      foreignKeys: [],
      indexes: [],
    }));
  }

  /**
   * Batch-load ALL columns for a schema in 1 query.
   * Returns a Map of tableName → columns (eliminates N+1 per-table queries).
   */
  private async discoverAllColumns(schemaName: string): Promise<Map<string, DiscoveredColumn[]>> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT table_name, column_name, ordinal_position, data_type, full_data_type,
              is_nullable, column_default, character_maximum_length,
              numeric_precision, numeric_scale, comment
       FROM ${this.catalog}.information_schema.columns
       WHERE table_schema = '${schemaName}'
         AND table_catalog = '${this.catalog}'
       ORDER BY table_name, ordinal_position`
    );

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = r.table_name as string;
      if (!map.has(tableName)) map.set(tableName, []);

      const dataType = (r.data_type as string)?.toLowerCase() ?? "string";
      const fullDataType = (r.full_data_type as string) ?? dataType;

      map.get(tableName)!.push({
        columnName: r.column_name as string,
        ordinalPosition: Number(r.ordinal_position ?? 0),
        dataType,
        fullDataType,
        isNullable: (r.is_nullable as string)?.toUpperCase() !== "NO",
        columnDefault: (r.column_default as string) || undefined,
        characterMaxLength: r.character_maximum_length != null ? Number(r.character_maximum_length) : undefined,
        numericPrecision: r.numeric_precision != null ? Number(r.numeric_precision) : undefined,
        numericScale: r.numeric_scale != null ? Number(r.numeric_scale) : undefined,
        columnComment: (r.comment as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement: false,
      });
    }
    return map;
  }
}
