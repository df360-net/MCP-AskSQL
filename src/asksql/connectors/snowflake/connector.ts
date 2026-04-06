/**
 * Snowflake Connector — Layer 2
 *
 * Reads metadata from Snowflake via INFORMATION_SCHEMA.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 * Also handles query execution and sample collection.
 *
 * Uses snowflake-sdk (official driver, callback-based — wrapped in Promises).
 * Connection string: snowflake://user:password@account/database/warehouse
 */

import snowflake from "snowflake-sdk";
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

const SNOWFLAKE_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: '"',
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: true,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "CURRENT_TIMESTAMP()",
  dateDiffHint: "Use DATEDIFF('day', date1, date2) or DATEADD('day', -N, CURRENT_DATE()) for date arithmetic",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "Use ILIKE for case-insensitive pattern matching",
    "Snowflake identifiers are case-insensitive unless double-quoted",
    "Use TRY_CAST(col AS type) for safe casting that returns NULL on failure",
    "Use FLATTEN() for semi-structured data (VARIANT/ARRAY/OBJECT columns)",
    "Use QUALIFY with window functions to filter without subquery",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "INFORMATION_SCHEMA",
]);

// Suppress snowflake-sdk's noisy console logging
snowflake.configure({ logLevel: "ERROR" });

export class SnowflakeConnector implements AskSQLConnector {
  readonly type: ConnectorType = "snowflake";
  readonly displayName = "Snowflake";
  readonly dialect: SQLDialect = "snowflake";
  readonly dialectHints = SNOWFLAKE_DIALECT_HINTS;

  private conn: snowflake.Connection | null = null;
  private account: string;
  private username: string;
  private password: string;
  private database: string;
  private warehouse: string | undefined;
  private role: string | undefined;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("SnowflakeConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.account = parsed.account;
    this.username = parsed.username;
    this.password = parsed.password;
    this.database = (config.catalog as string) ?? parsed.database;
    this.warehouse = parsed.warehouse;
    this.role = config.role as string | undefined;
  }

  /**
   * Parse: snowflake://user:password@account/database[/warehouse]
   * Account can be: xy12345, xy12345.us-east-1, or full hostname
   * Warehouse is optional — uses Snowflake user's default if omitted.
   */
  private parseConnectionString(url: string): {
    username: string; password: string; account: string; database: string; warehouse: string | undefined;
  } {
    const match = url.match(/^snowflake:\/\/([^:]+):([^@]+)@([^/]+)\/([^/]+?)(?:\/(.+))?$/i);
    if (!match) {
      throw new Error(
        "Invalid Snowflake connection string. Expected: snowflake://user:password@account/database[/warehouse]"
      );
    }
    // Strip .snowflakecomputing.com if present — sdk wants just the account identifier
    let account = match[3];
    account = account.replace(/\.snowflakecomputing\.com$/i, "");

    return {
      username: decodeURIComponent(match[1]),
      password: decodeURIComponent(match[2]),
      account,
      database: match[4],
      warehouse: match[5] || undefined,
    };
  }

  private async getConnection(): Promise<snowflake.Connection> {
    if (this.conn) return this.conn;

    this.conn = snowflake.createConnection({
      account: this.account,
      username: this.username,
      password: this.password,
      database: this.database,
      warehouse: this.warehouse,
      role: this.role,
      application: "AskSQL",
    });

    await new Promise<void>((resolve, reject) => {
      this.conn!.connect((err) => {
        if (err) reject(new Error(`Snowflake connect failed: ${err.message}`));
        else resolve();
      });
    });

    return this.conn;
  }

  private async query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    const conn = await this.getConnection();
    return new Promise((resolve, reject) => {
      conn.execute({
        sqlText,
        complete: (err, _stmt, rows) => {
          if (err) reject(err);
          else resolve((rows ?? []) as T[]);
        },
      });
    });
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const rows = await this.query<Record<string, unknown>>(
        "SELECT CURRENT_VERSION() AS VERSION, CURRENT_DATABASE() AS DB, CURRENT_WAREHOUSE() AS WH"
      );
      const row = rows[0];
      return {
        success: true,
        serverVersion: `Snowflake ${row?.VERSION ?? "unknown"}`,
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^snowflake:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.conn) {
      await new Promise<void>((resolve) => {
        this.conn!.destroy(() => resolve());
      });
      this.conn = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    // Get version
    let serverVersion = "Snowflake";
    try {
      const vRows = await this.query<Record<string, unknown>>(
        "SELECT CURRENT_VERSION() AS VERSION"
      );
      serverVersion = `Snowflake ${vRows[0]?.VERSION ?? ""}`;
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

      // Batch-load ALL columns for this schema in 1 query
      const columnsByTable = await this.discoverAllColumns(schemaName);

      // Batch-load PKs and FKs for this schema
      const pksByTable = await this.discoverPrimaryKeys(schemaName);
      const fksByTable = await this.discoverForeignKeys(schemaName);

      for (const table of filtered) {
        table.columns = columnsByTable.get(table.tableName) ?? [];

        // Mark PK columns
        const pk = pksByTable.get(table.tableName);
        if (pk) {
          table.primaryKey = pk;
          for (const col of table.columns) {
            if (pk.columns.includes(col.columnName)) {
              col.isPrimaryKey = true;
            }
          }
        }

        table.foreignKeys = fksByTable.get(table.tableName) ?? [];
        table.indexes = []; // Snowflake has no traditional indexes
      }

      schemas.push({ schemaName, tables: filtered });
    }

    return {
      databaseName: this.database,
      serverVersion,
      databaseType: "SNOWFLAKE",
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

    const ident = (name: string) => `"${name.replace(/"/g, "")}"`;

    for (const req of requests) {
      const samples: DiscoveredColumnSample[] = [];

      for (const colName of req.columns) {
        try {
          const maxDist = req.maxDistinctValues ?? 10;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          // Get distinct values
          const distinctRows = await this.query<{ VALUE: string }>(
            `SELECT DISTINCT TO_VARCHAR(${col}) AS VALUE FROM ${qualified}
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
              COUNT(DISTINCT ${col}) AS DISTINCT_COUNT,
              SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END)::FLOAT / GREATEST(COUNT(*), 1) AS NULL_FRACTION,
              MIN(TO_VARCHAR(${col})) AS MIN_VALUE,
              MAX(TO_VARCHAR(${col})) AS MAX_VALUE,
              AVG(LENGTH(TO_VARCHAR(${col})))::INT AS AVG_LENGTH
            FROM ${qualified}`
          );
          const stats = statsRows[0] ?? {};

          samples.push({
            columnName: colName,
            distinctCount: Number(stats.DISTINCT_COUNT ?? distinctCount),
            nullFraction: stats.NULL_FRACTION != null ? Number(stats.NULL_FRACTION) : undefined,
            sampleValues: distinctRows.map((r) => String(r.VALUE)),
            minValue: stats.MIN_VALUE != null ? String(stats.MIN_VALUE) : undefined,
            maxValue: stats.MAX_VALUE != null ? String(stats.MAX_VALUE) : undefined,
            avgLength: stats.AVG_LENGTH != null ? Number(stats.AVG_LENGTH) : undefined,
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
    const timeoutSec = Math.ceil(timeoutMs / 1000);

    const upperSql = sqlQuery.toUpperCase();
    const hasLimit = upperSql.includes("LIMIT");
    const safeSql = hasLimit
      ? sqlQuery
      : `${sqlQuery.replace(/;?\s*$/, "")} LIMIT ${maxRows + 1}`;

    const start = Date.now();
    try {
      // Set session-level statement timeout before executing the query
      await this.query(`ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ${timeoutSec}`);
      const result = await this.query<Record<string, unknown>>(safeSql);

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
      throw new Error(
        (err instanceof Error && (err.message.includes("timeout") || err.message.includes("STATEMENT_TIMEOUT")))
          ? `Query timed out after ${timeoutMs}ms`
          : (err instanceof Error ? err.message : "Query execution failed")
      );
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (via INFORMATION_SCHEMA)
  // ═══════════════════════════════════════════════════════════════

  private async discoverSchemaNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) return configSchemas;

    const rows = await this.query<{ SCHEMA_NAME: string }>(
      `SELECT SCHEMA_NAME FROM ${this.database}.INFORMATION_SCHEMA.SCHEMATA
       WHERE CATALOG_NAME = '${this.database}'
       ORDER BY SCHEMA_NAME`
    );

    return rows
      .map((r) => r.SCHEMA_NAME)
      .filter((s) => !EXCLUDED_SCHEMAS.has(s));
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, COMMENT, CLUSTERING_KEY
       FROM ${this.database}.INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = '${schemaName}'
         AND TABLE_CATALOG = '${this.database}'
         AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
       ORDER BY TABLE_NAME`
    );

    return rows.map((r) => ({
      tableName: r.TABLE_NAME as string,
      tableType: ((r.TABLE_TYPE as string)?.toUpperCase().includes("VIEW") ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
      detailedTableType: r.TABLE_TYPE as string,
      estimatedRowCount: r.ROW_COUNT != null ? Number(r.ROW_COUNT) : undefined,
      sizeBytes: r.BYTES != null ? Number(r.BYTES) : undefined,
      tableComment: (r.COMMENT as string) || undefined,
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
      `SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE,
              IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH,
              NUMERIC_PRECISION, NUMERIC_SCALE, COMMENT, IS_IDENTITY
       FROM ${this.database}.INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = '${schemaName}'
         AND TABLE_CATALOG = '${this.database}'
       ORDER BY TABLE_NAME, ORDINAL_POSITION`
    );

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = r.TABLE_NAME as string;
      if (!map.has(tableName)) map.set(tableName, []);

      const dataType = (r.DATA_TYPE as string) ?? "VARCHAR";
      const charMaxLen = r.CHARACTER_MAXIMUM_LENGTH != null ? Number(r.CHARACTER_MAXIMUM_LENGTH) : undefined;
      const numPrec = r.NUMERIC_PRECISION != null ? Number(r.NUMERIC_PRECISION) : undefined;
      const numScale = r.NUMERIC_SCALE != null ? Number(r.NUMERIC_SCALE) : undefined;

      // Build fullDataType: NUMBER(38,0), VARCHAR(256), etc.
      let fullDataType = dataType;
      if (numPrec != null && numScale != null && (dataType === "NUMBER" || dataType === "DECIMAL" || dataType === "NUMERIC")) {
        fullDataType = `${dataType}(${numPrec},${numScale})`;
      } else if (charMaxLen != null && charMaxLen > 0 && (dataType === "VARCHAR" || dataType === "CHAR" || dataType === "STRING")) {
        fullDataType = `${dataType}(${charMaxLen})`;
      }

      map.get(tableName)!.push({
        columnName: r.COLUMN_NAME as string,
        ordinalPosition: Number(r.ORDINAL_POSITION ?? 0),
        dataType,
        fullDataType,
        isNullable: (r.IS_NULLABLE as string)?.toUpperCase() !== "NO",
        columnDefault: (r.COLUMN_DEFAULT as string) || undefined,
        characterMaxLength: charMaxLen,
        numericPrecision: numPrec,
        numericScale: numScale,
        columnComment: (r.COMMENT as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement: (r.IS_IDENTITY as string)?.toUpperCase() === "YES",
      });
    }
    return map;
  }

  /**
   * Batch-load primary keys for all tables in a schema.
   */
  private async discoverPrimaryKeys(schemaName: string): Promise<Map<string, DiscoveredPrimaryKey>> {
    const map = new Map<string, DiscoveredPrimaryKey>();

    try {
      const rows = await this.query<Record<string, unknown>>(
        `SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
         FROM ${this.database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         JOIN ${this.database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
           ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
           AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
           AND kcu.TABLE_CATALOG = tc.TABLE_CATALOG
         WHERE tc.TABLE_SCHEMA = '${schemaName}'
           AND tc.TABLE_CATALOG = '${this.database}'
           AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
         ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION`
      );

      for (const r of rows) {
        const tableName = r.TABLE_NAME as string;
        if (!map.has(tableName)) {
          map.set(tableName, {
            constraintName: r.CONSTRAINT_NAME as string,
            columns: [],
          });
        }
        map.get(tableName)!.columns.push(r.COLUMN_NAME as string);
      }
    } catch {
      // PK discovery may fail on some Snowflake editions — non-fatal
    }

    return map;
  }

  /**
   * Batch-load foreign keys for all tables in a schema.
   */
  private async discoverForeignKeys(schemaName: string): Promise<Map<string, DiscoveredForeignKey[]>> {
    const map = new Map<string, DiscoveredForeignKey[]>();

    try {
      const rows = await this.query<Record<string, unknown>>(
        `SELECT
           tc.TABLE_NAME,
           tc.CONSTRAINT_NAME,
           kcu.COLUMN_NAME,
           kcu.ORDINAL_POSITION,
           rc.UNIQUE_CONSTRAINT_NAME,
           rc.UNIQUE_CONSTRAINT_SCHEMA,
           kcu2.TABLE_NAME AS REF_TABLE_NAME,
           kcu2.COLUMN_NAME AS REF_COLUMN_NAME
         FROM ${this.database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         JOIN ${this.database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
           ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
           AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
           AND kcu.TABLE_CATALOG = tc.TABLE_CATALOG
         JOIN ${this.database}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
           ON rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
           AND rc.CONSTRAINT_SCHEMA = tc.TABLE_SCHEMA
           AND rc.CONSTRAINT_CATALOG = tc.TABLE_CATALOG
         JOIN ${this.database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
           ON kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
           AND kcu2.TABLE_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
           AND kcu2.TABLE_CATALOG = tc.TABLE_CATALOG
           AND kcu2.ORDINAL_POSITION = kcu.ORDINAL_POSITION
         WHERE tc.TABLE_SCHEMA = '${schemaName}'
           AND tc.TABLE_CATALOG = '${this.database}'
           AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
         ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`
      );

      // Group by table + constraint
      const fkMap = new Map<string, {
        tableName: string; constraintName: string;
        columns: string[]; refSchema: string; refTable: string; refColumns: string[];
      }>();

      for (const r of rows) {
        const tableName = r.TABLE_NAME as string;
        const constraintName = r.CONSTRAINT_NAME as string;
        const key = `${tableName}.${constraintName}`;

        if (!fkMap.has(key)) {
          fkMap.set(key, {
            tableName,
            constraintName,
            columns: [],
            refSchema: (r.UNIQUE_CONSTRAINT_SCHEMA as string) ?? schemaName,
            refTable: r.REF_TABLE_NAME as string,
            refColumns: [],
          });
        }
        const fk = fkMap.get(key)!;
        fk.columns.push(r.COLUMN_NAME as string);
        fk.refColumns.push(r.REF_COLUMN_NAME as string);
      }

      for (const fk of fkMap.values()) {
        if (!map.has(fk.tableName)) map.set(fk.tableName, []);
        map.get(fk.tableName)!.push({
          constraintName: fk.constraintName,
          columns: fk.columns,
          referencedSchema: fk.refSchema,
          referencedTable: fk.refTable,
          referencedColumns: fk.refColumns,
        });
      }
    } catch {
      // FK discovery may fail on some Snowflake editions — non-fatal
    }

    return map;
  }
}
