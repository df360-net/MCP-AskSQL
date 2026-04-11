/**
 * MySQL Connector — Layer 2
 *
 * Reads metadata from MySQL's INFORMATION_SCHEMA.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 *
 * Uses `mysql2/promise` driver (Promise-native, types included, MariaDB compatible).
 * Connection string: mysql://user:password@host:3306/database
 *
 * MySQL hierarchy: Server → Database → Table (database = schema in AskSQL terms)
 */

import mysql from "mysql2/promise";
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

const MYSQL_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: "`",
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "NOW()",
  dateDiffHint: "Use DATEDIFF(date1, date2) for day difference, DATE_ADD(date, INTERVAL N DAY) for arithmetic",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "MySQL uses backticks for identifier quoting: `column_name`",
    "No ILIKE — use LOWER(col) LIKE LOWER('%pattern%') for case-insensitive matching",
    "Use GROUP_CONCAT(col SEPARATOR ', ') for string aggregation",
    "Use IFNULL(col, default) or COALESCE() for null handling",
    "Use CAST(col AS CHAR) to convert to string",
    "Use DATE_FORMAT(date, '%Y-%m-%d') for date formatting",
    "Use CHAR_LENGTH() for character length",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "information_schema", "performance_schema", "mysql", "sys",
]);

export class MySQLConnector implements AskSQLConnector {
  readonly type: ConnectorType = "mysql";
  readonly displayName = "MySQL";
  readonly dialect: SQLDialect = "mysql";
  readonly dialectHints = MYSQL_DIALECT_HINTS;

  private pool: mysql.Pool;
  private database: string;
  private maxSampleValues: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("MySQLConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.database = (config.catalog as string) ?? parsed.database;

    this.pool = mysql.createPool({
      host: parsed.host,
      port: parsed.port,
      user: parsed.user,
      password: parsed.password,
      database: this.database,
      waitForConnections: true,
      connectionLimit: (config.poolSize as number) ?? 5,
      connectTimeout: (config.connectTimeoutMs as number) ?? 10000,
      idleTimeout: (config.idleTimeoutMs as number) ?? 20000,
      enableKeepAlive: true,
    });
    this.maxSampleValues = (config.maxSampleValues as number) ?? 10;
  }

  /**
   * Parse: mysql://user:password@host[:port][/database]
   * Default port: 3306. Default database: mysql.
   */
  private parseConnectionString(url: string): {
    user: string; password: string; host: string; port: number; database: string;
  } {
    const match = url.match(/^mysql:\/\/([^:]+):([^@]+)@([^:/]+)(?::(\d+))?(?:\/([^?]*))?/i);
    if (!match) {
      throw new Error(
        "Invalid MySQL connection string. Expected: mysql://user:password@host[:port][/database]"
      );
    }

    return {
      user: decodeURIComponent(match[1]),
      password: decodeURIComponent(match[2]),
      host: match[3],
      port: match[4] ? parseInt(match[4], 10) : 3306,
      database: match[5] || "mysql",
    };
  }

  private async query<T = Record<string, unknown>>(sqlText: string, params?: unknown[]): Promise<T[]> {
    const [rows] = await this.pool.execute(sqlText, params as any[]);
    return rows as T[];
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const rows = await this.query<Record<string, unknown>>(
        "SELECT VERSION() AS version"
      );
      return {
        success: true,
        serverVersion: `MySQL ${(rows[0]?.version as string) ?? "unknown"}`,
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^mysql:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    const vRows = await this.query<Record<string, unknown>>("SELECT VERSION() AS version");
    const serverVersion = `MySQL ${(vRows[0]?.version as string) ?? "unknown"}`;

    // In MySQL, database = schema. Discover configured schemas or just the connected database.
    const schemaNames = options?.schemas?.length
      ? options.schemas
      : [this.database];

    const schemas: DiscoveredSchema[] = [];
    for (const schemaName of schemaNames) {
      if (EXCLUDED_SCHEMAS.has(schemaName)) continue;

      const tables = await this.discoverTablesAndViews(schemaName);

      // Apply include/exclude filters
      const includeRe = options?.includeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const excludeRe = options?.excludeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const filtered = tables.filter((t) => {
        if (includeRe?.length) return includeRe.some((r) => r.test(t.tableName));
        if (excludeRe?.length) return !excludeRe.some((r) => r.test(t.tableName));
        return true;
      });

      // Batch-load metadata for this schema
      const columnsByTable = await this.discoverAllColumns(schemaName);
      const pksByTable = await this.discoverAllPrimaryKeys(schemaName);
      const fksByTable = await this.discoverAllForeignKeys(schemaName);
      const indexesByTable = await this.discoverAllIndexes(schemaName);

      for (const table of filtered) {
        table.columns = columnsByTable.get(table.tableName) ?? [];
        table.indexes = indexesByTable.get(table.tableName) ?? [];

        const pk = pksByTable.get(table.tableName);
        if (pk) {
          table.primaryKey = pk;
          for (const col of table.columns) {
            if (pk.columns.includes(col.columnName)) col.isPrimaryKey = true;
          }
        }

        table.foreignKeys = fksByTable.get(table.tableName) ?? [];
      }

      schemas.push({ schemaName, tables: filtered });
    }

    return {
      databaseName: this.database,
      serverVersion,
      databaseType: "MYSQL",
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
          const maxDist = req.maxDistinctValues ?? this.maxSampleValues;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          const distinctRows = await this.query<{ value: string }>(
            `SELECT DISTINCT CAST(${col} AS CHAR) AS value FROM ${qualified}
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
              SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) / GREATEST(COUNT(*), 1) AS null_fraction,
              MIN(CAST(${col} AS CHAR)) AS min_value,
              MAX(CAST(${col} AS CHAR)) AS max_value,
              CAST(AVG(CHAR_LENGTH(CAST(${col} AS CHAR))) AS UNSIGNED) AS avg_length
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
      // Use a dedicated connection to set session-level timeout, then execute the query
      const conn = await this.pool.getConnection();
      try {
        await conn.execute(`SET SESSION MAX_EXECUTION_TIME = ${Number(timeoutMs)}`);
        const [rows] = await conn.execute(safeSql);
        const resultRows = rows as Record<string, unknown>[];

        const truncated = resultRows.length > maxRows;
        const finalRows = truncated ? resultRows.slice(0, maxRows) : resultRows;

        const columns = finalRows.length > 0
          ? Object.keys(finalRows[0]).map((name) => ({
              name,
              type: typeof finalRows[0][name] === "number" ? "number"
                : finalRows[0][name] instanceof Date ? "date"
                : "string",
            }))
          : [];

        return { rows: finalRows, columns, rowCount: finalRows.length, truncated, executionTimeMs: Date.now() - start };
      } finally {
        conn.release();
      }
    } catch (err: unknown) {
      throw new Error(
        (err instanceof Error && (err.message.includes("MAX_EXECUTION_TIME") || err.message.includes("Query execution was interrupted")))
          ? `Query timed out after ${timeoutMs}ms`
          : (err instanceof Error ? err.message : "Query execution failed")
      );
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (INFORMATION_SCHEMA, parameterized)
  // ═══════════════════════════════════════════════════════════════

  private async discoverTablesAndViews(database: string): Promise<DiscoveredTable[]> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, TABLE_COMMENT
       FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
       ORDER BY TABLE_NAME`,
      [database]
    );

    return rows.map((r) => ({
      tableName: r.TABLE_NAME as string,
      tableType: ((r.TABLE_TYPE as string) === "VIEW" ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
      estimatedRowCount: r.TABLE_ROWS != null ? Number(r.TABLE_ROWS) : undefined,
      sizeBytes: r.DATA_LENGTH != null ? Number(r.DATA_LENGTH) + Number(r.INDEX_LENGTH ?? 0) : undefined,
      tableComment: (r.TABLE_COMMENT as string) || undefined,
      columns: [],
      foreignKeys: [],
      indexes: [],
    }));
  }

  /**
   * Batch-load ALL columns for a database in 1 query.
   */
  private async discoverAllColumns(database: string): Promise<Map<string, DiscoveredColumn[]>> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE,
              IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH,
              NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_COMMENT, EXTRA
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ?
       ORDER BY TABLE_NAME, ORDINAL_POSITION`,
      [database]
    );

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = r.TABLE_NAME as string;
      if (!map.has(tableName)) map.set(tableName, []);

      map.get(tableName)!.push({
        columnName: r.COLUMN_NAME as string,
        ordinalPosition: Number(r.ORDINAL_POSITION),
        dataType: r.DATA_TYPE as string,
        fullDataType: r.COLUMN_TYPE as string,
        isNullable: (r.IS_NULLABLE as string) === "YES",
        columnDefault: (r.COLUMN_DEFAULT as string) ?? undefined,
        characterMaxLength: r.CHARACTER_MAXIMUM_LENGTH != null ? Number(r.CHARACTER_MAXIMUM_LENGTH) : undefined,
        numericPrecision: r.NUMERIC_PRECISION != null ? Number(r.NUMERIC_PRECISION) : undefined,
        numericScale: r.NUMERIC_SCALE != null ? Number(r.NUMERIC_SCALE) : undefined,
        columnComment: (r.COLUMN_COMMENT as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement: ((r.EXTRA as string) ?? "").includes("auto_increment"),
      });
    }
    return map;
  }

  /**
   * Batch-load ALL primary keys for a database.
   */
  private async discoverAllPrimaryKeys(database: string): Promise<Map<string, DiscoveredPrimaryKey>> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = ? AND CONSTRAINT_NAME = 'PRIMARY'
       ORDER BY TABLE_NAME, ORDINAL_POSITION`,
      [database]
    );

    const map = new Map<string, DiscoveredPrimaryKey>();
    for (const r of rows) {
      const tableName = r.TABLE_NAME as string;
      if (!map.has(tableName)) {
        map.set(tableName, { constraintName: "PRIMARY", columns: [] });
      }
      map.get(tableName)!.columns.push(r.COLUMN_NAME as string);
    }
    return map;
  }

  /**
   * Batch-load ALL foreign keys for a database.
   */
  private async discoverAllForeignKeys(database: string): Promise<Map<string, DiscoveredForeignKey[]>> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME,
              REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL
       ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION`,
      [database]
    );

    const fkMap = new Map<string, Map<string, DiscoveredForeignKey>>();
    for (const r of rows) {
      const tableName = r.TABLE_NAME as string;
      const constraintName = r.CONSTRAINT_NAME as string;

      if (!fkMap.has(tableName)) fkMap.set(tableName, new Map());
      const tableMap = fkMap.get(tableName)!;

      if (!tableMap.has(constraintName)) {
        tableMap.set(constraintName, {
          constraintName,
          columns: [],
          referencedSchema: r.REFERENCED_TABLE_SCHEMA as string,
          referencedTable: r.REFERENCED_TABLE_NAME as string,
          referencedColumns: [],
        });
      }
      const fk = tableMap.get(constraintName)!;
      fk.columns.push(r.COLUMN_NAME as string);
      fk.referencedColumns.push(r.REFERENCED_COLUMN_NAME as string);
    }

    const result = new Map<string, DiscoveredForeignKey[]>();
    for (const [tableName, tableMap] of fkMap) {
      result.set(tableName, Array.from(tableMap.values()));
    }
    return result;
  }

  /**
   * Batch-load ALL indexes for a database (excluding PRIMARY).
   */
  private async discoverAllIndexes(database: string): Promise<Map<string, DiscoveredIndex[]>> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE
       FROM INFORMATION_SCHEMA.STATISTICS
       WHERE TABLE_SCHEMA = ? AND INDEX_NAME != 'PRIMARY'
       ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`,
      [database]
    );

    const idxMap = new Map<string, Map<string, DiscoveredIndex>>();
    for (const r of rows) {
      const tableName = r.TABLE_NAME as string;
      const indexName = r.INDEX_NAME as string;

      if (!idxMap.has(tableName)) idxMap.set(tableName, new Map());
      const tableMap = idxMap.get(tableName)!;

      if (!tableMap.has(indexName)) {
        tableMap.set(indexName, {
          indexName,
          columns: [],
          isUnique: Number(r.NON_UNIQUE) === 0,
        });
      }
      tableMap.get(indexName)!.columns.push(r.COLUMN_NAME as string);
    }

    const result = new Map<string, DiscoveredIndex[]>();
    for (const [tableName, tableMap] of idxMap) {
      result.set(tableName, Array.from(tableMap.values()));
    }
    return result;
  }
}
