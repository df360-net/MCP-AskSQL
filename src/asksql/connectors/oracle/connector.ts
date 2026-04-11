/**
 * Oracle Connector — Layer 2
 *
 * Reads metadata from Oracle's ALL_* catalog views.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 * Also handles query execution and sample collection.
 *
 * Uses oracledb thin mode — no Oracle client installation required.
 * Uses oracledb thin mode — no Oracle client installation required.
 */

import oracledb from "oracledb";
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

// Thin mode by default in oracledb v6+ — no Oracle client installation needed

const ORACLE_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: '"',
  limitSyntax: "FETCH_FIRST",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "SYSTIMESTAMP",
  dateDiffHint: "Use TRUNC(SYSDATE) - INTERVAL 'N' DAY or NUMTODSINTERVAL for date arithmetic",
  booleanLiterals: { true: "'Y'", false: "'N'" },
  additionalPromptHints: [
    "Use UPPER() or LOWER() for case-insensitive comparisons (no ILIKE)",
    "Use TO_CHAR() for date/number formatting",
    "Use NVL() or COALESCE() for null handling",
    "Use FETCH FIRST N ROWS ONLY for row limiting (Oracle 12c+)",
    "Use ROWNUM in a subquery for older Oracle versions",
    "Oracle has no native BOOLEAN type — use 'Y'/'N' or 1/0",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "SYS", "SYSTEM", "CTXSYS", "MDSYS", "OLAPSYS", "ORDDATA", "ORDSYS",
  "OUTLN", "WMSYS", "XDB", "XDBADMIN", "APPQOSSYS", "DBSNMP",
  "GSMADMIN_INTERNAL", "LBACSYS", "ANONYMOUS", "FLOWS_FILES", "GGSYS",
  "GSMCATUSER", "GSMUSER", "OJVMSYS", "SI_INFORMTN_SCHEMA", "DVSYS",
  "AUDSYS", "DVF", "REMOTE_SCHEDULER_AGENT", "DBSFWUSER", "DGPDB_INT",
  "SYS$UMF", "APEX_PUBLIC_USER", "APEX_040000", "APEX_050000",
  "APEX_230100", "ORDS_METADATA", "ORDS_PUBLIC_USER",
]);

export class OracleConnector implements AskSQLConnector {
  readonly type: ConnectorType = "oracle";
  readonly displayName = "Oracle";
  readonly dialect: SQLDialect = "plsql";
  readonly dialectHints = ORACLE_DIALECT_HINTS;

  private pool: oracledb.Pool | null = null;
  private connString: string;
  private user: string;
  private password: string;
  private poolSize: number;
  private connectTimeoutMs: number;
  private idleTimeoutSec: number;
  private maxSampleValues: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("OracleConnector requires a connectionString");
    }

    // Parse oracle://user:password@host:port/service_name
    const parsed = this.parseConnectionString(connectionString);
    this.user = parsed.user;
    this.password = parsed.password;
    this.connString = parsed.connectString;
    this.poolSize = (config.poolSize as number) ?? 5;
    this.connectTimeoutMs = (config.connectTimeoutMs as number) ?? 10000;
    this.idleTimeoutSec = ((config.idleTimeoutMs as number) ?? 60000) / 1000;
    this.maxSampleValues = (config.maxSampleValues as number) ?? 20;
  }

  private parseConnectionString(url: string): { user: string; password: string; connectString: string } {
    // oracle://user:password@host:port/service_name
    const match = url.match(/^oracle:\/\/([^:]+):([^@]+)@(.+)$/i);
    if (!match) {
      throw new Error(
        "Invalid Oracle connection string. Expected format: oracle://user:password@host:port/service_name"
      );
    }
    return {
      user: decodeURIComponent(match[1]),
      password: decodeURIComponent(match[2]),
      connectString: match[3], // host:port/service_name
    };
  }

  private async getPool(): Promise<oracledb.Pool> {
    if (!this.pool) {
      this.pool = await oracledb.createPool({
        user: this.user,
        password: this.password,
        connectString: this.connString,
        poolMin: 1,
        poolMax: this.poolSize,
        poolTimeout: this.idleTimeoutSec,
      });
    }
    return this.pool;
  }

  private async query<T = Record<string, unknown>>(sql: string, binds: oracledb.BindParameters = {}): Promise<T[]> {
    const pool = await this.getPool();
    const conn = await pool.getConnection();
    try {
      const result = await conn.execute(sql, binds, { outFormat: oracledb.OUT_FORMAT_OBJECT });
      return (result.rows ?? []) as T[];
    } finally {
      await conn.close();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const rows = await this.query<{ BANNER: string }>(
        "SELECT banner AS BANNER FROM v$version WHERE ROWNUM = 1"
      );
      return {
        success: true,
        serverVersion: rows[0]?.BANNER ?? "Unknown",
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^oracle:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.close(0);
      this.pool = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    // Server version
    const versionRows = await this.query<{ BANNER: string }>(
      "SELECT banner AS BANNER FROM v$version WHERE ROWNUM = 1"
    );
    const serverVersion = versionRows[0]?.BANNER ?? "Oracle";

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

      // Enrich each table with columns, PKs, FKs, indexes
      for (const table of filtered) {
        table.columns = await this.discoverColumns(schemaName, table.tableName);
        table.primaryKey = await this.discoverPrimaryKey(schemaName, table.tableName);
        table.foreignKeys = await this.discoverForeignKeys(schemaName, table.tableName);
        table.indexes = await this.discoverIndexes(schemaName, table.tableName);

        // Mark PK columns
        if (table.primaryKey) {
          const pkCols = new Set(table.primaryKey.columns);
          for (const col of table.columns) {
            col.isPrimaryKey = pkCols.has(col.columnName);
          }
        }
      }

      schemas.push({ schemaName, tables: filtered });
    }

    // Database name (service name from connection)
    const dbRows = await this.query<{ DB_NAME: string }>(
      "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS DB_NAME FROM DUAL"
    );
    const databaseName = dbRows[0]?.DB_NAME ?? "unknown";

    return {
      databaseName,
      serverVersion,
      databaseType: "ORACLE",
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

    // Sanitize identifier — strip double quotes to prevent SQL injection
    const ident = (name: string) => `"${name.replace(/"/g, "")}"`;

    for (const req of requests) {
      const samples: DiscoveredColumnSample[] = [];

      for (const colName of req.columns) {
        try {
          const maxDist = req.maxDistinctValues ?? this.maxSampleValues;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          // Get distinct values (ROWNUM for Oracle limiting)
          const distinctRows = await this.query<{ VALUE: string }>(
            `SELECT DISTINCT ${col} AS VALUE FROM ${qualified}
             WHERE ${col} IS NOT NULL AND ROWNUM <= ${maxDist + 1}
             ORDER BY 1`
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
              ROUND(SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) / GREATEST(COUNT(*), 1), 4) AS NULL_FRACTION,
              MIN(TO_CHAR(${col})) AS MIN_VALUE,
              MAX(TO_CHAR(${col})) AS MAX_VALUE,
              ROUND(AVG(LENGTH(TO_CHAR(${col})))) AS AVG_LENGTH
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

  async executeQuery(sql: string, options?: ExecuteOptions): Promise<RawQueryResult> {
    const maxRows = options?.maxRows ?? 5000;
    const timeoutMs = options?.timeoutMs ?? 30000;

    const upperSql = sql.toUpperCase();
    // Oracle 12c+ supports FETCH FIRST; also check for ROWNUM
    const hasLimit = upperSql.includes("FETCH") || upperSql.includes("ROWNUM");
    const safeSql = hasLimit
      ? sql
      : `${sql.replace(/;?\s*$/, "")} FETCH FIRST ${maxRows + 1} ROWS ONLY`;

    const start = Date.now();
    const pool = await this.getPool();
    const conn = await pool.getConnection();
    try {
      // Set call timeout (oracledb uses milliseconds)
      conn.callTimeout = Number(timeoutMs);

      // Execute read-only
      await conn.execute("SET TRANSACTION READ ONLY");
      const result = await conn.execute(safeSql, {}, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
        maxRows: maxRows + 1,
      });

      const allRows = (result.rows ?? []) as Record<string, unknown>[];
      const truncated = allRows.length > maxRows;
      const rows = truncated ? allRows.slice(0, maxRows) : allRows;

      // Infer column types from metadata or first row
      const columns = result.metaData
        ? result.metaData.map((m: any) => ({
            name: m.name,
            type: this.mapOracleType(m.dbType),
          }))
        : rows.length > 0
          ? Object.keys(rows[0]).map((name) => ({
              name,
              type: typeof rows[0][name] === "number" ? "number"
                : rows[0][name] instanceof Date ? "date"
                : "string",
            }))
          : [];

      await conn.execute("COMMIT"); // end read-only transaction

      return { rows, columns, rowCount: rows.length, truncated, executionTimeMs: Date.now() - start };
    } catch (err: unknown) {
      throw new Error(
        (err instanceof Error && err.message.includes("timeout")) || (err instanceof Error && (err as any).errorNum === 1013)
          ? `Query timed out after ${timeoutMs}ms`
          : (err instanceof Error ? err.message : String(err))
      );
    } finally {
      await conn.close();
    }
  }

  private mapOracleType(dbType?: number): string {
    if (!dbType) return "string";
    // oracledb DB_TYPE constants
    switch (dbType) {
      case oracledb.DB_TYPE_NUMBER:
      case oracledb.DB_TYPE_BINARY_FLOAT:
      case oracledb.DB_TYPE_BINARY_DOUBLE:
      case oracledb.DB_TYPE_BINARY_INTEGER:
        return "number";
      case oracledb.DB_TYPE_DATE:
      case oracledb.DB_TYPE_TIMESTAMP:
      case oracledb.DB_TYPE_TIMESTAMP_TZ:
      case oracledb.DB_TYPE_TIMESTAMP_LTZ:
        return "date";
      default:
        return "string";
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (Oracle ALL_* catalog views)
  // ═══════════════════════════════════════════════════════════════

  private async discoverSchemaNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) {
      return configSchemas.map((s) => s.toUpperCase());
    }

    const rows = await this.query<{ OWNER: string }>(
      `SELECT DISTINCT OWNER FROM ALL_TABLES
       WHERE OWNER NOT LIKE 'APEX%'
       ORDER BY OWNER`
    );
    return rows
      .map((r) => r.OWNER)
      .filter((s) => !EXCLUDED_SCHEMAS.has(s));
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    // Tables
    const tableRows = await this.query<Record<string, unknown>>(
      `SELECT t.TABLE_NAME, t.NUM_ROWS, tc.COMMENTS
       FROM ALL_TABLES t
       LEFT JOIN ALL_TAB_COMMENTS tc
         ON tc.OWNER = t.OWNER AND tc.TABLE_NAME = t.TABLE_NAME AND tc.TABLE_TYPE = 'TABLE'
       WHERE t.OWNER = :schema
       ORDER BY t.TABLE_NAME`,
      { schema: schemaName }
    );

    const tables: DiscoveredTable[] = tableRows.map((r) => ({
      tableName: r.TABLE_NAME as string,
      tableType: "TABLE" as const,
      detailedTableType: "BASE TABLE",
      estimatedRowCount: r.NUM_ROWS != null ? Number(r.NUM_ROWS) : undefined,
      tableComment: (r.COMMENTS as string) || undefined,
      columns: [],
      foreignKeys: [],
      indexes: [],
    }));

    // Views
    const viewRows = await this.query<Record<string, unknown>>(
      `SELECT v.VIEW_NAME, tc.COMMENTS
       FROM ALL_VIEWS v
       LEFT JOIN ALL_TAB_COMMENTS tc
         ON tc.OWNER = v.OWNER AND tc.TABLE_NAME = v.VIEW_NAME AND tc.TABLE_TYPE = 'VIEW'
       WHERE v.OWNER = :schema
       ORDER BY v.VIEW_NAME`,
      { schema: schemaName }
    );

    for (const r of viewRows) {
      tables.push({
        tableName: r.VIEW_NAME as string,
        tableType: "VIEW",
        detailedTableType: "VIEW",
        tableComment: (r.COMMENTS as string) || undefined,
        columns: [],
        foreignKeys: [],
        indexes: [],
      });
    }

    return tables;
  }

  private async discoverColumns(schemaName: string, tableName: string): Promise<DiscoveredColumn[]> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT c.COLUMN_NAME, c.COLUMN_ID, c.DATA_TYPE,
              c.DATA_LENGTH, c.DATA_PRECISION, c.DATA_SCALE,
              c.NULLABLE, c.DATA_DEFAULT, cc.COMMENTS,
              c.IDENTITY_COLUMN, c.CHAR_LENGTH
       FROM ALL_TAB_COLUMNS c
       LEFT JOIN ALL_COL_COMMENTS cc
         ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME
       WHERE c.OWNER = :schema AND c.TABLE_NAME = :tableName
       ORDER BY c.COLUMN_ID`,
      { schema: schemaName, tableName }
    );

    return rows.map((r) => {
      const dataType = (r.DATA_TYPE as string).toLowerCase();
      const precision = r.DATA_PRECISION != null ? Number(r.DATA_PRECISION) : undefined;
      const scale = r.DATA_SCALE != null ? Number(r.DATA_SCALE) : undefined;
      const charLength = r.CHAR_LENGTH != null ? Number(r.CHAR_LENGTH) : undefined;
      const dataLength = r.DATA_LENGTH != null ? Number(r.DATA_LENGTH) : undefined;

      // Build full data type string
      let fullDataType = dataType.toUpperCase();
      if (precision != null && scale != null && scale > 0) {
        fullDataType = `${fullDataType}(${precision},${scale})`;
      } else if (precision != null) {
        fullDataType = `${fullDataType}(${precision})`;
      } else if (charLength != null && charLength > 0) {
        fullDataType = `${fullDataType}(${charLength})`;
      }

      const defaultVal = r.DATA_DEFAULT != null ? String(r.DATA_DEFAULT).trim() : undefined;

      return {
        columnName: r.COLUMN_NAME as string,
        ordinalPosition: Number(r.COLUMN_ID ?? 0),
        dataType,
        fullDataType,
        isNullable: r.NULLABLE === "Y",
        columnDefault: defaultVal,
        characterMaxLength: charLength ?? dataLength,
        numericPrecision: precision,
        numericScale: scale,
        columnComment: (r.COMMENTS as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement: r.IDENTITY_COLUMN === "YES",
      };
    });
  }

  private async discoverPrimaryKey(schemaName: string, tableName: string): Promise<DiscoveredPrimaryKey | undefined> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT ac.CONSTRAINT_NAME, acc.COLUMN_NAME
       FROM ALL_CONSTRAINTS ac
       JOIN ALL_CONS_COLUMNS acc
         ON ac.OWNER = acc.OWNER AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
       WHERE ac.OWNER = :schema
         AND ac.TABLE_NAME = :tableName
         AND ac.CONSTRAINT_TYPE = 'P'
       ORDER BY acc.POSITION`,
      { schema: schemaName, tableName }
    );

    if (rows.length === 0) return undefined;
    return {
      constraintName: rows[0].CONSTRAINT_NAME as string,
      columns: rows.map((r) => r.COLUMN_NAME as string),
    };
  }

  private async discoverForeignKeys(schemaName: string, tableName: string): Promise<DiscoveredForeignKey[]> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT ac.CONSTRAINT_NAME,
              acc.COLUMN_NAME,
              rac.OWNER AS REF_OWNER,
              rac.TABLE_NAME AS REF_TABLE,
              rcc.COLUMN_NAME AS REF_COLUMN,
              ac.DELETE_RULE
       FROM ALL_CONSTRAINTS ac
       JOIN ALL_CONS_COLUMNS acc
         ON ac.OWNER = acc.OWNER AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
       JOIN ALL_CONSTRAINTS rac
         ON ac.R_OWNER = rac.OWNER AND ac.R_CONSTRAINT_NAME = rac.CONSTRAINT_NAME
       JOIN ALL_CONS_COLUMNS rcc
         ON rac.OWNER = rcc.OWNER AND rac.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME
         AND acc.POSITION = rcc.POSITION
       WHERE ac.OWNER = :schema
         AND ac.TABLE_NAME = :tableName
         AND ac.CONSTRAINT_TYPE = 'R'
       ORDER BY ac.CONSTRAINT_NAME, acc.POSITION`,
      { schema: schemaName, tableName }
    );

    const fkMap = new Map<string, DiscoveredForeignKey>();
    for (const r of rows) {
      const name = r.CONSTRAINT_NAME as string;
      if (!fkMap.has(name)) {
        fkMap.set(name, {
          constraintName: name,
          columns: [],
          referencedSchema: r.REF_OWNER as string,
          referencedTable: r.REF_TABLE as string,
          referencedColumns: [],
          onDelete: r.DELETE_RULE as string,
        });
      }
      const fk = fkMap.get(name)!;
      fk.columns.push(r.COLUMN_NAME as string);
      fk.referencedColumns.push(r.REF_COLUMN as string);
    }
    return Array.from(fkMap.values());
  }

  private async discoverIndexes(schemaName: string, tableName: string): Promise<DiscoveredIndex[]> {
    const rows = await this.query<Record<string, unknown>>(
      `SELECT i.INDEX_NAME, ic.COLUMN_NAME, i.UNIQUENESS
       FROM ALL_INDEXES i
       JOIN ALL_IND_COLUMNS ic
         ON i.OWNER = ic.INDEX_OWNER AND i.INDEX_NAME = ic.INDEX_NAME
       WHERE i.TABLE_OWNER = :schema
         AND i.TABLE_NAME = :tableName
         AND i.INDEX_NAME NOT IN (
           SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS
           WHERE OWNER = :schema AND TABLE_NAME = :tableName AND CONSTRAINT_TYPE = 'P'
         )
       ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION`,
      { schema: schemaName, tableName }
    );

    const idxMap = new Map<string, DiscoveredIndex>();
    for (const r of rows) {
      const name = r.INDEX_NAME as string;
      if (!idxMap.has(name)) {
        idxMap.set(name, {
          indexName: name,
          columns: [],
          isUnique: r.UNIQUENESS === "UNIQUE",
        });
      }
      idxMap.get(name)!.columns.push(r.COLUMN_NAME as string);
    }
    return Array.from(idxMap.values());
  }
}
