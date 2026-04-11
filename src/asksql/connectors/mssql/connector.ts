/**
 * SQL Server Connector — Layer 2
 *
 * Reads metadata from INFORMATION_SCHEMA + sys catalog views.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 * Also handles query execution and sample collection.
 *
 * Also handles query execution and sample collection.
 */

import sql from "mssql";
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

const MSSQL_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: "[]",
  limitSyntax: "TOP",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "GETDATE()",
  dateDiffHint: "Use DATEADD(day, -N, GETDATE()) or DATEDIFF(day, date1, date2) for date arithmetic",
  booleanLiterals: { true: "1", false: "0" },
  additionalPromptHints: [
    "Use ISNULL() or COALESCE() for null handling",
    "Use TOP N in SELECT for row limiting (e.g., SELECT TOP 1000 ...)",
    "SQL Server has no native BOOLEAN — use BIT (1/0)",
    "Use square brackets [column] for reserved word identifiers",
    "Use CONVERT() or CAST() for type conversions",
    "Use LEN() instead of LENGTH() for string length",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "sys", "INFORMATION_SCHEMA", "guest", "db_owner", "db_accessadmin",
  "db_securityadmin", "db_ddladmin", "db_backupoperator", "db_datareader",
  "db_datawriter", "db_denydatareader", "db_denydatawriter",
]);

export class MSSQLConnector implements AskSQLConnector {
  readonly type: ConnectorType = "mssql";
  readonly displayName = "Microsoft SQL Server";
  readonly dialect: SQLDialect = "tsql";
  readonly dialectHints = MSSQL_DIALECT_HINTS;

  private pool: sql.ConnectionPool | null = null;
  private poolConfig: sql.config;
  private maxSampleValues: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("MSSQLConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.poolConfig = {
      server: parsed.host,
      port: parsed.port,
      user: parsed.user,
      password: parsed.password,
      database: parsed.database,
      options: { encrypt: false, trustServerCertificate: true },
      connectionTimeout: ((config.connectTimeoutMs as number) ?? 10000),
      pool: {
        min: 1,
        max: (config.poolSize as number) ?? 5,
        idleTimeoutMillis: (config.idleTimeoutMs as number) ?? 60000,
      },
    };
    this.maxSampleValues = (config.maxSampleValues as number) ?? 20;
  }

  private parseConnectionString(url: string): {
    user: string; password: string; host: string; port: number; database: string;
  } {
    // mssql://user:password@host:port/database or sqlserver://...
    const match = url.match(/^(?:mssql|sqlserver):\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/(.+)$/i);
    if (!match) {
      throw new Error(
        "Invalid SQL Server connection string. Expected format: mssql://user:password@host:port/database"
      );
    }
    return {
      user: decodeURIComponent(match[1]),
      password: decodeURIComponent(match[2]),
      host: match[3],
      port: Number(match[4]),
      database: match[5],
    };
  }

  private async getPool(): Promise<sql.ConnectionPool> {
    if (!this.pool) {
      this.pool = await new sql.ConnectionPool(this.poolConfig).connect();
    }
    return this.pool;
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const pool = await this.getPool();
      const result = await pool.request().query("SELECT @@VERSION AS version");
      const version = result.recordset[0]?.version as string;
      // Extract first line (e.g., "Microsoft SQL Server 2022 (RTM)")
      const shortVersion = version?.split("\n")[0]?.trim() ?? "Unknown";
      return { success: true, serverVersion: shortVersion, latencyMs: Date.now() - start };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^(mssql|sqlserver):\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.close();
      this.pool = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();
    const pool = await this.getPool();

    // Server version
    const versionResult = await pool.request().query("SELECT @@VERSION AS version");
    const serverVersion = (versionResult.recordset[0]?.version as string)?.split("\n")[0]?.trim() ?? "SQL Server";

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

    return {
      databaseName: this.poolConfig.database!,
      serverVersion,
      databaseType: "MSSQL",
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
    const pool = await this.getPool();

    // Sanitize identifier — wrap in brackets, escape internal brackets
    const ident = (name: string) => `[${name.replace(/\]/g, "]]")}]`;

    for (const req of requests) {
      const samples: DiscoveredColumnSample[] = [];

      for (const colName of req.columns) {
        try {
          const maxDist = req.maxDistinctValues ?? this.maxSampleValues;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          // Get distinct values using TOP
          const distinctResult = await pool.request().query(
            `SELECT DISTINCT TOP ${maxDist + 1} ${col} AS value FROM ${qualified} WHERE ${col} IS NOT NULL ORDER BY 1`
          );

          const distinctCount = distinctResult.recordset.length;
          if (distinctCount > maxDist) {
            samples.push({ columnName: colName, distinctCount, sampleValues: [] });
            continue;
          }

          // Get stats
          const statsResult = await pool.request().query(
            `SELECT
              COUNT(DISTINCT ${col}) AS distinct_count,
              CAST(SUM(CASE WHEN ${col} IS NULL THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0) AS FLOAT) AS null_fraction,
              MIN(CAST(${col} AS NVARCHAR(MAX))) AS min_value,
              MAX(CAST(${col} AS NVARCHAR(MAX))) AS max_value,
              AVG(LEN(CAST(${col} AS NVARCHAR(MAX)))) AS avg_length
            FROM ${qualified}`
          );
          const stats = statsResult.recordset[0] ?? {};

          samples.push({
            columnName: colName,
            distinctCount: Number(stats.distinct_count ?? distinctCount),
            nullFraction: stats.null_fraction != null ? Number(stats.null_fraction) : undefined,
            sampleValues: distinctResult.recordset.map((r: any) => String(r.value)),
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
    // SQL Server uses TOP N — check if already present
    const hasLimit = upperSql.includes(" TOP ") || upperSql.includes("FETCH") || upperSql.includes("ROWCOUNT");
    let safeSql = sqlQuery;
    if (!hasLimit) {
      // Inject TOP after SELECT
      safeSql = sqlQuery.replace(/^(\s*SELECT\s+)/i, `$1TOP ${maxRows + 1} `);
    }

    const start = Date.now();
    const pool = await this.getPool();
    const request = pool.request();
    (request as any).timeout = Number(timeoutMs);

    try {
      // SET TRANSACTION ISOLATION LEVEL for read safety
      const result = await request.query(
        `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n${safeSql}`
      );

      const allRows = (result.recordset ?? []) as Record<string, unknown>[];
      const truncated = allRows.length > maxRows;
      const rows = truncated ? allRows.slice(0, maxRows) : allRows;

      // Infer column types from result metadata or first row
      const columns = result.recordset.columns
        ? Object.entries(result.recordset.columns).map(([name, meta]: [string, any]) => ({
            name,
            type: this.mapMssqlType(meta.type),
          }))
        : rows.length > 0
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
        (err instanceof Error && err.message.includes("timeout")) || (err instanceof Error && (err as any).code === "ETIMEOUT")
          ? `Query timed out after ${timeoutMs}ms`
          : (err instanceof Error ? err.message : String(err))
      );
    }
  }

  private mapMssqlType(sqlType: any): string {
    if (!sqlType) return "string";
    const typeName = sqlType?.declaration?.toLowerCase() ?? "";
    if (typeName.includes("int") || typeName.includes("float") || typeName.includes("decimal")
      || typeName.includes("numeric") || typeName.includes("money") || typeName.includes("real")) {
      return "number";
    }
    if (typeName.includes("date") || typeName.includes("time")) {
      return "date";
    }
    return "string";
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (INFORMATION_SCHEMA + sys catalog views)
  // ═══════════════════════════════════════════════════════════════

  private async discoverSchemaNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) return configSchemas;

    const pool = await this.getPool();
    const result = await pool.request().query(`
      SELECT DISTINCT TABLE_SCHEMA
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
      ORDER BY TABLE_SCHEMA
    `);

    return result.recordset
      .map((r: any) => r.TABLE_SCHEMA as string)
      .filter((s: string) => !EXCLUDED_SCHEMAS.has(s));
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    const pool = await this.getPool();

    // Tables
    const tableResult = await pool.request()
      .input("schema", sql.NVarChar, schemaName)
      .query(`
        SELECT t.TABLE_NAME,
               p.rows AS estimated_row_count,
               ep.value AS table_comment
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.tables st
          ON st.name = t.TABLE_NAME AND SCHEMA_NAME(st.schema_id) = t.TABLE_SCHEMA
        LEFT JOIN sys.partitions p
          ON p.object_id = st.object_id AND p.index_id IN (0, 1)
        LEFT JOIN sys.extended_properties ep
          ON ep.major_id = st.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
        WHERE t.TABLE_SCHEMA = @schema AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_NAME
      `);

    const tables: DiscoveredTable[] = tableResult.recordset.map((r: any) => ({
      tableName: r.TABLE_NAME as string,
      tableType: "TABLE" as const,
      detailedTableType: "BASE TABLE",
      estimatedRowCount: r.estimated_row_count != null ? Number(r.estimated_row_count) : undefined,
      tableComment: (r.table_comment as string) || undefined,
      columns: [],
      foreignKeys: [],
      indexes: [],
    }));

    // Views
    const viewResult = await pool.request()
      .input("schema", sql.NVarChar, schemaName)
      .query(`
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = @schema
        ORDER BY TABLE_NAME
      `);

    for (const r of viewResult.recordset) {
      tables.push({
        tableName: r.TABLE_NAME as string,
        tableType: "VIEW",
        detailedTableType: "VIEW",
        columns: [],
        foreignKeys: [],
        indexes: [],
      });
    }

    return tables;
  }

  private async discoverColumns(schemaName: string, tableName: string): Promise<DiscoveredColumn[]> {
    const pool = await this.getPool();
    const result = await pool.request()
      .input("schema", sql.NVarChar, schemaName)
      .input("table", sql.NVarChar, tableName)
      .query(`
        SELECT c.COLUMN_NAME, c.ORDINAL_POSITION, c.DATA_TYPE,
               c.IS_NULLABLE, c.COLUMN_DEFAULT,
               c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE,
               COLUMNPROPERTY(OBJECT_ID(@schema + '.' + @table), c.COLUMN_NAME, 'IsIdentity') AS is_identity,
               ep.value AS column_comment
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.columns sc
          ON sc.object_id = OBJECT_ID(@schema + '.' + @table) AND sc.name = c.COLUMN_NAME
        LEFT JOIN sys.extended_properties ep
          ON ep.major_id = sc.object_id AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
        ORDER BY c.ORDINAL_POSITION
      `);

    return result.recordset.map((r: any) => {
      const dataType = r.DATA_TYPE as string;
      const charMaxLen = r.CHARACTER_MAXIMUM_LENGTH != null ? Number(r.CHARACTER_MAXIMUM_LENGTH) : undefined;
      const numPrec = r.NUMERIC_PRECISION != null ? Number(r.NUMERIC_PRECISION) : undefined;
      const numScale = r.NUMERIC_SCALE != null ? Number(r.NUMERIC_SCALE) : undefined;

      // Build full data type string
      let fullDataType = dataType;
      if (charMaxLen != null && charMaxLen > 0) {
        fullDataType = `${dataType}(${charMaxLen})`;
      } else if (charMaxLen === -1) {
        fullDataType = `${dataType}(max)`;
      } else if (numPrec != null && numScale != null && numScale > 0) {
        fullDataType = `${dataType}(${numPrec},${numScale})`;
      } else if (numPrec != null && !["int", "bigint", "smallint", "tinyint"].includes(dataType)) {
        fullDataType = `${dataType}(${numPrec})`;
      }

      return {
        columnName: r.COLUMN_NAME as string,
        ordinalPosition: Number(r.ORDINAL_POSITION),
        dataType,
        fullDataType,
        isNullable: r.IS_NULLABLE === "YES",
        columnDefault: r.COLUMN_DEFAULT ?? undefined,
        characterMaxLength: charMaxLen === -1 ? undefined : charMaxLen,
        numericPrecision: numPrec,
        numericScale: numScale,
        columnComment: (r.column_comment as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement: r.is_identity === 1,
      };
    });
  }

  private async discoverPrimaryKey(schemaName: string, tableName: string): Promise<DiscoveredPrimaryKey | undefined> {
    const pool = await this.getPool();
    const result = await pool.request()
      .input("schema", sql.NVarChar, schemaName)
      .input("table", sql.NVarChar, tableName)
      .query(`
        SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
          AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
          AND kcu.TABLE_NAME = tc.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = @schema
          AND tc.TABLE_NAME = @table
          AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY kcu.ORDINAL_POSITION
      `);

    if (result.recordset.length === 0) return undefined;
    return {
      constraintName: result.recordset[0].CONSTRAINT_NAME as string,
      columns: result.recordset.map((r: any) => r.COLUMN_NAME as string),
    };
  }

  private async discoverForeignKeys(schemaName: string, tableName: string): Promise<DiscoveredForeignKey[]> {
    const pool = await this.getPool();
    // Use sys catalog — INFORMATION_SCHEMA lacks FK referenced column info
    const result = await pool.request()
      .input("schema", sql.NVarChar, schemaName)
      .input("table", sql.NVarChar, tableName)
      .query(`
        SELECT
          fk.name AS constraint_name,
          COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
          SCHEMA_NAME(ref_t.schema_id) AS referenced_schema,
          ref_t.name AS referenced_table,
          COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.tables t ON t.object_id = fk.parent_object_id
        JOIN sys.tables ref_t ON ref_t.object_id = fk.referenced_object_id
        WHERE SCHEMA_NAME(t.schema_id) = @schema AND t.name = @table
        ORDER BY fk.name, fkc.constraint_column_id
      `);

    const fkMap = new Map<string, DiscoveredForeignKey>();
    for (const r of result.recordset) {
      const name = r.constraint_name as string;
      if (!fkMap.has(name)) {
        fkMap.set(name, {
          constraintName: name,
          columns: [],
          referencedSchema: r.referenced_schema as string,
          referencedTable: r.referenced_table as string,
          referencedColumns: [],
        });
      }
      const fk = fkMap.get(name)!;
      fk.columns.push(r.column_name as string);
      fk.referencedColumns.push(r.referenced_column as string);
    }
    return Array.from(fkMap.values());
  }

  private async discoverIndexes(schemaName: string, tableName: string): Promise<DiscoveredIndex[]> {
    const pool = await this.getPool();
    const result = await pool.request()
      .input("schema", sql.NVarChar, schemaName)
      .input("table", sql.NVarChar, tableName)
      .query(`
        SELECT
          i.name AS index_name,
          COL_NAME(ic.object_id, ic.column_id) AS column_name,
          i.is_unique
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.tables t ON t.object_id = i.object_id
        WHERE SCHEMA_NAME(t.schema_id) = @schema
          AND t.name = @table
          AND i.is_primary_key = 0
          AND i.name IS NOT NULL
        ORDER BY i.name, ic.key_ordinal
      `);

    const idxMap = new Map<string, DiscoveredIndex>();
    for (const r of result.recordset) {
      const name = r.index_name as string;
      if (!idxMap.has(name)) {
        idxMap.set(name, {
          indexName: name,
          columns: [],
          isUnique: r.is_unique as boolean,
        });
      }
      idxMap.get(name)!.columns.push(r.column_name as string);
    }
    return Array.from(idxMap.values());
  }
}
