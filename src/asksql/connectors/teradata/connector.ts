/**
 * Teradata Connector — Layer 2
 *
 * Reads metadata from Teradata's DBC system views.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 *
 * Uses `teradatasql` driver (official Teradata Node.js driver).
 * Connection string: teradata://user:password@host/database[?encryptdata=true]
 *
 * Teradata hierarchy: System → Database → Table (database = schema in AskSQL terms)
 * Metadata via: DBC.TablesV, DBC.ColumnsV, DBC.IndicesV, DBC.All_RI_ParentsV
 */

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

const TERADATA_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: '"',
  limitSyntax: "TOP",
  qualificationPattern: "schema.table",
  supportsILIKE: false,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "CURRENT_TIMESTAMP",
  dateDiffHint: "Use (date2 - date1) DAY for day difference, date + INTERVAL '1' DAY for arithmetic",
  booleanLiterals: { true: "1", false: "0" },
  additionalPromptHints: [
    'Teradata uses double quotes for identifier quoting: "column_name"',
    "No ILIKE — use UPPER(col) LIKE UPPER('%PATTERN%') for case-insensitive matching",
    "Use TOP N instead of LIMIT N: SELECT TOP 10 * FROM table",
    "Use CAST(col AS VARCHAR(1000)) for string conversion",
    "Use TRIM() for whitespace removal — Teradata CHAR fields are fixed-width padded",
    "Use CHAR_LENGTH() for string length",
    "Use QUALIFY with window functions: QUALIFY ROW_NUMBER() OVER (...) = 1",
    "Teradata 'databases' are equivalent to schemas in other RDBMS",
    "IMPORTANT: Teradata does NOT support the AS keyword for table aliases. Use implicit aliases: FROM table t, JOIN table2 t2 — using AS causes syntax errors",
  ],
};

const EXCLUDED_DATABASES = new Set([
  "DBC", "SystemFe", "Sys_Calendar", "SYSLIB", "SYSBAR", "SYSUDTLIB",
  "SYSUIF", "SYSJDBC", "SysAdmin", "TDQCD", "TDStats", "TDPUSER",
  "dbcmngr", "LockLogShredder", "SQLJ", "TD_SYSXML", "TD_SERVER_DB",
  "External_AP", "EXTUSER", "PUBLIC", "All", "Default", "TDWM",
  "console", "Crashdumps", "TD_SYSFNLIB", "TD_SYSGPL", "SYSSPATIAL",
  "TDBCMgmt", "TDMaps", "TD_ANALYTICS_DB", "GLOBAL_FUNCTIONS",
  "SAS_SYSFNLIB", "TD_SYSAI", "TD_DATASHARING_REPO", "TD_METRIC_SVC",
  "TD_MODELOPS", "TDSYSFLOW", "td_tapidb", "TDaaS_BAR", "TDaaS_DB",
  "TDaaS_Maint", "TDaaS_Monitor", "TDaaS_Support", "DemoNow_Monitor",
  "gs_tables_db", "mldb", "system", "tdwm", "pg_internal",
]);

/** Map Teradata column type codes to human-readable names */
const TYPE_MAP: Record<string, string> = {
  "I": "INTEGER", "I1": "BYTEINT", "I2": "SMALLINT", "I8": "BIGINT",
  "CF": "CHAR", "CV": "VARCHAR", "CO": "CLOB",
  "D": "DECIMAL", "F": "FLOAT", "D1": "DOUBLE",
  "DA": "DATE", "TS": "TIMESTAMP", "TZ": "TIMESTAMP WITH TIMEZONE",
  "AT": "TIME", "SZ": "TIME WITH TIMEZONE",
  "BF": "BYTE", "BV": "VARBYTE", "BO": "BLOB",
  "N": "NUMBER", "AN": "ARRAY", "JN": "JSON",
  "PM": "PERIOD(DATE)", "PS": "PERIOD(TIMESTAMP)",
  "UT": "UDT", "XM": "XML",
};

export class TeradataConnector implements AskSQLConnector {
  readonly type: ConnectorType = "teradata";
  readonly displayName = "Teradata";
  readonly dialect: SQLDialect = "teradata";
  readonly dialectHints = TERADATA_DIALECT_HINTS;

  private conn: any | null = null;
  private host: string;
  private user: string;
  private password: string;
  private database: string;
  private encryptdata: string;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("TeradataConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.host = parsed.host;
    this.user = parsed.user;
    this.password = parsed.password;
    this.database = (config.catalog as string) ?? parsed.database;
    this.encryptdata = parsed.encryptdata;
  }

  /**
   * Parse: teradata://user:password@host[:port]/database[?encryptdata=true]
   * Port is informational only (driver uses default 1025).
   */
  private parseConnectionString(url: string): {
    user: string; password: string; host: string; database: string; encryptdata: string;
  } {
    const match = url.match(/^teradata:\/\/([^:]+):([^@]+)@([^:/]+)(?::(\d+))?(?:\/([^?]*))?(?:\?(.+))?$/i);
    if (!match) {
      throw new Error(
        "Invalid Teradata connection string. Expected: teradata://user:password@host/database[?encryptdata=true]"
      );
    }

    const params = new URLSearchParams(match[6] ?? "");
    return {
      user: decodeURIComponent(match[1]),
      password: decodeURIComponent(match[2]),
      host: match[3],
      database: match[5] || "DBC",
      encryptdata: params.get("encryptdata") ?? "true",
    };
  }

  private async getConnection(): Promise<any> {
    if (!this.conn) {
      const td = await import("teradatasql");
      this.conn = new td.TeradataConnection();
      this.conn.connect({
        host: this.host,
        user: this.user,
        password: this.password,
        database: this.database,
        encryptdata: this.encryptdata,
        teradata_values: "false",
      });
    }
    return this.conn;
  }

  /**
   * Execute SQL and return rows as objects.
   * teradatasql returns arrays-of-arrays; cursor.description provides column names.
   */
  private async query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    const conn = await this.getConnection();
    const cur = conn.cursor();
    try {
      cur.execute(sqlText);
      const description: any[] = cur.description ?? [];
      const columnNames: string[] = description.map((d: any[]) => d[0].toLowerCase());
      const rawRows: any[][] = cur.fetchall();

      return rawRows.map((row: any[]) => {
        const obj: Record<string, unknown> = {};
        for (let i = 0; i < columnNames.length; i++) {
          const val = row[i];
          if (val == null) {
            obj[columnNames[i]] = null;
          } else if (typeof val === "string") {
            const trimmed = val.trimEnd();
            // With teradata_values=false, DECIMAL/NUMBER come back as strings — convert to numbers
            if (trimmed !== "" && !isNaN(Number(trimmed)) && trimmed !== " ") {
              obj[columnNames[i]] = Number(trimmed);
            } else {
              obj[columnNames[i]] = trimmed;
            }
          } else {
            obj[columnNames[i]] = val;
          }
        }
        return obj as T;
      });
    } finally {
      cur.close();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const rows = await this.query<Record<string, unknown>>(
        "SELECT InfoData AS version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
      );
      return {
        success: true,
        serverVersion: `Teradata ${(rows[0]?.version as string) ?? "unknown"}`,
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^teradata:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.conn) {
      this.conn.close();
      this.conn = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    const vRows = await this.query<Record<string, unknown>>(
      "SELECT InfoData AS version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
    );
    const serverVersion = `Teradata ${(vRows[0]?.version as string) ?? "unknown"}`;

    // In Teradata, database = schema
    const dbNames = options?.schemas?.length
      ? options.schemas
      : await this.discoverDatabaseNames();

    const schemas: DiscoveredSchema[] = [];
    for (const dbName of dbNames) {
      const tables = await this.discoverTablesAndViews(dbName);

      const includeRe = options?.includeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const excludeRe = options?.excludeTables?.map((p) => new RegExp(p.replace("*", ".*"), "i"));
      const filtered = tables.filter((t) => {
        if (includeRe?.length) return includeRe.some((r) => r.test(t.tableName));
        if (excludeRe?.length) return !excludeRe.some((r) => r.test(t.tableName));
        return true;
      });

      // Batch-load metadata
      const columnsByTable = await this.discoverAllColumns(dbName);
      const pksByTable = await this.discoverAllPrimaryKeys(dbName);
      const fksByTable = await this.discoverAllForeignKeys(dbName);
      const indexesByTable = await this.discoverAllIndexes(dbName);

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

      schemas.push({ schemaName: dbName, tables: filtered });
    }

    return {
      databaseName: this.database,
      serverVersion,
      databaseType: "TERADATA",
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
          const maxDist = req.maxDistinctValues ?? 10;
          const qualified = `${ident(req.schemaName)}.${ident(req.tableName)}`;
          const col = ident(colName);

          const distinctRows = await this.query<{ value: string }>(
            `SELECT DISTINCT TOP ${maxDist + 1} CAST(${col} AS VARCHAR(1000)) AS value
             FROM ${qualified} WHERE ${col} IS NOT NULL ORDER BY 1`
          );

          const distinctCount = distinctRows.length;
          if (distinctCount > maxDist) {
            samples.push({ columnName: colName, distinctCount, sampleValues: [] });
            continue;
          }

          const statsRows = await this.query<Record<string, unknown>>(
            `SELECT
              COUNT(DISTINCT ${col}) AS distinct_count,
              CAST(SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) AS FLOAT) / NULLIFZERO(COUNT(*)) AS null_fraction,
              MIN(CAST(${col} AS VARCHAR(1000))) AS min_value,
              MAX(CAST(${col} AS VARCHAR(1000))) AS max_value,
              CAST(AVG(CAST(CHAR_LENGTH(CAST(${col} AS VARCHAR(1000))) AS FLOAT)) AS INTEGER) AS avg_length
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

    const upperSql = sqlQuery.toUpperCase();
    const hasLimit = upperSql.includes(" TOP ") || upperSql.includes(" SAMPLE ");
    // Inject TOP N after SELECT
    const safeSql = hasLimit
      ? sqlQuery
      : sqlQuery.replace(/^(\s*SELECT\s)/i, `$1TOP ${maxRows + 1} `);

    const start = Date.now();
    try {
      const rows = await this.query<Record<string, unknown>>(safeSql);

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
  // PRIVATE — Discovery queries (DBC system views)
  // ═══════════════════════════════════════════════════════════════

  private async discoverDatabaseNames(): Promise<string[]> {
    const rows = await this.query<Record<string, unknown>>(`
      SELECT TRIM(DatabaseName) AS databasename
      FROM DBC.DatabasesV
      WHERE DBKind = 'D'
      ORDER BY DatabaseName
    `);

    return rows
      .map((r) => r.databasename as string)
      .filter((s) => !EXCLUDED_DATABASES.has(s));
  }

  private async discoverTablesAndViews(dbName: string): Promise<DiscoveredTable[]> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const rows = await this.query<Record<string, unknown>>(`
      SELECT TRIM(TableName) AS tablename,
             TableKind AS tablekind,
             TRIM(CommentString) AS commentstring
      FROM DBC.TablesV
      WHERE DatabaseName = '${esc(dbName)}'
        AND TableKind IN ('T', 'O', 'V')
      ORDER BY TableName
    `);

    return rows.map((r) => {
      const kind = (r.tablekind as string).trim();
      return {
        tableName: r.tablename as string,
        tableType: (kind === "V" ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
        detailedTableType: kind === "T" ? "TABLE (with PK)" : kind === "O" ? "TABLE (no PK)" : "VIEW",
        tableComment: (r.commentstring as string) || undefined,
        columns: [],
        foreignKeys: [],
        indexes: [],
      };
    });
  }

  /**
   * Batch-load ALL columns for a database via DBC.ColumnsV.
   */
  private async discoverAllColumns(dbName: string): Promise<Map<string, DiscoveredColumn[]>> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const rows = await this.query<Record<string, unknown>>(`
      SELECT TRIM(TableName) AS tablename,
             TRIM(ColumnName) AS columnname,
             ColumnId AS ordinalposition,
             TRIM(ColumnType) AS columntype,
             ColumnLength AS columnlength,
             Nullable AS nullable,
             DefaultValue AS defaultvalue,
             DecimalTotalDigits AS decimaltotaldigits,
             DecimalFractionalDigits AS decimalfractionaldigits,
             TRIM(CommentString) AS commentstring,
             IdColType AS idcoltype
      FROM DBC.ColumnsV
      WHERE DatabaseName = '${esc(dbName)}'
      ORDER BY TableName, ColumnId
    `);

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = r.tablename as string;
      if (!map.has(tableName)) map.set(tableName, []);

      const typeCode = (r.columntype as string).trim();
      const dataType = TYPE_MAP[typeCode] ?? typeCode;
      const colLength = r.columnlength != null ? Number(r.columnlength) : undefined;
      const totalDigits = r.decimaltotaldigits != null ? Number(r.decimaltotaldigits) : undefined;
      const fracDigits = r.decimalfractionaldigits != null ? Number(r.decimalfractionaldigits) : undefined;

      // Build fullDataType
      let fullDataType = dataType;
      if (totalDigits != null && fracDigits != null && (dataType === "DECIMAL" || dataType === "NUMBER")) {
        fullDataType = `${dataType}(${totalDigits},${fracDigits})`;
      } else if (colLength != null && (dataType === "VARCHAR" || dataType === "CHAR")) {
        fullDataType = `${dataType}(${colLength})`;
      }

      const isCharType = /CHAR|VARCHAR/i.test(dataType);
      const idColType = r.idcoltype as string | null;
      const isAutoIncrement = idColType != null && (idColType.trim() === "GA" || idColType.trim() === "GD");

      map.get(tableName)!.push({
        columnName: r.columnname as string,
        ordinalPosition: Number(r.ordinalposition),
        dataType: dataType.toLowerCase(),
        fullDataType,
        isNullable: (r.nullable as string)?.trim() === "Y",
        columnDefault: r.defaultvalue != null ? String(r.defaultvalue).trim() : undefined,
        characterMaxLength: isCharType ? colLength : undefined,
        numericPrecision: !isCharType ? totalDigits : undefined,
        numericScale: fracDigits,
        columnComment: (r.commentstring as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement,
      });
    }
    return map;
  }

  /**
   * Batch-load ALL primary keys via DBC.IndicesV (IndexType = 'K').
   */
  private async discoverAllPrimaryKeys(dbName: string): Promise<Map<string, DiscoveredPrimaryKey>> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const rows = await this.query<Record<string, unknown>>(`
      SELECT TRIM(TableName) AS tablename,
             TRIM(IndexName) AS indexname,
             TRIM(ColumnName) AS columnname,
             ColumnPosition AS columnposition
      FROM DBC.IndicesV
      WHERE DatabaseName = '${esc(dbName)}'
        AND IndexType = 'K'
      ORDER BY TableName, IndexName, ColumnPosition
    `);

    const map = new Map<string, DiscoveredPrimaryKey>();
    for (const r of rows) {
      const tableName = r.tablename as string;
      if (!map.has(tableName)) {
        map.set(tableName, { constraintName: (r.indexname as string) || "PRIMARY", columns: [] });
      }
      map.get(tableName)!.columns.push(r.columnname as string);
    }
    return map;
  }

  /**
   * Batch-load ALL foreign keys via DBC.All_RI_ParentsV.
   */
  private async discoverAllForeignKeys(dbName: string): Promise<Map<string, DiscoveredForeignKey[]>> {
    const esc = (s: string) => s.replace(/'/g, "''");
    try {
      const rows = await this.query<Record<string, unknown>>(`
        SELECT TRIM(ChildTable) AS childtable,
               TRIM(ChildKeyColumn) AS childkeycolumn,
               TRIM(ParentDB) AS parentdb,
               TRIM(ParentTable) AS parenttable,
               TRIM(ParentKeyColumn) AS parentkeycolumn,
               TRIM(IndexName) AS indexname
        FROM DBC.All_RI_ParentsV
        WHERE ChildDB = '${esc(dbName)}'
        ORDER BY ChildTable, IndexName
      `);

      const fkMap = new Map<string, Map<string, DiscoveredForeignKey>>();
      for (const r of rows) {
        const tableName = r.childtable as string;
        const indexName = r.indexname as string;

        if (!fkMap.has(tableName)) fkMap.set(tableName, new Map());
        const tableMap = fkMap.get(tableName)!;

        if (!tableMap.has(indexName)) {
          tableMap.set(indexName, {
            constraintName: indexName,
            columns: [],
            referencedSchema: r.parentdb as string,
            referencedTable: r.parenttable as string,
            referencedColumns: [],
          });
        }
        const fk = tableMap.get(indexName)!;
        fk.columns.push(r.childkeycolumn as string);
        fk.referencedColumns.push(r.parentkeycolumn as string);
      }

      const result = new Map<string, DiscoveredForeignKey[]>();
      for (const [tableName, tableMap] of fkMap) {
        result.set(tableName, Array.from(tableMap.values()));
      }
      return result;
    } catch {
      // DBC.All_RI_ParentsV may not be accessible
      return new Map();
    }
  }

  /**
   * Batch-load ALL indexes via DBC.IndicesV (excluding PK and partitioning).
   */
  private async discoverAllIndexes(dbName: string): Promise<Map<string, DiscoveredIndex[]>> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const rows = await this.query<Record<string, unknown>>(`
      SELECT TRIM(TableName) AS tablename,
             TRIM(IndexName) AS indexname,
             TRIM(ColumnName) AS columnname,
             UniqueFlag AS uniqueflag,
             ColumnPosition AS columnposition
      FROM DBC.IndicesV
      WHERE DatabaseName = '${esc(dbName)}'
        AND IndexType NOT IN ('K', 'Q')
      ORDER BY TableName, IndexName, ColumnPosition
    `);

    const idxMap = new Map<string, Map<string, DiscoveredIndex>>();
    for (const r of rows) {
      const tableName = r.tablename as string;
      const indexName = r.indexname as string;
      if (!indexName) continue;

      if (!idxMap.has(tableName)) idxMap.set(tableName, new Map());
      const tableMap = idxMap.get(tableName)!;

      if (!tableMap.has(indexName)) {
        tableMap.set(indexName, {
          indexName,
          columns: [],
          isUnique: (r.uniqueflag as string)?.trim() === "Y",
        });
      }
      tableMap.get(indexName)!.columns.push(r.columnname as string);
    }

    const result = new Map<string, DiscoveredIndex[]>();
    for (const [tableName, tableMap] of idxMap) {
      result.set(tableName, Array.from(tableMap.values()));
    }
    return result;
  }
}
