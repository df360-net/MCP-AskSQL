/**
 * Amazon Redshift Connector — Layer 2
 *
 * Reads metadata from Redshift via pg_catalog + INFORMATION_SCHEMA.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 *
 * Uses the `pg` driver (not postgres.js — Redshift lacks the `typarray`
 * catalog column that postgres.js requires). This is a hard requirement
 * proven in production Redshift environments.
 *
 * Connection string: redshift://user:password@host:5439/database
 *
 * Supports both Redshift Serverless and Provisioned clusters.
 * Redshift hierarchy: Cluster → Database → Schema → Table
 */

import pg from "pg";
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

const REDSHIFT_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: '"',
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: true,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "GETDATE()",
  dateDiffHint: "Use DATEDIFF(day, date1, date2) or DATEADD(day, -N, GETDATE()) for date arithmetic",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "Use ILIKE for case-insensitive pattern matching",
    "Use LEN() for string length (not LENGTH())",
    "Use LISTAGG(col, ',') WITHIN GROUP (ORDER BY col) for string aggregation",
    "Use NVL(col, default) or COALESCE() for null handling",
    "Use GETDATE() or SYSDATE for current timestamp (not NOW())",
    "Redshift does NOT support LATERAL joins — use subqueries or CTEs instead",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "pg_catalog", "information_schema", "pg_internal", "pg_auto_copy",
  "pg_temp_1", "pg_toast", "pg_toast_temp_1",
]);

/** Escape single quotes for string interpolation (required for pg_catalog queries on Redshift) */
const esc = (s: string) => s.replace(/'/g, "''");

export class RedshiftConnector implements AskSQLConnector {
  readonly type: ConnectorType = "redshift";
  readonly displayName = "Amazon Redshift";
  readonly dialect: SQLDialect = "redshift";
  readonly dialectHints = REDSHIFT_DIALECT_HINTS;

  private client: pg.Client | null = null;
  private pgConfig: pg.ClientConfig;
  private database: string;
  private maxSampleValues: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("RedshiftConnector requires a connectionString");
    }

    const parsed = this.parseConnectionString(connectionString);
    this.database = (config.catalog as string) ?? parsed.database;
    this.pgConfig = {
      host: parsed.host,
      port: parsed.port,
      user: parsed.user,
      password: parsed.password,
      database: this.database,
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: (config.connectTimeoutMs as number) ?? 60000, // Redshift Serverless cold start can take 30-45s
    };
    this.maxSampleValues = (config.maxSampleValues as number) ?? 10;
  }

  /**
   * Parse: redshift://user:password@host[:port][/database]
   * Also accepts postgres:// with .redshift.amazonaws.com hostname.
   * Default port: 5439. Default database: dev.
   */
  private parseConnectionString(url: string): {
    user: string; password: string; host: string; port: number; database: string;
  } {
    const match = url.match(/^(?:redshift|postgres(?:ql)?):\/\/([^:]+):([^@]+)@([^:/]+)(?::(\d+))?(?:\/([^?]*))?/i);
    if (!match) {
      throw new Error(
        "Invalid Redshift connection string. Expected: redshift://user:password@host[:port][/database]"
      );
    }

    return {
      user: decodeURIComponent(match[1]),
      password: decodeURIComponent(match[2]),
      host: match[3],
      port: match[4] ? parseInt(match[4], 10) : 5439,
      database: match[5] || "dev",
    };
  }

  private async getClient(): Promise<pg.Client> {
    if (!this.client) {
      this.client = new pg.Client(this.pgConfig);
      // Auto-reconnect on connection loss
      this.client.on("error", () => { this.client = null; });
      await this.client.connect();
    }
    return this.client;
  }

  private async query<T = Record<string, unknown>>(sqlText: string): Promise<T[]> {
    let client: pg.Client;
    try {
      client = await this.getClient();
    } catch {
      // Reconnect on stale connection
      this.client = null;
      client = await this.getClient();
    }
    const { rows } = await client.query(sqlText);
    return rows as T[];
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const rows = await this.query<Record<string, unknown>>(
        "SELECT current_database() AS db, version() AS ver"
      );
      return {
        success: true,
        serverVersion: (rows[0]?.ver as string) ?? "Amazon Redshift",
        latencyMs: Date.now() - start,
      };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^redshift:\/\//i.test(connectionString) ||
           /\.redshift\.amazonaws\.com/i.test(connectionString) ||
           /\.redshift-serverless\.amazonaws\.com/i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.end();
      this.client = null;
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    const vRows = await this.query<Record<string, unknown>>("SELECT version() AS ver");
    const serverVersion = (vRows[0]?.ver as string) ?? "Amazon Redshift";

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

      // Batch-load metadata for this schema
      const columnsByTable = await this.discoverAllColumns(schemaName);
      const pksByTable = await this.discoverAllPrimaryKeys(schemaName);
      const fksByTable = await this.discoverAllForeignKeys(schemaName);
      const sortKeysByTable = await this.discoverAllSortKeys(schemaName);

      for (const table of filtered) {
        table.columns = columnsByTable.get(table.tableName) ?? [];
        table.indexes = sortKeysByTable.get(table.tableName) ?? [];

        // PK
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
      databaseType: "REDSHIFT",
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
              CAST(SUM(CASE WHEN ${col} IS NULL THEN 1 ELSE 0 END) AS FLOAT) / GREATEST(COUNT(*), 1) AS null_fraction,
              MIN(CAST(${col} AS VARCHAR)) AS min_value,
              MAX(CAST(${col} AS VARCHAR)) AS max_value,
              CAST(AVG(LEN(CAST(${col} AS VARCHAR))) AS INTEGER) AS avg_length
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
    const hasLimit = upperSql.includes("LIMIT");
    const safeSql = hasLimit
      ? sqlQuery
      : `${sqlQuery.replace(/;?\s*$/, "")} LIMIT ${maxRows + 1}`;

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
  // PRIVATE — Discovery queries
  // Redshift Serverless doesn't support parameterized queries ($1/$2)
  // against pg_catalog tables — silently returns 0 rows.
  // Use string interpolation with esc() for trusted schema/table names.
  // ═══════════════════════════════════════════════════════════════

  private async discoverSchemaNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) return configSchemas;

    try {
      // Prefer svv_redshift_schemas (excludes external/Spectrum schemas)
      const rows = await this.query<Record<string, unknown>>(`
        SELECT DISTINCT schema_name
        FROM svv_redshift_schemas
        WHERE database_name = '${esc(this.database)}'
        ORDER BY schema_name
      `);
      return rows
        .map((r) => r.schema_name as string)
        .filter((s) => !EXCLUDED_SCHEMAS.has(s));
    } catch {
      // Fallback to information_schema
      const rows = await this.query<Record<string, unknown>>(`
        SELECT schema_name FROM information_schema.schemata ORDER BY schema_name
      `);
      return rows
        .map((r) => r.schema_name as string)
        .filter((s) => !EXCLUDED_SCHEMAS.has(s));
    }
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    // Tables with row counts from svv_table_info
    const tableRows = await this.query<Record<string, unknown>>(`
      SELECT c.relname AS table_name,
             COALESCE(ti.tbl_rows, 0)::bigint AS estimated_row_count,
             pgd.description AS table_comment,
             c.relkind AS relkind
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN svv_table_info ti ON ti.schema = n.nspname AND ti."table" = c.relname
      LEFT JOIN pg_catalog.pg_description pgd ON pgd.objsubid = 0 AND pgd.objoid = c.oid
      WHERE n.nspname = '${esc(schemaName)}'
        AND c.relkind IN ('r', 'v')
      ORDER BY c.relname
    `);

    return tableRows.map((r) => {
      const tableName = (r.table_name as string).trim(); // Redshift pads relname with spaces
      const isView = (r.relkind as string) === "v";
      const rowCount = r.estimated_row_count != null ? Number(r.estimated_row_count) : undefined;

      return {
        tableName,
        tableType: (isView ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
        estimatedRowCount: isView ? undefined : rowCount,
        tableComment: (r.table_comment as string) || undefined,
        columns: [],
        foreignKeys: [],
        indexes: [],
      };
    });
  }

  /**
   * Batch-load ALL columns for a schema via pg_catalog.
   * Uses string interpolation (not $1) — Redshift Serverless quirk.
   */
  private async discoverAllColumns(schemaName: string): Promise<Map<string, DiscoveredColumn[]>> {
    const rows = await this.query<Record<string, unknown>>(`
      SELECT c.relname AS table_name,
             att.attname AS column_name,
             att.attnum AS ordinal_position,
             format_type(att.atttypid, att.atttypmod) AS full_data_type,
             NOT att.attnotnull AS is_nullable,
             pg_catalog.col_description(att.attrelid, att.attnum) AS column_comment,
             pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS column_default
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_attribute att ON att.attrelid = c.oid
      LEFT JOIN pg_catalog.pg_attrdef ad ON (att.attrelid, att.attnum) = (ad.adrelid, ad.adnum)
      WHERE n.nspname = '${esc(schemaName)}'
        AND c.relkind IN ('r', 'v')
        AND att.attnum > 0 AND NOT att.attisdropped
      ORDER BY c.relname, att.attnum
    `);

    const map = new Map<string, DiscoveredColumn[]>();
    for (const r of rows) {
      const tableName = (r.table_name as string).trim();
      if (!map.has(tableName)) map.set(tableName, []);

      const fullType = r.full_data_type as string;
      const baseType = fullType.replace(/\(.*\)/, "").trim().toLowerCase();
      const colDefault = r.column_default as string | null;

      // Detect auto-increment (identity columns)
      const isAutoIncrement = colDefault != null && (
        colDefault.includes('"identity"') || colDefault.includes("default_identity")
      );

      // Parse precision/scale
      const precMatch = fullType.match(/\((\d+)(?:,(\d+))?\)/);
      const isCharType = /char|varchar|text/i.test(baseType);

      map.get(tableName)!.push({
        columnName: r.column_name as string,
        ordinalPosition: Number(r.ordinal_position),
        dataType: baseType,
        fullDataType: fullType,
        isNullable: r.is_nullable as boolean,
        columnDefault: colDefault ?? undefined,
        characterMaxLength: isCharType && precMatch ? parseInt(precMatch[1], 10) : undefined,
        numericPrecision: !isCharType && precMatch ? parseInt(precMatch[1], 10) : undefined,
        numericScale: precMatch?.[2] ? parseInt(precMatch[2], 10) : undefined,
        columnComment: (r.column_comment as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement,
      });
    }
    return map;
  }

  /**
   * Batch-load ALL primary keys for a schema.
   */
  private async discoverAllPrimaryKeys(schemaName: string): Promise<Map<string, DiscoveredPrimaryKey>> {
    const rows = await this.query<Record<string, unknown>>(`
      SELECT tc.table_name, tc.constraint_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name
        AND kcu.table_schema = tc.table_schema
      WHERE tc.table_schema = '${esc(schemaName)}'
        AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY tc.table_name, kcu.ordinal_position
    `);

    const map = new Map<string, DiscoveredPrimaryKey>();
    for (const r of rows) {
      const tableName = r.table_name as string;
      if (!map.has(tableName)) {
        map.set(tableName, { constraintName: r.constraint_name as string, columns: [] });
      }
      map.get(tableName)!.columns.push(r.column_name as string);
    }
    return map;
  }

  /**
   * Batch-load ALL foreign keys for a schema.
   */
  private async discoverAllForeignKeys(schemaName: string): Promise<Map<string, DiscoveredForeignKey[]>> {
    const rows = await this.query<Record<string, unknown>>(`
      SELECT tc.table_name, tc.constraint_name, kcu.column_name,
             ccu.table_schema AS referenced_schema,
             ccu.table_name AS referenced_table,
             ccu.column_name AS referenced_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name
        AND kcu.table_schema = tc.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.table_schema = '${esc(schemaName)}'
        AND tc.constraint_type = 'FOREIGN KEY'
      ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
    `);

    // Group by table + constraint
    const fkMap = new Map<string, Map<string, DiscoveredForeignKey>>();
    for (const r of rows) {
      const tableName = r.table_name as string;
      const constraintName = r.constraint_name as string;

      if (!fkMap.has(tableName)) fkMap.set(tableName, new Map());
      const tableMap = fkMap.get(tableName)!;

      if (!tableMap.has(constraintName)) {
        tableMap.set(constraintName, {
          constraintName,
          columns: [],
          referencedSchema: r.referenced_schema as string,
          referencedTable: r.referenced_table as string,
          referencedColumns: [],
        });
      }
      const fk = tableMap.get(constraintName)!;
      fk.columns.push(r.column_name as string);
      fk.referencedColumns.push(r.referenced_column as string);
    }

    const result = new Map<string, DiscoveredForeignKey[]>();
    for (const [tableName, tableMap] of fkMap) {
      result.set(tableName, Array.from(tableMap.values()));
    }
    return result;
  }

  /**
   * Batch-load ALL sort keys for a schema (Redshift's analog to indexes).
   */
  private async discoverAllSortKeys(schemaName: string): Promise<Map<string, DiscoveredIndex[]>> {
    const rows = await this.query<Record<string, unknown>>(`
      SELECT c.relname AS table_name,
             att.attname AS column_name,
             att.attsortkeyord AS sortkey_position
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_attribute att ON att.attrelid = c.oid
      WHERE n.nspname = '${esc(schemaName)}'
        AND att.attsortkeyord > 0
        AND att.attnum > 0 AND NOT att.attisdropped
      ORDER BY c.relname, att.attsortkeyord
    `);

    const map = new Map<string, string[]>();
    for (const r of rows) {
      const tableName = (r.table_name as string).trim();
      if (!map.has(tableName)) map.set(tableName, []);
      map.get(tableName)!.push(r.column_name as string);
    }

    const result = new Map<string, DiscoveredIndex[]>();
    for (const [tableName, columns] of map) {
      result.set(tableName, [{ indexName: "sortkey", columns, isUnique: false }]);
    }
    return result;
  }
}
