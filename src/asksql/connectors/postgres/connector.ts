/**
 * PostgreSQL Connector — Layer 2
 *
 * Reads metadata from PostgreSQL's information_schema + pg_catalog.
 * Returns standardized DiscoveredDatabase to Layer 1 (CatalogManager).
 * Also handles query execution and sample collection.
 *
 * Also handles query execution and sample collection.
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
  DiscoveredPrimaryKey,
  DiscoveredForeignKey,
  DiscoveredIndex,
  SampleCollectionRequest,
  SampleCollectionResult,
  DiscoveredColumnSample,
} from "../../core/connector/discovery-types.js";

const PG_DIALECT_HINTS: SQLDialectHints = {
  identifierQuote: '"',
  limitSyntax: "LIMIT",
  qualificationPattern: "schema.table",
  supportsILIKE: true,
  supportsCTE: true,
  supportsWindowFunctions: true,
  currentTimestampFunction: "NOW()",
  dateDiffHint: "Use CURRENT_DATE - INTERVAL 'N days' for date arithmetic",
  booleanLiterals: { true: "TRUE", false: "FALSE" },
  additionalPromptHints: [
    "Use ILIKE for case-insensitive pattern matching",
    "Use ::TEXT for explicit type casting when needed",
  ],
};

const EXCLUDED_SCHEMAS = new Set([
  "pg_catalog", "information_schema", "pg_toast", "pg_temp_1", "pg_toast_temp_1",
]);

export class PostgresConnector implements AskSQLConnector {
  readonly type: ConnectorType = "postgresql";
  readonly displayName = "PostgreSQL";
  readonly dialect: SQLDialect = "postgresql";
  readonly dialectHints = PG_DIALECT_HINTS;

  private sql: postgres.Sql;
  private maxSampleValues: number;
  private lockTimeoutMs: number;

  constructor(config: Record<string, unknown>) {
    const connectionString = config.connectionString as string | undefined;
    if (!connectionString) {
      throw new Error("PostgresConnector requires a connectionString");
    }
    this.sql = postgres(connectionString, {
      max: (config.poolSize as number) ?? 5,
      idle_timeout: ((config.idleTimeoutMs as number) ?? 20000) / 1000,
      connect_timeout: ((config.connectTimeoutMs as number) ?? 10000) / 1000,
    });
    this.maxSampleValues = (config.maxSampleValues as number) ?? 20;
    this.lockTimeoutMs = (config.lockTimeoutMs as number) ?? 5000;
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async testConnection(): Promise<ConnectionTestResult> {
    const start = Date.now();
    try {
      const [row] = await this.sql`SELECT version() as version`;
      return { success: true, serverVersion: (row?.version as string) ?? "Unknown", latencyMs: Date.now() - start };
    } catch (err: unknown) {
      return { success: false, error: (err instanceof Error ? err.message : String(err)), latencyMs: Date.now() - start };
    }
  }

  canHandle(connectionString: string): boolean {
    return /^postgres(ql)?:\/\//i.test(connectionString);
  }

  async disconnect(): Promise<void> {
    await this.sql.end();
  }

  // ═══════════════════════════════════════════════════════════════
  // DISCOVERY (Layer 2 → returns standardized DiscoveredDatabase)
  // ═══════════════════════════════════════════════════════════════

  async discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase> {
    const start = Date.now();

    const [versionRow] = await this.sql`SELECT version() as version`;
    const serverVersion = versionRow.version as string;

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

    const dbRows = await this.sql`SELECT current_database() as db`;
    const dbName = (dbRows[0]?.db as string) ?? "unknown";

    return {
      databaseName: dbName,
      serverVersion,
      databaseType: "POSTGRESQL",
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

          // Get distinct values (limit to maxDist + 1 to detect high cardinality)
          const distinctRows = await this.sql.unsafe(
            `SELECT DISTINCT ${col}::TEXT AS value FROM ${qualified} WHERE ${col} IS NOT NULL ORDER BY 1 LIMIT ${maxDist + 1}`
          );

          const distinctCount = distinctRows.length;
          if (distinctCount > maxDist) {
            // High cardinality — skip sampling, just record count
            samples.push({ columnName: colName, distinctCount, sampleValues: [] });
            continue;
          }

          // Get stats
          const [stats] = await this.sql.unsafe(
            `SELECT
              COUNT(DISTINCT ${col}) AS distinct_count,
              (COUNT(*) FILTER (WHERE ${col} IS NULL))::FLOAT / GREATEST(COUNT(*), 1) AS null_fraction,
              MIN(${col}::TEXT) AS min_value,
              MAX(${col}::TEXT) AS max_value,
              AVG(LENGTH(${col}::TEXT))::INT AS avg_length
            FROM ${qualified}`
          ) as Record<string, unknown>[];

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

  async executeQuery(sql: string, options?: ExecuteOptions): Promise<RawQueryResult> {
    const maxRows = options?.maxRows ?? 5000;
    const timeoutMs = options?.timeoutMs ?? 30000;

    const upperSql = sql.toUpperCase();
    const safeSql = upperSql.includes("LIMIT") ? sql : `${sql.replace(/;?\s*$/, "")} LIMIT ${maxRows + 1}`;

    const start = Date.now();
    try {
      const result = await this.sql.begin(async (tx) => {
        await tx.unsafe(`SET LOCAL statement_timeout = '${Number(timeoutMs)}'`);
        await tx.unsafe(`SET LOCAL lock_timeout = '${this.lockTimeoutMs}'`);
        await tx.unsafe("SET TRANSACTION READ ONLY");
        return await tx.unsafe(safeSql);
      }) as Record<string, unknown>[];

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
        (err instanceof Error && err.message.includes("statement timeout"))
          ? `Query timed out after ${timeoutMs}ms`
          : (err instanceof Error ? err.message : String(err))
      );
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // PRIVATE — Discovery queries (information_schema + pg_catalog)
  // ═══════════════════════════════════════════════════════════════

  private async discoverSchemaNames(configSchemas?: string[]): Promise<string[]> {
    if (configSchemas && configSchemas.length > 0) return configSchemas;

    const rows = await this.sql`
      SELECT schema_name FROM information_schema.schemata
      WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'
      ORDER BY schema_name
    `;
    return rows.map((r) => r.schema_name as string).filter((s) => !EXCLUDED_SCHEMAS.has(s));
  }

  private async discoverTablesAndViews(schemaName: string): Promise<DiscoveredTable[]> {
    const tables = await this.sql`
      SELECT t.table_name, t.table_type,
             c.reltuples::bigint AS estimated_row_count,
             obj_description(c.oid) AS table_comment
      FROM information_schema.tables t
      JOIN pg_catalog.pg_class c ON c.relname = t.table_name
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
      WHERE t.table_schema = ${schemaName}
        AND t.table_type IN ('BASE TABLE', 'VIEW')
      ORDER BY t.table_name
    `;

    return tables.map((r) => ({
      tableName: r.table_name as string,
      tableType: (r.table_type === "VIEW" ? "VIEW" : "TABLE") as "TABLE" | "VIEW",
      detailedTableType: r.table_type as string,
      estimatedRowCount: r.estimated_row_count != null && Number(r.estimated_row_count) >= 0
        ? Number(r.estimated_row_count) : undefined,
      tableComment: (r.table_comment as string) || undefined,
      columns: [],
      foreignKeys: [],
      indexes: [],
    }));
  }

  private async discoverColumns(schemaName: string, tableName: string): Promise<DiscoveredColumn[]> {
    const rows = await this.sql`
      SELECT column_name, ordinal_position, data_type, udt_name,
             is_nullable, column_default, character_maximum_length,
             numeric_precision, numeric_scale,
             col_description(
               (SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = ${tableName} AND n.nspname = ${schemaName}),
               ordinal_position
             ) AS column_comment
      FROM information_schema.columns
      WHERE table_schema = ${schemaName} AND table_name = ${tableName}
      ORDER BY ordinal_position
    `;

    return rows.map((r) => {
      const udtName = r.udt_name as string;
      const charMaxLen = r.character_maximum_length != null ? Number(r.character_maximum_length) : undefined;
      const numPrec = r.numeric_precision != null ? Number(r.numeric_precision) : undefined;
      const numScale = r.numeric_scale != null ? Number(r.numeric_scale) : undefined;

      let fullDataType = udtName;
      if (charMaxLen) fullDataType = `${udtName}(${charMaxLen})`;
      else if (numPrec != null && numScale != null) fullDataType = `${udtName}(${numPrec},${numScale})`;
      else if (numPrec != null) fullDataType = `${udtName}(${numPrec})`;

      const colDefault = r.column_default as string | null;
      const isAutoIncrement = colDefault != null && (
        colDefault.startsWith("nextval(") || colDefault.includes("generated")
      );

      return {
        columnName: r.column_name as string,
        ordinalPosition: Number(r.ordinal_position),
        dataType: udtName,
        fullDataType,
        isNullable: r.is_nullable === "YES",
        columnDefault: colDefault ?? undefined,
        characterMaxLength: charMaxLen,
        numericPrecision: numPrec,
        numericScale: numScale,
        columnComment: (r.column_comment as string) || undefined,
        isPrimaryKey: false,
        isAutoIncrement,
      };
    });
  }

  private async discoverPrimaryKey(schemaName: string, tableName: string): Promise<DiscoveredPrimaryKey | undefined> {
    const rows = await this.sql`
      SELECT tc.constraint_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
      WHERE tc.table_schema = ${schemaName}
        AND tc.table_name = ${tableName}
        AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.ordinal_position
    `;
    if (rows.length === 0) return undefined;
    return {
      constraintName: rows[0].constraint_name as string,
      columns: rows.map((r) => r.column_name as string),
    };
  }

  private async discoverForeignKeys(schemaName: string, tableName: string): Promise<DiscoveredForeignKey[]> {
    const rows = await this.sql`
      SELECT
        tc.constraint_name,
        kcu.column_name,
        ccu.table_schema AS referenced_schema,
        ccu.table_name AS referenced_table,
        ccu.column_name AS referenced_column,
        rc.delete_rule AS on_delete,
        rc.update_rule AS on_update
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
      JOIN information_schema.referential_constraints rc
        ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.table_schema
      WHERE tc.table_schema = ${schemaName}
        AND tc.table_name = ${tableName}
        AND tc.constraint_type = 'FOREIGN KEY'
      ORDER BY tc.constraint_name, kcu.ordinal_position
    `;

    const fkMap = new Map<string, DiscoveredForeignKey>();
    for (const r of rows) {
      const name = r.constraint_name as string;
      if (!fkMap.has(name)) {
        fkMap.set(name, {
          constraintName: name,
          columns: [],
          referencedSchema: r.referenced_schema as string,
          referencedTable: r.referenced_table as string,
          referencedColumns: [],
          onDelete: r.on_delete as string,
          onUpdate: r.on_update as string,
        });
      }
      const fk = fkMap.get(name)!;
      fk.columns.push(r.column_name as string);
      fk.referencedColumns.push(r.referenced_column as string);
    }
    return Array.from(fkMap.values());
  }

  private async discoverIndexes(schemaName: string, tableName: string): Promise<DiscoveredIndex[]> {
    const rows = await this.sql`
      SELECT i.relname AS index_name, a.attname AS column_name, ix.indisunique AS is_unique
      FROM pg_catalog.pg_index ix
      JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
      JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
      JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
      WHERE n.nspname = ${schemaName}
        AND t.relname = ${tableName}
        AND NOT ix.indisprimary
      ORDER BY i.relname, a.attnum
    `;

    const idxMap = new Map<string, DiscoveredIndex>();
    for (const r of rows) {
      const name = r.index_name as string;
      if (!idxMap.has(name)) {
        idxMap.set(name, { indexName: name, columns: [], isUnique: r.is_unique as boolean });
      }
      idxMap.get(name)!.columns.push(r.column_name as string);
    }
    return Array.from(idxMap.values());
  }
}
