/**
 * AskSQL Connector Interface
 *
 * Layer 2 of the 2-layer architecture.
 * Every connector implements this interface.
 *
 * Layer 2 (connector): Reads metadata from the specific database, returns standardized types.
 * Layer 1 (CatalogManager): Persists to ask_catalog_* tables, builds LLM context.
 */

import type {
  DiscoveredDatabase,
  SampleCollectionRequest,
  SampleCollectionResult,
} from "./discovery-types.js";

// ---------------------------------------------------------------------------
// Connector Types
// ---------------------------------------------------------------------------

export type ConnectorType =
  | "postgresql"
  | "mysql"
  | "mssql"
  | "oracle"
  | "sqlite"
  | "bigquery"
  | "snowflake"
  | "redshift"
  | "databricks"
  | "dremio"
  | "teradata";

export type SQLDialect =
  | "postgresql"
  | "mysql"
  | "tsql"
  | "plsql"
  | "sqlite"
  | "bigquery"
  | "snowflake"
  | "redshift"
  | "databricks-sql"
  | "teradata";

// ---------------------------------------------------------------------------
// SQL Dialect Hints (for prompt builder and validator)
// ---------------------------------------------------------------------------

export interface SQLDialectHints {
  identifierQuote: '"' | "`" | "[]";
  limitSyntax: "LIMIT" | "TOP" | "FETCH_FIRST";
  qualificationPattern:
    | "schema.table"
    | "database.schema.table"
    | "project.dataset.table";
  supportsILIKE: boolean;
  supportsCTE: boolean;
  supportsWindowFunctions: boolean;
  currentTimestampFunction: string;
  dateDiffHint: string;
  booleanLiterals: { true: string; false: string };
  additionalPromptHints: string[];
}

// ---------------------------------------------------------------------------
// Discovery Options
// ---------------------------------------------------------------------------

export interface DiscoveryOptions {
  schemas?: string[];
  includeTables?: string[];
  excludeTables?: string[];
}

// ---------------------------------------------------------------------------
// Query Execution Types
// ---------------------------------------------------------------------------

export interface ExecuteOptions {
  maxRows?: number;
  timeoutMs?: number;
}

export interface RawQueryResult {
  rows: Record<string, unknown>[];
  columns: Array<{ name: string; type: string }>;
  rowCount: number;
  truncated: boolean;
  executionTimeMs: number;
}

export interface ConnectionTestResult {
  success: boolean;
  serverVersion?: string;
  error?: string;
  latencyMs: number;
}

// ---------------------------------------------------------------------------
// The Core Connector Interface
// ---------------------------------------------------------------------------

export interface AskSQLConnector {
  /** Connector type identifier */
  readonly type: ConnectorType;
  /** Human-readable display name */
  readonly displayName: string;
  /** SQL dialect identifier */
  readonly dialect: SQLDialect;
  /** Dialect-specific hints for prompt builder and validator */
  readonly dialectHints: SQLDialectHints;

  /** Test connectivity, return server version */
  testConnection(): Promise<ConnectionTestResult>;

  /**
   * Discover all metadata from the connected database.
   * Returns a standardized DiscoveredDatabase structure.
   * Does NOT write to ask_catalog_* tables — that is CatalogManager's job.
   */
  discover(options?: DiscoveryOptions): Promise<DiscoveredDatabase>;

  /**
   * Collect sample values for specific columns.
   * Called by CatalogManager after catalog persist, for columns
   * identified as low-cardinality candidates.
   */
  collectSamples(requests: SampleCollectionRequest[]): Promise<SampleCollectionResult[]>;

  /** Execute a read-only SQL query with safety limits */
  executeQuery(sql: string, options?: ExecuteOptions): Promise<RawQueryResult>;

  /** Close connections and release resources */
  disconnect(): Promise<void>;

  /** Return true if this connector can handle the given connection string */
  canHandle(connectionString: string): boolean;
}
