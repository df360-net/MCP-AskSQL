// ---------------------------------------------------------------------------
// @asksql/core — public API (standalone, no catalog/events/db)
// ---------------------------------------------------------------------------

// Main class
export { AskSQL } from "./asksql.js";
export type { AskSQLConfig, AskSQLResult, GenerateSQLResult, QueryResult } from "./asksql.js";

// Connector interface & registry
export type {
  AskSQLConnector,
  ConnectorType,
  SQLDialect,
  SQLDialectHints,
  DiscoveryOptions,
  ExecuteOptions,
  RawQueryResult,
  ConnectionTestResult,
} from "./connector/interface.js";

export {
  registerConnector,
  createConnector,
  detectConnectorType,
  listConnectors,
} from "./connector/registry.js";

// Discovery types
export type {
  DiscoveredDatabase,
  DiscoveredSchema,
  DiscoveredTable,
  DiscoveredColumn,
  DiscoveredPrimaryKey,
  DiscoveredUniqueConstraint,
  DiscoveredForeignKey,
  DiscoveredIndex,
  DiscoveredPartitionInfo,
  DiscoveredColumnSample,
  SampleCollectionRequest,
  SampleCollectionResult,
  CatalogRefreshSummary,
  LLMContext,
} from "./connector/discovery-types.js";

// AI client
export { AIClient } from "./ai/client.js";
export type { AIConfig, ChatMessage, AICallResult, TokenUsage } from "./ai/client.js";

// SQL validator
export { validateSql } from "./validator/sql-validator.js";
export type { SqlValidationResult } from "./validator/sql-validator.js";
