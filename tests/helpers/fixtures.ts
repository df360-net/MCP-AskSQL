/**
 * Shared test fixtures — mock data for all test suites.
 */
import { jest } from "@jest/globals";

import type { LogEntry } from "../../src/query-logger.js";

// ── Discovered Database (schema cache format) ─────────────────────

export const MOCK_DISCOVERED_DB_PG = {
  databaseName: "df360_claude",
  databaseType: "postgresql",
  schemas: [
    {
      schemaName: "df360",
      tables: [
        {
          tableName: "df360_app",
          tableComment: "Application registry",
          columns: [
            { columnName: "app_id", dataType: "integer", nullable: false, isPrimaryKey: true },
            { columnName: "app_name", dataType: "varchar", nullable: false, isPrimaryKey: false },
            { columnName: "domain", dataType: "varchar", nullable: true, isPrimaryKey: false },
          ],
        },
        {
          tableName: "df360_data_element",
          tableComment: "Data element catalog",
          columns: [
            { columnName: "element_id", dataType: "integer", nullable: false, isPrimaryKey: true },
            { columnName: "element_name", dataType: "varchar", nullable: false, isPrimaryKey: false },
            { columnName: "app_id", dataType: "integer", nullable: false, isPrimaryKey: false },
          ],
        },
        {
          tableName: "df360_support_group",
          tableComment: "Support groups",
          columns: [
            { columnName: "group_id", dataType: "integer", nullable: false, isPrimaryKey: true },
            { columnName: "group_name", dataType: "varchar", nullable: false, isPrimaryKey: false },
          ],
        },
      ],
    },
  ],
};

export const MOCK_DISCOVERED_DB_SF = {
  databaseName: "SNOWFLAKE_SAMPLE_DATA",
  databaseType: "snowflake",
  schemas: [
    {
      schemaName: "TPCH_SF1",
      tables: [
        {
          tableName: "ORDERS",
          tableComment: "TPC-H orders",
          columns: [
            { columnName: "O_ORDERKEY", dataType: "NUMBER", nullable: false, isPrimaryKey: true },
            { columnName: "O_CUSTKEY", dataType: "NUMBER", nullable: false, isPrimaryKey: false },
            { columnName: "O_TOTALPRICE", dataType: "NUMBER", nullable: true, isPrimaryKey: false },
          ],
        },
        {
          tableName: "CUSTOMER",
          tableComment: "TPC-H customers",
          columns: [
            { columnName: "C_CUSTKEY", dataType: "NUMBER", nullable: false, isPrimaryKey: true },
            { columnName: "C_NAME", dataType: "VARCHAR", nullable: false, isPrimaryKey: false },
          ],
        },
      ],
    },
  ],
};

// ── Config fixtures ───────────────────────────────────────────────

export const VALID_FILE_CONFIG = {
  connectors: [
    {
      id: "pg_test",
      connectionString: "postgres://user:pass@localhost:5432/testdb",
      schemas: ["public"],
    },
  ],
  ai: {
    baseUrl: "https://api.example.com/v1",
    apiKey: "sk-test-key-1234567890",
    model: "test-model",
    maxTokens: 4096,
    temperature: 0.3,
  },
  schemaCacheTtlHours: 24,
};

export const VALID_FILE_CONFIG_MULTI = {
  connectors: [
    {
      id: "pg_test",
      connectionString: "postgres://user:pass@localhost:5432/testdb",
      schemas: ["public"],
    },
    {
      id: "sf_test",
      connectionString: "snowflake://user:pass@account/db",
      schemas: ["TPCH_SF1"],
    },
  ],
  ai: {
    baseUrl: "https://api.example.com/v1",
    apiKey: "sk-test-key-1234567890",
    model: "test-model",
  },
};

// ── Log entry fixtures ────────────────────────────────────────────

export const MOCK_LOG_ENTRIES: Omit<LogEntry, "id" | "timestamp">[] = [
  { tool: "ask", connector: "pg_test", question: "show all apps", sql: "SELECT * FROM df360_app", success: true, executionTimeMs: 150, rowCount: 10 },
  { tool: "ask", connector: "sf_test", question: "show orders", sql: "SELECT * FROM ORDERS", success: true, executionTimeMs: 300, rowCount: 50 },
  { tool: "execute_sql", connector: "pg_test", sql: "SELECT 1", success: true, executionTimeMs: 5, rowCount: 1 },
  { tool: "ask", connector: "pg_test", question: "bad query", success: false, error: "AI failed", executionTimeMs: 1000 },
  { tool: "health_check", connector: "sf_test", success: true, executionTimeMs: 200 },
];

// ── Mock AskSQL instance ──────────────────────────────────────────

export function createMockAskSQL() {
  return {
    ask: jest.fn<any>().mockResolvedValue({
      success: true,
      sql: "SELECT * FROM test",
      explanation: "Selects all rows from test",
      rows: [{ id: 1, name: "test" }],
      rowCount: 1,
      truncated: false,
      executionTimeMs: 42,
      totalTimeMs: 50,
      retries: 0,
      tokenUsage: { promptTokens: 100, completionTokens: 50, totalTokens: 150, estimatedCost: 0.01 },
      columns: [{ name: "id", type: "int" }, { name: "name", type: "varchar" }],
    }),
    generateSQL: jest.fn<any>().mockResolvedValue({
      sql: "SELECT * FROM test",
      explanation: "Selects all rows from test",
      tokenUsage: { promptTokens: 100, completionTokens: 50, totalTokens: 150, estimatedCost: 0.01 },
    }),
    executeSQL: jest.fn<any>().mockResolvedValue({
      rows: [{ id: 1 }],
      columns: [{ name: "id", type: "int" }],
      rowCount: 1,
      truncated: false,
      executionTimeMs: 10,
    }),
    healthCheck: jest.fn<any>().mockResolvedValue({
      database: { connected: true, version: "16.0" },
      ai: { reachable: true },
    }),
    refreshCatalog: jest.fn<any>().mockResolvedValue({
      tables: 5,
      columns: 20,
      discovered: MOCK_DISCOVERED_DB_PG,
    }),
    loadFromCache: jest.fn<any>().mockReturnValue({ tables: 5, columns: 20 }),
    close: jest.fn<any>().mockResolvedValue(undefined),
  };
}

// ── Mock ConnectorManager ─────────────────────────────────────────

export function createMockConnectorManager() {
  const mockAsksql = createMockAskSQL();

  return {
    get: jest.fn<any>().mockReturnValue(mockAsksql),
    listConnectors: jest.fn<any>().mockReturnValue([
      { id: "pg_test", type: "postgresql", schemas: ["public"], isDefault: true, cached: true, cacheAgeHours: 2.1 },
      { id: "sf_test", type: "snowflake", schemas: ["TPCH_SF1"], isDefault: false, cached: true, cacheAgeHours: 5.0 },
    ]),
    getConnectorConfig: jest.fn<any>().mockImplementation((id: string) => {
      if (id === "pg_test") return { id: "pg_test", connectionString: "postgres://user:secret@localhost:5432/db", schemas: ["public"] };
      if (id === "sf_test") return { id: "sf_test", connectionString: "snowflake://user:secret@account/db", schemas: ["TPCH_SF1"] };
      return undefined;
    }),
    getSchemaInfo: jest.fn<any>().mockReturnValue({ tables: 3, columns: 8, tableNames: ["df360.df360_app", "df360.df360_data_element", "df360.df360_support_group"], cacheAgeHours: 2.1 }),
    getSchemaDetail: jest.fn<any>().mockReturnValue(MOCK_DISCOVERED_DB_PG),
    getAIConfig: jest.fn<any>().mockReturnValue({ baseUrl: "https://api.example.com/v1", apiKey: "sk-test-key-1234567890", model: "test-model", maxTokens: 4096, temperature: 0.3 }),
    getSafetyConfig: jest.fn<any>().mockReturnValue({ maxRows: 5000, timeoutMs: 30000, maxRetries: 2 }),
    routeQuestion: jest.fn<any>().mockResolvedValue({ connectorId: "pg_test", method: "keyword", confidence: "3 keyword matches" }),
    refreshSchema: jest.fn<any>().mockResolvedValue({ connector: "pg_test", tables: 5, columns: 20 }),
    addConnector: jest.fn<any>().mockResolvedValue({ id: "new_conn", type: "mysql", schemas: ["public"], isDefault: false, cached: false, cacheAgeHours: null }),
    updateConnector: jest.fn<any>().mockResolvedValue({ id: "pg_test", type: "postgresql", schemas: ["public"], isDefault: true, cached: true, cacheAgeHours: 0 }),
    removeConnector: jest.fn<any>().mockResolvedValue(undefined),
    updateAIConfig: jest.fn<any>().mockResolvedValue(undefined),
    close: jest.fn<any>().mockResolvedValue(undefined),
    _mockAsksql: mockAsksql,
  };
}

// ── Mock ConfigStore ──────────────────────────────────────────────

export function createMockConfigStore() {
  const stored = { ...VALID_FILE_CONFIG_MULTI };
  return {
    read: jest.fn<any>().mockReturnValue(JSON.parse(JSON.stringify(stored))),
    write: jest.fn<any>(),
    updateConnectors: jest.fn<any>(),
    updateAI: jest.fn<any>(),
    getFilePath: jest.fn<any>().mockReturnValue("/fake/config.json"),
  };
}

// ── Mock QueryLogger ──────────────────────────────────────────────

export function createMockQueryLogger() {
  return {
    log: jest.fn<any>(),
    query: jest.fn<any>().mockReturnValue({ rows: [], total: 0 }),
    stats: jest.fn<any>().mockReturnValue({ totalQueries: 10, successful: 8, failed: 2, avgExecutionTimeMs: 150, byConnector: { pg_test: 7, sf_test: 3 }, byTool: { ask: 6, execute_sql: 3, health_check: 1 } }),
    clear: jest.fn<any>(),
  };
}
