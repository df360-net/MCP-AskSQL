/**
 * Query Executor — safe SQL execution via connector
 *
 * Responsibilities:
 * - Validate SQL before execution
 * - Delegate to connector.executeQuery()
 * - Return structured result with column metadata
 */

import type { AskSQLConnector, RawQueryResult } from "../connector/interface.js";
import { validateSql } from "../validator/sql-validator.js";

export interface QueryExecutorOptions {
  maxRows?: number;
  timeoutMs?: number;
  /** Skip SQL validation (already validated by caller) */
  skipValidation?: boolean;
}

export interface ExecutionResult {
  success: boolean;
  rows: Record<string, unknown>[];
  columns: Array<{ name: string; type: string }>;
  rowCount: number;
  truncated: boolean;
  executionTimeMs: number;
  error?: string;
}

/**
 * Execute validated SQL through the connector.
 * Validates safety first, then delegates to the connector.
 */
export async function executeQuery(
  connector: AskSQLConnector,
  sql: string,
  options: QueryExecutorOptions = {},
): Promise<ExecutionResult> {
  // Validate safety (skip if already validated by caller)
  if (!options.skipValidation) {
    const validation = validateSql(sql, connector.dialect);
    if (!validation.safe) {
      return {
        success: false,
        rows: [],
        columns: [],
        rowCount: 0,
        truncated: false,
        executionTimeMs: 0,
        error: `SQL validation failed: ${validation.reason}`,
      };
    }
  }

  try {
    const result = await connector.executeQuery(sql, {
      maxRows: options.maxRows ?? 5000,
      timeoutMs: options.timeoutMs ?? 30000,
    });

    return {
      success: true,
      rows: result.rows,
      columns: result.columns,
      rowCount: result.rowCount,
      truncated: result.truncated,
      executionTimeMs: result.executionTimeMs,
    };
  } catch (err: unknown) {
    return {
      success: false,
      rows: [],
      columns: [],
      rowCount: 0,
      truncated: false,
      executionTimeMs: 0,
      error: (err instanceof Error ? err.message : String(err)),
    };
  }
}
