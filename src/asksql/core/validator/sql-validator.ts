/**
 * SQL Validator — dialect-aware safety checks for AI-generated SQL
 *
 * Dialect-aware safety checks for AI-generated SQL.
 *
 * Layers:
 * 1. Must start with SELECT or WITH (CTE)
 * 2. Block dangerous keywords (DROP, DELETE, INSERT, etc.)
 * 3. Block dangerous functions (pg_sleep, dblink, etc.)
 * 4. Block system schema access
 * 5. Block statement chaining (semicolons mid-query)
 * 6. Strips quoted strings before checking (prevents false positives)
 */

import type { SQLDialect } from "../connector/interface.js";

export interface SqlValidationResult {
  safe: boolean;
  reason?: string;
}

const BLOCKED_KEYWORD_PATTERNS = [
  "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE",
  "CREATE", "GRANT", "REVOKE", "COPY", "EXECUTE", "CALL",
  "VACUUM", "CLUSTER", "REINDEX", "EXPLAIN",
].map((kw) => ({ kw, pattern: new RegExp(`\\b${kw}\\b`) }));

const BLOCKED_SCHEMAS: Record<string, string[]> = {
  postgresql: ["PG_CATALOG", "PG_TOAST", "PG_TEMP"],
  redshift:   ["PG_CATALOG", "PG_TOAST"],
  mysql:      ["MYSQL", "PERFORMANCE_SCHEMA", "SYS"],
  plsql:      ["SYS", "SYSTEM"],
  tsql:       ["SYS"],
  bigquery:   [],
  snowflake:  [],
  sqlite:     [],
  "databricks-sql": [],
};

const BLOCKED_FUNCTIONS = [
  /\bpg_sleep\b/i,
  /\bdblink\b/i,
  /\blo_import\b/i,
  /\blo_export\b/i,
  /\bpg_read_file\b/i,
  /\bpg_write_file\b/i,
  /\bxp_cmdshell\b/i,
  /\bsp_executesql\b/i,
];

/**
 * Validate AI-generated SQL for safety.
 * Strips quoted strings before keyword checking to prevent false positives
 * (e.g., column alias "Security Classification" won't trigger SECURITY block).
 */
export function validateSql(
  sql: string,
  dialect: SQLDialect = "postgresql",
): SqlValidationResult {
  if (!sql || !sql.trim()) {
    return { safe: false, reason: "Empty SQL" };
  }

  const upper = sql.toUpperCase().trim();

  // Must start with SELECT or WITH (CTE)
  if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) {
    return { safe: false, reason: "Only SELECT queries are allowed" };
  }

  // Strip quoted strings and identifiers before keyword checking
  const stripped = upper
    .replace(/"[^"]*"/g, "")    // double-quoted identifiers
    .replace(/'[^']*'/g, "")    // single-quoted string values
    .replace(/`[^`]*`/g, "");   // backtick-quoted identifiers (MySQL/BigQuery)

  // Block dangerous keywords
  for (const { kw, pattern } of BLOCKED_KEYWORD_PATTERNS) {
    if (pattern.test(stripped)) {
      return { safe: false, reason: `Blocked keyword: ${kw}` };
    }
  }

  // Block dangerous functions (check stripped to avoid false positives in quoted strings)
  for (const pattern of BLOCKED_FUNCTIONS) {
    if (pattern.test(stripped)) {
      return { safe: false, reason: `Blocked function detected` };
    }
  }

  // Block system schema access (check stripped — quotes removed)
  // Use schema-dot pattern (e.g., SYS.DUAL) to avoid false positives on
  // function names like SYSDATE, SYSTEM_TIME, etc.
  const schemas = BLOCKED_SCHEMAS[dialect] ?? BLOCKED_SCHEMAS.postgresql;
  for (const schemaName of schemas) {
    const schemaPattern = new RegExp(`\\b${schemaName}\\s*\\.`, "i");
    if (schemaPattern.test(stripped)) {
      return { safe: false, reason: `System schema access blocked: ${schemaName}` };
    }
  }

  // Block statement chaining (semicolons mid-query)
  // Allow a single trailing semicolon
  const withoutStrings = sql.replace(/'[^']*'/g, "").replace(/;\s*$/, "");
  if (withoutStrings.includes(";")) {
    return { safe: false, reason: "Multiple statements not allowed" };
  }

  return { safe: true };
}
