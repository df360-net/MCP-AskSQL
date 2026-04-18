/**
 * AskSQL — Main Class (Standalone Mode)
 *
 * Stripped-down version for mcp-asksql: no catalog persistence, no event queue,
 * no scheduler, no abbreviation learning. Schema context is built in-memory
 * from connector discovery.
 *
 * One class, three methods:
 *   ask()         — NL question → SQL → results
 *   generateSQL() — NL question → SQL only (no execution)
 *   executeSQL()  — raw SQL → results (bypass AI)
 */

import type { AskSQLConnector } from "./connector/interface.js";
import type { LLMContext } from "./connector/discovery-types.js";
import { createConnector, detectConnectorType } from "./connector/registry.js";
import { AIClient, type AIConfig, type TokenUsage } from "./ai/client.js";
import { validateSql } from "./validator/sql-validator.js";
import { executeQuery } from "./executor/query-executor.js";

// ---------------------------------------------------------------------------
// Config types
// ---------------------------------------------------------------------------

export interface AskSQLConfig {
  connector:
    | { connectionString: string; schemas?: string[]; type?: string; catalog?: string }
    | { connector: AskSQLConnector; schemas?: string[] };

  ai: AIConfig;
  abbreviations?: Record<string, string[]>;
  examples?: Array<{ question: string; sql: string }>;
  schemaPrefix?: string;
  safety?: {
    maxRows?: number;
    timeoutMs?: number;
    maxRetries?: number;
  };
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

export interface AskSQLResult {
  success: boolean;
  question: string;
  sql: string | null;
  explanation: string | null;
  rows: Record<string, unknown>[];
  columns: Array<{ name: string; type: string }>;
  rowCount: number;
  truncated: boolean;
  executionTimeMs: number;
  totalTimeMs: number;
  retries: number;
  tokenUsage: TokenUsage;
  error?: string;
}

export interface GenerateSQLResult {
  sql: string;
  explanation: string;
  tokenUsage: TokenUsage;
}

export interface QueryResult {
  rows: Record<string, unknown>[];
  columns: Array<{ name: string; type: string }>;
  rowCount: number;
  truncated: boolean;
  executionTimeMs: number;
}

// ---------------------------------------------------------------------------
// AskSQL Class
// ---------------------------------------------------------------------------

export class AskSQL {
  private connector: AskSQLConnector;
  private ai: AIClient;
  private schemas: string[] | undefined;
  private forwardMap: Record<string, string[]>;
  private llmContext: LLMContext | null = null;
  private examples: Array<{ question: string; sql: string }>;
  private schemaPrefix?: string;
  private maxRows: number;
  private timeoutMs: number;
  private maxRetries: number;
  private initialized = false;
  private initPromise: Promise<void> | null = null;

  constructor(config: AskSQLConfig) {
    // Resolve connector
    if ("connector" in config.connector && typeof config.connector.connector === "object") {
      this.connector = config.connector.connector;
      this.schemas = config.connector.schemas;
    } else {
      const cc = config.connector as Record<string, unknown> & { connectionString: string; schemas?: string[]; type?: string };
      const type = (cc.type as string | undefined) ?? detectConnectorType(cc.connectionString);
      if (!type) throw new Error(`Cannot detect connector type from: ${cc.connectionString}`);
      // Forward the full connector config (timeoutMs, connectTimeoutMs, poolSize, catalog, maxSampleValues, etc.)
      // so per-connector tuning in config.json actually reaches the connector implementation.
      const { type: _type, schemas: _schemas, ...connectorConfig } = cc;
      this.connector = createConnector(type, connectorConfig);
      this.schemas = cc.schemas;
    }

    // AI client
    this.ai = new AIClient(config.ai);

    // Abbreviation map (static, from config)
    this.forwardMap = config.abbreviations ?? {};

    // Config
    this.examples = config.examples ?? [];
    this.schemaPrefix = config.schemaPrefix;
    this.maxRows = config.safety?.maxRows ?? 5000;
    this.timeoutMs = config.safety?.timeoutMs ?? 30000;
    this.maxRetries = config.safety?.maxRetries ?? 2;
  }

  // ── Initialization (lazy) ──────────────────────────────────────

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;
    // Promise lock — second caller waits for the first, doesn't duplicate
    if (!this.initPromise) {
      this.initPromise = this.refreshCatalog().then(() => { this.initialized = true; });
    }
    await this.initPromise;
  }

  // ── Public API ─────────────────────────────────────────────────

  async ask(question: string, options?: { maxRows?: number; userId?: string }): Promise<AskSQLResult> {
    const totalStart = Date.now();
    await this.ensureInitialized();

    const maxRows = options?.maxRows ?? this.maxRows;
    let totalTokenUsage: TokenUsage = { promptTokens: 0, completionTokens: 0, totalTokens: 0, estimatedCost: 0 };

    // Build messages with LLM context
    const systemPrompt = this.buildSystemPrompt();
    const messages = [
      { role: "system" as const, content: systemPrompt },
      { role: "user" as const, content: question },
    ];

    const aiResult = await this.ai.call<{ sql: string; explanation: string }>(messages, true, "sql-generation");

    if (!aiResult.success || !aiResult.data) {
      return this.errorResult(question, aiResult.error ?? "AI generation failed", totalStart, totalTokenUsage);
    }

    totalTokenUsage = this.addTokenUsage(totalTokenUsage, aiResult.tokenUsage);
    let sql = aiResult.data.sql;
    let explanation = aiResult.data.explanation;

    // Execute with auto-retry
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      const validation = validateSql(sql, this.connector.dialect);
      if (!validation.safe) {
        if (attempt < this.maxRetries) {
          const retryResult = await this.retryWithError(question, sql, `Validation: ${validation.reason}`);
          if (retryResult.success && retryResult.data) {
            totalTokenUsage = this.addTokenUsage(totalTokenUsage, retryResult.tokenUsage);
            sql = retryResult.data.sql;
            explanation = retryResult.data.explanation;
            continue;
          }
        }
        return this.errorResult(question, `SQL validation failed: ${validation.reason}`, totalStart, totalTokenUsage, sql);
      }

      const result = await executeQuery(this.connector, sql, { maxRows, timeoutMs: this.timeoutMs, skipValidation: true });

      if (result.success) {
        return {
          success: true, question, sql, explanation,
          rows: result.rows, columns: result.columns,
          rowCount: result.rowCount, truncated: result.truncated,
          executionTimeMs: result.executionTimeMs,
          totalTimeMs: Date.now() - totalStart,
          retries: attempt, tokenUsage: totalTokenUsage,
        };
      }

      if (attempt < this.maxRetries && !result.error?.includes("timeout")) {
        const retryResult = await this.retryWithError(question, sql, result.error ?? "Unknown DB error");
        if (retryResult.success && retryResult.data) {
          totalTokenUsage = this.addTokenUsage(totalTokenUsage, retryResult.tokenUsage);
          sql = retryResult.data.sql;
          explanation = retryResult.data.explanation;
          continue;
        }
      }

      return this.errorResult(question, result.error ?? "Query execution failed", totalStart, totalTokenUsage, sql);
    }

    return this.errorResult(question, "Max retries exceeded", totalStart, totalTokenUsage, sql);
  }

  async generateSQL(question: string): Promise<GenerateSQLResult> {
    await this.ensureInitialized();

    const systemPrompt = this.buildSystemPrompt();
    const messages = [
      { role: "system" as const, content: systemPrompt },
      { role: "user" as const, content: question },
    ];

    const result = await this.ai.call<{ sql: string; explanation: string }>(messages, true, "sql-generation");
    if (!result.success || !result.data) {
      throw new Error(result.error ?? "AI generation failed");
    }

    return {
      sql: result.data.sql,
      explanation: result.data.explanation,
      tokenUsage: result.tokenUsage ?? { promptTokens: 0, completionTokens: 0, totalTokens: 0, estimatedCost: 0 },
    };
  }

  async executeSQL(sql: string, options?: { maxRows?: number }): Promise<QueryResult> {
    const result = await executeQuery(this.connector, sql, {
      maxRows: options?.maxRows ?? this.maxRows,
      timeoutMs: this.timeoutMs,
    });
    if (!result.success) throw new Error(result.error ?? "Query execution failed");
    return {
      rows: result.rows, columns: result.columns,
      rowCount: result.rowCount, truncated: result.truncated,
      executionTimeMs: result.executionTimeMs,
    };
  }

  /** Discover schema and build in-memory LLM context. Returns the raw discovered data for caching. */
  async refreshCatalog(): Promise<{ tables: number; columns: number; discovered: import("./connector/discovery-types.js").DiscoveredDatabase }> {
    const discovered = await this.connector.discover({ schemas: this.schemas });
    const tableCount = discovered.schemas.reduce((sum, s) => sum + s.tables.length, 0);
    const columnCount = discovered.schemas.reduce(
      (sum, s) => sum + s.tables.reduce((tSum, t) => tSum + t.columns.length, 0), 0,
    );

    this.llmContext = this.buildContextFromDiscovered(discovered);
    return { tables: tableCount, columns: columnCount, discovered };
  }

  /** Load schema from cached DiscoveredDatabase (skips connector.discover()) */
  loadFromCache(discovered: import("./connector/discovery-types.js").DiscoveredDatabase): { tables: number; columns: number } {
    const tableCount = discovered.schemas.reduce((sum, s) => sum + s.tables.length, 0);
    const columnCount = discovered.schemas.reduce(
      (sum, s) => sum + s.tables.reduce((tSum, t) => tSum + t.columns.length, 0), 0,
    );

    this.llmContext = this.buildContextFromDiscovered(discovered);
    this.initialized = true;
    this.initPromise = Promise.resolve();
    return { tables: tableCount, columns: columnCount };
  }

  getSchemaContext(): string {
    return this.llmContext?.schemaContext ?? "(not initialized)";
  }

  /** Reconstruct the full system prompt that would be sent to the AI */
  getSystemPrompt(): string {
    return this.buildSystemPrompt();
  }

  getConnectorType(): string {
    return this.connector.type;
  }

  async healthCheck() {
    const dbResult = await this.connector.testConnection();
    const aiResult = await this.ai.call([{ role: "user", content: "Say OK" }], false, "health-check");
    return {
      database: { connected: dbResult.success, version: dbResult.serverVersion },
      ai: { reachable: aiResult.success },
    };
  }

  async close(): Promise<void> {
    await this.connector.disconnect();
  }

  // ── Private helpers ────────────────────────────────────────────

  private buildSystemPrompt(): string {
    const ctx = this.llmContext;
    if (!ctx) return "No schema context available.";

    const lines: string[] = [
      `You are a SQL query generator for ${this.connector.displayName} (${this.connector.dialect}). Write safe, read-only SELECT queries.`,
      "",
      "RULES:",
      "1. Generate ONLY SELECT statements.",
      "2. Use exact column names from the schema below.",
      "3. Use exact values from SAMPLE VALUES — never guess with ILIKE.",
      "4. Join tables using FK relationships.",
      `5. ${this.connector.dialectHints.limitSyntax === "LIMIT" ? "Add LIMIT 1000." : this.connector.dialectHints.limitSyntax === "FETCH_FIRST" ? "Add FETCH FIRST 1000 ROWS ONLY." : "Add TOP 1000 in the SELECT clause."}`,
      "6. Use INFORMATION_SCHEMA only when the user is asking about database structure (tables, columns, types, constraints). For questions about actual data, use the tables listed in DATABASE SCHEMA below — never query INFORMATION_SCHEMA for data lookups.",
      `7. Current timestamp: ${this.connector.dialectHints.currentTimestampFunction}`,
      `8. ${this.connector.dialectHints.dateDiffHint}`,
    ];

    let ruleNum = 9;
    if (this.schemaPrefix) {
      lines.push(`${ruleNum++}. Prefix tables with "${this.schemaPrefix}." schema.`);
    }

    if (this.connector.dialectHints.supportsILIKE) {
      lines.push(`${ruleNum++}. Use ILIKE for case-insensitive text matching.`);
    } else {
      lines.push(`${ruleNum++}. Use LOWER(column) LIKE LOWER(pattern) for case-insensitive search.`);
    }

    for (const hint of this.connector.dialectHints.additionalPromptHints) {
      lines.push(`${ruleNum++}. ${hint}`);
    }

    lines.push("", "RESPONSE FORMAT:", 'Return JSON: { "sql": "...", "explanation": "..." }');

    if (ctx.abbreviationGuide) {
      lines.push("", "ABBREVIATION GUIDE:", ctx.abbreviationGuide);
    }

    lines.push("", ctx.schemaContext);

    if (this.examples.length > 0) {
      lines.push("", "EXAMPLES:");
      for (const ex of this.examples) {
        lines.push(`Question: ${ex.question}`, `SQL: ${ex.sql}`, "");
      }
    }

    return lines.join("\n");
  }

  private buildContextFromDiscovered(discovered: import("./connector/discovery-types.js").DiscoveredDatabase): LLMContext {
    const lines: string[] = ["DATABASE SCHEMA:"];
    let tableCount = 0, columnCount = 0;

    for (const s of discovered.schemas) {
      for (const t of s.tables) {
        tableCount++;
        const comment = t.tableComment ? ` -- ${t.tableComment}` : "";
        lines.push(`${s.schemaName}.${t.tableName}${comment}`);
        for (const c of t.columns) {
          columnCount++;
          const pk = c.isPrimaryKey ? " (PK)" : "";
          lines.push(`  ${c.columnName} ${c.fullDataType}${pk}`);
        }
        for (const fk of t.foreignKeys) {
          lines.push(`  FK: ${fk.columns.join(",")} → ${fk.referencedTable}(${fk.referencedColumns.join(",")})`);
        }
        lines.push("");
      }
    }

    // Build abbreviation guide from in-memory map
    const abbrLines: string[] = [];
    for (const [word, abbrevs] of Object.entries(this.forwardMap)) {
      abbrLines.push(`${abbrevs.slice(0, 2).join(", ")} = ${word}`);
    }

    return {
      schemaContext: lines.join("\n"),
      abbreviationGuide: abbrLines.join(", "),
      stats: { tableCount, columnCount, fkCount: 0, sampleCount: 0, abbreviationCount: abbrLines.length },
    };
  }

  private async retryWithError(question: string, failedSql: string, dbError: string) {
    const systemPrompt = this.buildSystemPrompt();
    const messages = [
      { role: "system" as const, content: systemPrompt },
      {
        role: "user" as const,
        content: `The user asked: "${question}"\n\nSQL failed:\n${failedSql}\n\nError: ${dbError}\n\nFix the SQL. Return JSON: { "sql": "...", "explanation": "..." }`,
      },
    ];
    return this.ai.call<{ sql: string; explanation: string }>(messages, true, "auto-retry");
  }

  private errorResult(question: string, error: string, startTime: number, tokenUsage: TokenUsage, sql?: string): AskSQLResult {
    return {
      success: false, question, sql: sql ?? null, explanation: null,
      rows: [], columns: [], rowCount: 0, truncated: false,
      executionTimeMs: 0, totalTimeMs: Date.now() - startTime,
      retries: 0, tokenUsage, error,
    };
  }

  private addTokenUsage(a: TokenUsage, b?: TokenUsage): TokenUsage {
    if (!b) return a;
    return {
      promptTokens: a.promptTokens + b.promptTokens,
      completionTokens: a.completionTokens + b.completionTokens,
      totalTokens: a.totalTokens + b.totalTokens,
      estimatedCost: a.estimatedCost + b.estimatedCost,
    };
  }
}
