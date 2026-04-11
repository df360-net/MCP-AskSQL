import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ConnectorManager } from "./connector-manager.js";
import type { QueryLogger } from "./query-logger.js";
import type { AskAgentAppConfig } from "./config.js";
import { askAgentLoop } from "./ask/agent-loop.js";

export function registerTools(
  server: McpServer,
  manager: ConnectorManager,
  logger?: QueryLogger,
  askConfig?: AskAgentAppConfig,
): void {
  const askAgentEnabled = askConfig?.enabled ?? false;
  const askAgentMaxTurns = askConfig?.maxTurns ?? 10;
  const askToolOutputMaxChars = askConfig?.toolOutputMaxChars;
  const safety = manager.getSafetyConfig();

  // Tool 1: ask
  server.tool(
    "ask",
    "Ask a natural language question about the database. Returns SQL query, explanation, and results.",
    {
      question: z.string().describe("Natural language question (e.g., 'show me all users with orders > $1000')"),
      connector: z.string().optional().describe("Connector ID (uses default if omitted)"),
      maxRows: z.number().optional().describe("Maximum rows to return (uses config safety.maxRows if omitted)"),
    },
    async ({ question, connector, maxRows }) => {
      const start = Date.now();
      try {
        // Auto-route if no connector specified
        let resolvedConnector = connector;
        let routeInfo: { method: string; confidence: string } | undefined;
        if (!connector) {
          const route = await manager.routeQuestion(question);
          resolvedConnector = route.connectorId;
          routeInfo = { method: route.method, confidence: route.confidence };
          console.error(`[auto-route] "${question.slice(0, 60)}" → ${route.connectorId} (${route.method}: ${route.confidence})`);
        }

        // ── Agent loop branch (2-layer intelligence) ──
        if (askAgentEnabled) {
          const agentResult = await askAgentLoop({
            question,
            connectorId: resolvedConnector,
            manager,
            aiConfig: manager.getAIConfig(),
            maxTurns: askAgentMaxTurns,
            maxRows: maxRows ?? safety.maxRows,
            toolOutputMaxChars: askToolOutputMaxChars,
          });
          logger?.log({
            tool: "ask",
            connector: resolvedConnector ?? "default",
            question,
            success: agentResult.success,
            executionTimeMs: Date.now() - start,
            explanation: agentResult.explanation || undefined,
            toolCalls: agentResult.toolCalls.length > 0 ? agentResult.toolCalls : undefined,
          });
          console.error(`[ask-agent] ${agentResult.turns} turns, ${agentResult.toolCalls.length} tool calls, ${agentResult.tokenUsage.totalTokens} tokens`);
          return {
            content: [{ type: "text", text: agentResult.answer }],
            isError: !agentResult.success,
          };
        }

        // ── Original single-shot path ──
        const asksql = manager.get(resolvedConnector);
        const result = await asksql.ask(question, { maxRows: maxRows ?? safety.maxRows });
        logger?.log({
          tool: "ask",
          connector: resolvedConnector ?? "default",
          question,
          sql: result.sql ?? undefined,
          success: result.success,
          error: result.error,
          executionTimeMs: Date.now() - start,
          rowCount: result.rowCount,
        });
        return {
          content: [{
            type: "text",
            text: JSON.stringify({
              success: result.success,
              sql: result.sql,
              explanation: result.explanation,
              rows: result.rows,
              rowCount: result.rowCount,
              executionTimeMs: result.executionTimeMs,
              error: result.error,
              ...(routeInfo && { routedTo: resolvedConnector, routeMethod: routeInfo.method, routeConfidence: routeInfo.confidence }),
            }, null, 2),
          }],
          isError: !result.success,
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger?.log({ tool: "ask", connector: connector ?? "default", question, success: false, error: msg, executionTimeMs: Date.now() - start });
        return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
      }
    },
  );

  // Tool 2: generate_sql
  server.tool(
    "generate_sql",
    "Generate SQL from a natural language question without executing it. Useful for review before execution.",
    {
      question: z.string().describe("Natural language question"),
      connector: z.string().optional().describe("Connector ID (optional)"),
    },
    async ({ question, connector }) => {
      const start = Date.now();
      try {
        let resolvedConnector = connector;
        if (!connector) {
          const route = await manager.routeQuestion(question);
          resolvedConnector = route.connectorId;
          console.error(`[auto-route] generate_sql "${question.slice(0, 60)}" → ${route.connectorId} (${route.method})`);
        }

        const asksql = manager.get(resolvedConnector);
        const result = await asksql.generateSQL(question);
        logger?.log({ tool: "generate_sql", connector: resolvedConnector ?? "default", question, sql: result.sql, success: true, executionTimeMs: Date.now() - start });
        return {
          content: [{ type: "text", text: JSON.stringify({ sql: result.sql, explanation: result.explanation, ...(connector ? {} : { routedTo: resolvedConnector }) }, null, 2) }],
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger?.log({ tool: "generate_sql", connector: connector ?? "default", question, success: false, error: msg, executionTimeMs: Date.now() - start });
        return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
      }
    },
  );

  // Tool 3: execute_sql
  server.tool(
    "execute_sql",
    "Execute a raw SQL query directly against the database. Use with caution.",
    {
      sql: z.string().describe("SQL query to execute"),
      connector: z.string().optional().describe("Connector ID (optional)"),
      maxRows: z.number().optional().describe("Maximum rows to return (uses config safety.maxRows if omitted)"),
    },
    async ({ sql, connector, maxRows }) => {
      const start = Date.now();
      try {
        const asksql = manager.get(connector);
        const result = await asksql.executeSQL(sql, { maxRows: maxRows ?? safety.maxRows });
        logger?.log({ tool: "execute_sql", connector: connector ?? "default", sql, success: true, executionTimeMs: Date.now() - start, rowCount: result.rowCount });
        return {
          content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger?.log({ tool: "execute_sql", connector: connector ?? "default", sql, success: false, error: msg, executionTimeMs: Date.now() - start });
        return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
      }
    },
  );

  // Tool 4: list_connectors
  server.tool(
    "list_connectors",
    "List all configured database connectors with their status.",
    {},
    async () => {
      const connectors = manager.listConnectors();
      logger?.log({ tool: "list_connectors", connector: "n/a", success: true, executionTimeMs: 0 });
      return {
        content: [{ type: "text", text: JSON.stringify(connectors, null, 2) }],
      };
    },
  );

  // Tool 5: health_check
  server.tool(
    "health_check",
    "Check the health of database and AI connections.",
    {
      connector: z.string().optional().describe("Connector ID (checks default if omitted)"),
    },
    async ({ connector }) => {
      const start = Date.now();
      try {
        const asksql = manager.get(connector);
        const health = await asksql.healthCheck();
        logger?.log({ tool: "health_check", connector: connector ?? "default", success: true, executionTimeMs: Date.now() - start });
        return {
          content: [{ type: "text", text: JSON.stringify(health, null, 2) }],
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger?.log({ tool: "health_check", connector: connector ?? "default", success: false, error: msg, executionTimeMs: Date.now() - start });
        return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
      }
    },
  );

  // Tool 6: refresh_schema
  server.tool(
    "refresh_schema",
    "Re-discover database schema from the live database and update the local cache. Use after schema changes (new tables, columns, etc.).",
    {
      connector: z.string().optional().describe("Connector ID (refreshes default if omitted)"),
    },
    async ({ connector }) => {
      const start = Date.now();
      try {
        const result = await manager.refreshSchema(connector);
        logger?.log({ tool: "refresh_schema", connector: connector ?? "default", success: true, executionTimeMs: Date.now() - start });
        return {
          content: [{
            type: "text",
            text: JSON.stringify({
              success: true,
              connector: result.connector,
              tables: result.tables,
              columns: result.columns,
              message: `Schema refreshed and cached. ${result.tables} tables, ${result.columns} columns discovered.`,
            }, null, 2),
          }],
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger?.log({ tool: "refresh_schema", connector: connector ?? "default", success: false, error: msg, executionTimeMs: Date.now() - start });
        return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
      }
    },
  );
}
