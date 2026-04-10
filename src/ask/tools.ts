import type { AskSQL } from "../asksql/core/asksql.js";
import type { ToolDefinition } from "../asksql/core/ai/client.js";

// ---------------------------------------------------------------------------
// Internal tool definitions (OpenAI function-calling format)
// ---------------------------------------------------------------------------

export const AGENT_TOOLS: ToolDefinition[] = [
  {
    type: "function",
    function: {
      name: "query",
      description: "Execute a SQL SELECT query directly against the database. Use when you know the exact SQL to run.",
      parameters: {
        type: "object",
        properties: {
          sql: { type: "string", description: "SQL SELECT query to execute" },
        },
        required: ["sql"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "list_tables",
      description: "Get the database schema: all tables, columns, types, and foreign keys.",
      parameters: {
        type: "object",
        properties: {},
      },
    },
  },
  {
    type: "function",
    function: {
      name: "ask_sql",
      description: "Ask a natural language question about the data. The system generates and executes SQL automatically. Use when you want the AI to figure out the SQL for you.",
      parameters: {
        type: "object",
        properties: {
          question: { type: "string", description: "Natural language question about the data" },
        },
        required: ["question"],
      },
    },
  },
];

// ---------------------------------------------------------------------------
// Internal tool executor
// ---------------------------------------------------------------------------

export async function executeInternalTool(
  toolName: string,
  args: Record<string, unknown>,
  asksql: AskSQL,
  maxRows: number,
): Promise<string> {
  switch (toolName) {
    case "query": {
      const sql = args.sql as string;
      if (!sql) return JSON.stringify({ error: "sql parameter is required" });
      try {
        const result = await asksql.executeSQL(sql, { maxRows });
        return JSON.stringify({
          rowCount: result.rowCount,
          columns: result.columns.map((c) => c.name),
          rows: result.rows,
          truncated: result.truncated,
        });
      } catch (err) {
        return JSON.stringify({ error: err instanceof Error ? err.message : String(err) });
      }
    }

    case "list_tables": {
      return asksql.getSchemaContext();
    }

    case "ask_sql": {
      const question = args.question as string;
      if (!question) return JSON.stringify({ error: "question parameter is required" });
      try {
        const result = await asksql.ask(question, { maxRows });
        return JSON.stringify({
          success: result.success,
          sql: result.sql,
          explanation: result.explanation,
          rowCount: result.rowCount,
          rows: result.rows,
          truncated: result.truncated,
          error: result.error,
        });
      } catch (err) {
        return JSON.stringify({ error: err instanceof Error ? err.message : String(err) });
      }
    }

    default:
      return JSON.stringify({ error: `Unknown tool: ${toolName}` });
  }
}
