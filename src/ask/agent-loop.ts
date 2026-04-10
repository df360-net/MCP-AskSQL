/**
 * Ask Agent Loop — Second Layer Intelligence
 *
 * An agent loop that can orchestrate multiple queries, explore schemas,
 * and return a complete narrated answer. Uses OpenAI function-calling
 * to let the AI decide which internal tools to invoke.
 */

import { AIClient, type ChatMessage, type TokenUsage } from "../asksql/core/ai/client.js";
import type { AskAgentConfig, AskAgentResult, ToolCallLog } from "./types.js";
import { AGENT_TOOLS, executeInternalTool } from "./tools.js";

// ---------------------------------------------------------------------------
// System prompt for the agent
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT = `You are a senior data analyst. You have access to a database and tools to explore it. Your job is to answer the user's question thoroughly.

APPROACH:
1. First, understand what data is available (use list_tables if needed).
2. Run queries to gather relevant data.
3. Analyze the results — look for patterns, trends, outliers.
4. If your first query isn't enough, run more queries.
5. When you have sufficient information, write a clear, insightful answer.

RULES:
- Be thorough but efficient. Don't run unnecessary queries.
- Always explain your findings with specific numbers from the data.
- If the question is open-ended ("give me insights"), explore multiple angles.
- If a query fails, try a different approach rather than giving up.
- Format your final answer in clear Markdown.`;

// ---------------------------------------------------------------------------
// Token usage helper
// ---------------------------------------------------------------------------

function addTokenUsage(a: TokenUsage, b: TokenUsage): TokenUsage {
  return {
    promptTokens: a.promptTokens + b.promptTokens,
    completionTokens: a.completionTokens + b.completionTokens,
    totalTokens: a.totalTokens + b.totalTokens,
    estimatedCost: a.estimatedCost + b.estimatedCost,
  };
}

// ---------------------------------------------------------------------------
// Build human-readable tool call line for explanation
// ---------------------------------------------------------------------------

function formatToolCallLine(toolName: string, args: Record<string, unknown>, durationMs: number): string {
  if (toolName === "ask_sql") {
    return `  -> ask_sql: "${args.question}" (${durationMs}ms)`;
  } else if (toolName === "query") {
    const sql = String(args.sql ?? "").replace(/\s+/g, " ").trim();
    const sqlPreview = sql.length > 120 ? sql.slice(0, 120) + "..." : sql;
    return `  -> query: ${sqlPreview} (${durationMs}ms)`;
  } else if (toolName === "list_tables") {
    return `  -> list_tables (${durationMs}ms)`;
  }
  return `  -> ${toolName}(${JSON.stringify(args)}) (${durationMs}ms)`;
}

// ---------------------------------------------------------------------------
// Agent Loop
// ---------------------------------------------------------------------------

export async function askAgentLoop(config: AskAgentConfig): Promise<AskAgentResult> {
  const { question, connectorId, manager, aiConfig, maxTurns = 10, maxRows = 100, onTurn } = config;
  const ai = new AIClient(aiConfig);
  const asksql = manager.get(connectorId);

  const messages: ChatMessage[] = [
    { role: "system", content: SYSTEM_PROMPT },
    { role: "user", content: question },
  ];

  const allToolCalls: ToolCallLog[] = [];
  const reasoningParts: string[] = [];
  let totalTokenUsage: TokenUsage = { promptTokens: 0, completionTokens: 0, totalTokens: 0, estimatedCost: 0 };

  for (let turn = 0; turn < maxTurns; turn++) {
    const response = await ai.callWithTools(messages, AGENT_TOOLS);
    totalTokenUsage = addTokenUsage(totalTokenUsage, response.tokenUsage);

    // If AI returned an error
    if (response.finishReason === "error") {
      const result: AskAgentResult = {
        answer: response.content,
        explanation: reasoningParts.join("\n\n"),
        turns: turn + 1,
        toolCalls: allToolCalls,
        tokenUsage: totalTokenUsage,
        success: false,
      };
      onTurn?.({ turn, done: true, answer: result.answer, explanation: result.explanation, tokenUsage: totalTokenUsage, success: false });
      return result;
    }

    // If AI returned a text answer with no tool calls → done
    if (response.toolCalls.length === 0) {
      const result: AskAgentResult = {
        answer: response.content,
        explanation: reasoningParts.join("\n\n"),
        turns: turn + 1,
        toolCalls: allToolCalls,
        tokenUsage: totalTokenUsage,
        success: true,
      };
      onTurn?.({ turn, done: true, answer: result.answer, explanation: result.explanation, tokenUsage: totalTokenUsage, success: true });
      return result;
    }

    // AI wants to call tools — append assistant message with tool_calls
    messages.push({
      role: "assistant",
      content: response.content || "",
      tool_calls: response.toolCalls,
    });

    // Execute each tool call and append results
    const turnToolCalls: ToolCallLog[] = [];
    const turnToolLines: string[] = [];
    for (const call of response.toolCalls) {
      const start = Date.now();
      let args: Record<string, unknown> = {};
      try {
        args = JSON.parse(call.function.arguments) as Record<string, unknown>;
      } catch {
        args = {};
      }

      const toolResult = await executeInternalTool(call.function.name, args, asksql, maxRows);
      const durationMs = Date.now() - start;

      const logEntry: ToolCallLog = {
        turn,
        tool: call.function.name,
        input: args,
        output: toolResult.length > 2000 ? toolResult.slice(0, 2000) + "... (truncated)" : toolResult,
        durationMs,
      };
      allToolCalls.push(logEntry);
      turnToolCalls.push(logEntry);
      turnToolLines.push(formatToolCallLine(call.function.name, args, durationMs));

      messages.push({
        role: "tool",
        content: toolResult,
        tool_call_id: call.id,
      });
    }

    // Build reasoning for this turn
    const thinkingText = response.content?.trim() || "(no reasoning text)";
    const toolDetails = turnToolLines.join("\n");
    const reasoning = `**Turn ${turn + 1}**: ${thinkingText}\n${toolDetails}`;
    reasoningParts.push(reasoning);

    // Stream this turn to the caller
    onTurn?.({ turn, reasoning, toolCalls: turnToolCalls, done: false });
  }

  // Max turns reached — force a final summary (no tools)
  messages.push({
    role: "user",
    content: "You have reached the maximum number of tool calls. Please summarize your findings now based on the data you've gathered.",
  });
  const finalResponse = await ai.callWithTools(messages); // no tools = text-only
  totalTokenUsage = addTokenUsage(totalTokenUsage, finalResponse.tokenUsage);

  const result: AskAgentResult = {
    answer: finalResponse.content || "Unable to generate a summary.",
    explanation: reasoningParts.join("\n\n"),
    turns: maxTurns,
    toolCalls: allToolCalls,
    tokenUsage: totalTokenUsage,
    success: true,
  };
  onTurn?.({ turn: maxTurns, done: true, answer: result.answer, explanation: result.explanation, tokenUsage: totalTokenUsage, success: true });
  return result;
}
