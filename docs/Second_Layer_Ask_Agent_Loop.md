# Second Layer: The `ask` Agent Loop

## 1. What We're Building

Today, MCP-AskSQL is a **tool server**. It exposes six MCP tools (`ask`, `generate_sql`, `execute_sql`, `list_connectors`, `health_check`, `refresh_schema`), and external AI clients (e.g. BridgeFlow's DeepSeek) decide what to call and when.

The current `ask` tool is **single-shot**: one question in, one SQL out, one result back. It cannot explore. If the user asks "Can you give me any insights about the TPCH data?", a single SQL query is not enough — you need to discover tables, sample data, run multiple queries, spot patterns, and narrate findings.

We're adding a **second layer of intelligence** inside MCP-AskSQL itself: an agent loop that can orchestrate the existing tools internally, think across multiple queries, and return a complete, narrated answer.

```
Before (1 layer):   MCP Client AI  →  ask tool  →  1 SQL  →  result
After  (2 layers):  MCP Client AI  →  ask tool  →  [agent loop: think → SQL → think → SQL → ... → narrate]  →  result
```

When a second MCP client AI (e.g. BridgeFlow) calls this enhanced `ask`, you get **three layers**:
```
BridgeFlow (DeepSeek)  →  MCP-AskSQL (ask agent loop)  →  Database
         Layer 1                    Layer 2                 Layer 3
```

---

## 2. Design Principles

1. **Independent module** — The agent loop lives in its own directory (`src/ask/`). It is pure backend code with zero dependency on MCP transport, Express routes, or UI. It consumes the existing `AskSQL` class and `ConnectorManager` as a library consumer.

2. **Zero changes to existing engine** — The `AskSQL` class, `ConnectorManager`, `AIClient`, connectors, validators, and all six existing tool handlers remain untouched. The agent loop calls the same public methods (`ask()`, `generateSQL()`, `executeSQL()`) that external MCP clients use.

3. **Configurable on/off** — A single config flag controls whether the `ask` MCP tool routes through the agent loop (2-layer intelligence) or the original single-shot path (1-layer intelligence). Missing or `false` = original behavior. No breaking change.

---

## 3. Architecture

### 3.1 New Module: `src/ask/`

```
src/ask/
├── agent-loop.ts       # The agent loop — heart of 2nd layer intelligence
├── tools.ts            # Internal tool definitions (what the agent can call)
└── types.ts            # AskAgentConfig, AskAgentResult, AgentTurn
```

### 3.2 How It Fits Into the Existing System

```
                         config.json
                             │
                     ┌───────┴───────┐
                     │  ask.enabled?  │
                     └───┬───────┬───┘
                     yes │       │ no
                         ▼       ▼
MCP "ask" tool ──► agent-loop  original single-shot
                       │            │
                       ▼            ▼
                 ┌─────────────────────┐
                 │   ConnectorManager  │
                 │     .get(id)        │
                 │       ▼             │
                 │    AskSQL           │
                 │  .ask()             │
                 │  .generateSQL()     │
                 │  .executeSQL()      │
                 └─────────────────────┘
                         │
                         ▼
                      Database
```

The branching happens in **one place**: the existing `ask` tool handler in `src/tools.ts`. A ~10-line `if/else` is the only change to existing code.

### 3.3 Touch Points to Existing Code

| File | Change | Size |
|------|--------|------|
| `src/tools.ts` | `ask` handler: if `askAgent` enabled, delegate to agent loop; else original path | ~10 lines |
| `src/config.ts` | Add optional `ask?: { enabled?: boolean; maxTurns?: number }` to `AppConfig` | ~3 lines |
| Everything else | **No changes** | 0 |

---

## 4. The Agent Loop (`src/ask/agent-loop.ts`)

### 4.1 Core Algorithm

```
function askAgentLoop(question, connector, manager, aiConfig, options):

  1. Build system prompt:
     - "You are a data analyst with access to a database."
     - "You can call tools to explore schemas and query data."
     - "Think step by step. Run multiple queries if needed."
     - "When you have enough information, write a final answer."

  2. Initialize conversation = [system prompt, user question]

  3. Loop (max N turns):
     a. Call AI with conversation + tool definitions
     b. If AI returns a final text answer → return it (done)
     c. If AI returns tool calls:
        - Execute each tool call against ConnectorManager/AskSQL
        - Append tool results to conversation
        - Continue loop

  4. If max turns reached:
     - Force one final AI call (no tools) asking for summary
     - Return that summary
```

### 4.2 Internal Tools

The agent loop exposes these **internal tools** to the AI (not MCP tools — these are chat-completions function-calling tools):

| Tool | Maps To | Purpose |
|------|---------|---------|
| `query` | `asksql.executeSQL(sql)` | Execute a SQL query |
| `list_tables` | `asksql.getSchemaContext()` | Get full schema context |
| `ask_sql` | `asksql.ask(question)` | NL → SQL → execute (uses the existing single-shot intelligence) |

Note: `ask_sql` is powerful — it lets the agent loop delegate SQL generation to the existing AI-powered `ask()` method. The agent thinks about *what* to ask; the inner `ask()` figures out *how* to write the SQL. This is the layered intelligence pattern.

### 4.3 System Prompt

```
You are a senior data analyst. You have access to a database and tools to
explore it. Your job is to answer the user's question thoroughly.

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
- Format your final answer in clear Markdown.
```

### 4.4 Conversation Flow Example

User: "Can you give me any insights about the TPCH data?"

```
Turn 1: AI calls list_tables → gets schema (ORDERS, CUSTOMER, LINEITEM, ...)
Turn 2: AI calls ask_sql("How many orders per year?") → gets yearly breakdown
Turn 3: AI calls ask_sql("Top 10 customers by revenue") → gets customer ranking
Turn 4: AI calls query("SELECT AVG(o_totalprice) ... GROUP BY o_orderpriority") → priority analysis
Turn 5: AI writes final answer:
        "# TPCH Database Insights
         ## Order Volume Trends
         The database contains 1.5M orders spanning 1992-1998...
         ## Top Customers
         Customer #123 leads with $2.3M in total orders...
         ## Order Priority Distribution
         High-priority orders represent 20% but 35% of revenue..."
```

---

## 5. Configuration

### 5.1 config.json

```json
{
  "connectors": [ ... ],
  "ai": { ... },
  "safety": { ... },
  "schemaCacheTtlHours": 24,

  "ask": {
    "enabled": true,
    "maxTurns": 10
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ask.enabled` | `boolean` | `false` | Enable the agent loop for the `ask` MCP tool |
| `ask.maxTurns` | `number` | `10` | Max AI ↔ tool round-trips before forced summary |

### 5.2 Behavior Matrix

| `ask.enabled` | `ask` MCP tool behavior | Other tools |
|---------------|------------------------|-------------|
| `false` / missing | Original single-shot: NL → 1 SQL → result | Unchanged |
| `true` | Agent loop: NL → [think → query → think → ...] → narrated answer | Unchanged |

### 5.3 Backward Compatibility

- Config field is optional. Missing = `false` = original behavior.
- The `ask` tool's MCP schema (parameters, return format) stays the same.
- External MCP clients don't know or care whether the agent loop is running internally.
- The other 5 tools (`generate_sql`, `execute_sql`, `list_connectors`, `health_check`, `refresh_schema`) are completely unaffected.

---

## 6. Implementation: `src/tools.ts` Change

The only modification to existing code — the `ask` tool handler gets a conditional branch:

```typescript
// In registerTools(), the "ask" handler becomes:

async ({ question, connector, maxRows }) => {
  // Auto-route connector (existing logic, unchanged)
  let resolvedConnector = connector;
  if (!connector) {
    const route = await manager.routeQuestion(question);
    resolvedConnector = route.connectorId;
  }

  // ── NEW: Agent loop branch ──
  if (askAgentEnabled) {
    const result = await askAgentLoop({
      question,
      connectorId: resolvedConnector,
      manager,
      aiConfig: manager.getAIConfig(),
      maxTurns: askAgentMaxTurns,
      maxRows: maxRows ?? 100,
    });
    logger?.log({ tool: "ask", connector: resolvedConnector, question, ... });
    return { content: [{ type: "text", text: result.answer }] };
  }

  // ── Original single-shot path (unchanged) ──
  const asksql = manager.get(resolvedConnector);
  const result = await asksql.ask(question, { maxRows: maxRows ?? 100 });
  // ... existing response formatting ...
}
```

---

## 7. Implementation: `src/ask/agent-loop.ts`

### 7.1 Types (`src/ask/types.ts`)

```typescript
export interface AskAgentConfig {
  question: string;
  connectorId?: string;
  manager: ConnectorManager;
  aiConfig: AIConfig;
  maxTurns?: number;     // default 10
  maxRows?: number;      // default 100
}

export interface AskAgentResult {
  answer: string;          // Final narrated answer (Markdown)
  turns: number;           // How many AI ↔ tool round-trips
  toolCalls: ToolCallLog[];// Audit trail
  tokenUsage: TokenUsage;  // Cumulative token usage
  success: boolean;
}

export interface ToolCallLog {
  turn: number;
  tool: string;
  input: Record<string, unknown>;
  output: string;
  durationMs: number;
}
```

### 7.2 Tool Definitions (OpenAI function-calling format)

```typescript
// src/ask/tools.ts

export const AGENT_TOOLS = [
  {
    type: "function",
    function: {
      name: "query",
      description: "Execute a SQL query directly. Use for precise queries you write yourself.",
      parameters: {
        type: "object",
        properties: {
          sql: { type: "string", description: "SQL SELECT query to execute" },
          connector: { type: "string", description: "Connector ID (optional)" },
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
      parameters: { type: "object", properties: {} },
    },
  },
  {
    type: "function",
    function: {
      name: "ask_sql",
      description: "Ask a natural language question about the data. Generates and executes SQL automatically.",
      parameters: {
        type: "object",
        properties: {
          question: { type: "string", description: "Natural language question about the data" },
          connector: { type: "string", description: "Connector ID (optional)" },
        },
        required: ["question"],
      },
    },
  },
];
```

### 7.3 Agent Loop (pseudo-code)

```typescript
// src/ask/agent-loop.ts

export async function askAgentLoop(config: AskAgentConfig): Promise<AskAgentResult> {
  const { question, connectorId, manager, aiConfig, maxTurns = 10, maxRows = 100 } = config;
  const ai = new AIClient(aiConfig);
  const asksql = manager.get(connectorId);

  const messages: ChatMessage[] = [
    { role: "system", content: SYSTEM_PROMPT },
    { role: "user", content: question },
  ];

  const toolCallLog: ToolCallLog[] = [];
  let totalTokenUsage = { promptTokens: 0, completionTokens: 0, totalTokens: 0, estimatedCost: 0 };

  for (let turn = 0; turn < maxTurns; turn++) {
    // Call AI with tools
    const response = await callAIWithTools(ai, messages, AGENT_TOOLS);
    totalTokenUsage = addTokenUsage(totalTokenUsage, response.tokenUsage);

    // If AI returned a text answer (no tool calls) → done
    if (response.finishReason === "stop" && !response.toolCalls?.length) {
      return {
        answer: response.content,
        turns: turn + 1,
        toolCalls: toolCallLog,
        tokenUsage: totalTokenUsage,
        success: true,
      };
    }

    // Execute each tool call
    for (const call of response.toolCalls) {
      const start = Date.now();
      const result = await executeInternalTool(call, asksql, manager, maxRows);
      toolCallLog.push({
        turn, tool: call.name, input: call.arguments,
        output: result, durationMs: Date.now() - start,
      });
      // Append tool result to conversation
      messages.push({ role: "tool", content: result, tool_call_id: call.id });
    }
  }

  // Max turns reached — force a summary
  messages.push({
    role: "user",
    content: "You have reached the maximum number of tool calls. Please summarize your findings now.",
  });
  const final = await callAIWithoutTools(ai, messages);
  return {
    answer: final.content,
    turns: maxTurns,
    toolCalls: toolCallLog,
    tokenUsage: addTokenUsage(totalTokenUsage, final.tokenUsage),
    success: true,
  };
}
```

### 7.4 Internal Tool Executor

```typescript
async function executeInternalTool(
  call: ToolCall, asksql: AskSQL, manager: ConnectorManager, maxRows: number,
): Promise<string> {
  switch (call.name) {
    case "query": {
      const result = await asksql.executeSQL(call.arguments.sql, { maxRows });
      return JSON.stringify({
        rowCount: result.rowCount,
        columns: result.columns.map(c => c.name),
        rows: result.rows,
        truncated: result.truncated,
      });
    }
    case "list_tables": {
      return asksql.getSchemaContext();
    }
    case "ask_sql": {
      const result = await asksql.ask(call.arguments.question, { maxRows });
      return JSON.stringify({
        sql: result.sql,
        explanation: result.explanation,
        rowCount: result.rowCount,
        rows: result.rows,
        truncated: result.truncated,
      });
    }
    default:
      return `Unknown tool: ${call.name}`;
  }
}
```

---

## 8. AI Provider Compatibility

The agent loop uses the **OpenAI function-calling** format (`tools` parameter in the chat completions API). This is supported by:

| Provider | Function Calling Support |
|----------|------------------------|
| OpenAI (GPT-4, etc.) | Native |
| DeepSeek | Native (OpenAI-compatible) |
| Anthropic Claude (via proxy) | Depends on proxy implementation |
| Ollama (Llama 3, etc.) | Supported in recent versions |

The `AIClient` class already talks to OpenAI-compatible endpoints. We only need to add `tools` to the request body — a small extension to the existing `call()` method, or a new `callWithTools()` method alongside it.

### 8.1 AIClient Extension

Two options (both preserve backward compatibility):

**Option A: New method** (preferred — no risk to existing callers)
```typescript
// Add to AIClient
async callWithTools<T>(messages, tools, parseJson?): Promise<AICallWithToolsResult>
```

**Option B: Optional parameter on existing call()**
```typescript
async call<T>(messages, parseJson?, feature?, tools?): Promise<AICallResult<T>>
```

Either way, the change to `AIClient` is additive — existing code doesn't break.

---

## 9. Two Deployment Modes

Once implemented, MCP-AskSQL supports two modes from the same codebase:

### Mode 1: Tool Server (ask.enabled = false)

```
External AI  ──[MCP]──►  ask tool  ──►  1 SQL  ──►  result
   (smart)                (dumb)
```

The external AI (BridgeFlow, Claude Desktop, etc.) does all the thinking. MCP-AskSQL is a passive tool.

### Mode 2: Intelligent Agent (ask.enabled = true)

```
External AI  ──[MCP]──►  ask tool  ──►  [agent loop]  ──►  narrated answer
  (optional)               (smart)         │
                                           ├─ query → DB
                                           ├─ ask_sql → AI + DB
                                           ├─ list_tables → schema
                                           └─ ... (up to N turns)
```

MCP-AskSQL thinks for itself. The external AI can be simpler (or absent — a thin UI can call the `ask` tool directly via MCP HTTP).

### Mode 2+: Layered Intelligence (BridgeFlow + ask.enabled = true)

```
BridgeFlow (DeepSeek)  ──►  MCP-AskSQL (agent loop)  ──►  Database
     Layer 1: Strategy          Layer 2: Data Analysis       Layer 3: Data
     "Ask about revenue"        "Run 5 queries, narrate"     "Return rows"
```

Three layers of intelligence, each with its own AI, composable by configuration.

---

## 10. What We're NOT Changing

| Component | Status |
|-----------|--------|
| `AskSQL` class | Untouched — used as-is by the agent loop |
| `ConnectorManager` | Untouched — agent loop calls `.get()` |
| `AIClient` | Minor additive extension (new method for tool calling) |
| 10 database connectors | Untouched |
| SQL validator | Untouched |
| Query executor | Untouched |
| Schema cache | Untouched |
| Auto-router | Untouched |
| Query logger | Untouched |
| Admin UI | Untouched |
| REST API routes | Untouched |
| 5 other MCP tools | Untouched |
| MCP transport (stdio/HTTP) | Untouched |

---

## 11. Implementation Checklist

```
Phase 1: Core
  [ ] Create src/ask/types.ts — AskAgentConfig, AskAgentResult, ToolCallLog
  [ ] Create src/ask/tools.ts — AGENT_TOOLS definitions, executeInternalTool()
  [ ] Create src/ask/agent-loop.ts — askAgentLoop() function
  [ ] Extend AIClient with callWithTools() method
  [ ] Add ask config to AppConfig type in src/config.ts
  [ ] Add conditional branch in src/tools.ts ask handler
  [ ] Add "ask" section to config.example.json

Phase 2: Polish
  [ ] Query logger integration — log each internal tool call
  [ ] Token usage aggregation — report total cost across all turns
  [ ] Error handling — catch tool failures, let AI retry or work around
  [ ] Timeout — overall timeout for the entire agent loop (not just per-query)

Phase 3: Test
  [ ] Test with ask.enabled = false → verify original behavior unchanged
  [ ] Test with ask.enabled = true → open-ended question
  [ ] Test with ask.enabled = true → simple question (should still work, just 1 turn)
  [ ] Test cross-connector routing with agent loop
```

---

## 12. Summary

| Aspect | Detail |
|--------|--------|
| **New code** | ~200-300 lines in `src/ask/` (3 files) |
| **Changed code** | ~15 lines across `tools.ts` + `config.ts` |
| **Config** | `"ask": { "enabled": true, "maxTurns": 10 }` |
| **Backward compatible** | Yes — missing config = original behavior |
| **AI provider** | Any OpenAI-compatible endpoint with function calling |
| **Dependencies** | None new — uses existing `AIClient`, `AskSQL`, `ConnectorManager` |
