# Generate Deterministic SQLs — Hardened Query Sequences

## 1. Overview

When a user asks a natural-language question, the Level 2 + Level 1 agent loop produces a series of SQL queries across multiple turns. Each turn has:

- **Title** — the sub-question Level 2 asked Level 1
- **SQL** — the query Level 1 generated
- **Execution Status** — whether the SQL ran successfully

This feature allows users to **save** those proven (Title, SQL) pairs as a **Hardened SQL Sequence**, then **replay** them at any time — skipping both Level 1 and Level 2 AI for data gathering — and only calling Level 2 AI once at the end to summarize all results into a markdown report.

## 2. Motivation

| Concern | Full AI Loop | Hardened Sequence |
|---------|-------------|-------------------|
| AI calls | 2N + 1 (N turns x Level1 + Level2 each, plus final summary) | 1 (final summary only) |
| Latency | High (multiple AI round-trips) | Low (direct SQL execution) |
| Cost | High (many tokens) | Minimal (one summarization call) |
| Determinism | AI may generate different SQL each run | Exact same SQL every time |
| Reliability | SQL could fail if AI generates bad query | Proven SQL that worked before |

Use cases:
- **Scheduled reports** — run the same analysis daily/weekly without AI variance
- **Dashboard queries** — deterministic data fetching behind dashboards
- **Cost control** — avoid repeated AI spend for known query patterns
- **Audit compliance** — exact SQL is known and reviewable in advance

## 3. Data Model

### 3.1 Hardened SQL Sequence

```typescript
interface HardenedSequence {
  id: string;                  // UUID
  name: string;                // User-given name (e.g., "Monthly Revenue Report")
  description?: string;        // Optional description
  connector: string;           // Target connector ID
  originalQuestion: string;    // The NL question that produced this sequence
  sourceLogId?: string;        // Reference to the original query log entry
  createdAt: string;           // ISO timestamp
  updatedAt: string;           // ISO timestamp

  steps: HardenedStep[];       // Ordered list of (Title, SQL) pairs
}

interface HardenedStep {
  order: number;               // Execution order (1-based)
  title: string;               // The sub-question / purpose of this SQL
  sql: string;                 // The proven SQL query
}
```

### 3.2 Execution Result

```typescript
interface SequenceExecutionResult {
  sequenceId: string;
  sequenceName: string;
  connector: string;
  executionTimeMs: number;
  steps: StepResult[];
  summary?: string;            // AI-generated markdown report (if summarize=true)
}

interface StepResult {
  order: number;
  title: string;
  sql: string;
  success: boolean;
  error?: string;
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
}
```

## 4. Architecture

### 4.1 Current Flow (Full AI)

```
User Question
  → Level 2 AI (orchestrator)
    → Turn 1: Level 2 asks sub-question (Title₁)
      → Level 1 AI generates SQL₁
      → Execute SQL₁ → Results₁
    → Turn 2: Level 2 asks sub-question (Title₂)
      → Level 1 AI generates SQL₂
      → Execute SQL₂ → Results₂
    → ...
    → Turn N: Titleₙ → SQLₙ → Resultsₙ
  → Level 2 AI summarizes all results
  → Final Markdown Report
```

### 4.2 Hardened Sequence Flow (Deterministic)

```
Saved Sequence (Title₁+SQL₁, Title₂+SQL₂, ..., Titleₙ+SQLₙ)
  → Execute SQL₁ → Results₁
  → Execute SQL₂ → Results₂
  → ...
  → Execute SQLₙ → Resultsₙ
  → ONE Level 2 AI call: "Here are N datasets with their titles, summarize into a report"
  → Final Markdown Report
```

**Key difference:** The data-gathering phase is 100% deterministic (no AI). Only the final summarization uses AI.

### 4.3 Component Diagram

```
┌─────────────────────────────────────────────────────┐
│                     UI Layer                         │
│                                                     │
│  Query Logs Page          Sequences Page             │
│  ┌─────────────────┐     ┌──────────────────────┐   │
│  │ [Save as         │     │ List / Edit / Delete  │   │
│  │  Sequence] btn   │     │ [Run] → Results +     │   │
│  │                  │     │        AI Summary     │   │
│  └─────────────────┘     └──────────────────────┘   │
└─────────────────┬───────────────────┬───────────────┘
                  │                   │
                  ▼                   ▼
┌─────────────────────────────────────────────────────┐
│                   REST API Layer                     │
│                                                     │
│  POST /api/sequences          — create from log     │
│  GET  /api/sequences          — list all            │
│  GET  /api/sequences/:id      — get one             │
│  PUT  /api/sequences/:id      — update (edit SQL)   │
│  DELETE /api/sequences/:id    — delete               │
│  POST /api/sequences/:id/run  — execute + summarize │
└─────────────────┬───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│               Sequence Runner                        │
│                                                     │
│  1. Load sequence steps                             │
│  2. For each step (sequential):                     │
│     → connector.executeSQL(step.sql)                │
│     → collect { title, rows, rowCount }             │
│  3. If summarize=true:                              │
│     → Build prompt with all (title + results)       │
│     → ONE AI call → markdown report                 │
│  4. Return SequenceExecutionResult                  │
└─────────────────────────────────────────────────────┘
```

## 5. Storage

Sequences are stored in the data directory as a single JSONL file:

```
data/
  query-log.jsonl          # existing
  sequences.jsonl          # new — one JSON object per line
```

This follows the same pattern as `query-logger.ts` — append-only JSONL with in-memory reads.

## 6. API Endpoints

### 6.1 Create Sequence (from Query Log)

```
POST /api/sequences
Body: {
  name: string,
  description?: string,
  sourceLogId: string        // ID of the query log entry to extract from
}
```

The server reads the log entry, extracts `toolCalls` (filtering for entries with `sql` and `sqlSuccess: true`), and pairs each with its title (parsed from the explanation or tool call input).

### 6.2 List Sequences

```
GET /api/sequences
Response: HardenedSequence[]
```

### 6.3 Get Sequence

```
GET /api/sequences/:id
Response: HardenedSequence
```

### 6.4 Update Sequence

```
PUT /api/sequences/:id
Body: Partial<{ name, description, steps }>
```

Allows editing SQL, reordering steps, adding/removing steps.

### 6.5 Delete Sequence

```
DELETE /api/sequences/:id
```

### 6.6 Run Sequence

```
POST /api/sequences/:id/run
Body: {
  summarize?: boolean,       // default true — call AI for final report
  maxRows?: number
}
Response: SequenceExecutionResult
```

Execution is sequential:
1. For each step in order, execute `sql` against the connector
2. Collect results (or error) for each step
3. If `summarize=true`, build a prompt and make one AI call
4. Return all step results + optional AI summary

### 6.7 Summarization Prompt

```
You are a data analyst. You have been given the results of N SQL queries,
each with a title describing what the data represents.

Analyze the data and produce a comprehensive markdown report.

{{for each step}}
## Dataset {{order}}: {{title}}
{{rows as markdown table or JSON}}
{{/for}}

Produce a well-structured markdown report with insights, comparisons,
and key findings.
```

## 7. UI Design

### 7.1 Save from Query Logs

On the Query Logs page, for entries that have `toolCalls` with SQL data, add a **"Save as Sequence"** button. Clicking it opens a dialog:

- **Name** (text input, required)
- **Description** (text input, optional)
- Preview of the steps (Title + SQL) that will be saved
- User can deselect steps they don't want
- **[Save]** button

### 7.2 Sequences Page (New)

A new page/tab in the UI navigation:

- **List view:** All saved sequences with name, connector, step count, created date
- **Detail view:** Shows all steps (Title + SQL), with inline editing
- **Run button:** Executes the sequence, shows progress (step 1/N, 2/N, ...), then displays:
  - Per-step results (collapsible tables)
  - AI-generated summary report (with copy + PDF export)

### 7.3 Navigation

Add "Sequences" tab to the main navigation bar, between "Query Logs" and other sections.

## 8. MCP Tool (Optional)

A new MCP tool `run_sequence` could allow AI agents to execute hardened sequences:

```typescript
server.tool(
  "run_sequence",
  "Execute a saved SQL sequence and optionally summarize results with AI.",
  {
    sequenceId: z.string().describe("ID of the saved sequence"),
    summarize: z.boolean().optional().describe("Whether to AI-summarize results (default true)"),
    maxRows: z.number().optional().describe("Max rows per step"),
  },
  async ({ sequenceId, summarize, maxRows }) => { ... }
);
```

## 9. Implementation Plan

### Phase 1 — Backend Core
1. Create `src/sequence-store.ts` — JSONL storage for sequences (CRUD)
2. Create `src/sequence-runner.ts` — sequential SQL execution + AI summarization
3. Add sequence API routes to `src/api-routes.ts`
4. Add extraction logic: parse log entry → hardened steps

### Phase 2 — UI
5. Add "Save as Sequence" button + dialog to Query Logs page
6. Create `SequencesPage.tsx` — list, detail, edit, run
7. Add "Sequences" tab to navigation
8. Run results view with progress, per-step tables, AI summary

### Phase 3 — Polish
9. MCP tool `run_sequence` (optional)
10. SSE streaming for run progress
11. Sequence versioning / history (optional)
12. Export sequence as standalone SQL script (optional)

## 10. Example Walkthrough

### Step 1: User asks a question
```
"What are the top 10 customers by revenue, and how does their order frequency compare to the average?"
```

### Step 2: Agent loop produces 4 turns
| Turn | Title | SQL |
|------|-------|-----|
| 1 | Get top 10 customers by total revenue | `SELECT c.name, SUM(o.total) as revenue FROM customers c JOIN orders o ON ... GROUP BY ... ORDER BY revenue DESC LIMIT 10` |
| 2 | Calculate average order frequency across all customers | `SELECT AVG(order_count) as avg_frequency FROM (SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id) sub` |
| 3 | Get order frequency for top 10 customers | `SELECT c.name, COUNT(o.id) as order_count FROM customers c JOIN orders o ON ... WHERE c.id IN (...) GROUP BY c.name` |
| 4 | Get monthly revenue trend for top 10 customers | `SELECT DATE_TRUNC('month', o.created_at) as month, SUM(o.total) as revenue FROM orders o WHERE o.customer_id IN (...) GROUP BY month ORDER BY month` |

### Step 3: User saves as "Top Customers Revenue Report"
All 4 (Title + SQL) pairs are saved as a hardened sequence.

### Step 4: User runs the sequence next week
- SQL₁ executes → 10 rows
- SQL₂ executes → 1 row
- SQL₃ executes → 10 rows
- SQL₄ executes → 48 rows
- ONE AI call summarizes all 4 datasets → markdown report

**Result:** Same quality report, fraction of the cost and time.
