# Generate Deterministic Workflow

## 1. Overview

When a user asks a natural-language question, the Level 2 + Level 1 agent loop produces a series of SQL queries across multiple turns. Each turn has:

- **Title** — the sub-question Level 2 asked Level 1
- **SQL** — the query Level 1 generated
- **Execution Status** — whether the SQL ran successfully

A **Workflow** allows users to **save** those proven (Title, SQL) pairs along with the original question and AI reasoning, then **replay** them at any time — skipping both Level 1 and Level 2 AI for data gathering — and only calling Level 2 AI once at the end to summarize all results into a markdown report.

## 2. Motivation

| Concern | Full AI Loop | Workflow |
|---------|-------------|----------|
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

### 3.1 Workflow

```typescript
interface Workflow {
  id: string;                  // UUID
  name: string;                // User-given name (e.g., "TPCH Comprehensive Analysis")
  description?: string;        // Optional description
  connector: string;           // Target connector ID
  originalQuestion: string;    // The NL question that produced this workflow
  aiReasoning?: string;        // Level 2 AI reasoning (turn-by-turn thought process)
  createdAt: string;           // ISO timestamp
  updatedAt: string;           // ISO timestamp

  steps: WorkflowStep[];       // Ordered list of (Title, SQL) pairs
}

interface WorkflowStep {
  order: number;               // Execution order (1-based)
  title: string;               // The sub-question / purpose of this SQL
  sql: string;                 // The proven SQL query
}
```

### 3.2 Execution Result

```typescript
interface WorkflowRunResult {
  workflowId: string;
  workflowName: string;
  connector: string;
  executionTimeMs: number;
  steps: StepResult[];
  summary?: string;            // AI-generated markdown report
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

### 4.2 Workflow Flow (Deterministic)

```
Saved Workflow (Title₁+SQL₁, Title₂+SQL₂, ..., Titleₙ+SQLₙ)
  → Execute SQL₁ → Results₁
  → Execute SQL₂ → Results₂
  → ...
  → Execute SQLₙ → Resultsₙ
  → ONE Level 2 AI call with:
      - Database schema (full connector schema context)
      - Original question
      - AI reasoning (analytical approach from original run)
      - All datasets with titles
  → Final Markdown Report
```

**Key difference:** The data-gathering phase is 100% deterministic (no AI). Only the final summarization uses AI — and it receives the full schema context for richer analysis.

### 4.3 Component Diagram

```
┌─────────────────────────────────────────────────────┐
│                     UI Layer                         │
│                                                     │
│  Query Logs Page          Workflows Page             │
│  ┌─────────────────┐     ┌──────────────────────┐   │
│  │ [Save as         │     │ Grouped by connector  │   │
│  │  Workflow] btn   │     │ [Run] → Results +     │   │
│  │                  │     │        AI Summary     │   │
│  └─────────────────┘     └──────────────────────┘   │
└─────────────────┬───────────────────┬───────────────┘
                  │                   │
                  ▼                   ▼
┌─────────────────────────────────────────────────────┐
│                   REST API Layer                     │
│                                                     │
│  POST /api/workflows          — create from log     │
│  GET  /api/workflows          — list all            │
│  GET  /api/workflows/:id      — get one             │
│  PUT  /api/workflows/:id      — update (edit SQL)   │
│  DELETE /api/workflows/:id    — delete               │
│  POST /api/workflows/:id/run  — execute + summarize │
└─────────────────┬───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│               Workflow Runner                        │
│                                                     │
│  1. Load workflow steps                             │
│  2. For each step (sequential):                     │
│     → connector.executeSQL(step.sql)                │
│     → collect { title, rows, rowCount }             │
│  3. Build summarization prompt:                     │
│     → Database schema (connector schema context)    │
│     → Original question                             │
│     → AI reasoning (if saved)                       │
│     → All datasets with titles and results          │
│  4. ONE AI call → markdown report                   │
│  5. Return WorkflowRunResult                        │
└─────────────────────────────────────────────────────┘
```

## 5. Storage

Workflows are stored in the data directory as a single JSONL file:

```
data/
  query-log.jsonl          # existing
  workflows.jsonl          # new — one JSON object per line
```

This follows the same pattern as `query-logger.ts` — JSONL with in-memory reads.

Implementation: `src/workflow-store.ts`

## 6. API Endpoints

### 6.1 Create Workflow (from Query Log)

```
POST /api/workflows
Body: {
  name: string,
  description?: string,
  connector: string,
  originalQuestion: string,
  aiReasoning?: string,
  steps: WorkflowStep[]
}
```

The UI extracts `toolCalls` from the log entry (filtering for entries with `sql` and `sqlSuccess: true`), and pairs each with its title.

### 6.2 List Workflows

```
GET /api/workflows
Response: Workflow[]
```

### 6.3 Get Workflow

```
GET /api/workflows/:id
Response: Workflow
```

### 6.4 Update Workflow

```
PUT /api/workflows/:id
Body: Partial<{ name, description, steps }>
```

Allows editing SQL, reordering steps, adding/removing steps.

### 6.5 Delete Workflow

```
DELETE /api/workflows/:id
```

### 6.6 Run Workflow

```
POST /api/workflows/:id/run
Body: {
  summarize?: boolean,       // default true — call AI for final report
  maxRows?: number
}
Response: WorkflowRunResult
```

Execution is sequential:
1. For each step in order, execute `sql` against the connector
2. Collect results (or error) for each step
3. If `summarize=true`, build a prompt and make one AI call
4. Return all step results + optional AI summary

### 6.7 Summarization Prompt Structure

The AI receives maximum context for rich analysis:

```
You are a senior data analyst.

This is the schema of the database:
<full connector schema: tables, columns, types, foreign keys, sample values>

The user asked: "<original question>"

The original analytical reasoning was:
<AI reasoning from the original run — turn-by-turn thought process>

You have been given the results of N SQL queries.
Analyze the data and produce a comprehensive markdown report.

## Dataset 1: <title>
<row count>
<column headers>
<data rows (up to 100 per dataset)>

---

## Dataset 2: <title>
...

SUMMARIZE a well-structured markdown report with insights,
comparisons, and key findings.
```

**Why include schema?** The AI understands table relationships, column semantics, foreign keys — producing richer insights instead of just describing raw numbers.

**Why include AI reasoning?** The reasoning captures the analytical approach from the original run — why each sub-question was asked, how intermediate results informed next steps. This guides the summarization to follow the same analytical logic.

## 7. UI Design

### 7.1 Save from Query Logs

On the Query Logs page, for entries that have `toolCalls` with successful SQL data, a green **"Save as Workflow"** button appears. Clicking it opens a modal dialog:

- **Name** (text input, required)
- **Description** (text input, optional)
- **Connector** (auto-filled from log entry)
- **Original Question** (auto-filled)
- Preview of the steps (Title + SQL) that will be saved
- **[Save Workflow]** button

The AI reasoning is automatically saved from the log entry's `explanation` field.

### 7.2 Workflows Page

A new page in the UI navigation (right after "NL Query"), organized by connector:

```
┌─ sf_tpch (3 workflows) ──────────────────────────┐
│                                                    │
│  TPCH Comprehensive Analysis      8 steps  [Run]  │
│  "Can you tell me with the data in sf_tpch..."     │
│  Created: 4/11/2026                                │
│                                                    │
│  Monthly Revenue Report            4 steps  [Run]  │
│  ...                                               │
└────────────────────────────────────────────────────┘

┌─ sales_db (0 workflows) ─────────────────────────┐
│  No workflows saved for this connector.            │
└────────────────────────────────────────────────────┘
```

**Workflow card features:**
- Click to expand: shows description, original question, AI reasoning (collapsible), and all steps (Title + SQL)
- **Run** button: executes all SQLs sequentially, then AI summarization
- **Delete** button

**Run results view:**
- Per-step results (collapsible, with OK/FAIL badge, row count, execution time, data table)
- **AI Summary Report** section with copy and PDF export buttons

### 7.3 Navigation

"Workflows" tab in the left sidebar, positioned right after "NL Query".

## 8. Implementation

### Files Created/Modified

| File | Purpose |
|------|---------|
| `src/workflow-store.ts` | JSONL-based CRUD storage for workflows |
| `src/api-routes.ts` | 6 new REST endpoints for workflow CRUD + run |
| `src/index.ts` | WorkflowStore initialization and wiring |
| `src/ui/src/types.ts` | `Workflow`, `WorkflowStep`, `WorkflowRunResult` types |
| `src/ui/src/components/WorkflowsPage.tsx` | Workflows page (grouped by connector) |
| `src/ui/src/components/QueryLogsPage.tsx` | "Save as Workflow" button + modal dialog |
| `src/ui/src/App.tsx` | Workflows nav item and page routing |

## 9. Example Walkthrough

### Step 1: User asks a question
```
"Can you tell me with the data in sf_tpch, what kind of insights
I can get? can you help me to do some useful data analysis?"
```

### Step 2: Agent loop produces 8 turns

| Turn | Title | Tool |
|------|-------|------|
| 1 | (schema exploration) | list_tables |
| 2 | What is the total number of records in each table? | ask_sql |
| 3 | What is the total sales revenue over time? | ask_sql |
| 4 | What are the top 10 customers by total spending? | ask_sql |
| 5 | What are the top 10 most profitable product types? | ask_sql |
| 6 | Which suppliers provide the most parts? | ask_sql |
| 7 | What is the distribution of order statuses? | ask_sql |
| 8 | Which regions and nations have the highest revenue? | ask_sql |
| 9 | What are the most common shipping modes? | ask_sql |

Level 2 AI produces a comprehensive markdown report.

### Step 3: User saves as "TPCH Comprehensive Analysis"
- 8 (Title + SQL) pairs saved (list_tables excluded — no SQL)
- Original question saved
- AI reasoning saved (turn-by-turn analytical thought process)

### Step 4: User runs the workflow later
- 8 SQLs execute sequentially (~2s total for direct SQL)
- ONE AI call with schema + question + reasoning + all 8 datasets (~40s)
- AI produces comprehensive markdown report with:
  - Executive Summary
  - Data Overview & Volume Analysis
  - Sales Performance Analysis
  - Customer Segmentation
  - Product Performance
  - Supplier Analysis
  - Geographic Revenue Analysis
  - Shipping & Logistics
  - Cross-Analysis & Strategic Recommendations

**Result:** Same quality report, fraction of the cost. ~44s vs ~134s, 1 AI call vs ~20 AI calls.

## 10. Workflow Scheduler

### 10.1 Overview

The Workflow Scheduler enables automatic, recurring execution of saved workflows. Borrowed from the MyHeadlines project scheduler architecture, adapted from SQLite to file-based (JSONL) storage.

**Core features:**
- **Tick-based engine** — checks for due jobs every 5 seconds
- **Interval-based scheduling** — run every N seconds (e.g., every hour)
- **Daily fixed-time scheduling** — run at a specific time (e.g., 7:00 AM)
- **Sequential execution** — one job at a time to avoid resource contention
- **Timeout protection** — `Promise.race` kills stuck jobs after N seconds
- **Crash recovery** — marks orphaned `RUNNING` jobs as `FAILED` on startup

### 10.2 Data Model

```typescript
interface ScheduledJob {
  id: string;                          // UUID
  workflowId: string;                  // Reference to saved Workflow
  name: string;                        // Display name (defaults to workflow name)
  scheduleType: "interval" | "daily";  // How to schedule
  intervalSeconds?: number;            // For interval-based (e.g., 3600 = hourly)
  dailyRunTime?: string;               // For daily (e.g., "07:00" in user's timezone)
  timeoutSeconds: number;              // Max execution time before kill
  isEnabled: boolean;                  // Admin can enable/disable
  nextRunAt: string;                   // ISO timestamp — source of truth for due jobs
  createdAt: string;
  updatedAt: string;
}

interface JobRun {
  id: string;                          // UUID
  jobId: string;                       // Reference to ScheduledJob
  workflowId: string;                  // Reference to Workflow
  status: "RUNNING" | "COMPLETED" | "FAILED" | "TIMED_OUT";
  startedAt: string;                   // ISO timestamp
  completedAt?: string;                // ISO timestamp
  durationMs?: number;
  stepsCompleted: number;              // How many SQL steps succeeded
  stepsTotal: number;                  // Total steps in workflow
  summary?: string;                    // AI-generated summary (if completed)
  error?: string;                      // Error message (if failed)
  triggeredBy: "SCHEDULER" | "MANUAL"; // Who triggered the run
}
```

### 10.3 Storage

File-based JSONL storage in the data directory:

```
data/
  workflows.jsonl          # existing — saved workflows
  scheduler-jobs.jsonl     # new — scheduled job configs
  scheduler-runs.jsonl     # new — execution history
```

### 10.4 Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Scheduler Engine                    │
│                                                     │
│  5-second tick loop:                                │
│  1. Query scheduler-jobs.jsonl for due jobs         │
│     WHERE isEnabled = true AND nextRunAt <= now     │
│  2. For each due job:                               │
│     → Advance nextRunAt (interval or daily calc)    │
│     → Create JobRun record as RUNNING               │
│     → Execute workflow (all SQLs + AI summarize)    │
│     → Mark JobRun as COMPLETED or FAILED            │
│  3. Timeout protection via Promise.race             │
│                                                     │
│  Crash recovery on startup:                         │
│  → Mark any RUNNING jobs as FAILED                  │
└─────────────────────────────────────────────────────┘
```

### 10.5 Execution Flow

```
Tick (every 5s)
  → Find due jobs (nextRunAt <= now && isEnabled)
  → For each due job:
      1. Advance nextRunAt to next scheduled time
      2. Create JobRun { status: RUNNING }
      3. Load Workflow by workflowId
      4. Execute each SQL step sequentially
      5. AI summarization call (schema + data + reasoning)
      6. Update JobRun { status: COMPLETED, summary, durationMs }
      7. On error: JobRun { status: FAILED, error }
      8. On timeout: JobRun { status: TIMED_OUT }
```

### 10.6 API Endpoints

```
GET    /api/scheduler/jobs              — list all scheduled jobs
POST   /api/scheduler/jobs              — create a schedule for a workflow
PUT    /api/scheduler/jobs/:id          — update schedule (interval, enabled, etc.)
DELETE /api/scheduler/jobs/:id          — delete a scheduled job
POST   /api/scheduler/jobs/:id/trigger  — manually trigger a run now
GET    /api/scheduler/jobs/:id/runs     — get run history for a job
```

### 10.7 Daily Run Time Calculation

For daily scheduled jobs, convert user's local time to UTC:

1. Get user's timezone (from config or system default)
2. Calculate next occurrence of the target time in the user's timezone
3. If target time has already passed today, add 24 hours
4. Store as UTC ISO timestamp in `nextRunAt`

### 10.8 UI Design

Add a "Scheduler" tab to the Workflows page or as a separate page:

```
┌─────────────────────────────────────────────────────┐
│  Scheduled Workflows                                │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ TPCH Daily Report          Every 24h  [ON]  │    │
│  │ Workflow: TPCH Comprehensive Analysis       │    │
│  │ Next run: 4/12/2026 7:00 AM                 │    │
│  │ Last run: COMPLETED  44s  4/11/2026 7:00 AM │    │
│  │                        [Trigger Now] [Edit]  │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Sales Hourly Check         Every 1h  [OFF]  │    │
│  │ Workflow: Sales Summary                      │    │
│  │ Next run: (disabled)                         │    │
│  │ Last run: FAILED  "timeout after 60s"       │    │
│  │                        [Trigger Now] [Edit]  │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### 10.9 Implementation Files

| File | Purpose |
|------|---------|
| `src/scheduler/engine.ts` | 5-second tick loop, execution with timeout, crash recovery |
| `src/scheduler/store.ts` | JSONL storage for jobs and runs |
| `src/scheduler/types.ts` | Type definitions |
| `src/api-routes.ts` | Scheduler REST endpoints |
| `src/index.ts` | Engine startup/shutdown lifecycle |
| `src/ui/src/components/SchedulerPage.tsx` | UI for managing scheduled workflows |

### 10.10 Lifecycle

**Startup** (in `src/index.ts`):
```
1. Load scheduled jobs from scheduler-jobs.jsonl
2. Crash recovery: mark any RUNNING jobs as FAILED
3. Start 5-second tick loop
```

**Shutdown** (on `SIGINT`/`SIGTERM`):
```
1. Set shuttingDown flag
2. Clear tick interval
3. Wait for any running job to finish
4. Close resources
```

## 11. Future Enhancements

- **MCP tool `run_workflow`** — allow AI agents to execute saved workflows
- **Workflow editing** — inline SQL editing in the UI
- **Workflow versioning** — track changes over time
- **Export as SQL script** — standalone SQL file for use outside MCP-AskSQL
- **Parameterized workflows** — template variables in SQL (e.g., date ranges)
- **Email/webhook notifications** — notify on workflow completion or failure
- **Run history dashboard** — charts and metrics for scheduled workflow runs
