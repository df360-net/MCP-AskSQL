export interface ConnectorInfo {
  id: string;
  type: string;
  schemas: string[];
  isDefault: boolean;
  cached: boolean;
  cacheAgeHours: number | null;
}

export interface ConnectorDetail {
  id: string;
  connectionString: string;
  schemas?: string[];
  schemaPrefix?: string;
  abbreviations?: Record<string, string[]>;
  examples?: Array<{ question: string; sql: string }>;
  schemaInfo: SchemaInfo | null;
}

export interface SchemaInfo {
  tables: number;
  columns: number;
  tableNames: string[];
  cacheAgeHours: number | null;
}

export interface HealthResult {
  database: { connected: boolean; version?: string };
  ai: { reachable: boolean };
}

export interface AISettings {
  baseUrl: string;
  model: string;
  maxTokens?: number;
  temperature?: number;
  timeoutMs?: number;
  apiKeyMasked: string;
}

export interface LogEntry {
  id: string;
  timestamp: string;
  tool: string;
  connector: string;
  question?: string;
  sql?: string;
  success: boolean;
  error?: string;
  executionTimeMs: number;
  rowCount?: number;
  /** Level 2 agent loop explanation (turn-by-turn reasoning) */
  explanation?: string;
  /** Level 2 agent loop final answer (markdown analysis) */
  answer?: string;
  /** Level 2 agent loop tool calls audit trail */
  toolCalls?: Array<{ turn: number; tool: string; input: Record<string, unknown>; output: string; durationMs: number; sql?: string; sqlSuccess?: boolean }>;
}

export interface LogStats {
  totalQueries: number;
  successful: number;
  failed: number;
  avgExecutionTimeMs: number;
  byConnector: Record<string, number>;
  byTool: Record<string, number>;
}

export interface ServerHealth {
  ok: boolean;
  server: string;
  connectors: number;
  activeSessions: number;
}

export interface AskResult {
  success: boolean;
  sql: string | null;
  explanation: string | null;
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
  error?: string;
  routedTo?: string;
  routeMethod?: string;
  routeConfidence?: string;
  // Agent loop fields (when ask.enabled = true)
  answer?: string;
  turns?: number;
  toolCalls?: Array<{ turn: number; tool: string; input: Record<string, unknown>; output: string; durationMs: number; sql?: string; sqlSuccess?: boolean }>;
  tokenUsage?: { promptTokens: number; completionTokens: number; totalTokens: number; estimatedCost: number };
}

export interface WorkflowStep {
  order: number;
  title: string;
  sql: string;
}

export interface Workflow {
  id: string;
  name: string;
  description?: string;
  connector: string;
  originalQuestion: string;
  aiReasoning?: string;
  steps: WorkflowStep[];
  createdAt: string;
  updatedAt: string;
}

export interface WorkflowRunResult {
  workflowId: string;
  workflowName: string;
  connector: string;
  executionTimeMs: number;
  steps: Array<{
    order: number;
    title: string;
    sql: string;
    success: boolean;
    error?: string;
    rows: Record<string, unknown>[];
    rowCount: number;
    executionTimeMs: number;
  }>;
  summary?: string;
}

export interface ScheduledJob {
  id: string;
  workflowId: string;
  name: string;
  scheduleType: "interval" | "daily";
  intervalSeconds?: number;
  dailyRunTime?: string;
  timeoutSeconds: number;
  emailRecipients?: string[];
  isEnabled: boolean;
  nextRunAt: string;
  createdAt: string;
  updatedAt: string;
  lastRun?: JobRun | null;
}

export interface JobRun {
  id: string;
  jobId: string;
  workflowId: string;
  status: "RUNNING" | "COMPLETED" | "FAILED" | "TIMED_OUT";
  startedAt: string;
  completedAt?: string;
  durationMs?: number;
  stepsCompleted: number;
  stepsTotal: number;
  summary?: string;
  error?: string;
  emailSent?: boolean;
  triggeredBy: "SCHEDULER" | "MANUAL";
}

export interface SQLExecuteResult {
  rows: Record<string, unknown>[];
  columns: Array<{ name: string; type: string }>;
  rowCount: number;
  truncated: boolean;
  executionTimeMs: number;
}
