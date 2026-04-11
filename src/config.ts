import { readFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { AIConfig } from "./asksql/core/ai/client.js";

// ---------------------------------------------------------------------------
// Config types
// ---------------------------------------------------------------------------

export interface ConnectorConfig {
  id: string;
  connectionString: string;
  schemas?: string[];
  abbreviations?: Record<string, string[]>;
  examples?: Array<{ question: string; sql: string }>;
  schemaPrefix?: string;
  pool?: {
    /** Connection timeout in milliseconds (default: 10000; Redshift default: 60000) */
    connectTimeoutMs?: number;
    /** Pool idle timeout in milliseconds (default: 20000) */
    idleTimeoutMs?: number;
    /** Max pool size (default: 5) */
    size?: number;
  };
}

export interface AskAgentAppConfig {
  /** Enable the agent loop for the ask MCP tool (default: false) */
  enabled?: boolean;
  /** Max AI ↔ tool round-trips before forced summary (default: 10) */
  maxTurns?: number;
  /** Max characters for tool output before truncation (default: 2000) */
  toolOutputMaxChars?: number;
}

export interface AppConfig {
  connectors: Array<ConnectorConfig>;
  ai: AIConfig;
  safety?: {
    maxRows?: number;
    timeoutMs?: number;
    maxRetries?: number;
    /** Max AI API retry attempts (default: 3) */
    aiRetries?: number;
    /** Graceful shutdown timeout in milliseconds (default: 10000) */
    shutdownTimeoutMs?: number;
    /** Max distinct sample values collected during schema discovery (default: 20) */
    maxSampleValues?: number;
    /** Postgres session-level lock timeout in milliseconds (default: 5000) */
    lockTimeoutMs?: number;
    /** BigQuery max bytes billed per query safety cap (default: "1073741824" = 1GB) */
    maxBytesBilled?: string;
    /** Dremio cloud job polling interval in milliseconds (default: 500) */
    jobPollIntervalMs?: number;
  };
  /** Hours before cached schema is considered stale (default: 24). 0 = never auto-refresh. */
  schemaCacheTtlHours: number;
  dataDir: string;
  /** Agent loop config for the ask MCP tool */
  ask?: AskAgentAppConfig;
  logging?: {
    /** Max log file size in MB before rotation (default: 10) */
    maxFileSizeMb?: number;
    /** Default page size for log queries (default: 50) */
    defaultPageSize?: number;
  };
  routing?: {
    /** Minimum token length for keyword matching (default: 3) */
    minTokenLength?: number;
    /** Score multiplier to determine clear winner (default: 2) */
    winnerScoreMultiplier?: number;
  };
}

// ---------------------------------------------------------------------------
// File config shape (what config.json looks like)
// ---------------------------------------------------------------------------

interface FileConfig {
  connectors: Array<{
    id: string;
    connectionString: string;
    schemas?: string[];
    abbreviations?: Record<string, string[]>;
    examples?: Array<{ question: string; sql: string }>;
    schemaPrefix?: string;
    pool?: {
      connectTimeoutMs?: number;
      idleTimeoutMs?: number;
      size?: number;
    };
  }>;
  ai: {
    baseUrl: string;
    apiKey?: string;
    model: string;
    maxTokens?: number;
    temperature?: number;
    timeoutMs?: number;
  };
  safety?: {
    maxRows?: number;
    timeoutMs?: number;
    maxRetries?: number;
    aiRetries?: number;
    shutdownTimeoutMs?: number;
    maxSampleValues?: number;
    lockTimeoutMs?: number;
    maxBytesBilled?: string;
    jobPollIntervalMs?: number;
  };
  /** Hours before cached schema is considered stale (default: 24). 0 = never auto-refresh. */
  schemaCacheTtlHours?: number;
  ask?: {
    enabled?: boolean;
    maxTurns?: number;
    toolOutputMaxChars?: number;
  };
  logging?: {
    maxFileSizeMb?: number;
    defaultPageSize?: number;
  };
  routing?: {
    minTokenLength?: number;
    winnerScoreMultiplier?: number;
  };
}

// ---------------------------------------------------------------------------
// Load config
// ---------------------------------------------------------------------------

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "..");

export function loadConfig(configPath?: string): AppConfig {
  const filePath = configPath ?? resolve(PROJECT_ROOT, "config.json");

  if (!existsSync(filePath)) {
    throw new Error(
      `Config file not found: ${filePath}\n` +
      `Copy config.example.json to config.json and fill in your values.`,
    );
  }

  let raw: FileConfig;
  try {
    raw = JSON.parse(readFileSync(filePath, "utf-8")) as FileConfig;
  } catch (err) {
    throw new Error(`Failed to parse config file: ${err instanceof Error ? err.message : err}`);
  }

  // Validate connectors
  if (!raw.connectors || raw.connectors.length === 0) {
    throw new Error("config.json: at least one connector is required");
  }
  for (const c of raw.connectors) {
    if (!c.id) throw new Error("config.json: each connector must have an 'id'");
    if (!c.connectionString) throw new Error(`config.json: connector '${c.id}' is missing 'connectionString'`);
  }

  // Validate AI config — apiKey can come from env var for security
  if (!raw.ai?.baseUrl || !raw.ai?.model) {
    throw new Error("config.json: ai.baseUrl and ai.model are required");
  }
  const apiKey = raw.ai.apiKey || process.env.AI_API_KEY;
  if (!apiKey) {
    throw new Error("AI API key is required: set ai.apiKey in config.json or AI_API_KEY env var");
  }

  // Only include defined fields — undefined values would override AIClient defaults
  const ai: AIConfig = { baseUrl: raw.ai.baseUrl, apiKey, model: raw.ai.model };
  if (raw.ai.maxTokens !== undefined) ai.maxTokens = raw.ai.maxTokens;
  if (raw.ai.temperature !== undefined) ai.temperature = raw.ai.temperature;
  if (raw.ai.timeoutMs !== undefined) ai.timeoutMs = raw.ai.timeoutMs;

  const connectors: ConnectorConfig[] = raw.connectors.map((c) => ({
    id: c.id,
    connectionString: c.connectionString,
    schemas: c.schemas ?? ["public"],
    abbreviations: c.abbreviations,
    examples: c.examples,
    schemaPrefix: c.schemaPrefix,
    pool: c.pool,
  }));

  const dataDir = resolve(PROJECT_ROOT, "data");
  const schemaCacheTtlHours = raw.schemaCacheTtlHours ?? 24;

  return {
    connectors, ai, safety: raw.safety, schemaCacheTtlHours, dataDir,
    ask: raw.ask, logging: raw.logging, routing: raw.routing,
  };
}
