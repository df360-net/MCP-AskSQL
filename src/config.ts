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
}

export interface AppConfig {
  connectors: Array<ConnectorConfig>;
  ai: AIConfig;
  safety?: {
    maxRows?: number;
    timeoutMs?: number;
    maxRetries?: number;
  };
  /** Hours before cached schema is considered stale (default: 24). 0 = never auto-refresh. */
  schemaCacheTtlHours: number;
  dataDir: string;
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
  };
  /** Hours before cached schema is considered stale (default: 24). 0 = never auto-refresh. */
  schemaCacheTtlHours?: number;
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
  }));

  const dataDir = resolve(PROJECT_ROOT, "data");
  const schemaCacheTtlHours = raw.schemaCacheTtlHours ?? 24;

  return { connectors, ai, safety: raw.safety, schemaCacheTtlHours, dataDir };
}
