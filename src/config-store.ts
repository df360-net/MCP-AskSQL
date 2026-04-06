import { readFileSync, writeFileSync, renameSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "..");

export interface FileConnectorConfig {
  id: string;
  connectionString: string;
  schemas?: string[];
  abbreviations?: Record<string, string[]>;
  examples?: Array<{ question: string; sql: string }>;
  schemaPrefix?: string;
}

export interface FileAIConfig {
  baseUrl: string;
  apiKey?: string;
  model: string;
  maxTokens?: number;
  temperature?: number;
  timeoutMs?: number;
}

export interface FileConfig {
  connectors: FileConnectorConfig[];
  ai: FileAIConfig;
  safety?: {
    maxRows?: number;
    timeoutMs?: number;
    maxRetries?: number;
  };
  schemaCacheTtlHours?: number;
}

export class ConfigStore {
  private filePath: string;

  constructor(filePath?: string) {
    this.filePath = filePath ?? resolve(PROJECT_ROOT, "config.json");
  }

  read(): FileConfig {
    return JSON.parse(readFileSync(this.filePath, "utf-8")) as FileConfig;
  }

  /** Atomic write: write to temp file, then rename over original */
  write(config: FileConfig): void {
    const tmp = this.filePath + ".tmp";
    writeFileSync(tmp, JSON.stringify(config, null, 2), "utf-8");
    renameSync(tmp, this.filePath);
  }

  updateConnectors(connectors: FileConnectorConfig[]): void {
    const config = this.read();
    config.connectors = connectors;
    this.write(config);
  }

  updateAI(ai: Partial<FileAIConfig>): void {
    const config = this.read();
    config.ai = { ...config.ai, ...ai };
    // Remove undefined fields
    for (const key of Object.keys(config.ai) as (keyof FileAIConfig)[]) {
      if (config.ai[key] === undefined) delete config.ai[key];
    }
    this.write(config);
  }

  getFilePath(): string {
    return this.filePath;
  }
}
