import { appendFileSync, readFileSync, writeFileSync, existsSync, renameSync, mkdirSync, statSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { resolve, dirname } from "node:path";

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

export interface LogFilters {
  connector?: string;
  status?: "success" | "fail";
  from?: string;
  to?: string;
  page?: number;
  pageSize?: number;
}

export interface LogStats {
  totalQueries: number;
  successful: number;
  failed: number;
  avgExecutionTimeMs: number;
  byConnector: Record<string, number>;
  byTool: Record<string, number>;
}

export class QueryLogger {
  private filePath: string;
  private maxFileSize: number;
  private defaultPageSize: number;

  constructor(dataDir: string, maxFileSize = 10 * 1024 * 1024, defaultPageSize = 50) {
    if (!existsSync(dataDir)) mkdirSync(dataDir, { recursive: true });
    this.filePath = resolve(dataDir, "query-log.jsonl");
    this.maxFileSize = maxFileSize;
    this.defaultPageSize = defaultPageSize;
  }

  log(entry: Omit<LogEntry, "id" | "timestamp">): void {
    const full: LogEntry = {
      id: randomUUID(),
      timestamp: new Date().toISOString(),
      ...entry,
    };

    this.rotateIfNeeded();
    appendFileSync(this.filePath, JSON.stringify(full) + "\n", "utf-8");
  }

  query(filters: LogFilters = {}): { rows: LogEntry[]; total: number } {
    const entries = this.readAll();
    const page = filters.page ?? 0;
    const pageSize = filters.pageSize ?? this.defaultPageSize;

    let filtered = entries;

    if (filters.connector) {
      filtered = filtered.filter((e) => e.connector === filters.connector);
    }
    if (filters.status === "success") {
      filtered = filtered.filter((e) => e.success);
    } else if (filters.status === "fail") {
      filtered = filtered.filter((e) => !e.success);
    }
    if (filters.from) {
      const from = new Date(filters.from).getTime();
      filtered = filtered.filter((e) => new Date(e.timestamp).getTime() >= from);
    }
    if (filters.to) {
      const to = new Date(filters.to).getTime();
      filtered = filtered.filter((e) => new Date(e.timestamp).getTime() <= to);
    }

    // Sort newest first
    filtered.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());

    const total = filtered.length;
    const rows = filtered.slice(page * pageSize, (page + 1) * pageSize);
    return { rows, total };
  }

  stats(): LogStats {
    const entries = this.readAll();
    const successful = entries.filter((e) => e.success).length;
    const totalExecTime = entries.reduce((sum, e) => sum + e.executionTimeMs, 0);
    const byConnector: Record<string, number> = {};
    const byTool: Record<string, number> = {};

    for (const e of entries) {
      byConnector[e.connector] = (byConnector[e.connector] ?? 0) + 1;
      byTool[e.tool] = (byTool[e.tool] ?? 0) + 1;
    }

    return {
      totalQueries: entries.length,
      successful,
      failed: entries.length - successful,
      avgExecutionTimeMs: entries.length > 0 ? Math.round(totalExecTime / entries.length) : 0,
      byConnector,
      byTool,
    };
  }

  clear(): void {
    writeFileSync(this.filePath, "", "utf-8");
  }

  /** Remove log entries older than the given number of days. Returns count of removed entries. */
  clearOlderThan(days: number): number {
    const entries = this.readAll();
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    const kept = entries.filter((e) => new Date(e.timestamp).getTime() >= cutoff);
    const removed = entries.length - kept.length;
    writeFileSync(this.filePath, kept.map((e) => JSON.stringify(e)).join("\n") + (kept.length > 0 ? "\n" : ""), "utf-8");
    return removed;
  }

  private readAll(): LogEntry[] {
    if (!existsSync(this.filePath)) return [];
    const content = readFileSync(this.filePath, "utf-8").trim();
    if (!content) return [];
    const entries: LogEntry[] = [];
    let skipped = 0;
    for (const line of content.split("\n")) {
      try {
        entries.push(JSON.parse(line) as LogEntry);
      } catch {
        skipped++;
      }
    }
    if (skipped > 0) {
      console.warn(`[QueryLogger] Skipped ${skipped} malformed log entries`);
    }
    return entries;
  }

  private rotateIfNeeded(): void {
    if (!existsSync(this.filePath)) return;
    try {
      const stat = statSync(this.filePath);
      if (stat.size > this.maxFileSize) {
        const ts = new Date().toISOString().replace(/[:.]/g, "-");
        const archive = resolve(dirname(this.filePath), `query-log-${ts}.jsonl`);
        renameSync(this.filePath, archive);
      }
    } catch (err) {
      console.error(`[QueryLogger] Log rotation failed:`, err instanceof Error ? err.message : err);
    }
  }
}
