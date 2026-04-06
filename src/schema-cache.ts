import { readFileSync, writeFileSync, existsSync, mkdirSync, unlinkSync, readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";

/**
 * Schema Cache — persists discovered schema to JSON files in data/ folder.
 *
 * File format: data/schema-{connectorId}.json
 * Contains the DiscoveredDatabase object from connector.discover().
 */

export class SchemaCache {
  private dataDir: string;

  constructor(dataDir: string) {
    this.dataDir = dataDir;
    if (!existsSync(this.dataDir)) {
      mkdirSync(this.dataDir, { recursive: true });
    }
  }

  private filePath(connectorId: string): string {
    return resolve(this.dataDir, `schema-${connectorId}.json`);
  }

  has(connectorId: string): boolean {
    return existsSync(this.filePath(connectorId));
  }

  /** Returns the age of the cache file in hours, or null if no cache exists. */
  ageHours(connectorId: string): number | null {
    const path = this.filePath(connectorId);
    if (!existsSync(path)) return null;
    try {
      const stat = statSync(path);
      return (Date.now() - stat.mtimeMs) / (1000 * 60 * 60);
    } catch {
      return null;
    }
  }

  /** Check if the cache is stale (older than ttlHours). Returns false if no cache exists. */
  isStale(connectorId: string, ttlHours: number): boolean {
    if (ttlHours <= 0) return false; // 0 = never auto-refresh
    const age = this.ageHours(connectorId);
    if (age === null) return false; // no cache = not stale (just missing)
    return age > ttlHours;
  }

  load(connectorId: string): unknown | null {
    const path = this.filePath(connectorId);
    if (!existsSync(path)) return null;
    try {
      return JSON.parse(readFileSync(path, "utf-8"));
    } catch {
      return null;
    }
  }

  save(connectorId: string, discovered: unknown): void {
    writeFileSync(this.filePath(connectorId), JSON.stringify(discovered, null, 2), "utf-8");
  }

  invalidate(connectorId: string): boolean {
    const path = this.filePath(connectorId);
    if (!existsSync(path)) return false;
    unlinkSync(path);
    return true;
  }

  listCached(): string[] {
    if (!existsSync(this.dataDir)) return [];
    return readdirSync(this.dataDir)
      .filter((f) => f.startsWith("schema-") && f.endsWith(".json"))
      .map((f) => f.replace(/^schema-/, "").replace(/\.json$/, ""));
  }

  /** Get schema metadata without loading the full cache into AskSQL */
  getSchemaInfo(connectorId: string): { tables: number; columns: number; tableNames: string[]; cacheAgeHours: number | null } | null {
    const data = this.load(connectorId) as { schemas?: Array<{ schemaName: string; tables: Array<{ tableName: string; columns: unknown[] }> }> } | null;
    if (!data?.schemas) return null;
    const tableNames: string[] = [];
    let columns = 0;
    for (const s of data.schemas) {
      for (const t of s.tables) {
        tableNames.push(`${s.schemaName}.${t.tableName}`);
        columns += t.columns?.length ?? 0;
      }
    }
    return { tables: tableNames.length, columns, tableNames, cacheAgeHours: this.ageHours(connectorId) };
  }
}
