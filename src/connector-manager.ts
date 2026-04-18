import { AskSQL } from "./asksql/core/asksql.js";
import type { AskSQLConfig } from "./asksql/core/asksql.js";
import type { DiscoveredDatabase } from "./asksql/core/connector/discovery-types.js";
import type { AIConfig } from "./asksql/core/ai/client.js";
import type { AppConfig, ConnectorConfig } from "./config.js";
import { detectConnectorType } from "./asksql/core/connector/registry.js";
import { SchemaCache } from "./schema-cache.js";
import { AutoRouter, type RouteResult } from "./auto-router.js";

// Side-effect imports: register all connector factories
import "./asksql/connectors/postgres/index.js";
import "./asksql/connectors/mysql/index.js";
import "./asksql/connectors/mssql/index.js";
import "./asksql/connectors/oracle/index.js";
import "./asksql/connectors/snowflake/index.js";
import "./asksql/connectors/bigquery/index.js";
import "./asksql/connectors/redshift/index.js";
import "./asksql/connectors/databricks/index.js";
import "./asksql/connectors/dremio/index.js";
import "./asksql/connectors/teradata/index.js";

export interface ConnectorInfo {
  id: string;
  type: string;
  schemas: string[];
  isDefault: boolean;
  cached: boolean;
  cacheAgeHours: number | null;
}

export class ConnectorManager {
  private instances = new Map<string, AskSQL>();
  private connectorConfigs = new Map<string, ConnectorConfig>();
  private defaultId: string;
  private config: AppConfig;
  private cache: SchemaCache;
  private ttlHours: number;
  private refreshing = new Set<string>();
  private backgroundPromises = new Set<Promise<void>>();
  private router: AutoRouter | null = null;

  constructor(config: AppConfig) {
    this.config = config;
    this.defaultId = config.connectors[0].id;
    this.cache = new SchemaCache(config.dataDir);
    this.ttlHours = config.schemaCacheTtlHours;
  }

  async init(): Promise<void> {
    for (const c of this.config.connectors) {
      await this.initConnector(c);
    }

    // Build auto-router from cached schemas
    this.rebuildRouter();

    // Pre-warm: validate connectivity in background (fire-and-forget, parallel)
    // Do not await — startup must not block on DB reachability.
    void Promise.allSettled(
      Array.from(this.instances.entries()).map(async ([id, asksql]) => {
        try {
          await asksql.healthCheck();
          console.error(`[${id}] health check passed`);
        } catch (err) {
          console.error(`[${id}] health check failed:`, err instanceof Error ? err.message : err);
        }
      }),
    );
  }

  private rebuildRouter(): void {
    this.router = new AutoRouter(
      Array.from(this.instances.keys()),
      this.defaultId,
      this.cache,
      this.config.ai,
      this.config.routing,
    );
    console.error(`[auto-router] built index for ${this.instances.size} connector(s)`);
  }

  private async initConnector(c: ConnectorConfig): Promise<void> {
    const asksql = this.createInstance(c);

    // Try loading schema from cache
    const cached = this.cache.load(c.id) as DiscoveredDatabase | null;
    if (cached) {
      const stats = asksql.loadFromCache(cached);
      const age = this.cache.ageHours(c.id);
      const ageStr = age !== null ? `${age.toFixed(1)}h old` : "";
      console.error(`[${c.id}] loaded schema from cache (${stats.tables} tables, ${stats.columns} columns, ${ageStr})`);

      if (this.cache.isStale(c.id, this.ttlHours)) {
        console.error(`[${c.id}] cache is stale (>${this.ttlHours}h), refreshing in background...`);
        this.backgroundRefresh(c.id);
      }
    } else {
      console.error(`[${c.id}] no cache found, discovering schema...`);
      try {
        const result = await asksql.refreshCatalog();
        this.cache.save(c.id, result.discovered);
        console.error(`[${c.id}] schema discovered and cached (${result.tables} tables, ${result.columns} columns)`);
      } catch (err) {
        console.error(`[${c.id}] schema discovery failed:`, err instanceof Error ? err.message : err);
      }
    }

    this.instances.set(c.id, asksql);
    this.connectorConfigs.set(c.id, c);
  }

  private createInstance(c: ConnectorConfig): AskSQL {
    return new AskSQL({
      connector: {
        connectionString: c.connectionString,
        ...(c.schemas !== undefined && { schemas: c.schemas }),
        ...(c.catalog !== undefined && { catalog: c.catalog }),
        ...(c.timeoutMs !== undefined && { timeoutMs: c.timeoutMs }),
        ...(c.pool?.connectTimeoutMs !== undefined && { connectTimeoutMs: c.pool.connectTimeoutMs }),
        ...(c.pool?.idleTimeoutMs !== undefined && { idleTimeoutMs: c.pool.idleTimeoutMs }),
        ...(c.pool?.size !== undefined && { poolSize: c.pool.size }),
        ...(this.config.safety?.maxSampleValues !== undefined && { maxSampleValues: this.config.safety.maxSampleValues }),
        ...(this.config.safety?.lockTimeoutMs !== undefined && { lockTimeoutMs: this.config.safety.lockTimeoutMs }),
        ...(this.config.safety?.maxBytesBilled !== undefined && { maxBytesBilled: this.config.safety.maxBytesBilled }),
        ...(this.config.safety?.jobPollIntervalMs !== undefined && { jobPollIntervalMs: this.config.safety.jobPollIntervalMs }),
      },
      ai: { ...this.config.ai, ...(this.config.safety?.aiRetries !== undefined && { maxRetries: this.config.safety.aiRetries }) },
      safety: this.config.safety,
      abbreviations: c.abbreviations,
      examples: c.examples,
      schemaPrefix: c.schemaPrefix,
    });
  }

  // ── Public API ─────────────────────────────────────────────────

  get(id?: string): AskSQL {
    const connectorId = id ?? this.defaultId;
    const instance = this.instances.get(connectorId);
    if (!instance) {
      const available = Array.from(this.instances.keys()).join(", ");
      console.error(`[connector-manager] Unknown connector '${connectorId}'. Available: ${available}`);
      throw new Error(`Unknown connector '${connectorId}'`);
    }

    if (this.ttlHours > 0 && this.cache.isStale(connectorId, this.ttlHours)) {
      this.backgroundRefresh(connectorId);
    }

    return instance;
  }

  async refreshSchema(id?: string): Promise<{ connector: string; tables: number; columns: number }> {
    const connectorId = id ?? this.defaultId;
    let asksql = this.instances.get(connectorId);
    if (!asksql) {
      throw new Error(`Unknown connector '${connectorId}'`);
    }

    try {
      const result = await asksql.refreshCatalog();
      this.cache.save(connectorId, result.discovered);
      this.rebuildRouter();
      console.error(`[${connectorId}] schema refreshed and cached (${result.tables} tables, ${result.columns} columns)`);
      return { connector: connectorId, tables: result.tables, columns: result.columns };
    } catch (err) {
      // Connection may be stale/closed — recreate the instance and retry
      console.error(`[${connectorId}] refresh failed, recreating connection...`, err instanceof Error ? err.message : err);
      const config = this.connectorConfigs.get(connectorId);
      if (!config) throw err;

      try { await asksql.close(); } catch (err) { console.error(`[${connectorId}] close error:`, err instanceof Error ? err.message : err); }
      asksql = this.createInstance(config);
      this.instances.set(connectorId, asksql);

      const result = await asksql.refreshCatalog();
      this.cache.save(connectorId, result.discovered);
      this.rebuildRouter();
      console.error(`[${connectorId}] schema refreshed after reconnect (${result.tables} tables, ${result.columns} columns)`);
      return { connector: connectorId, tables: result.tables, columns: result.columns };
    }
  }

  listConnectors(): ConnectorInfo[] {
    return Array.from(this.connectorConfigs.entries()).map(([id, c]) => ({
      id,
      type: detectConnectorType(c.connectionString) ?? "unknown",
      schemas: c.schemas ?? [],
      isDefault: id === this.defaultId,
      cached: this.cache.has(id),
      cacheAgeHours: this.cache.ageHours(id),
    }));
  }

  getConnectorConfig(id: string): ConnectorConfig | undefined {
    return this.connectorConfigs.get(id);
  }

  /** Auto-route a question to the best connector */
  async routeQuestion(question: string): Promise<RouteResult> {
    if (!this.router) {
      return { connectorId: this.defaultId, method: "default", confidence: "router not initialized" };
    }
    return this.router.route(question);
  }

  getSchemaInfo(id: string) {
    return this.cache.getSchemaInfo(id);
  }

  getSchemaDetail(id: string): unknown | null {
    return this.cache.load(id);
  }

  getAIConfig(): AIConfig {
    return { ...this.config.ai, ...(this.config.safety?.aiRetries !== undefined && { maxRetries: this.config.safety.aiRetries }) };
  }

  getSafetyConfig(): { maxRows: number; timeoutMs: number; maxRetries: number } {
    return {
      maxRows: this.config.safety?.maxRows ?? 5000,
      timeoutMs: this.config.safety?.timeoutMs ?? 30000,
      maxRetries: this.config.safety?.maxRetries ?? 2,
    };
  }

  // ── Mutation methods (for admin API) ───────────────────────────

  async addConnector(c: ConnectorConfig): Promise<ConnectorInfo> {
    if (this.instances.has(c.id)) {
      throw new Error(`Connector '${c.id}' already exists`);
    }
    await this.initConnector(c);
    this.config.connectors.push(c);
    this.rebuildRouter();
    const info = this.listConnectors().find((i) => i.id === c.id);
    if (!info) throw new Error(`Failed to locate newly added connector '${c.id}'`);
    return info;
  }

  async updateConnector(id: string, updates: Partial<Omit<ConnectorConfig, "id">>): Promise<ConnectorInfo> {
    const existing = this.connectorConfigs.get(id);
    if (!existing) throw new Error(`Connector '${id}' not found`);

    // Close old instance
    const oldInstance = this.instances.get(id);
    if (oldInstance) {
      try { await oldInstance.close(); } catch (err) { console.error(`[${id}] close error:`, err instanceof Error ? err.message : err); }
    }

    // Merge config
    const updated: ConnectorConfig = { ...existing, ...updates, id };
    this.instances.delete(id);
    this.connectorConfigs.delete(id);
    this.cache.invalidate(id);

    // Reinitialize
    await this.initConnector(updated);

    // Update in-memory config array
    const idx = this.config.connectors.findIndex((c) => c.id === id);
    if (idx !== -1) this.config.connectors[idx] = updated;

    this.rebuildRouter();
    const info = this.listConnectors().find((i) => i.id === id);
    if (!info) throw new Error(`Failed to locate updated connector '${id}'`);
    return info;
  }

  async removeConnector(id: string): Promise<void> {
    if (!this.instances.has(id)) throw new Error(`Connector '${id}' not found`);
    if (this.instances.size <= 1) throw new Error("Cannot remove the last connector");

    const instance = this.instances.get(id)!;
    try { await instance.close(); } catch (err) { console.error(`[${id}] close error:`, err instanceof Error ? err.message : err); }

    this.instances.delete(id);
    this.connectorConfigs.delete(id);
    this.cache.invalidate(id);
    this.config.connectors = this.config.connectors.filter((c) => c.id !== id);

    // Update default if we just removed it
    if (this.defaultId === id) {
      this.defaultId = Array.from(this.instances.keys())[0];
    }
    this.rebuildRouter();
  }

  async updateAIConfig(ai: AIConfig): Promise<void> {
    this.config.ai = ai;

    // Recreate all instances with new AI config
    for (const [id, instance] of this.instances) {
      try { await instance.close(); } catch (err) { console.error(`[${id}] close error:`, err instanceof Error ? err.message : err); }
    }
    this.instances.clear();

    for (const c of Array.from(this.connectorConfigs.values())) {
      const asksql = this.createInstance(c);
      // Reload from cache (don't re-discover)
      const cached = this.cache.load(c.id) as DiscoveredDatabase | null;
      if (cached) asksql.loadFromCache(cached);
      this.instances.set(c.id, asksql);
    }
    this.rebuildRouter();
  }

  // ── Internal ───────────────────────────────────────────────────

  private backgroundRefresh(connectorId: string): void {
    if (this.refreshing.has(connectorId)) return;
    this.refreshing.add(connectorId);

    const promise = this.refreshSchema(connectorId)
      .then((r) => {
        console.error(`[${connectorId}] background refresh complete (${r.tables} tables, ${r.columns} columns)`);
      })
      .catch((err) => {
        console.error(`[${connectorId}] background refresh failed:`, err instanceof Error ? err.message : err);
      })
      .finally(() => {
        this.refreshing.delete(connectorId);
        this.backgroundPromises.delete(promise);
      });
    this.backgroundPromises.add(promise);
  }

  async close(): Promise<void> {
    // Wait for in-flight background refreshes
    if (this.backgroundPromises.size > 0) {
      console.error(`[connector-manager] Waiting for ${this.backgroundPromises.size} background refresh(es) to complete...`);
      await Promise.allSettled(Array.from(this.backgroundPromises));
    }
    for (const [id, asksql] of this.instances) {
      try {
        await asksql.close();
      } catch (err) {
        console.error(`[${id}] close error:`, err instanceof Error ? err.message : err);
      }
    }
  }
}
