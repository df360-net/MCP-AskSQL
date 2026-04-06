import { Router } from "express";
import type { ConnectorManager } from "./connector-manager.js";
import type { ConnectorConfig } from "./config.js";
import type { ConfigStore } from "./config-store.js";
import type { QueryLogger } from "./query-logger.js";
import { AIClient, type AIConfig } from "./asksql/core/ai/client.js";

function maskApiKey(key: string): string {
  if (key.length <= 4) return "****";
  return "****" + key.slice(-4);
}

function maskConnectionString(cs: string): string {
  // Mask password in connection strings like proto://user:PASSWORD@host
  return cs.replace(/:([^/:@]+)@/, ":****@");
}

export function createApiRouter(
  manager: ConnectorManager,
  configStore: ConfigStore,
  logger: QueryLogger,
): Router {
  const router = Router();

  // ── Connectors ──────────────────────────────────────────────

  router.get("/connectors", (_req, res) => {
    res.json(manager.listConnectors());
  });

  router.get("/connectors/:id", (req, res) => {
    const config = manager.getConnectorConfig(req.params.id);
    if (!config) { res.status(404).json({ error: "Connector not found" }); return; }
    const schemaInfo = manager.getSchemaInfo(req.params.id);
    res.json({
      id: config.id,
      connectionString: maskConnectionString(config.connectionString),
      schemas: config.schemas,
      schemaPrefix: config.schemaPrefix,
      abbreviations: config.abbreviations,
      examples: config.examples,
      schemaInfo,
    });
  });

  router.post("/connectors", async (req, res) => {
    try {
      const c = req.body as ConnectorConfig;
      if (!c.id || !c.connectionString) {
        res.status(400).json({ error: "id and connectionString are required" }); return;
      }
      const info = await manager.addConnector(c);
      // Persist to config.json
      const fileConfig = configStore.read();
      fileConfig.connectors.push({
        id: c.id,
        connectionString: c.connectionString,
        schemas: c.schemas,
        abbreviations: c.abbreviations,
        examples: c.examples,
        schemaPrefix: c.schemaPrefix,
      });
      configStore.write(fileConfig);
      res.status(201).json(info);
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.put("/connectors/:id", async (req, res) => {
    try {
      const info = await manager.updateConnector(req.params.id, req.body);
      // Persist to config.json
      const fileConfig = configStore.read();
      const idx = fileConfig.connectors.findIndex((c) => c.id === req.params.id);
      if (idx !== -1) {
        fileConfig.connectors[idx] = { ...fileConfig.connectors[idx], ...req.body, id: req.params.id };
      }
      configStore.write(fileConfig);
      res.json(info);
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.delete("/connectors/:id", async (req, res) => {
    try {
      await manager.removeConnector(req.params.id);
      // Persist to config.json
      const fileConfig = configStore.read();
      fileConfig.connectors = fileConfig.connectors.filter((c) => c.id !== req.params.id);
      configStore.write(fileConfig);
      res.json({ ok: true });
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.post("/connectors/:id/health", async (req, res) => {
    try {
      const asksql = manager.get(req.params.id);
      const health = await asksql.healthCheck();
      res.json(health);
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.post("/connectors/:id/refresh-schema", async (req, res) => {
    try {
      const result = await manager.refreshSchema(req.params.id);
      res.json(result);
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.get("/connectors/:id/schema-info", (req, res) => {
    const info = manager.getSchemaInfo(req.params.id);
    if (!info) { res.status(404).json({ error: "No schema cache found" }); return; }
    res.json(info);
  });

  /** Full schema detail — returns the entire cached DiscoveredDatabase */
  router.get("/connectors/:id/schema-detail", (req, res) => {
    const data = manager.getSchemaDetail(req.params.id);
    if (!data) { res.status(404).json({ error: "No schema cache found" }); return; }
    res.json(data);
  });

  /** Reconstruct the AI system prompt for a given connector + question */
  router.post("/connectors/:id/prompt", (req, res) => {
    try {
      const asksql = manager.get(req.params.id);
      const question = (req.body as { question?: string }).question ?? "";
      const systemPrompt = asksql.getSystemPrompt();
      res.json({ systemPrompt, userMessage: question });
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  // ── AI Provider ─────────────────────────────────────────────

  router.get("/ai", (_req, res) => {
    const ai = manager.getAIConfig();
    res.json({
      baseUrl: ai.baseUrl,
      model: ai.model,
      maxTokens: ai.maxTokens,
      temperature: ai.temperature,
      timeoutMs: ai.timeoutMs,
      apiKeyMasked: maskApiKey(ai.apiKey),
    });
  });

  router.put("/ai", async (req, res) => {
    try {
      const updates = req.body as Partial<AIConfig>;
      const current = manager.getAIConfig();
      const newAi: AIConfig = {
        baseUrl: updates.baseUrl ?? current.baseUrl,
        apiKey: updates.apiKey ?? current.apiKey,
        model: updates.model ?? current.model,
        maxTokens: updates.maxTokens ?? current.maxTokens,
        temperature: updates.temperature ?? current.temperature,
        timeoutMs: updates.timeoutMs ?? current.timeoutMs,
      };
      await manager.updateAIConfig(newAi);
      // Persist to config.json
      configStore.updateAI({
        baseUrl: newAi.baseUrl,
        apiKey: newAi.apiKey,
        model: newAi.model,
        maxTokens: newAi.maxTokens,
        temperature: newAi.temperature,
        timeoutMs: newAi.timeoutMs,
      });
      res.json({ ok: true, apiKeyMasked: maskApiKey(newAi.apiKey) });
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  router.post("/ai/test", async (_req, res) => {
    try {
      const ai = manager.getAIConfig();
      const client = new AIClient(ai);
      const result = await client.call([{ role: "user", content: "Say OK" }], false, "health-check");
      res.json({ reachable: result.success, error: result.error });
    } catch (err) {
      res.json({ reachable: false, error: err instanceof Error ? err.message : String(err) });
    }
  });

  // ── Query Logs ──────────────────────────────────────────────

  router.get("/logs", (req, res) => {
    const filters = {
      connector: req.query.connector as string | undefined,
      status: req.query.status as "success" | "fail" | undefined,
      from: req.query.from as string | undefined,
      to: req.query.to as string | undefined,
      page: req.query.page ? parseInt(req.query.page as string, 10) : undefined,
      pageSize: req.query.pageSize ? parseInt(req.query.pageSize as string, 10) : undefined,
    };
    res.json(logger.query(filters));
  });

  router.get("/logs/stats", (_req, res) => {
    res.json(logger.stats());
  });

  router.delete("/logs/older-than/:days", (req, res) => {
    const days = parseInt(req.params.days, 10);
    if (isNaN(days) || days < 1) { res.status(400).json({ error: "Invalid days parameter" }); return; }
    const removed = logger.clearOlderThan(days);
    res.json({ ok: true, removed });
  });

  router.delete("/logs", (_req, res) => {
    logger.clear();
    res.json({ ok: true });
  });

  // ── Execute SQL (re-run from logs, no AI) ───────────────────

  router.post("/execute-sql", async (req, res) => {
    const { sql, connector } = req.body as { sql: string; connector?: string; maxRows?: number };
    if (!sql) { res.status(400).json({ error: "sql is required" }); return; }
    const maxRows = Math.min(Math.max(1, Number(req.body.maxRows) || 100), 10000);

    const start = Date.now();
    try {
      const asksql = manager.get(connector);
      const result = await asksql.executeSQL(sql, { maxRows });

      logger.log({
        tool: "execute_sql",
        connector: connector ?? "default",
        sql,
        success: true,
        executionTimeMs: Date.now() - start,
        rowCount: result.rowCount,
      });

      res.json(result);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.log({ tool: "execute_sql", connector: connector ?? "default", sql, success: false, error: msg, executionTimeMs: Date.now() - start });
      res.status(500).json({ error: msg });
    }
  });

  // ── NL Query (direct ask from UI) ──────────────────────────

  router.post("/ask", async (req, res) => {
    const { question, connector } = req.body as { question: string; connector?: string; maxRows?: number };
    if (!question) { res.status(400).json({ error: "question is required" }); return; }
    const maxRows = Math.min(Math.max(1, Number(req.body.maxRows) || 100), 10000);

    const start = Date.now();
    try {
      // Auto-route if no connector specified
      let resolvedConnector = connector;
      let routeInfo: { method: string; confidence: string } | undefined;
      if (!connector) {
        const route = await manager.routeQuestion(question);
        resolvedConnector = route.connectorId;
        routeInfo = { method: route.method, confidence: route.confidence };
      }

      const asksql = manager.get(resolvedConnector);
      const result = await asksql.ask(question, { maxRows });

      logger.log({
        tool: "ask",
        connector: resolvedConnector ?? "default",
        question,
        sql: result.sql ?? undefined,
        success: result.success,
        error: result.error,
        executionTimeMs: Date.now() - start,
        rowCount: result.rowCount,
      });

      res.json({
        success: result.success,
        sql: result.sql,
        explanation: result.explanation,
        rows: result.rows,
        rowCount: result.rowCount,
        executionTimeMs: result.executionTimeMs,
        error: result.error,
        ...(routeInfo && { routedTo: resolvedConnector, routeMethod: routeInfo.method, routeConfidence: routeInfo.confidence }),
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.log({ tool: "ask", connector: connector ?? "default", question, success: false, error: msg, executionTimeMs: Date.now() - start });
      res.status(500).json({ error: msg });
    }
  });

  return router;
}
