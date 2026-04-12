import { Router } from "express";
import type { ConnectorManager } from "./connector-manager.js";
import type { ConnectorConfig, AskAgentAppConfig } from "./config.js";
import type { ConfigStore } from "./config-store.js";
import type { QueryLogger } from "./query-logger.js";
import type { WorkflowStore } from "./workflow-store.js";
import type { SchedulerEngine } from "./scheduler/engine.js";
import type { SchedulerStore } from "./scheduler/store.js";
import { AIClient, type AIConfig } from "./asksql/core/ai/client.js";
import { askAgentLoop } from "./ask/agent-loop.js";
import type { AgentStreamEvent } from "./ask/types.js";

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
  askConfig?: AskAgentAppConfig,
  workflowStore?: WorkflowStore,
  schedulerEngine?: SchedulerEngine,
  schedulerStore?: SchedulerStore,
): Router {
  const askAgentEnabled = askConfig?.enabled ?? false;
  const askAgentMaxTurns = askConfig?.maxTurns ?? 10;
  const askToolOutputMaxChars = askConfig?.toolOutputMaxChars;
  const safety = manager.getSafetyConfig();
  const router = Router();

  // ── Settings (expose safety config to UI) ───────────────────

  router.get("/settings", (_req, res) => {
    const ai = manager.getAIConfig();
    res.json({
      maxRows: configStore.read().safety?.maxRows ?? 5000,
      askEnabled: askAgentEnabled,
      askMaxTurns: askAgentMaxTurns,
    });
  });

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
      console.error(`[admin] Connector created: ${c.id}`);
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
      console.error(`[admin] Connector updated: ${req.params.id}`);
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
      console.error(`[admin] Connector deleted: ${req.params.id}`);
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
      console.error(`[admin] AI config updated (model: ${newAi.model})`);
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
    if (sql.length > 50000) { res.status(400).json({ error: "SQL query too long (max 50000 chars)" }); return; }
    const maxRows = Math.min(Math.max(1, Number(req.body.maxRows) || safety.maxRows), safety.maxRows);

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
    const maxRows = Math.min(Math.max(1, Number(req.body.maxRows) || safety.maxRows), safety.maxRows);

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

      // ── Agent loop branch (2-layer intelligence) with SSE streaming ──
      if (askAgentEnabled) {
        // Set up SSE headers
        res.writeHead(200, {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
        });

        // Track client disconnect
        let clientDisconnected = false;
        req.on("close", () => { clientDisconnected = true; });

        // Send route info as first event
        if (routeInfo) {
          res.write(`data: ${JSON.stringify({ type: "route", routedTo: resolvedConnector, routeMethod: routeInfo.method, routeConfidence: routeInfo.confidence })}\n\n`);
        }

        const agentResult = await askAgentLoop({
          question,
          connectorId: resolvedConnector,
          manager,
          aiConfig: manager.getAIConfig(),
          maxTurns: askAgentMaxTurns,
          maxRows,
          toolOutputMaxChars: askToolOutputMaxChars,
          onTurn: (event: AgentStreamEvent) => {
            res.write(`data: ${JSON.stringify(event)}\n\n`);
          },
        });

        // Send final complete result
        res.write(`data: ${JSON.stringify({
          type: "result",
          success: agentResult.success,
          answer: agentResult.answer,
          explanation: agentResult.explanation,
          turns: agentResult.turns,
          toolCalls: agentResult.toolCalls,
          tokenUsage: agentResult.tokenUsage,
          executionTimeMs: Date.now() - start,
          ...(routeInfo && { routedTo: resolvedConnector, routeMethod: routeInfo.method, routeConfidence: routeInfo.confidence }),
        })}\n\n`);

        res.end();

        logger.log({
          tool: "ask",
          connector: resolvedConnector ?? "default",
          question,
          success: agentResult.success,
          executionTimeMs: Date.now() - start,
          explanation: agentResult.explanation || undefined,
          answer: agentResult.answer || undefined,
          toolCalls: agentResult.toolCalls.length > 0 ? agentResult.toolCalls : undefined,
        });
        console.error(`[ask-agent] REST /api/ask: ${agentResult.turns} turns, ${agentResult.toolCalls.length} tool calls, ${agentResult.tokenUsage.totalTokens} tokens`);
        return;
      }

      // ── Original single-shot path ──
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
      if (res.headersSent) {
        // Already streaming SSE — send error event and close
        res.write(`data: ${JSON.stringify({ type: "error", error: msg })}\n\n`);
        res.end();
      } else {
        res.status(500).json({ error: msg });
      }
    }
  });

  // ── Render Markdown as PDF ──────────────────────────────────

  router.post("/render-pdf", async (req, res) => {
    const { markdown } = req.body as { markdown?: string };
    if (!markdown) { res.status(400).json({ error: "markdown field required" }); return; }
    try {
      const { mdToPdf } = await import("md-to-pdf");
      const pdf = await mdToPdf(
        { content: markdown },
        {
          launch_options: { headless: true, args: ["--no-sandbox"] },
          pdf_options: { format: "A4", margin: { top: "20mm", bottom: "20mm", left: "15mm", right: "15mm" } },
          css: `
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; font-size: 13px; color: #1a1a2e; line-height: 1.6; }
            h1 { color: #1a1a2e; border-bottom: 2px solid #1a1a2e; padding-bottom: 6px; }
            h2 { color: #16213e; margin-top: 24px; }
            h3 { color: #0f3460; }
            table { border-collapse: collapse; width: 100%; margin: 12px 0; }
            th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; font-size: 12px; }
            th { background: #16213e; color: white; }
            tr:nth-child(even) { background: #f5f5f5; }
            code { background: #f0f0f0; padding: 2px 4px; border-radius: 3px; font-size: 12px; }
            pre { background: #f0f0f0; padding: 12px; border-radius: 6px; overflow-x: auto; }
            strong { color: #0f3460; }
          `,
        },
      );
      if (pdf.content) {
        res.setHeader("Content-Type", "application/pdf");
        res.setHeader("Content-Disposition", "attachment; filename=report.pdf");
        res.end(pdf.content);
      } else {
        res.status(500).json({ error: "PDF generation returned no content" });
      }
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  // ── Workflows ───────────────────────────────────────────

  if (workflowStore) {
    router.get("/workflows", (_req, res) => {
      res.json(workflowStore.list());
    });

    router.get("/workflows/:id", (req, res) => {
      const wf = workflowStore.get(req.params.id);
      if (!wf) { res.status(404).json({ error: "Workflow not found" }); return; }
      res.json(wf);
    });

    router.post("/workflows", (req, res) => {
      try {
        const { name, description, connector, originalQuestion, aiReasoning, steps } = req.body;
        if (!name || !connector || !steps?.length) {
          res.status(400).json({ error: "name, connector, and steps are required" }); return;
        }
        const wf = workflowStore.create({ name, description, connector, originalQuestion: originalQuestion ?? "", aiReasoning, steps });
        res.status(201).json(wf);
      } catch (err) {
        res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
      }
    });

    router.put("/workflows/:id", (req, res) => {
      const wf = workflowStore.update(req.params.id, req.body);
      if (!wf) { res.status(404).json({ error: "Workflow not found" }); return; }
      res.json(wf);
    });

    router.delete("/workflows/:id", (req, res) => {
      const ok = workflowStore.delete(req.params.id);
      if (!ok) { res.status(404).json({ error: "Workflow not found" }); return; }
      res.json({ ok: true });
    });

    router.post("/workflows/:id/run", async (req, res) => {
      const wf = workflowStore.get(req.params.id);
      if (!wf) { res.status(404).json({ error: "Workflow not found" }); return; }

      const summarize = req.body.summarize !== false;
      const maxRows = Math.min(Math.max(1, Number(req.body.maxRows) || safety.maxRows), safety.maxRows);
      const start = Date.now();

      // SSE streaming
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });

      // Track client disconnect
      let clientDisconnected = false;
      req.on("close", () => { clientDisconnected = true; });

      const stepResults: Array<{
        order: number; title: string; sql: string;
        success: boolean; error?: string;
        rows: Record<string, unknown>[]; rowCount: number; executionTimeMs: number;
      }> = [];

      try {
        const asksql = manager.get(wf.connector);

        for (const step of wf.steps) {
          if (clientDisconnected) break;
          // Notify: step starting
          res.write(`data: ${JSON.stringify({ type: "step-start", order: step.order, title: step.title })}\n\n`);

          const stepStart = Date.now();
          try {
            const result = await asksql.executeSQL(step.sql, { maxRows });
            const sr = {
              order: step.order, title: step.title, sql: step.sql,
              success: true, rows: result.rows, rowCount: result.rowCount,
              executionTimeMs: Date.now() - stepStart,
            };
            stepResults.push(sr);
            res.write(`data: ${JSON.stringify({ type: "step-done", ...sr })}\n\n`);
          } catch (err) {
            const sr = {
              order: step.order, title: step.title, sql: step.sql,
              success: false, error: err instanceof Error ? err.message : String(err),
              rows: [] as Record<string, unknown>[], rowCount: 0, executionTimeMs: Date.now() - stepStart,
            };
            stepResults.push(sr);
            res.write(`data: ${JSON.stringify({ type: "step-done", ...sr })}\n\n`);
          }
        }

        let summary: string | undefined;
        if (summarize) {
          res.write(`data: ${JSON.stringify({ type: "summarizing" })}\n\n`);

          const datasetsText = stepResults.map((sr) => {
            if (!sr.success) return `## Dataset ${sr.order}: ${sr.title}\nSQL execution failed: ${sr.error}`;
            if (sr.rows.length === 0) return `## Dataset ${sr.order}: ${sr.title}\n0 row(s)\n\nNo data returned.`;
            const header = Object.keys(sr.rows[0]).join(" | ");
            const rows = sr.rows.slice(0, 100).map((r) => Object.values(r).map((v) => v === null ? "NULL" : String(v)).join(" | ")).join("\n");
            return `## Dataset ${sr.order}: ${sr.title}\n${sr.rowCount} row(s)\n\n${header}\n${rows}`;
          }).join("\n\n---\n\n");

          const reasoningContext = wf.aiReasoning ? `\n\nThe original analytical reasoning was:\n${wf.aiReasoning}` : "";

          // Get schema context for richer analysis
          let schemaContext = "";
          try {
            const asksqlForSchema = manager.get(wf.connector);
            schemaContext = `\nThis is the schema of the database:\n${asksqlForSchema.getSchemaContext()}\n\n`;
          } catch { /* ignore if schema unavailable */ }

          const prompt = `You are a senior data analyst.${schemaContext}The user asked: "${wf.originalQuestion}"${reasoningContext}

You have been given the results of ${stepResults.length} SQL queries. Analyze the data and produce a comprehensive markdown report with insights, comparisons, and key findings.

${datasetsText}

SUMMARIZE a well-structured markdown report.`;

          const ai = new AIClient(manager.getAIConfig());
          console.error(`[workflow-run] Sending summarization prompt (${prompt.length} chars) to AI...`);
          const aiResult = await ai.call([{ role: "user", content: prompt }], false, "workflow-summarize");
          if (aiResult.success) {
            summary = aiResult.rawResponse;
            console.error(`[workflow-run] AI summarization complete (${summary?.length ?? 0} chars)`);
          } else {
            console.error(`[workflow-run] AI summarization failed: ${aiResult.error}`);
            summary = `**AI Summarization Failed**\n\n${aiResult.error ?? "Unknown error"}`;
          }
        }

        res.write(`data: ${JSON.stringify({
          type: "result",
          workflowId: wf.id,
          workflowName: wf.name,
          connector: wf.connector,
          executionTimeMs: Date.now() - start,
          steps: stepResults,
          summary,
        })}\n\n`);

        res.end();
      } catch (err) {
        res.write(`data: ${JSON.stringify({ type: "error", error: err instanceof Error ? err.message : String(err) })}\n\n`);
        res.end();
      }
    });
  }

  // ── Scheduler ───────────────────────────────────────

  if (schedulerStore && schedulerEngine) {
    router.get("/scheduler/jobs", (_req, res) => {
      const jobs = schedulerStore.listJobs();
      const jobsWithLastRun = jobs.map((j) => ({
        ...j,
        lastRun: schedulerStore.getLatestRun(j.id) ?? null,
      }));
      res.json(jobsWithLastRun);
    });

    router.post("/scheduler/jobs", (req, res) => {
      try {
        const { workflowId, name, scheduleType, intervalSeconds, dailyRunTime, timeoutSeconds, emailRecipients } = req.body;
        if (!workflowId || !scheduleType) {
          res.status(400).json({ error: "workflowId and scheduleType are required" }); return;
        }

        // Validate email recipients
        if (emailRecipients && Array.isArray(emailRecipients)) {
          const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
          const invalid = emailRecipients.filter((e: string) => !emailRegex.test(e));
          if (invalid.length > 0) {
            res.status(400).json({ error: `Invalid email address(es): ${invalid.join(", ")}` }); return;
          }
        }

        // Validate dailyRunTime format
        if (scheduleType === "daily" && dailyRunTime) {
          const timeParts = dailyRunTime.split(":");
          if (timeParts.length !== 2) { res.status(400).json({ error: "dailyRunTime must be HH:MM format" }); return; }
          const [h, m] = timeParts.map(Number);
          if (isNaN(h) || isNaN(m) || h < 0 || h > 23 || m < 0 || m > 59) {
            res.status(400).json({ error: "Invalid time. Hours must be 0-23, minutes 0-59" }); return;
          }
        }

        // Calculate initial nextRunAt
        let nextRunAt: string;
        if (scheduleType === "daily" && dailyRunTime) {
          const [hours, minutes] = dailyRunTime.split(":").map(Number);
          const target = new Date();
          target.setHours(hours, minutes, 0, 0);
          if (target.getTime() <= Date.now()) target.setDate(target.getDate() + 1);
          nextRunAt = target.toISOString();
        } else {
          nextRunAt = new Date(Date.now() + (intervalSeconds ?? 3600) * 1000).toISOString();
        }

        const job = schedulerStore.createJob({
          workflowId,
          name: name ?? "Scheduled Workflow",
          scheduleType,
          intervalSeconds: scheduleType === "interval" ? (intervalSeconds ?? 3600) : undefined,
          dailyRunTime: scheduleType === "daily" ? dailyRunTime : undefined,
          timeoutSeconds: timeoutSeconds ?? 300,
          emailRecipients: emailRecipients ?? [],
          isEnabled: true,
          nextRunAt,
        });
        res.status(201).json(job);
      } catch (err) {
        res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
      }
    });

    router.put("/scheduler/jobs/:id", (req, res) => {
      const job = schedulerStore.updateJob(req.params.id, req.body);
      if (!job) { res.status(404).json({ error: "Scheduled job not found" }); return; }
      res.json(job);
    });

    router.delete("/scheduler/jobs/:id", (req, res) => {
      const ok = schedulerStore.deleteJob(req.params.id);
      if (!ok) { res.status(404).json({ error: "Scheduled job not found" }); return; }
      res.json({ ok: true });
    });

    router.post("/scheduler/jobs/:id/trigger", async (req, res) => {
      const runId = await schedulerEngine.triggerJob(req.params.id);
      if (!runId) { res.status(404).json({ error: "Job or workflow not found" }); return; }
      res.json({ ok: true, runId, message: "Job triggered" });
    });

    router.get("/scheduler/jobs/:id/runs", (req, res) => {
      const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : 20;
      const runs = schedulerStore.listRuns(req.params.id, limit);
      res.json(runs);
    });
  }

  return router;
}
