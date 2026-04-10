#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import express from "express";
import { registerTools } from "./tools.js";
import { loadConfig, type AskAgentAppConfig } from "./config.js";
import { ConfigStore } from "./config-store.js";
import { ConnectorManager } from "./connector-manager.js";
import { QueryLogger } from "./query-logger.js";
import { createApiRouter } from "./api-routes.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Shared: create MCP server + register tools
function createServer(manager: ConnectorManager, logger: QueryLogger, askConfig?: AskAgentAppConfig): McpServer {
  const server = new McpServer({
    name: "mcp-asksql",
    version: "1.0.0",
  });
  registerTools(server, manager, logger, askConfig);
  if (askConfig?.enabled) {
    console.error(`[ask-agent] Agent loop enabled (maxTurns: ${askConfig.maxTurns ?? 10})`);
  }
  return server;
}

// --- stdio transport ---
async function startStdio(manager: ConnectorManager, logger: QueryLogger, askConfig?: AskAgentAppConfig) {
  const server = createServer(manager, logger, askConfig);
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("mcp-asksql running on stdio");
}

// --- HTTP transport (stateful — session per client) ---
async function startHttp(manager: ConnectorManager, logger: QueryLogger, configStore: ConfigStore, port: number, askConfig?: AskAgentAppConfig) {
  const app = express();
  app.use(express.json());

  // ── REST API for admin UI ──
  app.use("/api", createApiRouter(manager, configStore, logger, askConfig));

  // ── MCP protocol (stateful sessions) ──
  const sessions = new Map<string, StreamableHTTPServerTransport>();

  app.post("/mcp", async (req, res) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;

    if (!sessionId) {
      if (!isInitializeRequest(req.body)) {
        res.status(400).json({
          jsonrpc: "2.0",
          error: { code: -32600, message: "First request must be an initialize request" },
          id: null,
        });
        return;
      }

      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
      });

      const server = createServer(manager, logger, askConfig);
      await server.connect(transport);

      transport.onclose = () => {
        const sid = transport.sessionId;
        if (sid) sessions.delete(sid);
      };

      await transport.handleRequest(req, res, req.body);

      if (transport.sessionId) {
        sessions.set(transport.sessionId, transport);
      }
      return;
    }

    const transport = sessions.get(sessionId);
    if (!transport) {
      res.status(404).json({
        jsonrpc: "2.0",
        error: { code: -32000, message: "Session not found. Send an initialize request first." },
        id: null,
      });
      return;
    }

    await transport.handleRequest(req, res, req.body);
  });

  app.get("/mcp", async (req, res) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    if (!sessionId) { res.status(400).json({ error: "Missing Mcp-Session-Id header" }); return; }
    const transport = sessions.get(sessionId);
    if (!transport) { res.status(404).json({ error: "Session not found" }); return; }
    await transport.handleRequest(req, res);
  });

  app.delete("/mcp", async (req, res) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    if (!sessionId) { res.status(400).json({ error: "Missing Mcp-Session-Id header" }); return; }
    const transport = sessions.get(sessionId);
    if (!transport) { res.status(404).json({ error: "Session not found" }); return; }
    await transport.handleRequest(req, res);
    sessions.delete(sessionId);
  });

  // ── Health check ──
  app.get("/health", (_req, res) => {
    res.json({
      ok: true,
      server: "mcp-asksql",
      connectors: manager.listConnectors().length,
      activeSessions: sessions.size,
    });
  });

  // ── Serve built UI (production) ──
  const uiDist = resolve(__dirname, "..", "dist", "ui");
  if (existsSync(uiDist)) {
    app.use(express.static(uiDist));
    app.get("/{*path}", (_req, res) => {
      res.sendFile(resolve(uiDist, "index.html"));
    });
  }

  const httpServer = app.listen(port, () => {
    console.log(`mcp-asksql HTTP server listening on port ${port}`);
    console.log(`MCP endpoint: http://localhost:${port}/mcp`);
    console.log(`Admin API:    http://localhost:${port}/api`);
    console.log(`Health check: http://localhost:${port}/health`);
    if (existsSync(uiDist)) {
      console.log(`Admin UI:     http://localhost:${port}/`);
    }
  });

  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.on(signal, () => {
      console.error(`Received ${signal}, shutting down...`);
      // Stop accepting new connections, then close resources
      httpServer.close(async () => {
        await manager.close();
        console.error("Shutdown complete.");
        process.exit(0);
      });
      // Force exit after 10s if graceful shutdown stalls
      setTimeout(() => { console.error("Forced shutdown after timeout."); process.exit(1); }, 10000).unref();
    });
  }
}

// --- Main ---
async function main() {
  const args = process.argv.slice(2);

  const configIndex = args.indexOf("--config");
  let configPath: string | undefined;
  if (configIndex !== -1) {
    if (configIndex + 1 >= args.length) {
      console.error("--config requires a file path argument.");
      process.exit(1);
    }
    configPath = args[configIndex + 1];
  }

  const config = loadConfig(configPath);
  const configStore = new ConfigStore(configPath);
  const manager = new ConnectorManager(config);
  const logger = new QueryLogger(config.dataDir);

  await manager.init();
  console.error(`Loaded ${manager.listConnectors().length} connector(s)`);

  if (args.includes("--stdio")) {
    await startStdio(manager, logger, config.ask);
  } else {
    const portIndex = args.indexOf("--port");
    let port = 8080;
    if (portIndex !== -1) {
      const parsed = parseInt(args[portIndex + 1], 10);
      if (isNaN(parsed) || parsed < 1 || parsed > 65535) {
        console.error(`Invalid port: ${args[portIndex + 1]}. Using default 8080.`);
      } else {
        port = parsed;
      }
    }
    await startHttp(manager, logger, configStore, port, config.ask);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
