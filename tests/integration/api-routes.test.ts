import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import express from "express";
import request from "supertest";
import { createApiRouter } from "../../src/api-routes.js";
import {
  createMockConnectorManager,
  createMockConfigStore,
  createMockQueryLogger,
} from "../helpers/fixtures.js";

let app: express.Express;
let mockManager: ReturnType<typeof createMockConnectorManager>;
let mockConfigStore: ReturnType<typeof createMockConfigStore>;
let mockLogger: ReturnType<typeof createMockQueryLogger>;

beforeEach(() => {
  mockManager = createMockConnectorManager();
  mockConfigStore = createMockConfigStore();
  mockLogger = createMockQueryLogger();

  app = express();
  app.use(express.json());
  app.use("/api", createApiRouter(mockManager as any, mockConfigStore as any, mockLogger as any));
});

describe("API Routes", () => {
  // ── Connectors ──────────────────────────────────────────────────

  describe("GET /api/connectors", () => {
    it("returns list of connectors", async () => {
      const res = await request(app).get("/api/connectors");
      expect(res.status).toBe(200);
      expect(res.body).toHaveLength(2);
      expect(res.body[0].id).toBe("pg_test");
      expect(res.body[1].id).toBe("sf_test");
    });
  });

  describe("GET /api/connectors/:id", () => {
    it("returns connector detail with masked connection string", async () => {
      const res = await request(app).get("/api/connectors/pg_test");
      expect(res.status).toBe(200);
      expect(res.body.id).toBe("pg_test");
      expect(res.body.connectionString).toContain("****");
      expect(res.body.connectionString).not.toContain("secret");
    });

    it("returns 404 for unknown connector", async () => {
      mockManager.getConnectorConfig.mockReturnValue(undefined);
      const res = await request(app).get("/api/connectors/nope");
      expect(res.status).toBe(404);
    });
  });

  describe("POST /api/connectors", () => {
    it("creates a new connector", async () => {
      const res = await request(app)
        .post("/api/connectors")
        .send({ id: "new_conn", connectionString: "mysql://x@y/z" });
      expect(res.status).toBe(201);
      expect(mockManager.addConnector).toHaveBeenCalled();
      expect(mockConfigStore.write).toHaveBeenCalled();
    });

    it("returns 400 when id is missing", async () => {
      const res = await request(app)
        .post("/api/connectors")
        .send({ connectionString: "mysql://x@y/z" });
      expect(res.status).toBe(400);
    });

    it("returns 400 when connectionString is missing", async () => {
      const res = await request(app)
        .post("/api/connectors")
        .send({ id: "new_conn" });
      expect(res.status).toBe(400);
    });
  });

  describe("PUT /api/connectors/:id", () => {
    it("updates connector", async () => {
      const res = await request(app)
        .put("/api/connectors/pg_test")
        .send({ schemas: ["public", "custom"] });
      expect(res.status).toBe(200);
      expect(mockManager.updateConnector).toHaveBeenCalledWith("pg_test", { schemas: ["public", "custom"] });
    });
  });

  describe("DELETE /api/connectors/:id", () => {
    it("removes connector", async () => {
      const res = await request(app).delete("/api/connectors/sf_test");
      expect(res.status).toBe(200);
      expect(mockManager.removeConnector).toHaveBeenCalledWith("sf_test");
    });

    it("returns 400 when removing last connector", async () => {
      mockManager.removeConnector.mockRejectedValue(new Error("Cannot remove the last connector"));
      const res = await request(app).delete("/api/connectors/pg_test");
      expect(res.status).toBe(400);
      expect(res.body.error).toContain("last connector");
    });
  });

  describe("POST /api/connectors/:id/health", () => {
    it("returns health check result", async () => {
      const res = await request(app).post("/api/connectors/pg_test/health");
      expect(res.status).toBe(200);
      expect(res.body.database.connected).toBe(true);
    });

    it("returns 400 for unknown connector", async () => {
      mockManager.get.mockImplementation(() => { throw new Error("Unknown connector"); });
      const res = await request(app).post("/api/connectors/nope/health");
      expect(res.status).toBe(400);
    });
  });

  describe("POST /api/connectors/:id/refresh-schema", () => {
    it("triggers schema refresh", async () => {
      const res = await request(app).post("/api/connectors/pg_test/refresh-schema");
      expect(res.status).toBe(200);
      expect(res.body.tables).toBe(5);
      expect(res.body.columns).toBe(20);
    });
  });

  describe("GET /api/connectors/:id/schema-info", () => {
    it("returns schema metadata", async () => {
      const res = await request(app).get("/api/connectors/pg_test/schema-info");
      expect(res.status).toBe(200);
      expect(res.body.tables).toBe(3);
    });

    it("returns 404 when no cache exists", async () => {
      mockManager.getSchemaInfo.mockReturnValue(null);
      const res = await request(app).get("/api/connectors/pg_test/schema-info");
      expect(res.status).toBe(404);
    });
  });

  describe("GET /api/connectors/:id/schema-detail", () => {
    it("returns full cached schema", async () => {
      const res = await request(app).get("/api/connectors/pg_test/schema-detail");
      expect(res.status).toBe(200);
      expect(res.body.databaseName).toBe("df360_claude");
    });

    it("returns 404 when no cache exists", async () => {
      mockManager.getSchemaDetail.mockReturnValue(null);
      const res = await request(app).get("/api/connectors/nope/schema-detail");
      expect(res.status).toBe(404);
    });
  });

  // ── AI Provider ─────────────────────────────────────────────────

  describe("GET /api/ai", () => {
    it("returns AI config with masked API key", async () => {
      const res = await request(app).get("/api/ai");
      expect(res.status).toBe(200);
      expect(res.body.model).toBe("test-model");
      expect(res.body.apiKeyMasked).toBe("****7890");
      expect(res.body.apiKey).toBeUndefined(); // raw key should not leak
    });
  });

  describe("PUT /api/ai", () => {
    it("updates AI config", async () => {
      const res = await request(app)
        .put("/api/ai")
        .send({ model: "new-model" });
      expect(res.status).toBe(200);
      expect(mockManager.updateAIConfig).toHaveBeenCalled();
      expect(mockConfigStore.updateAI).toHaveBeenCalled();
    });
  });

  describe("POST /api/ai/test", () => {
    it("returns reachable status", async () => {
      // Mock global fetch for AI test
      global.fetch = jest.fn<any>().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          choices: [{ message: { content: "OK" } }],
          usage: { prompt_tokens: 5, completion_tokens: 1, total_tokens: 6 },
        }),
      }) as any;

      const res = await request(app).post("/api/ai/test");
      expect(res.status).toBe(200);
      expect(res.body.reachable).toBe(true);

      delete (global as any).fetch;
    });

    it("returns reachable: false when AI fails", async () => {
      global.fetch = jest.fn<any>().mockRejectedValue(new Error("Network error")) as any;

      const res = await request(app).post("/api/ai/test");
      expect(res.status).toBe(200);
      expect(res.body.reachable).toBe(false);

      delete (global as any).fetch;
    });
  });

  // ── Query Logs ──────────────────────────────────────────────────

  describe("GET /api/logs", () => {
    it("returns paginated logs", async () => {
      mockLogger.query.mockReturnValue({ rows: [{ id: "1", tool: "ask" }], total: 1 });
      const res = await request(app).get("/api/logs?page=0&pageSize=10");
      expect(res.status).toBe(200);
      expect(res.body.total).toBe(1);
    });

    it("passes filter parameters to logger", async () => {
      await request(app).get("/api/logs?connector=pg_test&status=success&page=1&pageSize=5");
      expect(mockLogger.query).toHaveBeenCalledWith(
        expect.objectContaining({
          connector: "pg_test",
          status: "success",
          page: 1,
          pageSize: 5,
        }),
      );
    });
  });

  describe("GET /api/logs/stats", () => {
    it("returns log statistics", async () => {
      const res = await request(app).get("/api/logs/stats");
      expect(res.status).toBe(200);
      expect(res.body.totalQueries).toBe(10);
      expect(res.body.successful).toBe(8);
    });
  });

  describe("DELETE /api/logs", () => {
    it("clears all logs", async () => {
      const res = await request(app).delete("/api/logs");
      expect(res.status).toBe(200);
      expect(mockLogger.clear).toHaveBeenCalled();
    });
  });

  // ── Execute SQL ─────────────────────────────────────────────────

  describe("POST /api/execute-sql", () => {
    it("executes SQL and returns result", async () => {
      const res = await request(app)
        .post("/api/execute-sql")
        .send({ sql: "SELECT 1", connector: "pg_test" });
      expect(res.status).toBe(200);
      expect(res.body.rowCount).toBe(1);
      expect(mockLogger.log).toHaveBeenCalled();
    });

    it("returns 400 when sql is missing", async () => {
      const res = await request(app)
        .post("/api/execute-sql")
        .send({});
      expect(res.status).toBe(400);
    });

    it("returns 500 when execution fails", async () => {
      mockManager._mockAsksql.executeSQL.mockRejectedValueOnce(new Error("Query failed"));
      const res = await request(app)
        .post("/api/execute-sql")
        .send({ sql: "SELECT bad" });
      expect(res.status).toBe(500);
      expect(mockLogger.log).toHaveBeenCalledWith(
        expect.objectContaining({ success: false }),
      );
    });
  });

  // ── NL Ask ──────────────────────────────────────────────────────

  describe("POST /api/ask", () => {
    it("processes NL question and returns result", async () => {
      const res = await request(app)
        .post("/api/ask")
        .send({ question: "show all apps", connector: "pg_test" });
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
      expect(res.body.sql).toBeDefined();
    });

    it("auto-routes when no connector specified", async () => {
      const res = await request(app)
        .post("/api/ask")
        .send({ question: "show all apps" });
      expect(res.status).toBe(200);
      expect(mockManager.routeQuestion).toHaveBeenCalledWith("show all apps");
      expect(res.body.routedTo).toBeDefined();
      expect(res.body.routeMethod).toBeDefined();
    });

    it("returns 400 when question is missing", async () => {
      const res = await request(app)
        .post("/api/ask")
        .send({});
      expect(res.status).toBe(400);
    });

    it("returns 500 when ask fails", async () => {
      mockManager._mockAsksql.ask.mockRejectedValueOnce(new Error("AI error"));
      const res = await request(app)
        .post("/api/ask")
        .send({ question: "fail" });
      expect(res.status).toBe(500);
    });

    it("logs the query", async () => {
      await request(app)
        .post("/api/ask")
        .send({ question: "show all apps", connector: "pg_test" });
      expect(mockLogger.log).toHaveBeenCalledWith(
        expect.objectContaining({ tool: "ask", connector: "pg_test" }),
      );
    });
  });
});
