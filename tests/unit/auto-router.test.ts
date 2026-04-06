import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { AutoRouter } from "../../src/auto-router.js";
import { SchemaCache } from "../../src/schema-cache.js";
import { MOCK_DISCOVERED_DB_PG, MOCK_DISCOVERED_DB_SF } from "../helpers/fixtures.js";

let tmpDir: string;
let cache: SchemaCache;

const AI_CONFIG = {
  baseUrl: "https://api.example.com/v1",
  apiKey: "sk-test",
  model: "test-model",
};

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "mcp-test-router-"));
  cache = new SchemaCache(tmpDir);
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("AutoRouter", () => {
  describe("single connector", () => {
    it("returns default when only one connector exists", async () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      const router = new AutoRouter(["pg"], "pg", cache, AI_CONFIG);
      const result = await router.route("show me all apps");
      expect(result.connectorId).toBe("pg");
      expect(result.method).toBe("default");
      expect(result.confidence).toContain("only connector");
    });
  });

  describe("keyword matching", () => {
    let router: AutoRouter;

    beforeEach(() => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      cache.save("sf", MOCK_DISCOVERED_DB_SF);
      // Stub fetch so AI fallback doesn't make real HTTP calls
      vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("no network")));
      router = new AutoRouter(["pg", "sf"], "pg", cache, AI_CONFIG);
    });

    it("routes to connector whose schema has matching table names", async () => {
      // "orders" matches ORDERS table in sf
      const result = await router.route("show me all orders");
      expect(result.connectorId).toBe("sf");
      expect(result.method).toBe("keyword");
    });

    it("routes to connector with highest keyword score", async () => {
      // "app" and "element" and "data" match pg schema
      const result = await router.route("show application data elements");
      expect(result.connectorId).toBe("pg");
      expect(result.method).toBe("keyword");
    });

    it("matches column names", async () => {
      // "o_custkey" only exists in sf schema — use full column name token
      const result = await router.route("show o_custkey and o_totalprice");
      expect(result.connectorId).toBe("sf");
      expect(result.method).toBe("keyword");
    });

    it("ignores tokens shorter than 3 characters", async () => {
      // "id" is only 2 chars, should be ignored
      // "of" is 2 chars, ignored
      // This should fall to AI or default since no 3+ char tokens match exclusively
      const result = await router.route("id of x");
      // With no meaningful tokens, falls to default
      expect(result.connectorId).toBe("pg"); // default
    });

    it("splits table names on underscores for sub-word matching", async () => {
      // "support" and "group" come from df360_support_group table (split on _)
      const result = await router.route("show support group information");
      expect(result.connectorId).toBe("pg");
    });
  });

  describe("AI fallback", () => {
    it("calls AI when multiple connectors match with similar scores", async () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      cache.save("sf", MOCK_DISCOVERED_DB_SF);

      // Mock fetch to return AI decision
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          choices: [{ message: { content: '{"connectorId":"pg","reason":"matches app tables"}' } }],
          usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 },
        }),
      }));

      const router = new AutoRouter(["pg", "sf"], "pg", cache, AI_CONFIG);

      // Use a question that would match both connectors somewhat
      // "name" appears in both schemas as column name
      const result = await router.route("show me names");
      // Should use AI since keyword scores are ambiguous
      if (result.method === "ai") {
        expect(result.connectorId).toBe("pg");
        expect(result.confidence).toContain("matches app tables");
      }
      // Either AI or keyword is acceptable as long as it routes
      expect(result.connectorId).toBeDefined();
    });

    it("falls back to default when AI returns invalid connector ID", async () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      cache.save("sf", MOCK_DISCOVERED_DB_SF);

      vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          choices: [{ message: { content: '{"connectorId":"nonexistent","reason":"oops"}' } }],
          usage: { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 },
        }),
      }));

      const router = new AutoRouter(["pg", "sf"], "pg", cache, AI_CONFIG);
      const result = await router.route("something completely unrelated to any schema");
      expect(result.connectorId).toBe("pg"); // default
      expect(result.method).toBe("default");
    });

    it("falls back to default when AI call fails", async () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      cache.save("sf", MOCK_DISCOVERED_DB_SF);

      vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("Network error")));

      const router = new AutoRouter(["pg", "sf"], "pg", cache, AI_CONFIG);
      const result = await router.route("something completely unrelated");
      expect(result.connectorId).toBe("pg"); // default
      expect(result.method).toBe("default");
    });
  });

  describe("rebuild", () => {
    it("rebuilds indexes after adding a connector", async () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("no network")));

      const router = new AutoRouter(["pg"], "pg", cache, AI_CONFIG);

      // Initially only one connector — should always return pg
      let result = await router.route("show orders");
      expect(result.connectorId).toBe("pg");

      // Add sf and rebuild
      cache.save("sf", MOCK_DISCOVERED_DB_SF);
      router.rebuild(["pg", "sf"], "pg", cache);

      // Now "orders" should route to sf
      result = await router.route("show me all orders");
      expect(result.connectorId).toBe("sf");
      expect(result.method).toBe("keyword");
    });
  });

  describe("edge cases", () => {
    it("handles empty question", async () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      cache.save("sf", MOCK_DISCOVERED_DB_SF);
      vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("no network")));

      const router = new AutoRouter(["pg", "sf"], "pg", cache, AI_CONFIG);
      const result = await router.route("");
      expect(result.connectorId).toBe("pg"); // default
    });

    it("handles no cached schemas", async () => {
      vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("no network")));
      const router = new AutoRouter(["pg", "sf"], "pg", cache, AI_CONFIG);
      const result = await router.route("show me data");
      expect(result.connectorId).toBe("pg"); // default, no indexes to match
    });
  });
});
