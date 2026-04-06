import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { QueryLogger } from "../../src/query-logger.js";
import { MOCK_LOG_ENTRIES } from "../helpers/fixtures.js";

let tmpDir: string;
let logger: QueryLogger;

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "mcp-test-log-"));
  logger = new QueryLogger(tmpDir);
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
});

describe("QueryLogger", () => {
  describe("log", () => {
    it("appends a JSONL line with auto-generated id and timestamp", () => {
      logger.log(MOCK_LOG_ENTRIES[0]);
      const result = logger.query();
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].id).toBeDefined();
      expect(result.rows[0].timestamp).toBeDefined();
      expect(result.rows[0].tool).toBe("ask");
      expect(result.rows[0].connector).toBe("pg_test");
    });

    it("writes multiple entries as separate lines", () => {
      for (const entry of MOCK_LOG_ENTRIES) {
        logger.log(entry);
      }
      const result = logger.query();
      expect(result.total).toBe(MOCK_LOG_ENTRIES.length);
    });
  });

  describe("query", () => {
    beforeEach(() => {
      for (const entry of MOCK_LOG_ENTRIES) {
        logger.log(entry);
      }
    });

    it("returns all entries newest-first", () => {
      const result = logger.query();
      expect(result.total).toBe(5);
      // Newest first — last logged entry should be first
      for (let i = 1; i < result.rows.length; i++) {
        expect(new Date(result.rows[i - 1].timestamp).getTime())
          .toBeGreaterThanOrEqual(new Date(result.rows[i].timestamp).getTime());
      }
    });

    it("filters by connector", () => {
      const result = logger.query({ connector: "pg_test" });
      expect(result.total).toBe(3); // 2 asks + 1 execute_sql
      expect(result.rows.every((r) => r.connector === "pg_test")).toBe(true);
    });

    it("filters by status=success", () => {
      const result = logger.query({ status: "success" });
      expect(result.total).toBe(4);
      expect(result.rows.every((r) => r.success)).toBe(true);
    });

    it("filters by status=fail", () => {
      const result = logger.query({ status: "fail" });
      expect(result.total).toBe(1);
      expect(result.rows[0].error).toBe("AI failed");
    });

    it("paginates correctly", () => {
      const page0 = logger.query({ pageSize: 2, page: 0 });
      expect(page0.rows).toHaveLength(2);
      expect(page0.total).toBe(5);

      const page1 = logger.query({ pageSize: 2, page: 1 });
      expect(page1.rows).toHaveLength(2);

      const page2 = logger.query({ pageSize: 2, page: 2 });
      expect(page2.rows).toHaveLength(1);
    });

    it("returns empty when no logs exist", () => {
      const fresh = new QueryLogger(mkdtempSync(join(tmpdir(), "mcp-test-empty-")));
      const result = fresh.query();
      expect(result.rows).toEqual([]);
      expect(result.total).toBe(0);
    });
  });

  describe("stats", () => {
    beforeEach(() => {
      for (const entry of MOCK_LOG_ENTRIES) {
        logger.log(entry);
      }
    });

    it("calculates totalQueries, successful, failed", () => {
      const stats = logger.stats();
      expect(stats.totalQueries).toBe(5);
      expect(stats.successful).toBe(4);
      expect(stats.failed).toBe(1);
    });

    it("calculates avgExecutionTimeMs", () => {
      const stats = logger.stats();
      const expectedAvg = Math.round((150 + 300 + 5 + 1000 + 200) / 5);
      expect(stats.avgExecutionTimeMs).toBe(expectedAvg);
    });

    it("groups by connector and tool", () => {
      const stats = logger.stats();
      expect(stats.byConnector.pg_test).toBe(3);
      expect(stats.byConnector.sf_test).toBe(2);
      expect(stats.byTool.ask).toBe(3);
      expect(stats.byTool.execute_sql).toBe(1);
      expect(stats.byTool.health_check).toBe(1);
    });

    it("returns zeros for empty log", () => {
      const fresh = new QueryLogger(mkdtempSync(join(tmpdir(), "mcp-test-empty-")));
      const stats = fresh.stats();
      expect(stats.totalQueries).toBe(0);
      expect(stats.successful).toBe(0);
      expect(stats.avgExecutionTimeMs).toBe(0);
    });
  });

  describe("clear", () => {
    it("empties the log file", () => {
      logger.log(MOCK_LOG_ENTRIES[0]);
      logger.clear();
      const result = logger.query();
      expect(result.rows).toEqual([]);
      expect(result.total).toBe(0);
    });
  });

  describe("rotation", () => {
    it("rotates when file exceeds maxFileSize", () => {
      // Create logger with tiny maxFileSize
      const rotLogger = new QueryLogger(tmpDir, 100);
      // Write enough entries to exceed 100 bytes
      for (let i = 0; i < 5; i++) {
        rotLogger.log(MOCK_LOG_ENTRIES[0]);
      }
      // After rotation, the archive file should exist
      const files = require("node:fs").readdirSync(tmpDir) as string[];
      const archives = files.filter((f: string) => f.startsWith("query-log-") && f !== "query-log.jsonl");
      expect(archives.length).toBeGreaterThanOrEqual(1);
    });
  });
});
