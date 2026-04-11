import { describe, it, expect, beforeEach, afterEach } from "@jest/globals";
import { mkdtempSync, rmSync, writeFileSync, utimesSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { SchemaCache } from "../../src/schema-cache.js";
import { MOCK_DISCOVERED_DB_PG } from "../helpers/fixtures.js";

let tmpDir: string;
let cache: SchemaCache;

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "mcp-test-cache-"));
  cache = new SchemaCache(tmpDir);
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
});

describe("SchemaCache", () => {
  it("creates data directory if it doesn't exist", () => {
    const nested = join(tmpDir, "sub", "dir");
    const c = new SchemaCache(nested);
    // Should not throw
    expect(c.has("x")).toBe(false);
  });

  describe("save + load", () => {
    it("round-trips data correctly", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      const loaded = cache.load("pg");
      expect(loaded).toEqual(MOCK_DISCOVERED_DB_PG);
    });
  });

  describe("has", () => {
    it("returns false for missing connector", () => {
      expect(cache.has("nonexistent")).toBe(false);
    });

    it("returns true after save", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      expect(cache.has("pg")).toBe(true);
    });
  });

  describe("load", () => {
    it("returns null for missing connector", () => {
      expect(cache.load("nonexistent")).toBeNull();
    });
  });

  describe("ageHours", () => {
    it("returns null for missing connector", () => {
      expect(cache.ageHours("nonexistent")).toBeNull();
    });

    it("returns a positive number after save", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      const age = cache.ageHours("pg");
      expect(age).not.toBeNull();
      expect(age!).toBeGreaterThanOrEqual(-0.01); // allow tiny clock skew
      expect(age!).toBeLessThan(1); // just saved, should be < 1 hour
    });
  });

  describe("isStale", () => {
    it("returns false when ttlHours=0 (disabled)", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      expect(cache.isStale("pg", 0)).toBe(false);
    });

    it("returns false for fresh cache", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      expect(cache.isStale("pg", 24)).toBe(false);
    });

    it("returns false for missing cache (not stale, just missing)", () => {
      expect(cache.isStale("nonexistent", 24)).toBe(false);
    });

    it("returns true when cache exceeds TTL", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      // Backdate the file's mtime to 25 hours ago
      const filePath = join(tmpDir, "schema-pg.json");
      const pastTime = new Date(Date.now() - 25 * 60 * 60 * 1000);
      utimesSync(filePath, pastTime, pastTime);
      expect(cache.isStale("pg", 24)).toBe(true);
    });
  });

  describe("invalidate", () => {
    it("removes cache file", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      expect(cache.invalidate("pg")).toBe(true);
      expect(cache.has("pg")).toBe(false);
    });

    it("returns false for missing file", () => {
      expect(cache.invalidate("nonexistent")).toBe(false);
    });
  });

  describe("listCached", () => {
    it("returns all cached connector IDs", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      cache.save("sf", { schemas: [] });
      const list = cache.listCached();
      expect(list).toContain("pg");
      expect(list).toContain("sf");
      expect(list).toHaveLength(2);
    });

    it("returns empty for no cached data", () => {
      expect(cache.listCached()).toEqual([]);
    });
  });

  describe("getSchemaInfo", () => {
    it("returns table/column counts from cached data", () => {
      cache.save("pg", MOCK_DISCOVERED_DB_PG);
      const info = cache.getSchemaInfo("pg");
      expect(info).not.toBeNull();
      expect(info!.tables).toBe(3);
      expect(info!.columns).toBe(8); // 3 + 3 + 2
      expect(info!.tableNames).toContain("df360.df360_app");
      expect(info!.tableNames).toContain("df360.df360_data_element");
      expect(info!.tableNames).toContain("df360.df360_support_group");
    });

    it("returns null for missing connector", () => {
      expect(cache.getSchemaInfo("nonexistent")).toBeNull();
    });
  });
});
