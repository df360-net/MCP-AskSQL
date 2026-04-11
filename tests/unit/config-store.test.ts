import { describe, it, expect, beforeEach, afterEach } from "@jest/globals";
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { ConfigStore } from "../../src/config-store.js";
import { VALID_FILE_CONFIG } from "../helpers/fixtures.js";

let tmpDir: string;
let configPath: string;
let store: ConfigStore;

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "mcp-test-config-"));
  configPath = join(tmpDir, "config.json");
  writeFileSync(configPath, JSON.stringify(VALID_FILE_CONFIG, null, 2), "utf-8");
  store = new ConfigStore(configPath);
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
});

describe("ConfigStore", () => {
  describe("read", () => {
    it("reads and parses config file", () => {
      const config = store.read();
      expect(config.connectors).toHaveLength(1);
      expect(config.connectors[0].id).toBe("pg_test");
      expect(config.ai.baseUrl).toBe("https://api.example.com/v1");
    });

    it("throws on missing file", () => {
      const bad = new ConfigStore(join(tmpDir, "nope.json"));
      expect(() => bad.read()).toThrow();
    });
  });

  describe("write", () => {
    it("atomically writes config", () => {
      const modified = { ...VALID_FILE_CONFIG, schemaCacheTtlHours: 48 };
      store.write(modified);
      const raw = JSON.parse(readFileSync(configPath, "utf-8"));
      expect(raw.schemaCacheTtlHours).toBe(48);
    });

    it("round-trips config data correctly", () => {
      const original = store.read();
      store.write(original);
      const reread = store.read();
      expect(reread).toEqual(original);
    });
  });

  describe("updateConnectors", () => {
    it("replaces the connectors array and preserves other fields", () => {
      const newConnectors = [
        { id: "new_pg", connectionString: "postgres://x@y/z" },
      ];
      store.updateConnectors(newConnectors);
      const config = store.read();
      expect(config.connectors).toHaveLength(1);
      expect(config.connectors[0].id).toBe("new_pg");
      // AI config preserved
      expect(config.ai.baseUrl).toBe("https://api.example.com/v1");
    });
  });

  describe("updateAI", () => {
    it("merges partial AI config into existing", () => {
      store.updateAI({ model: "new-model" });
      const config = store.read();
      expect(config.ai.model).toBe("new-model");
      expect(config.ai.baseUrl).toBe("https://api.example.com/v1"); // preserved
    });

    it("removes undefined fields after merge", () => {
      store.updateAI({ temperature: undefined });
      const config = store.read();
      // temperature should be removed since it was explicitly set to undefined
      expect("temperature" in config.ai).toBe(false);
    });
  });

  describe("getFilePath", () => {
    it("returns the config file path", () => {
      expect(store.getFilePath()).toBe(configPath);
    });
  });
});
