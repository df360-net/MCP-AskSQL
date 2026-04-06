import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { loadConfig } from "../../src/config.js";
import { VALID_FILE_CONFIG } from "../helpers/fixtures.js";

let tmpDir: string;

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "mcp-test-loadcfg-"));
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
  delete process.env.AI_API_KEY;
});

function writeConfig(data: unknown): string {
  const p = join(tmpDir, "config.json");
  writeFileSync(p, JSON.stringify(data), "utf-8");
  return p;
}

describe("loadConfig", () => {
  it("loads a valid config with all fields", () => {
    const p = writeConfig(VALID_FILE_CONFIG);
    const config = loadConfig(p);
    expect(config.connectors).toHaveLength(1);
    expect(config.connectors[0].id).toBe("pg_test");
    expect(config.ai.baseUrl).toBe("https://api.example.com/v1");
    expect(config.ai.model).toBe("test-model");
    expect(config.ai.maxTokens).toBe(4096);
  });

  it("defaults schemas to ['public'] when omitted", () => {
    const data = {
      ...VALID_FILE_CONFIG,
      connectors: [{ id: "x", connectionString: "postgres://x@y/z" }],
    };
    const p = writeConfig(data);
    const config = loadConfig(p);
    expect(config.connectors[0].schemas).toEqual(["public"]);
  });

  it("defaults schemaCacheTtlHours to 24", () => {
    const data = { ...VALID_FILE_CONFIG };
    delete (data as any).schemaCacheTtlHours;
    const p = writeConfig(data);
    const config = loadConfig(p);
    expect(config.schemaCacheTtlHours).toBe(24);
  });

  it("reads apiKey from env var when missing in file", () => {
    const data = { ...VALID_FILE_CONFIG, ai: { ...VALID_FILE_CONFIG.ai } };
    delete (data.ai as any).apiKey;
    process.env.AI_API_KEY = "env-key-12345";
    const p = writeConfig(data);
    const config = loadConfig(p);
    expect(config.ai.apiKey).toBe("env-key-12345");
  });

  it("throws when file not found", () => {
    expect(() => loadConfig(join(tmpDir, "nope.json"))).toThrow("Config file not found");
  });

  it("throws when JSON is malformed", () => {
    const p = join(tmpDir, "bad.json");
    writeFileSync(p, "not json{{{", "utf-8");
    expect(() => loadConfig(p)).toThrow("Failed to parse");
  });

  it("throws when connectors array is empty", () => {
    const data = { ...VALID_FILE_CONFIG, connectors: [] };
    const p = writeConfig(data);
    expect(() => loadConfig(p)).toThrow("at least one connector");
  });

  it("throws when connector missing id", () => {
    const data = { ...VALID_FILE_CONFIG, connectors: [{ connectionString: "pg://x" }] };
    const p = writeConfig(data);
    expect(() => loadConfig(p)).toThrow("must have an 'id'");
  });

  it("throws when connector missing connectionString", () => {
    const data = { ...VALID_FILE_CONFIG, connectors: [{ id: "x" }] };
    const p = writeConfig(data);
    expect(() => loadConfig(p)).toThrow("connectionString");
  });

  it("throws when ai.baseUrl missing", () => {
    const data = { ...VALID_FILE_CONFIG, ai: { model: "x", apiKey: "y" } };
    const p = writeConfig(data);
    expect(() => loadConfig(p)).toThrow("baseUrl");
  });

  it("throws when ai.model missing", () => {
    const data = { ...VALID_FILE_CONFIG, ai: { baseUrl: "http://x", apiKey: "y" } };
    const p = writeConfig(data);
    expect(() => loadConfig(p)).toThrow("model");
  });

  it("throws when no API key anywhere", () => {
    const data = { ...VALID_FILE_CONFIG, ai: { ...VALID_FILE_CONFIG.ai } };
    delete (data.ai as any).apiKey;
    const p = writeConfig(data);
    expect(() => loadConfig(p)).toThrow("API key");
  });

  it("includes optional ai fields only when defined", () => {
    const data = {
      ...VALID_FILE_CONFIG,
      ai: { baseUrl: "http://x", apiKey: "y", model: "m" },
    };
    const p = writeConfig(data);
    const config = loadConfig(p);
    expect(config.ai.maxTokens).toBeUndefined();
    expect(config.ai.temperature).toBeUndefined();
  });
});
