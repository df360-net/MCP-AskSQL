import { describe, it, expect, beforeEach, afterEach, jest } from "@jest/globals";
import { AIClient } from "../../src/asksql/core/ai/client.js";

const CONFIG = {
  baseUrl: "https://api.example.com/v1",
  apiKey: "sk-test",
  model: "test-model",
  maxTokens: 1024,
  temperature: 0.3,
  timeoutMs: 5000,
};

function mockFetchOk(content: string, usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number }) {
  return jest.fn<any>().mockResolvedValue({
    ok: true,
    json: () => Promise.resolve({
      choices: [{ message: { content } }],
      usage: usage ?? { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
    }),
  });
}

beforeEach(() => {
  jest.useFakeTimers();
});

afterEach(() => {
  jest.restoreAllMocks();
  jest.useRealTimers();
  delete (global as any).fetch;
});

describe("AIClient", () => {
  describe("call — raw text mode", () => {
    it("returns rawResponse from API", async () => {
      global.fetch = mockFetchOk("Hello world") as any;
      const client = new AIClient(CONFIG);
      const result = await client.call([{ role: "user", content: "Hi" }], false);
      expect(result.success).toBe(true);
      expect(result.rawResponse).toBe("Hello world");
    });

    it("includes token usage", async () => {
      global.fetch = mockFetchOk("OK", { prompt_tokens: 100, completion_tokens: 50, total_tokens: 150 }) as any;
      const client = new AIClient(CONFIG);
      const result = await client.call([{ role: "user", content: "Hi" }]);
      expect(result.tokenUsage).toBeDefined();
      expect(result.tokenUsage!.promptTokens).toBe(100);
      expect(result.tokenUsage!.completionTokens).toBe(50);
      expect(result.tokenUsage!.totalTokens).toBe(150);
      expect(result.tokenUsage!.estimatedCost).toBeGreaterThan(0);
    });

    it("does not retry on 400", async () => {
      const fetchMock = jest.fn<any>().mockResolvedValue({
        ok: false,
        status: 400,
        text: () => Promise.resolve("Bad request"),
      });
      global.fetch = fetchMock as any;
      const client = new AIClient(CONFIG);
      const result = await client.call([{ role: "user", content: "Hi" }]);
      expect(result.success).toBe(false);
      expect(result.error).toContain("400");
      expect(fetchMock).toHaveBeenCalledTimes(1); // No retries
    });
  });

  describe("call — JSON parse mode", () => {
    it("parses clean JSON response", async () => {
      global.fetch = mockFetchOk('{"sql":"SELECT 1","explanation":"test"}') as any;
      const client = new AIClient(CONFIG);
      const result = await client.call<{ sql: string; explanation: string }>(
        [{ role: "user", content: "generate sql" }],
        true,
      );
      expect(result.success).toBe(true);
      expect(result.data?.sql).toBe("SELECT 1");
      expect(result.data?.explanation).toBe("test");
    });

    it("extracts JSON from markdown code fence", async () => {
      global.fetch = mockFetchOk('Here is the result:\n```json\n{"sql":"SELECT 1"}\n```\nDone.') as any;
      const client = new AIClient(CONFIG);
      const result = await client.call<{ sql: string }>([{ role: "user", content: "q" }], true);
      expect(result.success).toBe(true);
      expect(result.data?.sql).toBe("SELECT 1");
    });

    it("extracts JSON object from mixed text", async () => {
      global.fetch = mockFetchOk('Sure! {"sql":"SELECT 1"} Hope that helps.') as any;
      const client = new AIClient(CONFIG);
      const result = await client.call<{ sql: string }>([{ role: "user", content: "q" }], true);
      expect(result.success).toBe(true);
      expect(result.data?.sql).toBe("SELECT 1");
    });

    it("returns error when JSON cannot be parsed", async () => {
      global.fetch = mockFetchOk("Just plain text, no JSON here") as any;
      const client = new AIClient(CONFIG);
      const result = await client.call<unknown>([{ role: "user", content: "q" }], true);
      expect(result.success).toBe(false);
      expect(result.error).toContain("parse JSON");
    });
  });

  describe("estimateCost", () => {
    it("uses default pricing", async () => {
      global.fetch = mockFetchOk("OK", {
        prompt_tokens: 1_000_000,
        completion_tokens: 1_000_000,
        total_tokens: 2_000_000,
      }) as any;
      const client = new AIClient(CONFIG);
      const result = await client.call([{ role: "user", content: "Hi" }]);
      // Default: 0.07/M prompt + 0.14/M completion = 0.07 + 0.14 = 0.21
      expect(result.tokenUsage!.estimatedCost).toBeCloseTo(0.21, 2);
    });

    it("uses custom pricing when provided", async () => {
      global.fetch = mockFetchOk("OK", {
        prompt_tokens: 1_000_000,
        completion_tokens: 1_000_000,
        total_tokens: 2_000_000,
      }) as any;
      const client = new AIClient({ ...CONFIG, promptPricePerMillion: 1.0, completionPricePerMillion: 2.0 });
      const result = await client.call([{ role: "user", content: "Hi" }]);
      // 1.0 + 2.0 = 3.0
      expect(result.tokenUsage!.estimatedCost).toBeCloseTo(3.0, 2);
    });
  });
});
