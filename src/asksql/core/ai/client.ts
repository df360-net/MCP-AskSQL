/**
 * AI Client — OpenAI-compatible HTTP client
 *
 * Works with DeepSeek, OpenAI, Claude (via proxy), Ollama, or any
 * endpoint that implements the /v1/chat/completions API.
 *
 * Features:
 * - Exponential-backoff retry on 429 / 5xx
 * - AbortController timeout
 * - JSON extraction from markdown fences
 * - Token usage tracking
 */

export interface AIConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
  maxTokens?: number;
  temperature?: number;
  timeoutMs?: number;
  headers?: Record<string, string>;
  /** Cost per 1M prompt tokens in USD (default: 0.07 for DeepSeek-chat) */
  promptPricePerMillion?: number;
  /** Cost per 1M completion tokens in USD (default: 0.14 for DeepSeek-chat) */
  completionPricePerMillion?: number;
}

export interface ChatMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string;
  tool_calls?: ToolCall[];
  tool_call_id?: string;
}

export interface ToolDefinition {
  type: "function";
  function: {
    name: string;
    description: string;
    parameters: Record<string, unknown>;
  };
}

export interface ToolCall {
  id: string;
  type: "function";
  function: {
    name: string;
    arguments: string; // JSON string
  };
}

export interface AICallWithToolsResult {
  content: string;
  toolCalls: ToolCall[];
  finishReason: string;
  tokenUsage: TokenUsage;
}

export interface AICallResult<T = string> {
  success: boolean;
  data?: T;
  rawResponse?: string;
  error?: string;
  tokenUsage?: TokenUsage;
}

export interface TokenUsage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  estimatedCost: number;
}

interface APIResponse {
  choices?: Array<{
    message?: {
      content?: string | null;
      tool_calls?: ToolCall[];
    };
    finish_reason?: string;
  }>;
  usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
}

export class AIClient {
  private config: Required<Pick<AIConfig, "baseUrl" | "apiKey" | "model">> &
    AIConfig;
  private promptPrice: number;
  private completionPrice: number;

  constructor(config: AIConfig) {
    this.config = {
      maxTokens: 4096,
      temperature: 0.3,
      timeoutMs: 30000,
      ...config,
    };
    this.promptPrice = config.promptPricePerMillion ?? 0.07;
    this.completionPrice = config.completionPricePerMillion ?? 0.14;
  }

  /**
   * Call the chat completions API.
   *
   * @param messages  Chat messages
   * @param parseJson If true, parse the response as JSON
   * @param feature   Label for cost tracking
   */
  async call<T = string>(
    messages: ChatMessage[],
    parseJson: boolean = false,
    feature: string = "generic",
  ): Promise<AICallResult<T>> {
    const maxRetries = 3;
    let lastError = "Unknown error";

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timer = setTimeout(
          () => controller.abort(),
          this.config.timeoutMs!,
        );

        const res = await fetch(
          `${this.config.baseUrl}/chat/completions`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${this.config.apiKey}`,
              ...this.config.headers,
            },
            body: JSON.stringify({
              model: this.config.model,
              messages,
              temperature: this.config.temperature,
              max_tokens: this.config.maxTokens,
            }),
            signal: controller.signal,
          },
        );

        clearTimeout(timer);

        if (!res.ok) {
          const body = await res.text();
          const retryable = res.status >= 500 || res.status === 429;
          if (retryable && attempt < maxRetries - 1) {
            await this.backoff(attempt);
            continue;
          }
          lastError = `AI API ${res.status}: ${body.slice(0, 200)}`;
          break;
        }

        const json = (await res.json()) as APIResponse;
        const content = json.choices?.[0]?.message?.content ?? "";
        const usage = json.usage ?? {
          prompt_tokens: 0,
          completion_tokens: 0,
          total_tokens: 0,
        };

        const tokenUsage: TokenUsage = {
          promptTokens: usage.prompt_tokens,
          completionTokens: usage.completion_tokens,
          totalTokens: usage.total_tokens,
          estimatedCost: this.estimateCost(
            usage.prompt_tokens,
            usage.completion_tokens,
          ),
        };

        if (!parseJson) {
          return { success: true, rawResponse: content, tokenUsage };
        }

        const jsonStr = this.extractJson(content);
        try {
          const parsed = JSON.parse(jsonStr) as T;
          return { success: true, data: parsed, tokenUsage };
        } catch {
          return {
            success: false,
            rawResponse: content,
            error: "Failed to parse JSON from AI response",
            tokenUsage,
          };
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes("abort")) {
          lastError = `AI API timeout (${this.config.timeoutMs}ms)`;
          break;
        }
        lastError = msg;
        if (attempt < maxRetries - 1) {
          await this.backoff(attempt);
        }
      }
    }

    return { success: false, error: lastError };
  }

  /**
   * Call the chat completions API with tool (function-calling) support.
   * Used by the agent loop. Does NOT parse JSON — returns raw content + tool calls.
   */
  async callWithTools(
    messages: ChatMessage[],
    tools?: ToolDefinition[],
  ): Promise<AICallWithToolsResult> {
    const maxRetries = 3;
    let lastError = "Unknown error";

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.config.timeoutMs!);

        const body: Record<string, unknown> = {
          model: this.config.model,
          messages,
          temperature: this.config.temperature,
          max_tokens: this.config.maxTokens,
        };
        if (tools && tools.length > 0) {
          body.tools = tools;
        }

        const res = await fetch(`${this.config.baseUrl}/chat/completions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${this.config.apiKey}`,
            ...this.config.headers,
          },
          body: JSON.stringify(body),
          signal: controller.signal,
        });

        clearTimeout(timer);

        if (!res.ok) {
          const text = await res.text();
          const retryable = res.status >= 500 || res.status === 429;
          if (retryable && attempt < maxRetries - 1) {
            await this.backoff(attempt);
            continue;
          }
          lastError = `AI API ${res.status}: ${text.slice(0, 200)}`;
          break;
        }

        const json = (await res.json()) as APIResponse;
        const choice = json.choices?.[0];
        const content = choice?.message?.content ?? "";
        const toolCalls = choice?.message?.tool_calls ?? [];
        const finishReason = choice?.finish_reason ?? "stop";
        const usage = json.usage ?? { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };

        return {
          content,
          toolCalls,
          finishReason,
          tokenUsage: {
            promptTokens: usage.prompt_tokens,
            completionTokens: usage.completion_tokens,
            totalTokens: usage.total_tokens,
            estimatedCost: this.estimateCost(usage.prompt_tokens, usage.completion_tokens),
          },
        };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes("abort")) {
          lastError = `AI API timeout (${this.config.timeoutMs}ms)`;
          break;
        }
        lastError = msg;
        if (attempt < maxRetries - 1) {
          await this.backoff(attempt);
        }
      }
    }

    // Return an error result with empty tool calls
    return {
      content: `Error: ${lastError}`,
      toolCalls: [],
      finishReason: "error",
      tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0, estimatedCost: 0 },
    };
  }

  /** Strip markdown code fences if present, extract JSON */
  private extractJson(content: string): string {
    // 1. Try fenced code block
    const fenced = content.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fenced) return fenced[1].trim();

    // 2. Try raw JSON object
    const objMatch = content.match(/\{[\s\S]*\}/);
    if (objMatch) {
      const candidate = objMatch[0];
      try { JSON.parse(candidate); return candidate; } catch {}
      // 3. Try fixing truncated JSON — close open braces/brackets
      const repaired = this.repairJson(candidate);
      try { JSON.parse(repaired); return repaired; } catch {}
      return candidate;
    }

    // 4. Try raw JSON array
    const arrMatch = content.match(/\[[\s\S]*\]/);
    if (arrMatch) return arrMatch[0];

    return content.trim();
  }

  /** Attempt to repair truncated JSON by closing open braces/brackets */
  private repairJson(json: string): string {
    let openBraces = 0;
    let openBrackets = 0;
    let inString = false;
    let escape = false;

    for (const ch of json) {
      if (escape) { escape = false; continue; }
      if (ch === "\\") { escape = true; continue; }
      if (ch === '"') { inString = !inString; continue; }
      if (inString) continue;
      if (ch === "{") openBraces++;
      else if (ch === "}") openBraces--;
      else if (ch === "[") openBrackets++;
      else if (ch === "]") openBrackets--;
    }

    let repaired = json;
    // Close any trailing incomplete string
    if (inString) repaired += '"';
    // Close open brackets/braces
    while (openBrackets > 0) { repaired += "]"; openBrackets--; }
    while (openBraces > 0) { repaired += "}"; openBraces--; }
    return repaired;
  }

  /** Exponential backoff: 1s, 2s, 4s */
  private async backoff(attempt: number): Promise<void> {
    const ms = 1000 * Math.pow(2, attempt);
    await new Promise((r) => setTimeout(r, ms));
  }

  /** Estimate cost based on configured pricing (default: DeepSeek-chat) */
  private estimateCost(promptTokens: number, completionTokens: number): number {
    return (promptTokens * this.promptPrice + completionTokens * this.completionPrice) / 1_000_000;
  }
}
