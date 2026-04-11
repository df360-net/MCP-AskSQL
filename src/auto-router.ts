/**
 * Auto-Router — determines which connector to use for a question.
 *
 * Strategy:
 * 1. Keyword matching: tokenize the question, match against table/column names
 *    in each connector's cached schema. If exactly one connector matches → use it.
 * 2. AI fallback: if zero or multiple connectors match, ask the AI to decide.
 */

import { AIClient, type AIConfig } from "./asksql/core/ai/client.js";
import type { SchemaCache } from "./schema-cache.js";

interface SchemaIndex {
  connectorId: string;
  /** All table and column names, lowercased, for fast matching */
  tokens: Set<string>;
  /** Short summary for AI prompt */
  summary: string;
}

export interface RouteResult {
  connectorId: string;
  method: "keyword" | "ai" | "default";
  confidence: string;
  candidates?: Array<{ id: string; score: number }>;
}

export interface RoutingConfig {
  minTokenLength?: number;
  winnerScoreMultiplier?: number;
}

export class AutoRouter {
  private indexes: SchemaIndex[] = [];
  private ai: AIClient;
  private connectorIds: string[];
  private defaultId: string;
  private minTokenLength: number;
  private winnerScoreMultiplier: number;

  constructor(connectorIds: string[], defaultId: string, cache: SchemaCache, aiConfig: AIConfig, routingConfig?: RoutingConfig) {
    this.connectorIds = connectorIds;
    this.defaultId = defaultId;
    this.ai = new AIClient(aiConfig);
    this.minTokenLength = routingConfig?.minTokenLength ?? 3;
    this.winnerScoreMultiplier = routingConfig?.winnerScoreMultiplier ?? 2;
    this.buildIndexes(cache);
  }

  private buildIndexes(cache: SchemaCache): void {
    this.indexes = [];

    for (const id of this.connectorIds) {
      const data = cache.load(id) as {
        databaseName?: string;
        databaseType?: string;
        schemas?: Array<{
          schemaName: string;
          tables: Array<{
            tableName: string;
            tableComment?: string;
            columns: Array<{ columnName: string }>;
          }>;
        }>;
      } | null;

      if (!data?.schemas) continue;

      const tokens = new Set<string>();
      const tableNames: string[] = [];
      let totalColumns = 0;

      for (const s of data.schemas) {
        tokens.add(s.schemaName.toLowerCase());
        for (const t of s.tables) {
          const tName = t.tableName.toLowerCase();
          tokens.add(tName);
          tableNames.push(`${s.schemaName}.${t.tableName}`);
          // Also add individual words from table name (e.g., "df360_app" → "df360", "app")
          for (const word of tName.split(/[_\s]+/)) {
            if (word.length >= this.minTokenLength) tokens.add(word);
          }
          // Add column names
          for (const c of t.columns) {
            tokens.add(c.columnName.toLowerCase());
          }
          totalColumns += t.columns.length;
          // Add comment words
          if (t.tableComment) {
            for (const word of t.tableComment.toLowerCase().split(/\W+/)) {
              if (word.length >= this.minTokenLength) tokens.add(word);
            }
          }
        }
      }

      // Short summary for AI prompt
      const sampleTables = tableNames.slice(0, 10).join(", ");
      const more = tableNames.length > 10 ? ` ... and ${tableNames.length - 10} more` : "";
      const summary = `${id} (${data.databaseType ?? "unknown"}, ${data.databaseName ?? id}): ${tableNames.length} tables, ${totalColumns} columns. Tables: ${sampleTables}${more}`;

      this.indexes.push({ connectorId: id, tokens, summary });
    }
  }

  /** Rebuild indexes (call after schema refresh or connector add/remove) */
  rebuild(connectorIds: string[], defaultId: string, cache: SchemaCache): void {
    this.connectorIds = connectorIds;
    this.defaultId = defaultId;
    this.buildIndexes(cache);
  }

  /**
   * Route a question to the best connector.
   * Returns the connector ID and the method used.
   */
  async route(question: string): Promise<RouteResult> {
    // Only 1 connector? No need to route.
    if (this.indexes.length <= 1) {
      return { connectorId: this.defaultId, method: "default", confidence: "only connector" };
    }

    // Step 1: Keyword matching
    const scores = this.keywordMatch(question);

    // Exactly one winner with a meaningful score
    if (scores.length === 1 && scores[0].score > 0) {
      return {
        connectorId: scores[0].id,
        method: "keyword",
        confidence: `${scores[0].score} keyword matches`,
        candidates: scores,
      };
    }

    // Clear winner (top score > 2x second place)
    if (scores.length >= 2 && scores[0].score > 0 && scores[0].score > scores[1].score * this.winnerScoreMultiplier) {
      return {
        connectorId: scores[0].id,
        method: "keyword",
        confidence: `${scores[0].score} vs ${scores[1].score} keyword matches`,
        candidates: scores,
      };
    }

    // Step 2: AI fallback
    if (scores.length >= 2 && scores[0].score > 0) {
      // Multiple connectors matched — ask AI to decide
      const aiResult = await this.aiRoute(question, scores.filter((s) => s.score > 0));
      if (aiResult) return aiResult;
    }

    // No matches at all — try AI with all connectors
    if (scores.every((s) => s.score === 0)) {
      const aiResult = await this.aiRoute(question, scores);
      if (aiResult) return aiResult;
    }

    // Fallback to default
    return { connectorId: this.defaultId, method: "default", confidence: "no match found", candidates: scores };
  }

  private keywordMatch(question: string): Array<{ id: string; score: number }> {
    // Tokenize the question
    const questionTokens = question.toLowerCase()
      .split(/\W+/)
      .filter((w) => w.length >= this.minTokenLength);

    const scores: Array<{ id: string; score: number }> = [];

    for (const index of this.indexes) {
      let score = 0;
      for (const token of questionTokens) {
        if (index.tokens.has(token)) score++;
      }
      scores.push({ id: index.connectorId, score });
    }

    // Sort by score descending
    scores.sort((a, b) => b.score - a.score);
    return scores;
  }

  private async aiRoute(
    question: string,
    candidates: Array<{ id: string; score: number }>,
  ): Promise<RouteResult | null> {
    const connectorSummaries = this.indexes
      .filter((idx) => candidates.some((c) => c.id === idx.connectorId))
      .map((idx) => idx.summary)
      .join("\n");

    const messages = [
      {
        role: "system" as const,
        content: `You are a database routing assistant. Given a user's question and a list of available database connectors with their schemas, determine which connector the question is about.\n\nAvailable connectors:\n${connectorSummaries}\n\nRespond with JSON: { "connectorId": "the_id", "reason": "brief explanation" }`,
      },
      {
        role: "user" as const,
        content: question,
      },
    ];

    try {
      const result = await this.ai.call<{ connectorId: string; reason: string }>(messages, true, "auto-route");
      if (result.success && result.data?.connectorId) {
        // Validate the AI's choice
        if (this.connectorIds.includes(result.data.connectorId)) {
          return {
            connectorId: result.data.connectorId,
            method: "ai",
            confidence: result.data.reason,
            candidates,
          };
        }
      }
    } catch (err) {
      console.error(`[auto-route] AI routing failed:`, err instanceof Error ? err.message : err);
    }

    return null;
  }
}
