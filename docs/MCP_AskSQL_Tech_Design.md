# MCP-AskSQL Technical Design

## 1. Overview

**mcp-asksql** is a Model Context Protocol (MCP) server that wraps the [AskSQL](https://github.com/asksql-dev/asksql) natural-language-to-SQL library. It enables AI agents and MCP clients to query any of 10 supported databases using natural language, with automatic connector routing, schema caching, and an admin UI.

**Key capabilities:**
- 6 MCP tools: `ask`, `generate_sql`, `execute_sql`, `list_connectors`, `health_check`, `refresh_schema`
- 10 database connectors: PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery, Redshift, Databricks, Dremio, Teradata
- Auto-routing across multiple databases (keyword matching + AI fallback)
- File-based schema cache with configurable TTL and background refresh
- Admin UI (React + Tailwind) for connector management, AI config, and query logs
- Dual transport: stdio (local) and HTTP (cloud/remote)
- No external database dependency — everything is file-based

---

## 2. Architecture

```
┌─────────────────────────────────┐
│ MCP Client (BridgeFlow, etc.)   │
│  HTTP or stdio JSON-RPC 2.0     │
└────────────────────┬────────────┘
                     │
┌────────────────────▼──────────────────────┐
│         mcp-asksql Server                 │
│                                           │
│  ┌──────────────────────────────────┐     │
│  │ MCP Server (SDK v1.10.1)         │     │
│  │  6 tools with Zod schemas        │     │
│  └──────────────────────────────────┘     │
│                                           │
│  ┌──────────────────────────────────┐     │
│  │ ConnectorManager                 │     │
│  │  Map<id, AskSQL> instances       │     │
│  │  SchemaCache (file-based)        │     │
│  │  AutoRouter (keyword + AI)       │     │
│  │  Runtime mutations (CRUD)        │     │
│  └──────────────────────────────────┘     │
│                                           │
│  ┌──────────────────────────────────┐     │
│  │ Express Server                   │     │
│  │  /mcp   (MCP protocol)          │     │
│  │  /api/* (admin REST API)         │     │
│  │  /health (monitoring)            │     │
│  │  /      (React admin UI)         │     │
│  └──────────────────────────────────┘     │
│                                           │
│  ┌──────────────────────────────────┐     │
│  │ Embedded AskSQL (Standalone)     │     │
│  │  ask(), generateSQL(), executeSQL│     │
│  │  AI Client (OpenAI-compatible)   │     │
│  │  10 database connectors          │     │
│  │  SQL validator, query executor   │     │
│  └──────────────────────────────────┘     │
│                                           │
│  ┌──────────────────────────────────┐     │
│  │ File-based persistence           │     │
│  │  config.json (configuration)     │     │
│  │  data/schema-*.json (cache)      │     │
│  │  data/query-log.jsonl (logs)     │     │
│  └──────────────────────────────────┘     │
└───────────────────────────────────────────┘
        │           │           │
    PostgreSQL    Snowflake   Teradata  ...
```

---

## 3. Embedded AskSQL Library

The AskSQL project includes a full catalog database (30 Drizzle tables), event queue, scheduler, and abbreviation learner. For mcp-asksql, we embed only the core NL-to-SQL pipeline in `src/asksql/`, stripping unnecessary layers:

**Included (copied as-is):**
- `core/ai/client.ts` — OpenAI-compatible HTTP client with retry, timeout, JSON extraction
- `core/connector/interface.ts` — AskSQLConnector interface and types
- `core/connector/registry.ts` — Connector factory registry with auto-detection
- `core/connector/discovery-types.ts` — Standardized metadata types
- `core/validator/sql-validator.ts` — SQL safety checks (blocks DROP, DELETE, INSERT, etc.)
- `core/executor/query-executor.ts` — Safe SQL execution with row truncation
- `connectors/*/` — All 10 database connector implementations

**Modified:**
- `core/asksql.ts` — Removed 4 imports (db, schema, events, catalog), simplified to standalone-only path
- `core/index.ts` — Reduced barrel exports (no CatalogManager, db, abbreviation utilities)

**Eliminated (not copied):**
- `core/db/` — Drizzle ORM + PostgreSQL catalog (DATABASE_URL no longer needed)
- `core/catalog/` — Catalog persistence layer (508+ lines)
- `core/events/` — Event queue and handlers
- `core/scheduler/` — Background job engine
- `core/abbreviation/` — Bayesian abbreviation learning

**Trade-offs:** No sample values in LLM context, no query learning, no abbreviation learning. Acceptable for a lightweight MCP server.

---

## 4. MCP Tools

### ask
Ask a natural language question. Returns SQL, explanation, and query results.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `question` | string | Yes | Natural language question |
| `connector` | string | No | Connector ID. Auto-routes if omitted |
| `maxRows` | number | No | Max rows to return (default 100) |

**Response:** `{ success, sql, explanation, rows, rowCount, executionTimeMs, error?, routedTo?, routeMethod?, routeConfidence? }`

### generate_sql
Generate SQL without executing. Useful for review.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `question` | string | Yes | Natural language question |
| `connector` | string | No | Connector ID |

**Response:** `{ sql, explanation, routedTo? }`

### execute_sql
Execute raw SQL directly.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | string | Yes | SQL query (must pass safety validation) |
| `connector` | string | No | Connector ID |
| `maxRows` | number | No | Max rows (default 100) |

**Response:** `{ rows, columns, rowCount, truncated, executionTimeMs }`

### list_connectors
List all configured database connectors with status.

**Response:** `[{ id, type, schemas, isDefault, cached, cacheAgeHours }]`

### health_check
Test database and AI connectivity.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `connector` | string | No | Connector ID (default if omitted) |

**Response:** `{ database: { connected, version }, ai: { reachable } }`

### refresh_schema
Re-discover database schema and update cache. Automatically reconnects if connection was stale.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `connector` | string | No | Connector ID |

**Response:** `{ connector, tables, columns }`

---

## 5. Auto-Routing

When `connector` is omitted from `ask` or `generate_sql`, the server automatically determines which database to query.

**Stage 1 — Keyword Matching (instant, free):**
1. Tokenize the question into words (3+ letters)
2. Match against table names, column names, and schema names in each connector's cached schema
3. If exactly one connector matches, or one clearly dominates (score > 2x second place) — use it

**Stage 2 — AI Fallback (smart, ~1 token call):**
1. If keyword matching is ambiguous (multiple connectors tie)
2. Send question + connector schema summaries to the AI
3. AI decides which connector is most relevant

**Fallback:** If both stages fail, use the default connector.

**Example:**
- "show me all orders" → keyword matches `ORDERS` table in `sf_tpch` → routes to Snowflake
- "show me applications with data elements" → ambiguous → AI recognizes `df360_app` → routes to PostgreSQL

The router index rebuilds automatically on schema refresh, connector add/remove, and AI config changes.

---

## 6. Transport Layer

### stdio
For local subprocess invocation. MCP protocol on stdin/stdout, logging on stderr.
```bash
npx tsx src/index.ts --stdio
```

### HTTP (Stateful Sessions)
For cloud/remote access. Express server with per-client sessions.
```bash
npx tsx src/index.ts --http --port 8080
```

**Session flow:**
1. Client POSTs to `/mcp` without session ID → must be `initialize` request
2. Server creates `StreamableHTTPServerTransport`, returns `Mcp-Session-Id` header
3. Client includes `Mcp-Session-Id` in all subsequent requests
4. Sessions tracked in memory map, cleaned up on close

---

## 7. REST API (Admin)

### Connectors

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/connectors` | List all connectors |
| GET | `/api/connectors/:id` | Get connector details (connection string masked) |
| POST | `/api/connectors` | Add new connector |
| PUT | `/api/connectors/:id` | Update connector |
| DELETE | `/api/connectors/:id` | Remove connector |
| POST | `/api/connectors/:id/health` | Run health check |
| POST | `/api/connectors/:id/refresh-schema` | Refresh schema cache |
| GET | `/api/connectors/:id/schema-info` | Schema summary (table/column counts) |
| GET | `/api/connectors/:id/schema-detail` | Full cached schema (tables, columns, PKs, FKs, indexes) |

### AI Provider

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/ai` | Get settings (API key masked) |
| PUT | `/api/ai` | Update settings |
| POST | `/api/ai/test` | Test AI connectivity |

### Query Logs

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/logs` | Get logs (paginated, filterable by connector/status/date) |
| GET | `/api/logs/stats` | Aggregate stats (total, success rate, avg time) |
| DELETE | `/api/logs` | Clear all logs |

### Direct Query (from UI)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/ask` | NL question with auto-routing |
| POST | `/api/execute-sql` | Direct SQL execution |

---

## 8. Admin UI

React 19 + TypeScript + Vite + Tailwind CSS. Served as static files from the same Express server.

### Pages

**NL Query** (`#query`, default) — Natural language query interface with auto-detect connector, generated SQL display with copy button, results table, and query history.

**Connectors** (`#connectors`) — Card-based list of all connectors with type badges, cache age, connection status. Actions: Test, Refresh, Schema viewer, Edit, Remove. Add Connector modal. Rich schema detail panel showing tables, columns, PKs, FKs, and indexes.

**AI Provider** (`#ai`) — Configuration form for base URL, model, max tokens, temperature slider, and masked API key. Test connectivity button.

**Query Logs** (`#logs`) — Paginated table with zebra striping, filters (connector, status), expandable rows showing full SQL. Re-run button (▶) executes the exact SQL again on the same connector, with results displayed inline. Stats bar showing totals, success rate, and average execution time.

### Development
```bash
npm run dev      # Backend on :8080
npm run dev:ui   # Vite on :5173 (proxies /api to :8080)
```

### Production
```bash
npm run build    # Compiles backend + builds UI to dist/ui/
npm start        # Serves everything on :8080
```

---

## 9. Configuration

### config.json

```json
{
  "connectors": [
    {
      "id": "mydb",
      "connectionString": "postgres://user:pass@host:5432/db",
      "schemas": ["public"],
      "abbreviations": {},
      "examples": [],
      "schemaPrefix": ""
    }
  ],
  "ai": {
    "baseUrl": "https://api.deepseek.com/v1",
    "apiKey": "sk-...",
    "model": "deepseek-chat",
    "maxTokens": 6144,
    "temperature": 0.3
  },
  "safety": {
    "maxRows": 5000,
    "timeoutMs": 30000,
    "maxRetries": 2
  },
  "schemaCacheTtlHours": 24
}
```

- API key can also come from `AI_API_KEY` environment variable
- Custom config path: `--config /path/to/config.json`
- Config changes via REST API are written atomically (temp file + rename)

---

## 10. Schema Cache

Each connector's discovered schema is cached to `data/schema-{connectorId}.json`.

**TTL-based auto-refresh:**
- `schemaCacheTtlHours` (default 24, set to 0 to disable)
- On every tool call: if cache is stale, trigger background refresh
- Current (stale) schema serves the query immediately
- Next query gets fresh data

**Startup behavior:**
- Cache exists and fresh → load instantly (no DB call)
- Cache exists but stale → load from cache, refresh in background
- No cache → discover live, save to cache

**Reconnection:** If refresh fails due to stale connection, ConnectorManager recreates the AskSQL instance and retries.

---

## 11. Query Logging

All tool calls are logged to `data/query-log.jsonl` (JSON Lines, append-only).

**Log entry:** `{ id, timestamp, tool, connector, question?, sql?, success, error?, executionTimeMs, rowCount? }`

**Features:**
- Filterable by connector, status, date range
- Paginated queries
- Aggregate stats (total, success rate, avg time, by connector, by tool)
- Auto-rotation at 10MB

---

## 12. Supported Databases

| Database | Connection String | Driver |
|----------|------------------|--------|
| PostgreSQL | `postgres://user:pass@host:5432/db` | `pg` |
| MySQL | `mysql://user:pass@host:3306/db` | `mysql2` |
| SQL Server | `mssql://user:pass@host:1433/db` | `mssql` |
| Oracle | `oracle://user:pass@host:1521/SID` | `oracledb` |
| Snowflake | `snowflake://user:pass@account/db` | `snowflake-sdk` |
| BigQuery | `bigquery://project?keyFile=/path` | `@google-cloud/bigquery` |
| Redshift | `redshift://user:pass@cluster/db` | `pg` |
| Databricks | `databricks://token:dapi@host/path` | `@databricks/sql` |
| Dremio | `dremio://user:pass@host:31010/src` | embedded |
| Teradata | `teradata://user:pass@host/db` | `teradatasql` |

Connection type is auto-detected from the URL scheme. Each connector auto-registers via side-effect imports.

---

## 13. Project Structure

```
mcp-asksql/
├── src/
│   ├── index.ts              # Entry point, dual transport, API + UI serving
│   ├── config.ts             # Load config.json
│   ├── config-store.ts       # Atomic config read/write
│   ├── connector-manager.ts  # AskSQL instance management + mutations
│   ├── tools.ts              # 6 MCP tools with Zod schemas + logging
│   ├── api-routes.ts         # REST API for admin UI
│   ├── auto-router.ts        # Keyword + AI fallback routing
│   ├── schema-cache.ts       # File-based schema cache with TTL
│   ├── query-logger.ts       # JSONL query logging
│   ├── asksql/               # Embedded AskSQL (standalone)
│   │   ├── core/             # AI client, connectors, validator, executor
│   │   └── connectors/       # 10 database drivers
│   └── ui/                   # React admin UI (Vite + Tailwind)
│       ├── index.html
│       ├── vite.config.ts
│       └── src/
│           ├── App.tsx
│           └── components/   # NLQuery, Connectors, AIProvider, QueryLogs
├── config.json               # Runtime config (gitignored)
├── config.example.json       # Template with all 10 connectors
├── data/                     # Schema cache + query logs (gitignored)
├── package.json
└── tsconfig.json
```

---

## 14. Running

```bash
# Install
npm install
cp config.example.json config.json  # Edit with your DB + AI credentials

# Development
npm run dev       # Backend on :8080
npm run dev:ui    # UI on :5173

# Production
npm run build     # Compile backend + build UI
npm start         # Serve on :8080

# stdio mode (for MCP subprocess)
npm run start:stdio
```

---

## 15. MCP Client Integration

### BridgeFlow Configuration

In BridgeFlow's `data/mcp/servers.json`:
```json
{
  "id": "asksql",
  "transport": "http",
  "url": "http://192.168.1.100:8080/mcp"
}
```

### Protocol Flow
```
POST /mcp → initialize (get Mcp-Session-Id header)
POST /mcp → notifications/initialized
POST /mcp → tools/list (discover 6 tools)
POST /mcp → tools/call (invoke any tool)
```

All requests after initialize must include the `Mcp-Session-Id` header. Responses use Server-Sent Events format (`text/event-stream`).

### Deployment
- **Local:** `http://localhost:8080/mcp`
- **Network:** `http://192.168.x.x:8080/mcp`
- **Cloud:** `https://mcp-asksql.myapp.com/mcp` (add TLS reverse proxy)

Zero code changes between environments — just update the URL.
