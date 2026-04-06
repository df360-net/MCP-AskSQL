# MCP-AskSQL

![version](https://img.shields.io/github/v/release/df360-net/MCP-AskSQL?label=version&color=blue) ![CI](https://github.com/df360-net/MCP-AskSQL/actions/workflows/test.yml/badge.svg) ![license MIT](https://img.shields.io/badge/license-MIT-green)

An [MCP](https://modelcontextprotocol.io/) (Model Context Protocol) server that enables AI agents to query databases using natural language. Supports 10 databases with automatic connector routing, schema caching, and an admin UI.

## Features

- **6 MCP tools** &mdash; `ask`, `generate_sql`, `execute_sql`, `list_connectors`, `health_check`, `refresh_schema`
- **10 database connectors** &mdash; PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery, Redshift, Databricks, Dremio, Teradata
- **Auto-routing** &mdash; keyword matching + AI fallback across multiple databases
- **Schema caching** &mdash; file-based with configurable TTL and background refresh
- **Admin UI** &mdash; React + Tailwind for connector management, AI config, NL queries, and query logs
- **Dual transport** &mdash; stdio (local) and HTTP (cloud/remote)
- **No external database dependency** &mdash; config and cache are file-based

## Quick Start

```bash
# Install
npm install

# Configure
cp config.example.json config.json
# Edit config.json with your database and AI provider credentials

# Run (HTTP mode with admin UI)
npm start

# Run (stdio mode for MCP subprocess)
npm run start:stdio
```

The server starts on `http://localhost:8080` with:
- MCP endpoint: `/mcp`
- Admin UI: `/`
- REST API: `/api/*`
- Health check: `/health`

## Configuration

Edit `config.json` (see `config.example.json` for all options):

```json
{
  "connectors": [
    {
      "id": "mydb",
      "connectionString": "postgres://user:pass@host:5432/db",
      "schemas": ["public"]
    }
  ],
  "ai": {
    "baseUrl": "https://api.openai.com/v1",
    "apiKey": "sk-your-key-here",
    "model": "gpt-4o",
    "maxTokens": 4096,
    "temperature": 0.3
  },
  "schemaCacheTtlHours": 24
}
```

The AI API key can also be set via the `AI_API_KEY` environment variable.

### Supported Databases

| Database | Connection String |
|----------|------------------|
| PostgreSQL | `postgres://user:pass@host:5432/db` |
| MySQL | `mysql://user:pass@host:3306/db` |
| SQL Server | `mssql://user:pass@host:1433/db` |
| Oracle | `oracle://user:pass@host:1521/SID` |
| Snowflake | `snowflake://user:pass@account/db` |
| BigQuery | `bigquery://project?keyFile=/path` |
| Redshift | `redshift://user:pass@cluster/db` |
| Databricks | `databricks://token:dapi@host/path` |
| Dremio | `dremio://user:pass@host:31010/src` |
| Teradata | `teradata://user:pass@host/db` |

## Auto-Routing

When multiple databases are configured, mcp-asksql automatically determines which database to query. If a specific connector is selected (in the UI dropdown or via the `connector` parameter in MCP tools), the question goes directly to that database with no routing.

When no connector is specified, auto-routing runs in two stages:

1. **Keyword matching (instant, free)** &mdash; tokenizes the question and matches against table names, column names, and schema names in each connector's cached schema. If one connector clearly dominates (score > 2x the runner-up), it wins.

2. **AI fallback (smart, ~1 API call)** &mdash; if keyword matching is ambiguous (multiple connectors tie), the AI is asked to decide based on the question and schema summaries.

3. **Default fallback** &mdash; if both stages fail, the first configured connector is used.

Each AskSQL instance is pre-loaded with only its own database's schema. The AI never sees other databases' tables &mdash; it generates SQL for whichever schema it receives.

## Admin UI Guide

The admin UI is available at `http://localhost:5173` during development (`npm run dev:ui`) or `http://localhost:8080` in production (`npm start` after `npm run build`). It has four pages:

### NL Query

Ask questions about your data in plain English.

- Type a question in the text box and click **Run Query** (or press Enter)
- Use the **Select Database** dropdown to target a specific database, or leave it on "Select Database" to let auto-routing decide
- The result shows: routing info (which database was selected and why), generated SQL with a copy button, explanation, and a results table
- Recent questions are saved in a history list below &mdash; click any to re-ask

### Connectors

Manage your database connections.

- Each connector shows its type, schemas, and cache age
- **Test** &mdash; verify the database is reachable (shows Connected/Disconnected status)
- **Refresh** &mdash; re-discover the database schema and update the cache (also tests connectivity)
- **Schema** &mdash; view all tables, columns, primary keys, foreign keys, and indexes
- **Edit** &mdash; update connection string, schemas, or other settings
- **Remove** &mdash; delete the connector (cannot remove the last one)
- **Add Connector** &mdash; add a new database with its connection string

### AI Provider

Configure your AI model settings.

- Set the **Base URL** for any OpenAI-compatible API (OpenAI, DeepSeek, Ollama, etc.)
- Set the **API Key** (leave blank to keep the current key; displayed masked)
- Choose the **Model** name (e.g., `gpt-4o`, `deepseek-chat`)
- Adjust **Max Tokens** and **Temperature** (slider from Precise to Creative)
- **Test Connection** &mdash; verify the AI endpoint is reachable
- **Save Configuration** &mdash; persist changes to `config.json`

### Query Logs

View and manage the history of all queries.

- Shows all tool calls with timestamp, tool type, connector, query, status, execution time, and row count
- Filter by **connector** or **status** (Success/Failed) using the dropdowns
- Click any row to expand and see the full SQL and error details
- **Re-run** (&#9654;) &mdash; re-execute the exact SQL on the same connector, with results displayed inline
- **Clear Logs** &mdash; delete all log entries
- Stats bar shows totals, success rate, and average execution time

## MCP Client Integration

Add to your MCP client configuration:

```json
{
  "id": "asksql",
  "transport": "http",
  "url": "http://localhost:8080/mcp"
}
```

### Protocol Flow

```
POST /mcp -> initialize (get Mcp-Session-Id header)
POST /mcp -> notifications/initialized
POST /mcp -> tools/list (discover 6 tools)
POST /mcp -> tools/call (invoke any tool)
```

## Development

```bash
# Backend (auto-reload)
npm run dev

# UI (Vite dev server with HMR, proxies /api to :8080)
npm run dev:ui

# Run tests
npm test

# Run tests in watch mode
npm run test:watch

# Build for production
npm run build
```

## Architecture

```
MCP Client (JSON-RPC 2.0)
    |
    v
mcp-asksql Server
    ├── MCP Server (SDK v1.10.1) - 6 tools with Zod schemas
    ├── ConnectorManager - AskSQL instances + schema cache + auto-router
    ├── Express Server - /mcp, /api/*, /health, / (admin UI)
    ├── Embedded AskSQL - AI client, connectors, SQL validator
    └── File-based persistence - config.json, data/schema-*.json, data/query-log.jsonl
```

See [docs/MCP_AskSQL_Tech_Design.md](docs/MCP_AskSQL_Tech_Design.md) for detailed architecture documentation.

## Testing

The project has 150+ tests covering:
- SQL validator safety checks
- Connector type detection
- Config loading and validation
- Schema cache TTL and lifecycle
- Query logging with filters and pagination
- Auto-router keyword matching and AI fallback
- AI client JSON extraction and retry logic
- REST API endpoints (integration tests with supertest)

All tests run without AI or database dependencies (fully mocked for CI).

## License

[MIT](LICENSE)
