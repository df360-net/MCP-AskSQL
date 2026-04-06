# Contributing to MCP-AskSQL

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/df360-net/MCP-AskSQL.git
cd MCP-AskSQL

# Install dependencies
npm install

# Copy and edit config
cp config.example.json config.json
# Edit config.json with your database and AI credentials

# Start backend (auto-reload on changes)
npm run dev

# Start UI dev server (in a separate terminal)
npm run dev:ui
```

The backend runs on `http://localhost:8080` and the UI dev server on `http://localhost:5173` (proxies `/api` to the backend).

## Running Tests

```bash
# Run all tests
npm test

# Watch mode (re-runs on file changes)
npm run test:watch

# With coverage report
npm run test:coverage
```

All tests are fully mocked — no AI provider or database connections needed.

## Project Structure

```
src/
  index.ts              # Entry point, dual transport
  config.ts             # Config loading
  config-store.ts       # Atomic config read/write
  connector-manager.ts  # AskSQL instance management
  tools.ts              # 6 MCP tools
  api-routes.ts         # REST API for admin UI
  auto-router.ts        # Keyword + AI routing
  schema-cache.ts       # File-based schema cache
  query-logger.ts       # JSONL query logging
  asksql/               # Embedded AskSQL library
  ui/                   # React admin UI (Vite + Tailwind)
```

## Pull Request Process

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Add or update tests if applicable
4. Run `npm test` and ensure all tests pass
5. Open a pull request with a clear description of the change

## Reporting Bugs

Open a GitHub issue with:
- Steps to reproduce
- Expected vs actual behavior
- Node version and OS
- Relevant error messages or logs

## Code Style

- TypeScript with strict mode
- ESM only (`import`/`export`, `.js` extensions in imports)
- No unused variables or imports
- Prefer `const` over `let`
- Error messages should be actionable

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
