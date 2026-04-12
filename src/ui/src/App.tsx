import { useState, useEffect } from "react";
import { useFetch } from "./hooks/useApi.js";
import type { ServerHealth } from "./types.js";
import { NLQueryPage } from "./components/NLQueryPage.js";
import { ConnectorsPage } from "./components/ConnectorsPage.js";
import { AIProviderPage } from "./components/AIProviderPage.js";
import { QueryLogsPage } from "./components/QueryLogsPage.js";
import { WorkflowsPage } from "./components/WorkflowsPage.js";
import { SchedulerPage } from "./components/SchedulerPage.js";

type Page = "query" | "workflows" | "scheduler" | "connectors" | "ai" | "logs";

function getPage(): Page {
  const hash = window.location.hash.replace("#", "");
  if (hash === "workflows" || hash === "scheduler" || hash === "connectors" || hash === "ai" || hash === "logs") return hash;
  return "query";
}

export function App() {
  const [page, setPage] = useState<Page>(getPage);
  const { data: health } = useFetch<ServerHealth>("/health");

  useEffect(() => {
    const handler = () => setPage(getPage());
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);

  const nav = (p: Page) => { window.location.hash = p; };

  return (
    <div className="app">
      <header className="topbar">
        <div className="topbar-brand">
          <div className="brand-icon">
            <svg width="22" height="22" fill="none" stroke="#fff" viewBox="0 0 24 24">
              <rect x="3" y="3" width="18" height="18" rx="3" strokeWidth={2} />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4" />
            </svg>
          </div>
          <div>
            <h1>MCP-AskSQL Admin</h1>
            <p className="brand-subtitle">Database Management Console</p>
          </div>
        </div>
        <div className="topbar-status">
          <span className={`dot ${health?.ok ? "green" : "red"}`} />
          {health ? `${health.connectors} connector(s) | ${health.activeSessions} session(s)` : "connecting..."}
        </div>
      </header>

      <div className="layout">
        <nav className="sidebar">
          <button className={page === "query" ? "active" : ""} onClick={() => nav("query")}>
            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" /></svg>
            NL Query
          </button>
          <button className={page === "workflows" ? "active" : ""} onClick={() => nav("workflows")}>
            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 10h16M4 14h16M4 18h16" /><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 6v12" /></svg>
            Workflows
          </button>
          <button className={page === "scheduler" ? "active" : ""} onClick={() => nav("scheduler")}>
            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" strokeWidth={2} /><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6v6l4 2" /></svg>
            Scheduler
          </button>
          <button className={page === "connectors" ? "active" : ""} onClick={() => nav("connectors")}>
            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" /></svg>
            Connectors
          </button>
          <button className={page === "ai" ? "active" : ""} onClick={() => nav("ai")}>
            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" /></svg>
            AI Provider
          </button>
          <button className={page === "logs" ? "active" : ""} onClick={() => nav("logs")}>
            <svg width="18" height="18" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>
            Query Logs
          </button>
        </nav>

        <main className="content">
          {page === "query" && <NLQueryPage />}
          {page === "workflows" && <WorkflowsPage />}
          {page === "scheduler" && <SchedulerPage />}
          {page === "connectors" && <ConnectorsPage />}
          {page === "ai" && <AIProviderPage />}
          {page === "logs" && <QueryLogsPage />}
        </main>
      </div>
    </div>
  );
}
