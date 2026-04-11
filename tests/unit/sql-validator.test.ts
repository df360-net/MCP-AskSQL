import { describe, it, expect } from "@jest/globals";
import { validateSql } from "../../src/asksql/core/validator/sql-validator.js";

describe("validateSql", () => {
  // ── Valid queries ───────────────────────────────────────────────

  it("accepts simple SELECT", () => {
    expect(validateSql("SELECT * FROM users")).toEqual({ safe: true });
  });

  it("accepts SELECT with WHERE", () => {
    expect(validateSql("SELECT id, name FROM users WHERE id = 1")).toEqual({ safe: true });
  });

  it("accepts WITH (CTE)", () => {
    expect(validateSql("WITH cte AS (SELECT 1) SELECT * FROM cte")).toEqual({ safe: true });
  });

  it("accepts nested subqueries", () => {
    expect(validateSql("SELECT * FROM (SELECT id FROM users) t")).toEqual({ safe: true });
  });

  it("allows trailing semicolon", () => {
    expect(validateSql("SELECT 1;")).toEqual({ safe: true });
  });

  // ── Blocked keywords ──────────────────────────────────────────

  it("rejects empty SQL", () => {
    const r = validateSql("");
    expect(r.safe).toBe(false);
    expect(r.reason).toBe("Empty SQL");
  });

  it("rejects whitespace-only SQL", () => {
    expect(validateSql("   ").safe).toBe(false);
  });

  it("rejects INSERT", () => {
    const r = validateSql("INSERT INTO users VALUES (1)");
    expect(r.safe).toBe(false);
    expect(r.reason).toContain("Only SELECT");
  });

  it("rejects DROP TABLE", () => {
    const r = validateSql("SELECT 1; DROP TABLE users");
    expect(r.safe).toBe(false);
  });

  it("rejects DELETE", () => {
    const r = validateSql("DELETE FROM users");
    expect(r.safe).toBe(false);
  });

  it("rejects UPDATE", () => {
    const r = validateSql("UPDATE users SET name = 'x'");
    expect(r.safe).toBe(false);
  });

  it("rejects TRUNCATE", () => {
    const r = validateSql("TRUNCATE TABLE users");
    expect(r.safe).toBe(false);
  });

  it("rejects EXPLAIN in SELECT", () => {
    const r = validateSql("SELECT EXPLAIN FROM t");
    expect(r.safe).toBe(false);
    expect(r.reason).toContain("EXPLAIN");
  });

  it("rejects ALTER", () => {
    const r = validateSql("ALTER TABLE users ADD COLUMN x INT");
    expect(r.safe).toBe(false);
  });

  // ── Quoted strings (no false positives) ────────────────────────

  it("allows DROP inside a single-quoted string", () => {
    expect(validateSql("SELECT 'DROP TABLE' AS x FROM t")).toEqual({ safe: true });
  });

  it("allows DELETE inside a double-quoted identifier", () => {
    expect(validateSql('SELECT x AS "Delete Flag" FROM t')).toEqual({ safe: true });
  });

  it("allows blocked words inside backtick identifiers", () => {
    expect(validateSql("SELECT `DROP` FROM t")).toEqual({ safe: true });
  });

  // ── Blocked functions ─────────────────────────────────────────

  it("rejects pg_sleep function", () => {
    const r = validateSql("SELECT pg_sleep(10)");
    expect(r.safe).toBe(false);
    expect(r.reason).toContain("Blocked function");
  });

  it("rejects xp_cmdshell", () => {
    const r = validateSql("SELECT xp_cmdshell('dir')");
    expect(r.safe).toBe(false);
  });

  it("rejects dblink", () => {
    const r = validateSql("SELECT * FROM dblink('host=evil')");
    expect(r.safe).toBe(false);
  });

  // ── System schema blocking ────────────────────────────────────

  it("rejects PG_CATALOG.x for postgresql", () => {
    const r = validateSql("SELECT * FROM pg_catalog.pg_tables", "postgresql");
    expect(r.safe).toBe(false);
    expect(r.reason).toContain("PG_CATALOG");
  });

  it("allows SYSDATE for plsql (no dot = not schema access)", () => {
    expect(validateSql("SELECT SYSDATE FROM dual", "plsql")).toEqual({ safe: true });
  });

  it("rejects SYS.DUAL for plsql", () => {
    const r = validateSql("SELECT * FROM SYS.DUAL", "plsql");
    expect(r.safe).toBe(false);
    expect(r.reason).toContain("SYS");
  });

  it("allows SYS reference without dot for tsql", () => {
    expect(validateSql("SELECT SYSTEM_TIME FROM t", "tsql")).toEqual({ safe: true });
  });

  // ── Multiple statements ───────────────────────────────────────

  it("rejects semicolon mid-query", () => {
    const r = validateSql("SELECT 1; SELECT 2");
    expect(r.safe).toBe(false);
    expect(r.reason).toContain("Multiple statements");
  });

  it("rejects DROP after semicolon", () => {
    const r = validateSql("SELECT 1; DROP TABLE x");
    expect(r.safe).toBe(false);
  });

  // ── Dialect-specific ──────────────────────────────────────────

  it("does not block system schemas for snowflake", () => {
    expect(validateSql("SELECT * FROM t", "snowflake")).toEqual({ safe: true });
  });

  it("does not block system schemas for bigquery", () => {
    expect(validateSql("SELECT * FROM t", "bigquery")).toEqual({ safe: true });
  });
});
