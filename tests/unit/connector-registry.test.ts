import { describe, it, expect } from "@jest/globals";
import { detectConnectorType } from "../../src/asksql/core/connector/registry.js";

describe("detectConnectorType", () => {
  it("detects postgresql://", () => {
    expect(detectConnectorType("postgresql://user:pass@host:5432/db")).toBe("postgresql");
  });

  it("detects postgres://", () => {
    expect(detectConnectorType("postgres://user:pass@host:5432/db")).toBe("postgresql");
  });

  it("detects mysql://", () => {
    expect(detectConnectorType("mysql://user:pass@host:3306/db")).toBe("mysql");
  });

  it("detects mssql://", () => {
    expect(detectConnectorType("mssql://user:pass@host:1433/db")).toBe("mssql");
  });

  it("detects sqlserver://", () => {
    expect(detectConnectorType("sqlserver://user:pass@host:1433/db")).toBe("mssql");
  });

  it("detects oracle://", () => {
    expect(detectConnectorType("oracle://user:pass@host:1521/SID")).toBe("oracle");
  });

  it("detects bigquery://", () => {
    expect(detectConnectorType("bigquery://project?keyFile=/path")).toBe("bigquery");
  });

  it("detects snowflake://", () => {
    expect(detectConnectorType("snowflake://user:pass@account/db")).toBe("snowflake");
  });

  it("detects databricks://", () => {
    expect(detectConnectorType("databricks://token:dapi@host/path")).toBe("databricks");
  });

  it("detects dremio://", () => {
    expect(detectConnectorType("dremio://user:pass@host:31010/src")).toBe("dremio");
  });

  it("detects redshift://", () => {
    expect(detectConnectorType("redshift://user:pass@cluster/db")).toBe("redshift");
  });

  it("detects redshift via amazonaws.com host (scheme takes priority)", () => {
    // postgres:// matches postgresql first — amazonaws.com is a fallback for non-scheme URLs
    expect(detectConnectorType("postgres://user:pass@cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db")).toBe("postgresql");
  });

  it("detects redshift via amazonaws.com host fallback", () => {
    // When no scheme matches, amazonaws.com fallback kicks in
    expect(detectConnectorType("host=cluster.abc123.us-east-1.redshift.amazonaws.com port=5439 dbname=db")).toBe("redshift");
  });

  it("detects teradata://", () => {
    expect(detectConnectorType("teradata://user:pass@host/db")).toBe("teradata");
  });

  it("returns null for unknown scheme", () => {
    expect(detectConnectorType("ftp://some-server/data")).toBeNull();
  });

  it("returns null for empty string", () => {
    expect(detectConnectorType("")).toBeNull();
  });

  it("is case-insensitive", () => {
    expect(detectConnectorType("POSTGRES://user:pass@host/db")).toBe("postgresql");
    expect(detectConnectorType("MySQL://user:pass@host/db")).toBe("mysql");
  });
});
