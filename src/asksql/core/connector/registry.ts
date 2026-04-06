import type { AskSQLConnector, ConnectorType } from "./interface.js";

export type ConnectorFactory = (config: Record<string, unknown>) => AskSQLConnector;

const factories = new Map<string, ConnectorFactory>();

/** Register a connector factory. Called by connector packages on import. */
export function registerConnector(type: ConnectorType, factory: ConnectorFactory): void {
  if (factories.has(type)) {
    throw new Error(`Connector '${type}' is already registered`);
  }
  factories.set(type, factory);
}

/** Create a connector by type */
export function createConnector(type: string, config: Record<string, unknown>): AskSQLConnector {
  const factory = factories.get(type);
  if (!factory) {
    const available = Array.from(factories.keys()).join(", ");
    throw new Error(
      `Unknown connector '${type}'. Available: ${available || "(none)"}. ` +
      `Install the connector package: npm install @asksql/connector-${type}`
    );
  }
  return factory(config);
}

/** Detect connector type from connection string */
export function detectConnectorType(connectionString: string): ConnectorType | null {
  if (/^postgres(ql)?:\/\//i.test(connectionString)) return "postgresql";
  if (/^mysql:\/\//i.test(connectionString)) return "mysql";
  if (/^oracle:\/\//i.test(connectionString)) return "oracle";
  if (/^(mssql|sqlserver):\/\//i.test(connectionString)) return "mssql";
  if (/^bigquery:\/\//i.test(connectionString)) return "bigquery";
  if (/^snowflake:\/\//i.test(connectionString)) return "snowflake";
  if (/^databricks:\/\//i.test(connectionString)) return "databricks";
  if (/^dremio:\/\//i.test(connectionString)) return "dremio";
  if (/^redshift:\/\//i.test(connectionString)) return "redshift";
  if (/^teradata:\/\//i.test(connectionString)) return "teradata";
  if (/\.redshift\.amazonaws\.com/i.test(connectionString)) return "redshift";
  return null;
}

/** List all registered connector types */
export function listConnectors(): string[] {
  return Array.from(factories.keys());
}
