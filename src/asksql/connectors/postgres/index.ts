/**
 * @asksql/connector-postgres
 *
 * PostgreSQL connector for AskSQL.
 * Auto-registers on import.
 */

import { registerConnector } from "../../core/index.js";
import { PostgresConnector } from "./connector.js";

registerConnector("postgresql", (config) => new PostgresConnector(config));

export { PostgresConnector } from "./connector.js";
