/**
 * @asksql/connector-oracle
 *
 * Oracle connector for AskSQL.
 * Uses oracledb thin mode — no Oracle client installation required.
 * Auto-registers on import.
 */

import { registerConnector } from "../../core/index.js";
import { OracleConnector } from "./connector.js";

registerConnector("oracle", (config) => new OracleConnector(config));

export { OracleConnector } from "./connector.js";
