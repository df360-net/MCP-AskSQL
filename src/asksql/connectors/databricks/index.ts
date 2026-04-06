/**
 * @asksql/connector-databricks
 *
 * Databricks connector for AskSQL.
 * Uses @databricks/sql driver (Thrift-based).
 * Auto-registers on import.
 */

import { registerConnector } from "../../core/index.js";
import { DatabricksConnector } from "./connector.js";

registerConnector("databricks", (config) => new DatabricksConnector(config));

export { DatabricksConnector } from "./connector.js";
