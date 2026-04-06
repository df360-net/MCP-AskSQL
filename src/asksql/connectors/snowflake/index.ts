/**
 * @asksql/connector-snowflake
 *
 * Snowflake connector for AskSQL.
 * Uses snowflake-sdk (official driver).
 * Auto-registers on import.
 */

import { registerConnector } from "../../core/index.js";
import { SnowflakeConnector } from "./connector.js";

registerConnector("snowflake", (config) => new SnowflakeConnector(config));

export { SnowflakeConnector } from "./connector.js";
