/**
 * @asksql/connector-mssql
 *
 * Microsoft SQL Server connector for AskSQL.
 * Uses the mssql (tedious) driver.
 * Auto-registers on import.
 */

import { registerConnector } from "../../core/index.js";
import { MSSQLConnector } from "./connector.js";

registerConnector("mssql", (config) => new MSSQLConnector(config));

export { MSSQLConnector } from "./connector.js";
