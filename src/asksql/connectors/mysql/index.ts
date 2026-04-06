import { registerConnector } from "../../core/index.js";
import { MySQLConnector } from "./connector.js";

registerConnector("mysql", (config) => new MySQLConnector(config));

export { MySQLConnector } from "./connector.js";
