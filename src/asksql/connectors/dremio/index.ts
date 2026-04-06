import { registerConnector } from "../../core/index.js";
import { DremioConnector } from "./connector.js";

registerConnector("dremio", (config) => new DremioConnector(config));

export { DremioConnector } from "./connector.js";
