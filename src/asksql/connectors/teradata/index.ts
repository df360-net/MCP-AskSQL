import { registerConnector } from "../../core/index.js";
import { TeradataConnector } from "./connector.js";

registerConnector("teradata", (config) => new TeradataConnector(config));

export { TeradataConnector } from "./connector.js";
