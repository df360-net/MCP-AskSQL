import { registerConnector } from "../../core/index.js";
import { RedshiftConnector } from "./connector.js";

registerConnector("redshift", (config) => new RedshiftConnector(config));

export { RedshiftConnector } from "./connector.js";
