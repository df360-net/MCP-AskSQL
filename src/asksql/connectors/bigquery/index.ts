/**
 * @asksql/connector-bigquery
 *
 * Google BigQuery connector for AskSQL.
 * Uses @google-cloud/bigquery (official SDK).
 * Auto-registers on import.
 */

import { registerConnector } from "../../core/index.js";
import { BigQueryConnector } from "./connector.js";

registerConnector("bigquery", (config) => new BigQueryConnector(config));

export { BigQueryConnector } from "./connector.js";
