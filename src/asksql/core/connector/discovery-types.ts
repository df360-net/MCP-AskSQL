/**
 * Standardized Discovery Types
 *
 * The contract between Layer 2 (connector) and Layer 1 (CatalogManager).
 * Every connector returns a DiscoveredDatabase.
 * CatalogManager persists it to ask_catalog_* tables.
 */

// ---------------------------------------------------------------------------
// Top-level: Database
// ---------------------------------------------------------------------------

export interface DiscoveredDatabase {
  databaseName: string;
  serverVersion: string;
  databaseType: string; // POSTGRESQL, MYSQL, ORACLE, BIGQUERY, SNOWFLAKE, etc.
  schemas: DiscoveredSchema[];
  discoveredAt: Date;
  durationMs: number;
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

export interface DiscoveredSchema {
  schemaName: string;
  tables: DiscoveredTable[];
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

export interface DiscoveredTable {
  tableName: string;
  tableType: "TABLE" | "VIEW" | "MATERIALIZED VIEW";
  detailedTableType?: string;
  estimatedRowCount?: number;
  sizeBytes?: number;
  tableComment?: string;
  ddl?: string;
  isSharded?: boolean;
  shardedBaseName?: string;
  snapshotBaseTable?: string;

  columns: DiscoveredColumn[];
  primaryKey?: DiscoveredPrimaryKey;
  uniqueConstraints?: DiscoveredUniqueConstraint[];
  foreignKeys: DiscoveredForeignKey[];
  indexes: DiscoveredIndex[];
  tags?: Record<string, string>;
  partitioning?: DiscoveredPartitionInfo;
  clusteringColumns?: string[];
}

// ---------------------------------------------------------------------------
// Column
// ---------------------------------------------------------------------------

export interface DiscoveredColumn {
  columnName: string;
  ordinalPosition: number;
  dataType: string;
  fullDataType: string;
  isNullable: boolean;
  columnDefault?: string;
  characterMaxLength?: number;
  numericPrecision?: number;
  numericScale?: number;
  columnComment?: string;
  isPrimaryKey: boolean;
  isAutoIncrement: boolean;
  fieldPath?: string;
  isPartitionColumn?: boolean;
  clusteringPosition?: number;
}

// ---------------------------------------------------------------------------
// Constraints
// ---------------------------------------------------------------------------

export interface DiscoveredPrimaryKey {
  constraintName: string;
  columns: string[];
}

export interface DiscoveredUniqueConstraint {
  constraintName: string;
  columns: string[];
  isDeferrable?: boolean;
  isDeferred?: boolean;
}

export interface DiscoveredForeignKey {
  constraintName: string;
  columns: string[];
  referencedSchema: string;
  referencedTable: string;
  referencedColumns: string[];
  onDelete?: string;
  onUpdate?: string;
  matchType?: string;
  isDeferrable?: boolean;
  isDeferred?: boolean;
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

export interface DiscoveredIndex {
  indexName: string;
  columns: string[];
  isUnique: boolean;
}

// ---------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------

export interface DiscoveredPartitionInfo {
  field: string;
  type: string;
  numPartitions?: number;
  expirationMs?: number;
  requirePartitionFilter?: boolean;
}

// ---------------------------------------------------------------------------
// Column Samples (returned by collectSamples)
// ---------------------------------------------------------------------------

export interface DiscoveredColumnSample {
  columnName: string;
  distinctCount: number;
  nullFraction?: number;
  sampleValues: string[];
  minValue?: string;
  maxValue?: string;
  avgLength?: number;
}

// ---------------------------------------------------------------------------
// Sample collection request/result
// ---------------------------------------------------------------------------

export interface SampleCollectionRequest {
  schemaName: string;
  tableName: string;
  columns: string[];
  maxDistinctValues?: number;
}

export interface SampleCollectionResult {
  schemaName: string;
  tableName: string;
  samples: DiscoveredColumnSample[];
}

// ---------------------------------------------------------------------------
// Refresh summary
// ---------------------------------------------------------------------------

export interface CatalogRefreshSummary {
  schemasFound: number;
  tablesFound: number;
  viewsFound: number;
  columnsFound: number;
  fksFound: number;
  indexesFound: number;
  tablesAdded: number;
  tablesRemoved: number;
  columnsAdded: number;
  columnsRemoved: number;
  columnsModified: number;
  samplesCollected: number;
  durationMs: number;
}

// ---------------------------------------------------------------------------
// LLM Context (output from CatalogManager.buildLLMContext)
// ---------------------------------------------------------------------------

export interface LLMContext {
  schemaContext: string;
  abbreviationGuide: string;
  stats: {
    tableCount: number;
    columnCount: number;
    fkCount: number;
    sampleCount: number;
    abbreviationCount: number;
  };
}
