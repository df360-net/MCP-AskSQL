declare module "oracledb" {
  interface Pool {
    getConnection(): Promise<Connection>;
    close(drainTime?: number): Promise<void>;
  }

  interface Connection {
    callTimeout: number;
    execute(sql: string, binds?: BindParameters, options?: ExecuteOptions): Promise<Result<any>>;
    close(): Promise<void>;
  }

  interface ExecuteOptions {
    outFormat?: number;
    fetchArraySize?: number;
    maxRows?: number;
  }

  interface Result<T> {
    rows?: T[];
    metaData?: Array<{ name: string; dbType: number }>;
    rowsAffected?: number;
  }

  type BindParameters = Record<string, unknown> | unknown[];

  const OUT_FORMAT_OBJECT: number;
  const DB_TYPE_NUMBER: number;
  const DB_TYPE_DATE: number;
  const DB_TYPE_TIMESTAMP: number;
  const DB_TYPE_TIMESTAMP_TZ: number;
  const DB_TYPE_TIMESTAMP_LTZ: number;
  const DB_TYPE_BINARY_FLOAT: number;
  const DB_TYPE_BINARY_DOUBLE: number;
  const DB_TYPE_BINARY_INTEGER: number;

  function createPool(attrs: Record<string, unknown>): Promise<Pool>;
  function initOracleClient(options?: Record<string, unknown>): void;
}
