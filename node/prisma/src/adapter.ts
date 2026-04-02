import {
    SqlDriverAdapter,
    SqlQueryable,
    SqlQuery,
    SqlResultSet,
    Transaction,
    TransactionOptions,
    ColumnType,
    ColumnTypeEnum,
    ConnectionInfo,
} from "@prisma/driver-adapter-utils";
import { PyroSqlConnector, PyroSqlConnectorOptions } from "./connector";
import { Client, Transaction as PyroTransaction } from "pyrosql";

/**
 * Map PyroSQL / information_schema type names to Prisma ColumnType.
 */
function mapColumnType(typeName: string): ColumnType {
    const t = (typeName || "").toLowerCase();
    switch (t) {
        case "int":
        case "int4":
        case "integer":
        case "serial":
            return ColumnTypeEnum.Int32;
        case "int2":
        case "smallint":
        case "smallserial":
            return ColumnTypeEnum.Int32;
        case "int8":
        case "bigint":
        case "bigserial":
            return ColumnTypeEnum.Int64;
        case "float":
        case "float4":
        case "real":
            return ColumnTypeEnum.Float;
        case "float8":
        case "double precision":
        case "double":
            return ColumnTypeEnum.Double;
        case "numeric":
        case "decimal":
            return ColumnTypeEnum.Numeric;
        case "boolean":
        case "bool":
            return ColumnTypeEnum.Boolean;
        case "varchar":
        case "character varying":
        case "char":
        case "character":
        case "text":
            return ColumnTypeEnum.Text;
        case "bytea":
        case "blob":
            return ColumnTypeEnum.Bytes;
        case "date":
            return ColumnTypeEnum.Date;
        case "time":
        case "timetz":
            return ColumnTypeEnum.Time;
        case "timestamp":
        case "timestamp without time zone":
            return ColumnTypeEnum.DateTime;
        case "timestamptz":
        case "timestamp with time zone":
            return ColumnTypeEnum.DateTime;
        case "json":
            return ColumnTypeEnum.Json;
        case "jsonb":
            return ColumnTypeEnum.Json;
        case "uuid":
            return ColumnTypeEnum.Uuid;
        case "enum":
            return ColumnTypeEnum.Enum;
        default:
            return ColumnTypeEnum.UnknownNumber;
    }
}

/**
 * Convert a raw value from PWire result rows into the form Prisma expects.
 */
function coerceValue(value: any, colType: ColumnType): any {
    if (value === null || value === undefined) return null;

    switch (colType) {
        case ColumnTypeEnum.Int32:
            return typeof value === "string" ? parseInt(value, 10) : Number(value);
        case ColumnTypeEnum.Int64:
            return typeof value === "bigint" ? value.toString() : String(value);
        case ColumnTypeEnum.Float:
        case ColumnTypeEnum.Double:
        case ColumnTypeEnum.Numeric:
            return typeof value === "string" ? parseFloat(value) : Number(value);
        case ColumnTypeEnum.Boolean:
            if (typeof value === "string") return value === "true" || value === "t" || value === "1";
            return Boolean(value);
        case ColumnTypeEnum.DateTime:
        case ColumnTypeEnum.Date:
        case ColumnTypeEnum.Time:
            return value instanceof Date ? value.toISOString() : String(value);
        case ColumnTypeEnum.Json:
            return typeof value === "string" ? value : JSON.stringify(value);
        case ColumnTypeEnum.Bytes:
            if (Buffer.isBuffer(value)) return Array.from(value);
            return value;
        default:
            return value;
    }
}

/**
 * Interpolate $1, $2, ... placeholders in SQL with literal values.
 */
function interpolateParams(sql: string, args: any[]): string {
    if (!args || args.length === 0) return sql;
    let result = sql;
    for (let i = args.length - 1; i >= 0; i--) {
        const placeholder = `$${i + 1}`;
        const val = args[i];
        let literal: string;
        if (val === null || val === undefined) {
            literal = "NULL";
        } else if (typeof val === "boolean") {
            literal = val ? "TRUE" : "FALSE";
        } else if (typeof val === "number" || typeof val === "bigint") {
            literal = String(val);
        } else if (typeof val === "string") {
            literal = `'${val.replace(/'/g, "''")}'`;
        } else if (val instanceof Date) {
            literal = `'${val.toISOString()}'`;
        } else if (Buffer.isBuffer(val)) {
            literal = `'\\x${val.toString("hex")}'`;
        } else {
            literal = `'${JSON.stringify(val).replace(/'/g, "''")}'`;
        }
        result = result.split(placeholder).join(literal);
    }
    return result;
}

/**
 * Determine whether a SQL string is a read query.
 */
function isReadQuery(sql: string): boolean {
    const upper = sql.trimStart().toUpperCase();
    return (
        upper.startsWith("SELECT") ||
        upper.startsWith("WITH") ||
        upper.startsWith("SHOW") ||
        sql.toUpperCase().includes("RETURNING")
    );
}

/**
 * Prisma Transaction implementation backed by a PyroSQL transaction.
 */
class PyroSqlTransaction implements Transaction {
    readonly provider = "postgres" as const;
    readonly adapterName = "prisma-pyrosql";
    readonly options: TransactionOptions;

    private _tx: PyroTransaction;

    constructor(tx: PyroTransaction, options: TransactionOptions) {
        this._tx = tx;
        this.options = options;
    }

    async queryRaw(params: SqlQuery): Promise<SqlResultSet> {
        const sql = interpolateParams(params.sql, params.args as any[]);

        if (isReadQuery(sql)) {
            const result = this._tx.query(sql);
            const columnTypes = result.columns.map(() => mapColumnType("text"));
            const coercedRows = result.rows.map((row) =>
                row.map((val, idx) => coerceValue(val, columnTypes[idx]))
            );
            return {
                columnNames: result.columns,
                columnTypes: columnTypes as ColumnType[],
                rows: coercedRows,
                lastInsertId: undefined,
            };
        } else {
            this._tx.execute(sql);
            return {
                columnNames: [],
                columnTypes: [],
                rows: [],
                lastInsertId: undefined,
            };
        }
    }

    async executeRaw(params: SqlQuery): Promise<number> {
        const sql = interpolateParams(params.sql, params.args as any[]);

        if (sql.toUpperCase().includes("RETURNING")) {
            const result = this._tx.query(sql);
            return result.rows_affected || result.rows.length;
        }

        return this._tx.execute(sql);
    }

    async commit(): Promise<void> {
        this._tx.commit();
    }

    async rollback(): Promise<void> {
        this._tx.rollback();
    }
}

/**
 * Prisma SqlDriverAdapter for PyroSQL.
 *
 * Implements the Prisma driver-adapter-utils interface so that
 * Prisma Client can use PyroSQL as a database backend.
 *
 * @example
 * ```ts
 * import { PrismaClient } from '@prisma/client';
 * import { PyroSqlAdapter } from 'prisma-pyrosql';
 *
 * const adapter = new PyroSqlAdapter({ url: 'vsql://localhost:12520/mydb' });
 * const prisma = new PrismaClient({ adapter });
 * ```
 */
export class PyroSqlAdapter implements SqlDriverAdapter {
    readonly provider = "postgres" as const;
    readonly adapterName = "prisma-pyrosql";

    private _connector: PyroSqlConnector;
    private _client: Client;

    constructor(options: PyroSqlConnectorOptions) {
        const connector = new PyroSqlConnector(options);
        connector.connect();
        const client = connector.getClient();
        this._connector = connector;
        this._client = client;
    }

    getConnectionInfo(): ConnectionInfo {
        return {
            schemaName: "public",
            supportsRelationJoins: true,
        };
    }

    async queryRaw(params: SqlQuery): Promise<SqlResultSet> {
        const sql = interpolateParams(params.sql, params.args as any[]);

        if (isReadQuery(sql)) {
            const result = this._client.query(sql);
            const columnTypes = result.columns.map(() => mapColumnType("text"));
            const coercedRows = result.rows.map((row) =>
                row.map((val, idx) => coerceValue(val, columnTypes[idx]))
            );
            return {
                columnNames: result.columns,
                columnTypes: columnTypes as ColumnType[],
                rows: coercedRows,
                lastInsertId: undefined,
            };
        } else {
            this._client.execute(sql);
            return {
                columnNames: [],
                columnTypes: [],
                rows: [],
                lastInsertId: undefined,
            };
        }
    }

    async executeRaw(params: SqlQuery): Promise<number> {
        const sql = interpolateParams(params.sql, params.args as any[]);

        if (sql.toUpperCase().includes("RETURNING")) {
            const result = this._client.query(sql);
            return result.rows_affected || result.rows.length;
        }

        return this._client.execute(sql);
    }

    async executeScript(script: string): Promise<void> {
        const statements = script
            .split(";")
            .map((s) => s.trim())
            .filter((s) => s.length > 0);
        for (const stmt of statements) {
            this._client.execute(stmt);
        }
    }

    async startTransaction(isolationLevel?: string): Promise<Transaction> {
        const tx = this._client.begin();
        if (isolationLevel) {
            tx.execute(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
        }
        return new PyroSqlTransaction(tx, {
            usePhantomQuery: false,
        });
    }

    async dispose(): Promise<void> {
        this._connector.close();
    }
}

export { PyroSqlConnector, PyroSqlConnectorOptions } from "./connector";
