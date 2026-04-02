import {
    entityKind,
    Placeholder,
} from "drizzle-orm";
import {
    PgPreparedQuery,
    PreparedQueryConfig,
} from "drizzle-orm/pg-core";
import { Client, Transaction as PyroTransaction } from "pyrosql";
import { PyroSqlDialect } from "./dialect";

/**
 * Result brand type for Drizzle HKT.
 */
export interface PyroSqlQueryResultHKT {
    readonly $brand: "PyroSqlQueryResultHKT";
    readonly row: unknown;
    type: PyroSqlQueryResult<this["row"]>;
}

/**
 * Query result wrapper.
 */
export interface PyroSqlQueryResult<T> {
    rows: T[];
    rowCount: number;
}

/**
 * Interpolate $1, $2, ... parameters into SQL text.
 */
function interpolateParams(sqlText: string, params: any[]): string {
    if (!params || params.length === 0) return sqlText;
    let result = sqlText;
    for (let i = params.length - 1; i >= 0; i--) {
        const placeholder = `$${i + 1}`;
        const val = params[i];
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
 * Map raw array-of-arrays result into array-of-objects using column names.
 */
function mapRows(columns: string[], rows: any[][]): Record<string, any>[] {
    return rows.map((row) => {
        const record: Record<string, any> = {};
        for (let i = 0; i < columns.length; i++) {
            record[columns[i]] = row[i];
        }
        return record;
    });
}

/**
 * Whether SQL is a read (SELECT-like) query.
 */
function isReadQuery(sql: string): boolean {
    const upper = sql.trimStart().toUpperCase();
    return (
        upper.startsWith("SELECT") ||
        upper.startsWith("WITH") ||
        sql.toUpperCase().includes("RETURNING")
    );
}

/**
 * A queryable executor interface -- either Client or Transaction.
 */
interface Executor {
    query(sql: string, params?: any[]): { columns: string[]; rows: any[][]; rows_affected: number };
    execute(sql: string, params?: any[]): number;
}

/**
 * Prepared query implementation for PyroSQL.
 *
 * This class does not extend PgPreparedQuery to avoid coupling to
 * Drizzle's internal class hierarchy which changes across versions.
 */
export class PyroSqlPreparedQuery<T extends PreparedQueryConfig = PreparedQueryConfig> {
    static readonly [entityKind] = "PyroSqlPreparedQuery";

    private _sqlText: string;
    private _params: any[];
    private _executor: Executor;
    private _customResultMapper?: (rows: any[][]) => T["execute"];

    constructor(
        executor: Executor,
        queryString: string,
        params: any[],
        customResultMapper?: (rows: any[][]) => T["execute"],
    ) {
        this._executor = executor;
        this._sqlText = queryString;
        this._params = params;
        this._customResultMapper = customResultMapper;
    }

    async execute(placeholderValues?: Record<string, unknown>): Promise<T["execute"]> {
        const params = this._resolveParams(placeholderValues);
        const interpolated = interpolateParams(this._sqlText, params);

        if (isReadQuery(interpolated)) {
            const result = this._executor.query(interpolated);
            if (this._customResultMapper) {
                return this._customResultMapper(result.rows);
            }
            const mapped = mapRows(result.columns, result.rows);
            return { rows: mapped, rowCount: result.rows_affected } as T["execute"];
        } else {
            const affected = this._executor.execute(interpolated);
            return { rows: [], rowCount: affected } as T["execute"];
        }
    }

    async all(placeholderValues?: Record<string, unknown>): Promise<T["all"]> {
        const params = this._resolveParams(placeholderValues);
        const interpolated = interpolateParams(this._sqlText, params);
        const result = this._executor.query(interpolated);

        if (this._customResultMapper) {
            return this._customResultMapper(result.rows);
        }
        return mapRows(result.columns, result.rows) as T["all"];
    }

    async values(placeholderValues?: Record<string, unknown>): Promise<T["values"]> {
        const params = this._resolveParams(placeholderValues);
        const interpolated = interpolateParams(this._sqlText, params);
        const result = this._executor.query(interpolated);
        return result.rows as T["values"];
    }

    private _resolveParams(placeholderValues?: Record<string, unknown>): any[] {
        if (!placeholderValues) return this._params;
        return this._params.map((param) => {
            if (param instanceof Placeholder) {
                const val = placeholderValues[param.name];
                return val === undefined ? null : val;
            }
            return param;
        });
    }
}

/**
 * PyroSQL session for Drizzle ORM.
 *
 * Provides the session interface that Drizzle requires for query building,
 * execution, and transaction management. Built without extending PgSession
 * to avoid breakage from Drizzle internal API changes.
 */
export class PyroSqlSession {
    static readonly [entityKind] = "PyroSqlSession";

    private _client: Client;
    private _dialect: PyroSqlDialect;

    constructor(client: Client, dialect: PyroSqlDialect, _schema: any) {
        this._client = client;
        this._dialect = dialect;
    }

    get client(): Client {
        return this._client;
    }

    get dialect(): PyroSqlDialect {
        return this._dialect;
    }

    prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
        query: { sql: string; params: unknown[] },
        _fields?: any,
        _name?: string,
        _isResponseInArrayMode?: boolean,
        customResultMapper?: (rows: any[][]) => T["execute"],
    ): PyroSqlPreparedQuery<T> {
        return new PyroSqlPreparedQuery<T>(
            this._client,
            query.sql,
            query.params as any[],
            customResultMapper,
        );
    }

    async execute<T>(queryStr: string): Promise<T> {
        if (isReadQuery(queryStr)) {
            const result = this._client.query(queryStr);
            const mapped = mapRows(result.columns, result.rows);
            return { rows: mapped, rowCount: result.rows_affected } as unknown as T;
        } else {
            const affected = this._client.execute(queryStr);
            return { rows: [], rowCount: affected } as unknown as T;
        }
    }

    async transaction<T>(
        transaction: (tx: PyroSqlTransactionSession) => Promise<T>,
        config?: { isolationLevel?: string; accessMode?: string },
    ): Promise<T> {
        const pyroTx = this._client.begin();

        if (config?.isolationLevel) {
            pyroTx.execute(`SET TRANSACTION ISOLATION LEVEL ${config.isolationLevel}`);
        }
        if (config?.accessMode) {
            pyroTx.execute(`SET TRANSACTION ${config.accessMode}`);
        }

        const txSession = new PyroSqlTransactionSession(
            this._dialect,
            pyroTx,
            0,
        );

        try {
            const result = await transaction(txSession);
            pyroTx.commit();
            return result;
        } catch (err) {
            pyroTx.rollback();
            throw err;
        }
    }
}

/**
 * Transaction session for PyroSQL within Drizzle.
 *
 * Wraps a PyroSQL Transaction handle and provides query execution
 * scoped to that transaction, including nested savepoints.
 */
export class PyroSqlTransactionSession {
    static readonly [entityKind] = "PyroSqlTransactionSession";

    private _tx: PyroTransaction;
    private _dialect: PyroSqlDialect;
    readonly nestedIndex: number;

    constructor(dialect: PyroSqlDialect, tx: PyroTransaction, nestedIndex: number) {
        this._dialect = dialect;
        this._tx = tx;
        this.nestedIndex = nestedIndex;
    }

    get dialect(): PyroSqlDialect {
        return this._dialect;
    }

    prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
        query: { sql: string; params: unknown[] },
        _fields?: any,
        _name?: string,
        _isResponseInArrayMode?: boolean,
        customResultMapper?: (rows: any[][]) => T["execute"],
    ): PyroSqlPreparedQuery<T> {
        return new PyroSqlPreparedQuery<T>(
            this._tx,
            query.sql,
            query.params as any[],
            customResultMapper,
        );
    }

    async execute<T>(queryStr: string): Promise<T> {
        if (isReadQuery(queryStr)) {
            const result = this._tx.query(queryStr);
            const mapped = mapRows(result.columns, result.rows);
            return { rows: mapped, rowCount: result.rows_affected } as unknown as T;
        } else {
            const affected = this._tx.execute(queryStr);
            return { rows: [], rowCount: affected } as unknown as T;
        }
    }

    async transaction<T>(transaction: (tx: PyroSqlTransactionSession) => Promise<T>): Promise<T> {
        const savepointName = `sp_${this.nestedIndex + 1}`;
        this._tx.execute(`SAVEPOINT ${savepointName}`);

        const nestedTxSession = new PyroSqlTransactionSession(
            this._dialect,
            this._tx,
            this.nestedIndex + 1,
        );

        try {
            const result = await transaction(nestedTxSession);
            this._tx.execute(`RELEASE SAVEPOINT ${savepointName}`);
            return result;
        } catch (err) {
            this._tx.execute(`ROLLBACK TO SAVEPOINT ${savepointName}`);
            throw err;
        }
    }
}
