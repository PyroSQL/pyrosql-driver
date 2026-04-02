import { Client } from "pyrosql";
import { PyroSqlDialect } from "./dialect";
import { PyroSqlSession, PyroSqlQueryResultHKT } from "./session";

/**
 * Options for creating a PyroSQL Drizzle instance.
 */
export interface PyroSqlDrizzleOptions {
    /**
     * PyroSQL connection URL.
     *
     * Supported schemes:
     * - vsql://host:12520/db  (PyroSQL QUIC)
     * - postgres://host:5432/db
     * - mysql://host:3306/db
     * - unix:///path/to/sock?db=mydb
     */
    url: string;

    /**
     * Optional Drizzle schema for relations and type inference.
     */
    schema?: Record<string, unknown>;

    /**
     * Optional logger. Pass `true` for the default console logger,
     * or provide a custom logger object.
     */
    logger?: boolean | { logQuery(query: string, params: unknown[]): void };
}

/**
 * PyroSQL Drizzle database wrapper.
 *
 * Provides Drizzle-compatible select/insert/update/delete/execute methods
 * backed by a PyroSQL session.
 */
export class PyroSqlDatabase {
    private _session: PyroSqlSession;
    private _dialect: PyroSqlDialect;
    private _client: Client;
    private _schema: Record<string, unknown> | undefined;

    constructor(client: Client, dialect: PyroSqlDialect, session: PyroSqlSession, schema?: Record<string, unknown>) {
        this._client = client;
        this._dialect = dialect;
        this._session = session;
        this._schema = schema;
    }

    /**
     * Get the underlying PyroSQL session.
     */
    get session(): PyroSqlSession {
        return this._session;
    }

    /**
     * Get the dialect.
     */
    get dialect(): PyroSqlDialect {
        return this._dialect;
    }

    /**
     * Execute a raw SQL query string.
     * For SELECT queries, returns { rows, rowCount }.
     * For DML, returns { rows: [], rowCount: affected }.
     */
    async execute<T = any>(sql: string): Promise<T> {
        return this._session.execute<T>(sql);
    }

    /**
     * Execute a raw SELECT query and return mapped row objects.
     */
    async select<T extends Record<string, any> = Record<string, any>>(
        sql: string,
        params: any[] = [],
    ): Promise<T[]> {
        const pq = this._session.prepareQuery({ sql, params });
        const result: any = await pq.execute();
        return result.rows || result;
    }

    /**
     * Execute a raw DML statement and return the number of affected rows.
     */
    async run(sql: string, params: any[] = []): Promise<number> {
        const pq = this._session.prepareQuery({ sql, params });
        const result: any = await pq.execute();
        return result.rowCount || 0;
    }

    /**
     * Run a function inside a transaction.
     */
    async transaction<T>(
        fn: (tx: any) => Promise<T>,
        config?: { isolationLevel?: string; accessMode?: string },
    ): Promise<T> {
        return this._session.transaction(fn, config);
    }

    /**
     * Close the underlying client connection.
     */
    close(): void {
        this._client.close();
    }
}

/**
 * Create a Drizzle-style database instance connected to PyroSQL.
 *
 * @example
 * ```ts
 * import { drizzle } from 'drizzle-pyrosql';
 *
 * const db = drizzle({ url: 'vsql://localhost:12520/mydb' });
 * const users = await db.select('SELECT * FROM users');
 * await db.run('INSERT INTO users (name) VALUES ($1)', ['Alice']);
 * await db.transaction(async (tx) => {
 *   // ...
 * });
 * db.close();
 * ```
 */
export function drizzle(options: PyroSqlDrizzleOptions): PyroSqlDatabase {
    const client = new Client(options.url);
    const dialect = new PyroSqlDialect();
    const session = new PyroSqlSession(client, dialect, options.schema);
    return new PyroSqlDatabase(client, dialect, session, options.schema);
}

/**
 * Create a Drizzle database instance from an existing PyroSQL Client.
 *
 * @example
 * ```ts
 * import { Client } from 'pyrosql';
 * import { drizzleFromClient } from 'drizzle-pyrosql';
 *
 * const client = new Client('vsql://localhost:12520/mydb');
 * const db = drizzleFromClient(client);
 * ```
 */
export function drizzleFromClient(client: Client, schema?: Record<string, unknown>): PyroSqlDatabase {
    const dialect = new PyroSqlDialect();
    const session = new PyroSqlSession(client, dialect, schema);
    return new PyroSqlDatabase(client, dialect, session, schema);
}

export { PyroSqlDialect } from "./dialect";
export { PyroSqlSession, PyroSqlTransactionSession, PyroSqlPreparedQuery, PyroSqlQueryResultHKT } from "./session";
