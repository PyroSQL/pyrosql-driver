/**
 * PyroLink — Node.js Client for PyroSQL (TypeScript type definitions).
 */

/** The result of a SELECT query. */
export interface QueryResult {
    /** Column names in order. */
    columns: string[];
    /** Row data — each inner array corresponds to one row. */
    rows: any[][];
    /** Number of rows affected (for DML statements, 0 for SELECT). */
    rows_affected: number;
}

/**
 * A PyroSQL client connection.
 *
 * Connects to PyroSQL via the QUIC-based PyroLink protocol using the
 * C FFI shared library.
 *
 * @example
 * ```ts
 * import { Client } from 'pyrosql';
 *
 * const client = new Client('vsql://localhost:12520/mydb');
 * const result = client.query('SELECT * FROM users WHERE id = $1', [42]);
 * console.log(result.rows);
 * client.close();
 * ```
 */
export class Client {
    /**
     * Connect to a PyroSQL server.
     *
     * Supported URL schemes:
     * - `vsql://host:12520/db` — PyroLink QUIC (fastest)
     * - `postgres://host:5432/db` — PostgreSQL wire protocol
     * - `mysql://host:3306/db` — MySQL wire protocol
     * - `unix:///path/to/sock?db=mydb` — Unix domain socket
     *
     * Append `?syntax_mode=mysql` to override the SQL syntax mode.
     *
     * @param url - Connection URL
     * @throws If the connection fails
     */
    constructor(url: string);

    /**
     * Execute a SELECT query and return the result.
     * @param sql - SQL query with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     */
    query(sql: string, params?: any[]): QueryResult;

    /**
     * Execute a DML statement (INSERT/UPDATE/DELETE).
     * @param sql - SQL statement with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     * @returns Number of rows affected
     */
    execute(sql: string, params?: any[]): number;

    /**
     * Begin a transaction.
     * @returns A Transaction handle that must be committed or rolled back
     */
    begin(): Transaction;

    /**
     * Prepare a statement for repeated execution.
     * @param sql - SQL statement to prepare
     * @returns A PreparedStatement handle
     */
    prepare(sql: string): PreparedStatement;

    /**
     * Bulk insert rows into a table.
     * @param table - Target table name
     * @param columns - Column names
     * @param rows - Row data
     * @returns Number of rows inserted
     */
    bulkInsert(table: string, columns: string[], rows: any[][]): number;

    /**
     * Execute a SELECT query with auto-reconnect on connection failure.
     * @param sql - SQL query with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     */
    queryRetry(sql: string, params?: any[]): QueryResult;

    /**
     * Execute a DML statement with auto-reconnect on connection failure.
     * @param sql - SQL statement with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     * @returns Number of rows affected
     */
    executeRetry(sql: string, params?: any[]): number;

    /**
     * COPY OUT: execute a query and return CSV data.
     * @param sql - SQL query (e.g. 'COPY (SELECT * FROM t) TO STDOUT')
     * @returns CSV data including header
     */
    copyOut(sql: string): string;

    /**
     * COPY IN: send CSV data to a table.
     * @param table - Target table name
     * @param columns - Column names
     * @param csvData - CSV data (no header)
     * @returns Number of rows inserted
     */
    copyIn(table: string, columns: string[], csvData: string): number;

    /**
     * Execute a query and return a Cursor for row-by-row iteration.
     *
     * v1: fetches all rows up-front and iterates locally.
     * True server-side streaming cursors are planned for v2.
     *
     * @param sql - SQL query with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     */
    queryCursor(sql: string, params?: any[]): Cursor;

    /**
     * Subscribe to CDC events on a table.
     * @param table - Table name to subscribe to
     * @returns Subscription info with subscription_id and table
     */
    subscribeCdc(table: string): { subscription_id: string; table: string };

    /**
     * Subscribe to a reactive query — returns the NOTIFY channel name.
     * The server pushes notifications when the query result changes.
     * @param sql - SELECT query to watch
     */
    watch(sql: string): string;

    /**
     * Unsubscribe from a reactive query.
     * @param channel - Channel name returned by watch()
     */
    unwatch(channel: string): void;

    /**
     * Subscribe to a NOTIFY channel.
     * @param channel - Channel name to listen on
     */
    listen(channel: string): void;

    /**
     * Unsubscribe from a NOTIFY channel.
     * @param channel - Channel name to stop listening on
     */
    unlisten(channel: string): void;

    /**
     * Send a notification on a channel.
     * @param channel - Channel name
     * @param payload - Notification payload string
     */
    notify(channel: string, payload: string): void;

    /**
     * Register a callback for incoming notifications (WATCH/LISTEN).
     * @param callback - Called with {channel, payload} for each notification
     */
    onNotification(callback: (notification: { channel: string; payload: string }) => void): void;

    /**
     * Close the connection and release resources.
     */
    close(): void;
}

/**
 * A transaction handle tied to a Client connection.
 *
 * Created by `Client.begin()`. Must be explicitly committed or rolled back.
 */
export class Transaction {
    /** The server-assigned transaction ID. */
    readonly id: string;

    /**
     * Execute a SELECT query within this transaction.
     * @param sql - SQL query with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     */
    query(sql: string, params?: any[]): QueryResult;

    /**
     * Execute a DML statement within this transaction.
     * @param sql - SQL statement with $1, $2, ... placeholders
     * @param params - Parameter values (optional)
     * @returns Number of rows affected
     */
    execute(sql: string, params?: any[]): number;

    /**
     * Commit the transaction.
     */
    commit(): void;

    /**
     * Rollback the transaction.
     */
    rollback(): void;
}

/**
 * A prepared statement handle.
 *
 * Created by `Client.prepare()`. Can be executed multiple times with
 * different parameters.
 */
export class PreparedStatement {
    /** The server-assigned statement handle. */
    readonly handle: string;

    /** The original SQL string. */
    readonly sql: string;

    /**
     * Execute the prepared statement as a query.
     * @param params - Parameter values (optional)
     */
    query(params?: any[]): QueryResult;

    /**
     * Execute the prepared statement as a DML operation.
     * @param params - Parameter values (optional)
     * @returns Number of rows affected
     */
    execute(params?: any[]): number;
}

/**
 * A connection pool for PyroSQL.
 *
 * Manages a pool of reusable connections with a configurable maximum size.
 */
export class Pool {
    /**
     * Create a new connection pool.
     * @param url - Connection URL (e.g. 'vsql://localhost:12520/mydb')
     * @param maxSize - Maximum number of connections (default 10)
     */
    constructor(url: string, maxSize?: number);

    /**
     * Get a connection from the pool.
     * @returns A PooledClient handle
     */
    get(): PooledClient;

    /**
     * Destroy the pool and free all resources.
     */
    destroy(): void;
}

/**
 * A client connection borrowed from a Pool.
 *
 * When done, call `returnToPool()` to return the connection.
 */
export class PooledClient {
    /**
     * Execute a SELECT query.
     * @param sql - SQL query
     */
    query(sql: string): QueryResult;

    /**
     * Execute a DML statement.
     * @param sql - SQL statement
     * @returns Number of rows affected
     */
    execute(sql: string): number;

    /**
     * Return this connection to the pool.
     */
    returnToPool(): void;
}

/**
 * A client-side cursor for iterating query results row by row.
 *
 * v1: the full result set is fetched up-front and rows are returned one at
 * a time via next(). True server-side streaming is planned for v2.
 */
export class Cursor {
    /** Column names of the result set. */
    readonly columns: string[];

    /** Whether there are more rows to read. */
    hasNext(): boolean;

    /** Return the next row, or null if exhausted. */
    next(): any[] | null;

    /** Reset the cursor to the beginning. */
    reset(): void;
}
