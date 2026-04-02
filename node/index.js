/**
 * PyroSQL — Node.js Client for PyroSQL
 *
 * Connects to PyroSQL via the C FFI shared library (libpyrosql_ffi_pwire).
 * Supports queries, DML, transactions, and bulk inserts.
 *
 * @example
 * const { Client } = require('pyrosql');
 * const client = new Client('vsql://localhost:12520/mydb');
 * const result = client.query('SELECT * FROM users WHERE id = $1', [42]);
 * console.log(result.rows);
 * client.close();
 */

'use strict';

const ffi = require('ffi-napi');
const path = require('path');

// Resolve the shared library path based on platform
const LIB_NAME = 'pyrosql_ffi_pwire';
const libPath = (() => {
    // Check for explicit path via environment variable
    if (process.env.PYROSQL_FFI_LIB) {
        return process.env.PYROSQL_FFI_LIB;
    }
    // Default: look next to this module
    switch (process.platform) {
        case 'linux':
            return path.join(__dirname, `lib${LIB_NAME}.so`);
        case 'darwin':
            return path.join(__dirname, `lib${LIB_NAME}.dylib`);
        case 'win32':
            return path.join(__dirname, `${LIB_NAME}.dll`);
        default:
            throw new Error(`Unsupported platform: ${process.platform}`);
    }
})();

// Bind the C FFI functions
const lib = ffi.Library(libPath, {
    'pyro_pwire_init':              ['void',    []],
    'pyro_pwire_connect':           ['pointer', ['string']],
    'pyro_pwire_query':             ['pointer', ['pointer', 'string']],
    'pyro_pwire_execute':           ['int64',   ['pointer', 'string']],
    'pyro_pwire_begin':             ['pointer', ['pointer']],
    'pyro_pwire_commit':            ['int32',   ['pointer', 'string']],
    'pyro_pwire_rollback':          ['int32',   ['pointer', 'string']],
    'pyro_pwire_bulk_insert':       ['int64',   ['pointer', 'string', 'string']],
    'pyro_pwire_prepare':           ['pointer', ['pointer', 'string']],
    'pyro_pwire_execute_prepared':  ['pointer', ['pointer', 'string', 'string']],
    'pyro_pwire_pool_create':       ['pointer', ['string', 'uint32']],
    'pyro_pwire_pool_get':          ['pointer', ['pointer']],
    'pyro_pwire_pool_return':       ['void',    ['pointer', 'pointer']],
    'pyro_pwire_pool_destroy':      ['void',    ['pointer']],
    'pyro_pwire_query_retry':       ['pointer', ['pointer', 'string']],
    'pyro_pwire_execute_retry':     ['int64',   ['pointer', 'string']],
    'pyro_pwire_watch':             ['pointer', ['pointer', 'string']],
    'pyro_pwire_unwatch':           ['int32',   ['pointer', 'string']],
    'pyro_pwire_listen':            ['int32',   ['pointer', 'string']],
    'pyro_pwire_unlisten':          ['int32',   ['pointer', 'string']],
    'pyro_pwire_notify':            ['int32',   ['pointer', 'string', 'string']],
    'pyro_pwire_on_notification':   ['void',    ['pointer', 'pointer']],
    'pyro_pwire_copy_out':          ['pointer', ['pointer', 'string']],
    'pyro_pwire_copy_in':           ['int64',   ['pointer', 'string', 'string', 'string']],
    'pyro_pwire_subscribe_cdc':     ['pointer', ['pointer', 'string']],
    'pyro_pwire_free_string':       ['void',    ['pointer']],
    'pyro_pwire_close':             ['void',    ['pointer']],
    'pyro_pwire_shutdown':          ['void',    []],
});

// Initialize the Tokio runtime once
lib.pyro_pwire_init();

/**
 * Read a C string from a pointer and free it.
 * @param {Buffer} ptr - Pointer returned by FFI
 * @returns {string|null}
 */
function readAndFreeString(ptr) {
    if (ptr.isNull()) return null;
    try {
        const str = ptr.readCString();
        return str;
    } finally {
        lib.pyro_pwire_free_string(ptr);
    }
}

/**
 * PyroSQL client connection.
 */
class Client {
    /**
     * Connect to a PyroSQL server.
     *
     * Supported URL schemes:
     * - 'vsql://host:12520/db' — PyroSQL QUIC (fastest)
     * - 'postgres://host:5432/db' — PostgreSQL wire protocol
     * - 'mysql://host:3306/db' — MySQL wire protocol
     * - 'unix:///path/to/sock?db=mydb' — Unix domain socket
     *
     * Append '?syntax_mode=mysql' to override the SQL syntax mode.
     *
     * @param {string} url - Connection URL
     * @throws {Error} If the connection fails
     */
    constructor(url) {
        this._handle = lib.pyro_pwire_connect(url);
        if (this._handle.isNull()) {
            throw new Error(`Failed to connect to ${url}`);
        }
    }

    /**
     * Execute a SELECT query and return the result.
     * Uses server-side binding when params are provided.
     * @param {string} sql - SQL query with $1, $2, ... placeholders
     * @param {Array} [params=[]] - Parameter values
     * @returns {{ columns: string[], rows: any[][], rows_affected: number }}
     * @throws {Error} If the query fails
     */
    query(sql, params = []) {
        this._checkOpen();
        if (params.length > 0) {
            const stmt = this.prepare(sql);
            try {
                return stmt.query(params);
            } finally {
                // PreparedStatement is cleaned up by GC via the FFI layer
            }
        }
        const ptr = lib.pyro_pwire_query(this._handle, sql);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Query failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Query error: ${result.error}`);
        return result;
    }

    /**
     * Execute a DML statement (INSERT/UPDATE/DELETE).
     * Uses server-side binding when params are provided.
     * @param {string} sql - SQL statement with $1, $2, ... placeholders
     * @param {Array} [params=[]] - Parameter values
     * @returns {number} Number of rows affected
     * @throws {Error} If the statement fails
     */
    execute(sql, params = []) {
        this._checkOpen();
        if (params.length > 0) {
            const stmt = this.prepare(sql);
            try {
                return stmt.execute(params);
            } finally {
                // PreparedStatement is cleaned up by GC via the FFI layer
            }
        }
        const affected = lib.pyro_pwire_execute(this._handle, sql);
        if (affected < 0) throw new Error('Execute failed');
        return Number(affected);
    }

    /**
     * Begin a transaction.
     * @returns {Transaction}
     * @throws {Error} If the BEGIN fails
     */
    begin() {
        this._checkOpen();
        return new Transaction(this);
    }

    /**
     * Bulk insert rows into a table.
     * @param {string} table - Target table name
     * @param {string[]} columns - Column names
     * @param {any[][]} rows - Row data
     * @returns {number} Number of rows inserted
     * @throws {Error} If the bulk insert fails
     */
    bulkInsert(table, columns, rows) {
        this._checkOpen();
        const json = JSON.stringify({ columns, rows });
        const affected = lib.pyro_pwire_bulk_insert(this._handle, table, json);
        if (affected < 0) throw new Error('Bulk insert failed');
        return Number(affected);
    }

    /**
     * Prepare a statement for repeated execution.
     * @param {string} sql - SQL statement to prepare
     * @returns {PreparedStatement}
     * @throws {Error} If the prepare fails
     */
    prepare(sql) {
        this._checkOpen();
        const ptr = lib.pyro_pwire_prepare(this._handle, sql);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Prepare failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Prepare error: ${result.error}`);
        return new PreparedStatement(this, result);
    }

    /**
     * Execute a SELECT query with auto-reconnect on connection failure.
     * Uses server-side binding when params are provided.
     * @param {string} sql - SQL query with $1, $2, ... placeholders
     * @param {Array} [params=[]] - Parameter values
     * @returns {{ columns: string[], rows: any[][], rows_affected: number }}
     * @throws {Error} If the query fails after retry
     */
    queryRetry(sql, params = []) {
        this._checkOpen();
        if (params.length > 0) {
            // Fall back to prepare+execute for parameterized queries
            const stmt = this.prepare(sql);
            try {
                return stmt.query(params);
            } finally {
                // Cleanup handled by GC
            }
        }
        const ptr = lib.pyro_pwire_query_retry(this._handle, sql);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Query retry failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Query retry error: ${result.error}`);
        return result;
    }

    /**
     * Execute a DML statement with auto-reconnect on connection failure.
     * Uses server-side binding when params are provided.
     * @param {string} sql - SQL statement with $1, $2, ... placeholders
     * @param {Array} [params=[]] - Parameter values
     * @returns {number} Number of rows affected
     * @throws {Error} If the statement fails after retry
     */
    executeRetry(sql, params = []) {
        this._checkOpen();
        if (params.length > 0) {
            const stmt = this.prepare(sql);
            try {
                return stmt.execute(params);
            } finally {
                // Cleanup handled by GC
            }
        }
        const affected = lib.pyro_pwire_execute_retry(this._handle, sql);
        if (affected < 0) throw new Error('Execute retry failed');
        return Number(affected);
    }

    /**
     * Subscribe to a reactive query via WATCH.
     * @param {string} sql - SQL query to watch
     * @returns {string} The channel name for change notifications
     * @throws {Error} If the WATCH command fails
     */
    watch(sql) {
        this._checkOpen();
        const ptr = lib.pyro_pwire_watch(this._handle, sql);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Watch failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Watch error: ${result.error}`);
        return result.channel;
    }

    /**
     * Unsubscribe from a WATCH channel.
     * @param {string} channel - Channel name returned by watch()
     * @throws {Error} If the UNWATCH command fails
     */
    unwatch(channel) {
        this._checkOpen();
        const rc = lib.pyro_pwire_unwatch(this._handle, channel);
        if (rc !== 0) throw new Error('Unwatch failed');
    }

    /**
     * Subscribe to a PubSub channel via LISTEN.
     * @param {string} channel - Channel name
     * @throws {Error} If the LISTEN command fails
     */
    listen(channel) {
        this._checkOpen();
        const rc = lib.pyro_pwire_listen(this._handle, channel);
        if (rc !== 0) throw new Error('Listen failed');
    }

    /**
     * Unsubscribe from a PubSub channel via UNLISTEN.
     * @param {string} channel - Channel name
     * @throws {Error} If the UNLISTEN command fails
     */
    unlisten(channel) {
        this._checkOpen();
        const rc = lib.pyro_pwire_unlisten(this._handle, channel);
        if (rc !== 0) throw new Error('Unlisten failed');
    }

    /**
     * Send a notification to a PubSub channel.
     * @param {string} channel - Channel name
     * @param {string} payload - Notification payload
     * @throws {Error} If the NOTIFY command fails
     */
    notify(channel, payload) {
        this._checkOpen();
        const rc = lib.pyro_pwire_notify(this._handle, channel, payload);
        if (rc !== 0) throw new Error('Notify failed');
    }

    /**
     * Register a callback for server-pushed notifications.
     * The callback receives a parsed object: { channel: string, payload: string }
     * @param {function} callback - Notification handler
     */
    onNotification(callback) {
        this._checkOpen();
        // Store the callback so it isn't GC'd
        if (!this._notifCallbacks) this._notifCallbacks = [];
        const ffiCb = ffi.Callback('void', ['pointer'], (ptr) => {
            if (ptr.isNull()) return;
            try {
                const json = ptr.readCString();
                lib.pyro_pwire_free_string(ptr);
                const notif = JSON.parse(json);
                callback(notif);
            } catch (e) {
                // Notification parse error — ignore
            }
        });
        this._notifCallbacks.push(ffiCb);
        lib.pyro_pwire_on_notification(this._handle, ffiCb);
    }

    /**
     * COPY OUT: execute a query and return CSV data.
     * @param {string} sql - SQL query (e.g. 'COPY (SELECT * FROM t) TO STDOUT')
     * @returns {string} CSV data including header
     * @throws {Error} If the COPY fails
     */
    copyOut(sql) {
        this._checkOpen();
        const ptr = lib.pyro_pwire_copy_out(this._handle, sql);
        const csv = readAndFreeString(ptr);
        if (!csv) throw new Error('COPY OUT failed: null response');
        if (csv.startsWith('{"error"')) {
            const err = JSON.parse(csv);
            throw new Error(`COPY OUT error: ${err.error}`);
        }
        return csv;
    }

    /**
     * COPY IN: send CSV data to a table.
     * @param {string} table - Target table name
     * @param {string[]} columns - Column names
     * @param {string} csvData - CSV data (no header)
     * @returns {number} Number of rows inserted
     * @throws {Error} If the COPY fails
     */
    copyIn(table, columns, csvData) {
        this._checkOpen();
        const columnsJson = JSON.stringify(columns);
        const affected = lib.pyro_pwire_copy_in(this._handle, table, columnsJson, csvData);
        if (affected < 0) throw new Error('COPY IN failed');
        return Number(affected);
    }

    /**
     * Execute a query and return a Cursor for row-by-row iteration.
     *
     * v1 implementation: fetches all rows up-front and iterates locally.
     * True server-side streaming cursors are planned for v2.
     *
     * @param {string} sql - SQL query with $1, $2, ... placeholders
     * @param {Array} [params=[]] - Parameter values
     * @returns {Cursor}
     * @throws {Error} If the query fails
     */
    queryCursor(sql, params = []) {
        const result = this.query(sql, params);
        return new Cursor(result);
    }

    /**
     * Subscribe to CDC events on a table.
     * @param {string} table - Table name to subscribe to
     * @returns {{ subscription_id: string, table: string }}
     * @throws {Error} If the subscription fails
     */
    subscribeCdc(table) {
        this._checkOpen();
        const ptr = lib.pyro_pwire_subscribe_cdc(this._handle, table);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Subscribe CDC failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Subscribe CDC error: ${result.error}`);
        return result;
    }

    /**
     * Close the connection and release resources.
     */
    close() {
        if (this._handle && !this._handle.isNull()) {
            lib.pyro_pwire_close(this._handle);
            this._handle = null;
        }
    }

    /**
     * Interpolate parameters into SQL (client-side).
     * Replaces $1, $2, ... with escaped literal values.
     * @private
     */
    _interpolate(sql, params) {
        if (!params || params.length === 0) return sql;
        let result = sql;
        // Replace in reverse order so $10 is replaced before $1
        for (let i = params.length - 1; i >= 0; i--) {
            const placeholder = `$${i + 1}`;
            const val = params[i];
            let literal;
            if (val === null || val === undefined) {
                literal = 'NULL';
            } else if (typeof val === 'boolean') {
                literal = val ? 'TRUE' : 'FALSE';
            } else if (typeof val === 'number') {
                literal = String(val);
            } else if (typeof val === 'string') {
                literal = `'${val.replace(/'/g, "''")}'`;
            } else {
                literal = `'${String(val).replace(/'/g, "''")}'`;
            }
            result = result.split(placeholder).join(literal);
        }
        return result;
    }

    /**
     * @private
     */
    _checkOpen() {
        if (!this._handle || this._handle.isNull()) {
            throw new Error('Client is closed');
        }
    }
}

/**
 * A transaction handle.
 * Created by Client.begin(). Must be committed or rolled back.
 */
class Transaction {
    /**
     * @param {Client} client - The parent client
     * @throws {Error} If BEGIN fails
     */
    constructor(client) {
        this._client = client;
        const ptr = lib.pyro_pwire_begin(client._handle);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Begin transaction failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Begin error: ${result.error}`);
        this._id = result.transaction_id;
        this._finished = false;
    }

    /**
     * The server-assigned transaction ID.
     * @returns {string}
     */
    get id() {
        return this._id;
    }

    /**
     * Execute a SELECT query within this transaction.
     * @param {string} sql
     * @param {Array} [params=[]]
     * @returns {{ columns: string[], rows: any[][], rows_affected: number }}
     */
    query(sql, params = []) {
        this._checkActive();
        const txSql = `/* tx:${this._id} */ ${this._client._interpolate(sql, params)}`;
        return this._client.query(txSql);
    }

    /**
     * Execute a DML statement within this transaction.
     * @param {string} sql
     * @param {Array} [params=[]]
     * @returns {number}
     */
    execute(sql, params = []) {
        this._checkActive();
        const txSql = `/* tx:${this._id} */ ${this._client._interpolate(sql, params)}`;
        return this._client.execute(txSql);
    }

    /**
     * Commit the transaction.
     * @throws {Error} If the commit fails
     */
    commit() {
        this._checkActive();
        const rc = lib.pyro_pwire_commit(this._client._handle, this._id);
        this._finished = true;
        if (rc !== 0) throw new Error('Commit failed');
    }

    /**
     * Rollback the transaction.
     * @throws {Error} If the rollback fails
     */
    rollback() {
        this._checkActive();
        const rc = lib.pyro_pwire_rollback(this._client._handle, this._id);
        this._finished = true;
        if (rc !== 0) throw new Error('Rollback failed');
    }

    /**
     * @private
     */
    _checkActive() {
        if (this._finished) {
            throw new Error('Transaction already committed or rolled back');
        }
    }
}

/**
 * A prepared statement handle.
 * Created by Client.prepare(). Can be executed multiple times with different parameters.
 */
class PreparedStatement {
    /**
     * @param {Client} client - The parent client
     * @param {{ handle: string, sql: string }} info - Prepared statement info from server
     */
    constructor(client, info) {
        this._client = client;
        this._handle = info.handle;
        this._sql = info.sql;
        this._json = JSON.stringify(info);
    }

    /**
     * The server-assigned statement handle.
     * @returns {string}
     */
    get handle() {
        return this._handle;
    }

    /**
     * The original SQL string.
     * @returns {string}
     */
    get sql() {
        return this._sql;
    }

    /**
     * Execute the prepared statement as a query.
     * @param {Array} [params=[]] - Parameter values
     * @returns {{ columns: string[], rows: any[][], rows_affected: number }}
     * @throws {Error} If the query fails
     */
    query(params = []) {
        this._client._checkOpen();
        const paramsJson = JSON.stringify(params.map(v => v === undefined ? null : v));
        const ptr = lib.pyro_pwire_execute_prepared(
            this._client._handle, this._json, paramsJson
        );
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Execute prepared query failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Execute prepared error: ${result.error}`);
        return result;
    }

    /**
     * Execute the prepared statement as a DML operation.
     * @param {Array} [params=[]] - Parameter values
     * @returns {number} Number of rows affected
     * @throws {Error} If the execute fails
     */
    execute(params = []) {
        const result = this.query(params);
        return result.rows_affected || 0;
    }
}

/**
 * A connection pool for PyroSQL.
 *
 * Manages a pool of reusable connections with a configurable maximum size.
 *
 * @example
 * const { Pool } = require('pyrosql');
 * const pool = new Pool('vsql://localhost:12520/mydb', 10);
 * const client = pool.get();
 * const result = client.query('SELECT 1');
 * pool.returnClient(client);
 * pool.destroy();
 */
class Pool {
    /**
     * Create a new connection pool.
     * @param {string} url - Connection URL (e.g. 'vsql://localhost:12520/mydb')
     * @param {number} [maxSize=10] - Maximum number of connections
     * @throws {Error} If pool creation fails
     */
    constructor(url, maxSize = 10) {
        this._handle = lib.pyro_pwire_pool_create(url, maxSize);
        if (this._handle.isNull()) {
            throw new Error(`Failed to create pool for ${url}`);
        }
    }

    /**
     * Get a connection from the pool.
     * @returns {PooledClient}
     * @throws {Error} If getting a connection fails
     */
    get() {
        this._checkOpen();
        const clientHandle = lib.pyro_pwire_pool_get(this._handle);
        if (clientHandle.isNull()) {
            throw new Error('Failed to get connection from pool');
        }
        return new PooledClient(this, clientHandle);
    }

    /**
     * Return a client handle to the pool (internal use).
     * @param {Buffer} clientHandle
     * @private
     */
    _returnHandle(clientHandle) {
        if (this._handle && !this._handle.isNull()) {
            lib.pyro_pwire_pool_return(this._handle, clientHandle);
        }
    }

    /**
     * Destroy the pool and free all resources.
     */
    destroy() {
        if (this._handle && !this._handle.isNull()) {
            lib.pyro_pwire_pool_destroy(this._handle);
            this._handle = null;
        }
    }

    /**
     * @private
     */
    _checkOpen() {
        if (!this._handle || this._handle.isNull()) {
            throw new Error('Pool is destroyed');
        }
    }
}

/**
 * A client connection borrowed from a Pool.
 *
 * When done, call returnToPool() to return the connection.
 */
class PooledClient {
    /**
     * @param {Pool} pool - The parent pool
     * @param {Buffer} handle - Opaque client handle
     */
    constructor(pool, handle) {
        this._pool = pool;
        this._handle = handle;
        this._returned = false;
    }

    /**
     * Execute a SELECT query.
     * @param {string} sql
     * @returns {{ columns: string[], rows: any[][], rows_affected: number }}
     */
    query(sql) {
        this._checkActive();
        const ptr = lib.pyro_pwire_query(this._handle, sql);
        const json = readAndFreeString(ptr);
        if (!json) throw new Error('Query failed: null response');
        const result = JSON.parse(json);
        if (result.error) throw new Error(`Query error: ${result.error}`);
        return result;
    }

    /**
     * Execute a DML statement.
     * @param {string} sql
     * @returns {number} Rows affected
     */
    execute(sql) {
        this._checkActive();
        const affected = lib.pyro_pwire_execute(this._handle, sql);
        if (affected < 0) throw new Error('Execute failed');
        return Number(affected);
    }

    /**
     * Return this connection to the pool.
     */
    returnToPool() {
        if (!this._returned) {
            this._pool._returnHandle(this._handle);
            this._returned = true;
            this._handle = null;
        }
    }

    /**
     * @private
     */
    _checkActive() {
        if (this._returned || !this._handle || this._handle.isNull()) {
            throw new Error('PooledClient already returned to pool');
        }
    }
}

/**
 * A client-side cursor for iterating query results row by row.
 *
 * v1 implementation: the full result set is fetched up-front and rows are
 * returned one at a time via next(). True server-side streaming is planned
 * for v2.
 */
class Cursor {
    /**
     * @param {{ columns: string[], rows: any[][], rows_affected: number }} result
     */
    constructor(result) {
        this._columns = result.columns;
        this._rows = result.rows;
        this._index = 0;
    }

    /**
     * The column names of the result set.
     * @returns {string[]}
     */
    get columns() {
        return this._columns;
    }

    /**
     * Whether there are more rows to read.
     * @returns {boolean}
     */
    hasNext() {
        return this._index < this._rows.length;
    }

    /**
     * Return the next row, or null if exhausted.
     * @returns {any[]|null}
     */
    next() {
        if (this._index >= this._rows.length) return null;
        return this._rows[this._index++];
    }

    /**
     * Reset the cursor to the beginning.
     */
    reset() {
        this._index = 0;
    }
}

module.exports = { Client, Transaction, PreparedStatement, Pool, PooledClient, Cursor };
