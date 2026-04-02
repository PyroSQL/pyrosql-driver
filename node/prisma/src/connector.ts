import { Client, Pool, Transaction } from "pyrosql";

/**
 * Connection options for the PyroSQL Prisma connector.
 */
export interface PyroSqlConnectorOptions {
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
     * Whether to use a connection pool. Default: true.
     */
    usePool?: boolean;

    /**
     * Maximum pool size. Default: 10.
     */
    poolSize?: number;
}

/**
 * Represents a queryable surface -- either a Client or a Transaction.
 */
export interface Queryable {
    query(sql: string, params?: any[]): { columns: string[]; rows: any[][]; rows_affected: number };
    execute(sql: string, params?: any[]): number;
}

/**
 * PyroSQL connector that manages PWire connections.
 *
 * Wraps the low-level Client/Pool and exposes a simple interface
 * that the Prisma adapter can consume.
 */
export class PyroSqlConnector {
    private _client: Client | null = null;
    private _pool: Pool | null = null;
    private _options: PyroSqlConnectorOptions;
    private _connected: boolean = false;

    constructor(options: PyroSqlConnectorOptions) {
        this._options = options;
    }

    /**
     * Open the connection (and pool if configured).
     */
    connect(): void {
        if (this._connected) return;

        this._client = new Client(this._options.url);

        if (this._options.usePool !== false) {
            this._pool = new Pool(this._options.url, this._options.poolSize || 10);
        }

        this._connected = true;
    }

    /**
     * Close all connections and destroy the pool.
     */
    close(): void {
        if (this._client) {
            this._client.close();
            this._client = null;
        }
        if (this._pool) {
            this._pool.destroy();
            this._pool = null;
        }
        this._connected = false;
    }

    /**
     * Get the underlying PWire client. Auto-connects if needed.
     */
    getClient(): Client {
        if (!this._client) {
            this.connect();
        }
        return this._client!;
    }

    /**
     * Execute a SELECT query and return { columns, rows, rows_affected }.
     */
    queryRaw(sql: string, params: any[] = []): { columns: string[]; rows: any[][]; rows_affected: number } {
        return this.getClient().query(sql, params);
    }

    /**
     * Execute a DML statement and return the number of affected rows.
     */
    executeRaw(sql: string, params: any[] = []): number {
        return this.getClient().execute(sql, params);
    }

    /**
     * Begin a transaction on the underlying client.
     */
    beginTransaction(): Transaction {
        return this.getClient().begin();
    }

    /**
     * Whether the connector is currently connected.
     */
    get isConnected(): boolean {
        return this._connected;
    }
}
