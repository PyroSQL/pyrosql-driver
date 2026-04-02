/**
 * PyroSQL connection options for TypeORM.
 *
 * Uses the PWire protocol to connect to PyroSQL servers.
 */
export interface PyroSqlConnectionOptions {
    /**
     * Database driver type identifier.
     */
    readonly type: "pyrosql";

    /**
     * Full connection URL.
     *
     * Supported schemes:
     * - vsql://host:12520/db  (PyroSQL QUIC -- fastest)
     * - postgres://host:5432/db
     * - mysql://host:3306/db
     * - unix:///path/to/sock?db=mydb
     */
    readonly url: string;

    /**
     * Database name (extracted from URL if not provided).
     */
    readonly database?: string;

    /**
     * Connection pool size. Default: 10.
     */
    readonly poolSize?: number;

    /**
     * Whether to use the connection pool. Default: true.
     */
    readonly usePool?: boolean;
}
