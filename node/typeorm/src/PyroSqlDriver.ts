import {
    ObjectLiteral,
    DataSource,
    EntityMetadata,
    TableColumn,
} from "typeorm";
import { Client, Pool } from "pyrosql";
import { PyroSqlQueryRunner } from "./PyroSqlQueryRunner";
import { PyroSqlConnectionOptions } from "./PyroSqlConnectionOptions";

/**
 * TypeORM Driver implementation for PyroSQL.
 *
 * Bridges the TypeORM DataSource lifecycle with the PWire client,
 * handling connections, schema introspection, migrations, and queries.
 *
 * This class implements the TypeORM Driver contract without a direct
 * `implements Driver` clause to avoid coupling to internal TypeORM
 * type-only exports that change across minor versions.
 */
export class PyroSqlDriver {
    connection: DataSource;
    options: PyroSqlConnectionOptions;
    database?: string;
    schema?: string;
    isReplicated: boolean = false;
    treeSupport: boolean = true;
    transactionSupport: "simple" | "nested" | "none" = "simple";

    supportedDataTypes: string[] = [
        "int",
        "int2",
        "int4",
        "int8",
        "integer",
        "smallint",
        "bigint",
        "tinyint",
        "float",
        "float4",
        "float8",
        "double precision",
        "real",
        "numeric",
        "decimal",
        "boolean",
        "bool",
        "varchar",
        "character varying",
        "char",
        "character",
        "text",
        "bytea",
        "date",
        "time",
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
        "timestamptz",
        "interval",
        "json",
        "jsonb",
        "uuid",
        "serial",
        "bigserial",
        "smallserial",
        "enum",
    ];

    spatialTypes: string[] = [];

    withLengthColumnTypes: string[] = [
        "varchar",
        "character varying",
        "char",
        "character",
    ];

    withPrecisionColumnTypes: string[] = [
        "numeric",
        "decimal",
        "float",
        "real",
        "double precision",
        "time",
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
    ];

    withScaleColumnTypes: string[] = [
        "numeric",
        "decimal",
    ];

    supportedUpsertTypes: string[] = [];

    mappedDataTypes: Record<string, any> = {
        createDate: "timestamp",
        createDateDefault: "now()",
        createDatePrecision: 6,
        updateDate: "timestamp",
        updateDateDefault: "now()",
        updateDatePrecision: 6,
        deleteDate: "timestamp",
        deleteDateNullable: true,
        deleteDatePrecision: 6,
        version: "int4",
        treeLevel: "int4",
        migrationId: "int4",
        migrationTimestamp: "bigint",
        migrationName: "varchar",
        cacheId: "int4",
        cacheIdentifier: "varchar",
        cacheTime: "bigint",
        cacheDuration: "int4",
        cacheQuery: "text",
        cacheResult: "text",
        metadataType: "varchar",
        metadataDatabase: "varchar",
        metadataSchema: "varchar",
        metadataTable: "varchar",
        metadataName: "varchar",
        metadataValue: "text",
    };

    dataTypeDefaults: Record<string, any> = {
        varchar: { length: 255 },
        char: { length: 1 },
        numeric: { precision: 10, scale: 0 },
        decimal: { precision: 10, scale: 0 },
        float: { precision: 8 },
        "double precision": { precision: 16 },
        int: { width: 11 },
        smallint: { width: 6 },
        bigint: { width: 20 },
        tinyint: { width: 4 },
    };

    maxAliasLength: number = 63;
    cteCapabilities = {
        enabled: true,
        requiresRecursiveHint: true,
        writable: false,
    };

    private _client: Client | null = null;
    private _pool: Pool | null = null;

    constructor(connection: DataSource) {
        this.connection = connection;
        this.options = connection.options as unknown as PyroSqlConnectionOptions;
        this.database = this.options.database;

        // Extract database from URL if not provided
        if (!this.database && this.options.url) {
            const match = this.options.url.match(/\/([^/?]+)(\?|$)/);
            if (match) {
                this.database = match[1];
            }
        }
    }

    // ---------------------------------------------------------------
    // Connection Lifecycle
    // ---------------------------------------------------------------

    async connect(): Promise<void> {
        if (this.options.usePool !== false) {
            const poolSize = this.options.poolSize || 10;
            this._pool = new Pool(this.options.url, poolSize);
        }
        this._client = new Client(this.options.url);
    }

    async disconnect(): Promise<void> {
        if (this._client) {
            this._client.close();
            this._client = null;
        }
        if (this._pool) {
            this._pool.destroy();
            this._pool = null;
        }
    }

    async afterConnect(): Promise<void> {
        // No post-connect initialization needed
    }

    /**
     * Get the underlying PWire client.
     */
    getClient(): Client {
        if (!this._client) {
            throw new Error("PyroSQL driver not connected. Call connect() first.");
        }
        return this._client;
    }

    /**
     * Get the connection pool.
     */
    getPool(): Pool | null {
        return this._pool;
    }

    // ---------------------------------------------------------------
    // QueryRunner
    // ---------------------------------------------------------------

    createQueryRunner(mode?: string): PyroSqlQueryRunner {
        return new PyroSqlQueryRunner(this, mode);
    }

    // ---------------------------------------------------------------
    // Type Mapping
    // ---------------------------------------------------------------

    normalizeType(column: { type?: string; length?: number | string; precision?: number | null; scale?: number }): string {
        const type = (column.type as string || "varchar").toLowerCase();

        const normalizationMap: Record<string, string> = {
            integer: "int",
            int2: "smallint",
            int4: "int",
            int8: "bigint",
            bool: "boolean",
            "character varying": "varchar",
            character: "char",
            float4: "float",
            float8: "double precision",
            "timestamp without time zone": "timestamp",
            "timestamp with time zone": "timestamptz",
        };

        return normalizationMap[type] || type;
    }

    normalizeDefault(columnMetadata: any): string | undefined {
        const defaultValue = columnMetadata.default;
        if (defaultValue === undefined || defaultValue === null) return undefined;
        if (typeof defaultValue === "number" || typeof defaultValue === "boolean") {
            return String(defaultValue);
        }
        if (typeof defaultValue === "function") {
            return (defaultValue as () => string)();
        }
        return String(defaultValue);
    }

    normalizeIsGeneratedValue(column: any): boolean {
        return column.generatedType === "increment" || column.generatedType === "uuid";
    }

    createFullType(column: TableColumn): string {
        let type = column.type;
        if (column.length) {
            type += `(${column.length})`;
        } else if (column.precision !== undefined && column.precision !== null) {
            if (column.scale !== undefined && column.scale !== null) {
                type += `(${column.precision}, ${column.scale})`;
            } else {
                type += `(${column.precision})`;
            }
        }
        if (column.isArray) type += "[]";
        return type;
    }

    obtainMasterConnection(): Promise<any> {
        return Promise.resolve(this._client);
    }

    obtainSlaveConnection(): Promise<any> {
        return Promise.resolve(this._client);
    }

    createGeneratedMap(metadata: EntityMetadata, insertResult: ObjectLiteral, entityIndex: number): ObjectLiteral | undefined {
        if (!insertResult) return undefined;

        const generatedMap: ObjectLiteral = {};
        let hasValues = false;

        for (const column of metadata.generatedColumns) {
            const value = insertResult[column.databaseName];
            if (value !== undefined) {
                const propName = column.propertyName;
                generatedMap[propName] = column.isObjectId ? value : column.createValueMap(value);
                hasValues = true;
            }
        }

        return hasValues ? generatedMap : undefined;
    }

    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: any[]): any[] {
        return columnMetadatas.filter((metadata) => {
            const tableColumn = tableColumns.find((tc) => tc.name === metadata.databaseName);
            if (!tableColumn) return false;
            const normalizedType = this.normalizeType({ type: metadata.type });
            return tableColumn.type !== normalizedType;
        });
    }

    isReturningSqlSupported(returnType: string): boolean {
        return returnType === "insert" || returnType === "update" || returnType === "delete";
    }

    isUUIDGenerationSupported(): boolean {
        return true;
    }

    isFullTextColumnTypeSupported(): boolean {
        return false;
    }

    createParameter(parameterName: string, index: number): string {
        return `$${index + 1}`;
    }

    buildTableName(tableName: string, schema?: string, database?: string): string {
        let name = tableName;
        if (schema) name = `${schema}.${name}`;
        return name;
    }

    parseTableName(target: any): { database?: string; schema?: string; tableName: string } {
        const tableName = typeof target === "string" ? target : (target as any).name || (target as any).tableName;
        const parts = tableName.split(".");
        if (parts.length === 2) {
            return { schema: parts[0], tableName: parts[1] };
        }
        return { tableName: parts[0] };
    }

    escape(name: string): string {
        return `"${name}"`;
    }

    preparePersistentValue(value: any, columnMetadata: any): any {
        if (value === null || value === undefined) return value;

        const type = this.normalizeType({ type: columnMetadata.type });
        switch (type) {
            case "json":
            case "jsonb":
                return typeof value === "string" ? value : JSON.stringify(value);
            case "date":
                return value instanceof Date ? value.toISOString().split("T")[0] : value;
            case "timestamp":
            case "timestamptz":
                return value instanceof Date ? value.toISOString() : value;
            case "boolean":
                return !!value;
            default:
                return value;
        }
    }

    prepareHydratedValue(value: any, columnMetadata: any): any {
        if (value === null || value === undefined) return value;

        const type = this.normalizeType({ type: columnMetadata.type });
        switch (type) {
            case "json":
            case "jsonb":
                return typeof value === "string" ? JSON.parse(value) : value;
            case "int":
            case "smallint":
            case "bigint":
            case "tinyint":
                return typeof value === "string" ? parseInt(value, 10) : value;
            case "float":
            case "real":
            case "double precision":
            case "numeric":
            case "decimal":
                return typeof value === "string" ? parseFloat(value) : value;
            case "boolean":
                if (typeof value === "string") {
                    return value === "true" || value === "t" || value === "1";
                }
                return !!value;
            case "date":
            case "timestamp":
            case "timestamptz":
                return typeof value === "string" ? new Date(value) : value;
            default:
                return value;
        }
    }
}

// Re-export everything for convenient imports
import { Table, View } from "typeorm";
export { PyroSqlQueryRunner } from "./PyroSqlQueryRunner";
export { PyroSqlConnectionOptions } from "./PyroSqlConnectionOptions";
