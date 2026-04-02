import {
    ObjectLiteral,
    TableColumn,
    Table,
    TableForeignKey,
    TableIndex,
    TableUnique,
    TableCheck,
    TableExclusion,
    View,
} from "typeorm";
import type { PyroSqlDriver } from "./PyroSqlDriver";
import type { Client, Transaction as PyroTransaction } from "pyrosql";

/**
 * Column type mapping from TypeORM abstract types to PyroSQL DDL types.
 */
const TYPEORM_TO_PYROSQL: Record<string, string> = {
    // Integers
    int: "INT",
    int2: "SMALLINT",
    int4: "INT",
    int8: "BIGINT",
    integer: "INT",
    smallint: "SMALLINT",
    bigint: "BIGINT",
    tinyint: "SMALLINT",
    // Floating point
    float: "FLOAT",
    float4: "FLOAT",
    float8: "DOUBLE PRECISION",
    double: "DOUBLE PRECISION",
    "double precision": "DOUBLE PRECISION",
    real: "REAL",
    numeric: "NUMERIC",
    decimal: "NUMERIC",
    // Boolean
    boolean: "BOOLEAN",
    bool: "BOOLEAN",
    // Text
    varchar: "VARCHAR",
    "character varying": "VARCHAR",
    char: "CHAR",
    character: "CHAR",
    text: "TEXT",
    // Binary
    bytea: "BYTEA",
    blob: "BYTEA",
    // Date/time
    date: "DATE",
    time: "TIME",
    timestamp: "TIMESTAMP",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPTZ",
    timestamptz: "TIMESTAMPTZ",
    interval: "INTERVAL",
    // JSON
    json: "JSON",
    jsonb: "JSONB",
    // UUID
    uuid: "UUID",
    // Serial
    serial: "SERIAL",
    bigserial: "BIGSERIAL",
    smallserial: "SMALLSERIAL",
    // Array (handled separately)
    // Enum (handled separately)
};

/**
 * TypeORM QueryRunner implementation for PyroSQL.
 *
 * Handles DDL operations, schema management, queries, and transactions
 * against a PyroSQL database via the PWire protocol.
 */
export class PyroSqlQueryRunner {
    driver: PyroSqlDriver;
    connection: any;
    manager: any;

    isReleased: boolean = false;
    isTransactionActive: boolean = false;
    data: ObjectLiteral = {};
    loadedTables: Table[] = [];
    loadedViews: View[] = [];

    private _client: Client;
    private _transaction: PyroTransaction | null = null;
    private _mode: string;

    constructor(driver: PyroSqlDriver, mode?: string) {
        this.driver = driver;
        this.connection = driver.connection;
        this.manager = this.connection.createEntityManager(this);
        this._client = driver.getClient();
        this._mode = mode || "master";
    }

    // ---------------------------------------------------------------
    // Transaction Management
    // ---------------------------------------------------------------

    async startTransaction(isolationLevel?: string): Promise<void> {
        if (this.isTransactionActive) {
            throw new Error("Transaction already started");
        }
        this._transaction = this._client.begin();
        this.isTransactionActive = true;
        if (isolationLevel) {
            this._transaction.execute(
                `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
            );
        }
    }

    async commitTransaction(): Promise<void> {
        if (!this.isTransactionActive || !this._transaction) {
            throw new Error("No active transaction to commit");
        }
        this._transaction.commit();
        this._transaction = null;
        this.isTransactionActive = false;
    }

    async rollbackTransaction(): Promise<void> {
        if (!this.isTransactionActive || !this._transaction) {
            throw new Error("No active transaction to rollback");
        }
        this._transaction.rollback();
        this._transaction = null;
        this.isTransactionActive = false;
    }

    // ---------------------------------------------------------------
    // Query Execution
    // ---------------------------------------------------------------

    async query(queryStr: string, parameters?: any[], useStructuredResult?: boolean): Promise<any> {
        const sql = this._buildSql(queryStr, parameters);
        const isSelect = sql.trimStart().toUpperCase().startsWith("SELECT")
            || sql.trimStart().toUpperCase().startsWith("WITH")
            || sql.trimStart().toUpperCase().startsWith("SHOW")
            || sql.trimStart().toUpperCase().startsWith("EXPLAIN");

        try {
            if (isSelect) {
                const executor = this._transaction || this._client;
                const result = executor.query(sql);
                const records = this._mapRows(result.columns, result.rows);
                if (useStructuredResult) {
                    return {
                        records,
                        raw: result.rows,
                        affected: result.rows_affected,
                    };
                }
                return records;
            } else {
                const executor = this._transaction || this._client;
                if (sql.trimStart().toUpperCase().startsWith("INSERT") && sql.toUpperCase().includes("RETURNING")) {
                    const result = executor.query(sql);
                    const records = this._mapRows(result.columns, result.rows);
                    if (useStructuredResult) {
                        return {
                            records,
                            raw: result.rows,
                            affected: result.rows_affected,
                        };
                    }
                    return records;
                }
                const affected = executor.execute(sql);
                if (useStructuredResult) {
                    return { records: [], raw: [], affected };
                }
                return [];
            }
        } catch (err: any) {
            throw new Error(`PyroSQL query error: ${err.message}\nQuery: ${sql}`);
        }
    }

    stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<any> {
        throw new Error("Streaming is not supported by PyroSQL driver. Use query() instead.");
    }

    // ---------------------------------------------------------------
    // Schema / DDL Operations
    // ---------------------------------------------------------------

    async hasDatabase(database: string): Promise<boolean> {
        const result = await this.query(
            `SELECT 1 FROM information_schema.schemata WHERE schema_name = '${this._escape(database)}'`
        );
        return result.length > 0;
    }

    async hasSchema(schema: string): Promise<boolean> {
        const result = await this.query(
            `SELECT 1 FROM information_schema.schemata WHERE schema_name = '${this._escape(schema)}'`
        );
        return result.length > 0;
    }

    async hasTable(table: Table | string): Promise<boolean> {
        const tableName = typeof table === "string" ? table : table.name;
        const parsed = this._parseTableName(tableName);
        const result = await this.query(
            `SELECT 1 FROM information_schema.tables WHERE table_schema = '${this._escape(parsed.schema)}' AND table_name = '${this._escape(parsed.name)}'`
        );
        return result.length > 0;
    }

    async hasColumn(table: Table | string, columnName: string): Promise<boolean> {
        const tableName = typeof table === "string" ? table : table.name;
        const parsed = this._parseTableName(tableName);
        const result = await this.query(
            `SELECT 1 FROM information_schema.columns WHERE table_schema = '${this._escape(parsed.schema)}' AND table_name = '${this._escape(parsed.name)}' AND column_name = '${this._escape(columnName)}'`
        );
        return result.length > 0;
    }

    async createDatabase(database: string, ifNotExist?: boolean): Promise<void> {
        const ifNotExistStr = ifNotExist ? " IF NOT EXISTS" : "";
        await this.query(`CREATE DATABASE${ifNotExistStr} "${database}"`);
    }

    async dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        const ifExistStr = ifExist ? " IF EXISTS" : "";
        await this.query(`DROP DATABASE${ifExistStr} "${database}"`);
    }

    async createSchema(schemaPath: string, ifNotExist?: boolean): Promise<void> {
        const ifNotExistStr = ifNotExist ? " IF NOT EXISTS" : "";
        await this.query(`CREATE SCHEMA${ifNotExistStr} "${schemaPath}"`);
    }

    async dropSchema(schemaPath: string, ifExist?: boolean, isCascade?: boolean): Promise<void> {
        const ifExistStr = ifExist ? " IF EXISTS" : "";
        const cascadeStr = isCascade ? " CASCADE" : "";
        await this.query(`DROP SCHEMA${ifExistStr} "${schemaPath}"${cascadeStr}`);
    }

    async createTable(table: Table, ifNotExist?: boolean, createForeignKeys?: boolean, createIndices?: boolean): Promise<void> {
        const ifNotExistStr = ifNotExist ? " IF NOT EXISTS" : "";
        const columnDefs = table.columns.map((col) => this._buildColumnDef(col)).join(", ");

        const constraints: string[] = [];

        // Primary key
        const pkCols = table.columns.filter((c) => c.isPrimary);
        if (pkCols.length > 0) {
            const pkNames = pkCols.map((c) => `"${c.name}"`).join(", ");
            constraints.push(`PRIMARY KEY (${pkNames})`);
        }

        // Unique constraints
        if (table.uniques) {
            for (const uniq of table.uniques) {
                const cols = uniq.columnNames.map((c) => `"${c}"`).join(", ");
                constraints.push(`CONSTRAINT "${uniq.name}" UNIQUE (${cols})`);
            }
        }

        // Check constraints
        if (table.checks) {
            for (const check of table.checks) {
                if (check.expression) {
                    constraints.push(`CONSTRAINT "${check.name}" CHECK (${check.expression})`);
                }
            }
        }

        const allDefs = [columnDefs, ...constraints].filter(Boolean).join(", ");
        await this.query(`CREATE TABLE${ifNotExistStr} ${this._escapeTableName(table.name)} (${allDefs})`);

        // Foreign keys
        if (createForeignKeys !== false && table.foreignKeys) {
            for (const fk of table.foreignKeys) {
                await this.createForeignKey(table, fk);
            }
        }

        // Indices
        if (createIndices !== false && table.indices) {
            for (const idx of table.indices) {
                await this.createIndex(table, idx);
            }
        }
    }

    async dropTable(table: Table | string, ifExist?: boolean, dropForeignKeys?: boolean, dropIndices?: boolean): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const ifExistStr = ifExist ? " IF EXISTS" : "";
        await this.query(`DROP TABLE${ifExistStr} ${this._escapeTableName(tableName)} CASCADE`);
    }

    async renameTable(oldTableOrName: Table | string, newTableName: string): Promise<void> {
        const oldName = typeof oldTableOrName === "string" ? oldTableOrName : oldTableOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(oldName)} RENAME TO "${newTableName}"`);
    }

    async addColumn(table: Table | string, column: TableColumn): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const colDef = this._buildColumnDef(column);
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ADD COLUMN ${colDef}`);
    }

    async addColumns(table: Table | string, columns: TableColumn[]): Promise<void> {
        for (const col of columns) {
            await this.addColumn(table, col);
        }
    }

    async renameColumn(table: Table | string, oldColumnOrName: TableColumn | string, newColumnOrName: TableColumn | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const oldName = typeof oldColumnOrName === "string" ? oldColumnOrName : oldColumnOrName.name;
        const newName = typeof newColumnOrName === "string" ? newColumnOrName : newColumnOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} RENAME COLUMN "${oldName}" TO "${newName}"`);
    }

    async changeColumn(table: Table | string, oldColumnOrName: TableColumn | string, newColumn: TableColumn): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const oldName = typeof oldColumnOrName === "string" ? oldColumnOrName : oldColumnOrName.name;

        // Rename if name changed
        if (oldName !== newColumn.name) {
            await this.renameColumn(table, oldName, newColumn.name);
        }

        // Alter type
        const pyroType = this._getColumnType(newColumn);
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ALTER COLUMN "${newColumn.name}" TYPE ${pyroType}`);

        // Nullability
        if (newColumn.isNullable) {
            await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ALTER COLUMN "${newColumn.name}" DROP NOT NULL`);
        } else {
            await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ALTER COLUMN "${newColumn.name}" SET NOT NULL`);
        }

        // Default
        if (newColumn.default !== undefined && newColumn.default !== null) {
            await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ALTER COLUMN "${newColumn.name}" SET DEFAULT ${newColumn.default}`);
        } else {
            await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ALTER COLUMN "${newColumn.name}" DROP DEFAULT`);
        }
    }

    async changeColumns(table: Table | string, changedColumns: { oldColumn: TableColumn; newColumn: TableColumn }[]): Promise<void> {
        for (const { oldColumn, newColumn } of changedColumns) {
            await this.changeColumn(table, oldColumn, newColumn);
        }
    }

    async dropColumn(table: Table | string, columnOrName: TableColumn | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const colName = typeof columnOrName === "string" ? columnOrName : columnOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP COLUMN "${colName}"`);
    }

    async dropColumns(table: Table | string, columns: (TableColumn | string)[]): Promise<void> {
        for (const col of columns) {
            await this.dropColumn(table, col);
        }
    }

    async createPrimaryKey(table: Table | string, columnNames: string[]): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const cols = columnNames.map((c) => `"${c}"`).join(", ");
        const constraintName = `PK_${tableName.replace(/[^a-zA-Z0-9]/g, "_")}`;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ADD CONSTRAINT "${constraintName}" PRIMARY KEY (${cols})`);
    }

    async updatePrimaryKeys(table: Table | string, columns: TableColumn[]): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        // Drop old PK
        try {
            const constraintName = `PK_${tableName.replace(/[^a-zA-Z0-9]/g, "_")}`;
            await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP CONSTRAINT IF EXISTS "${constraintName}"`);
        } catch (_) {
            // ignore if no existing PK
        }
        const pkCols = columns.filter((c) => c.isPrimary);
        if (pkCols.length > 0) {
            await this.createPrimaryKey(table, pkCols.map((c) => c.name));
        }
    }

    async dropPrimaryKey(table: Table | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const constraintName = `PK_${tableName.replace(/[^a-zA-Z0-9]/g, "_")}`;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP CONSTRAINT "${constraintName}"`);
    }

    async createUniqueConstraint(table: Table | string, uniqueConstraint: TableUnique): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const cols = uniqueConstraint.columnNames.map((c) => `"${c}"`).join(", ");
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ADD CONSTRAINT "${uniqueConstraint.name}" UNIQUE (${cols})`);
    }

    async createUniqueConstraints(table: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        for (const uc of uniqueConstraints) {
            await this.createUniqueConstraint(table, uc);
        }
    }

    async dropUniqueConstraint(table: Table | string, uniqueOrName: TableUnique | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const name = typeof uniqueOrName === "string" ? uniqueOrName : uniqueOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP CONSTRAINT "${name}"`);
    }

    async dropUniqueConstraints(table: Table | string, uniqueConstraints: TableUnique[]): Promise<void> {
        for (const uc of uniqueConstraints) {
            await this.dropUniqueConstraint(table, uc);
        }
    }

    async createCheckConstraint(table: Table | string, checkConstraint: TableCheck): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ADD CONSTRAINT "${checkConstraint.name}" CHECK (${checkConstraint.expression})`);
    }

    async createCheckConstraints(table: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        for (const cc of checkConstraints) {
            await this.createCheckConstraint(table, cc);
        }
    }

    async dropCheckConstraint(table: Table | string, checkOrName: TableCheck | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const name = typeof checkOrName === "string" ? checkOrName : checkOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP CONSTRAINT "${name}"`);
    }

    async dropCheckConstraints(table: Table | string, checkConstraints: TableCheck[]): Promise<void> {
        for (const cc of checkConstraints) {
            await this.dropCheckConstraint(table, cc);
        }
    }

    async createExclusionConstraint(table: Table | string, exclusionConstraint: TableExclusion): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} ADD ${exclusionConstraint.expression}`);
    }

    async createExclusionConstraints(table: Table | string, exclusionConstraints: TableExclusion[]): Promise<void> {
        for (const ec of exclusionConstraints) {
            await this.createExclusionConstraint(table, ec);
        }
    }

    async dropExclusionConstraint(table: Table | string, exclusionOrName: TableExclusion | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const name = typeof exclusionOrName === "string" ? exclusionOrName : exclusionOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP CONSTRAINT "${name}"`);
    }

    async dropExclusionConstraints(table: Table | string, exclusionConstraints: TableExclusion[]): Promise<void> {
        for (const ec of exclusionConstraints) {
            await this.dropExclusionConstraint(table, ec);
        }
    }

    async createForeignKey(table: Table | string, foreignKey: TableForeignKey): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const cols = foreignKey.columnNames.map((c) => `"${c}"`).join(", ");
        const refCols = foreignKey.referencedColumnNames.map((c) => `"${c}"`).join(", ");
        let sql = `ALTER TABLE ${this._escapeTableName(tableName)} ADD CONSTRAINT "${foreignKey.name}" FOREIGN KEY (${cols}) REFERENCES ${this._escapeTableName(foreignKey.referencedTableName!)} (${refCols})`;
        if (foreignKey.onDelete) sql += ` ON DELETE ${foreignKey.onDelete}`;
        if (foreignKey.onUpdate) sql += ` ON UPDATE ${foreignKey.onUpdate}`;
        await this.query(sql);
    }

    async createForeignKeys(table: Table | string, foreignKeys: TableForeignKey[]): Promise<void> {
        for (const fk of foreignKeys) {
            await this.createForeignKey(table, fk);
        }
    }

    async dropForeignKey(table: Table | string, foreignKeyOrName: TableForeignKey | string): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const name = typeof foreignKeyOrName === "string" ? foreignKeyOrName : foreignKeyOrName.name;
        await this.query(`ALTER TABLE ${this._escapeTableName(tableName)} DROP CONSTRAINT "${name}"`);
    }

    async dropForeignKeys(table: Table | string, foreignKeys: TableForeignKey[]): Promise<void> {
        for (const fk of foreignKeys) {
            await this.dropForeignKey(table, fk);
        }
    }

    async createIndex(table: Table | string, index: TableIndex): Promise<void> {
        const tableName = typeof table === "string" ? table : table.name;
        const uniqueStr = index.isUnique ? " UNIQUE" : "";
        const cols = index.columnNames!.map((c) => `"${c}"`).join(", ");
        const whereStr = index.where ? ` WHERE ${index.where}` : "";
        await this.query(`CREATE${uniqueStr} INDEX "${index.name}" ON ${this._escapeTableName(tableName)} (${cols})${whereStr}`);
    }

    async createIndices(table: Table | string, indices: TableIndex[]): Promise<void> {
        for (const idx of indices) {
            await this.createIndex(table, idx);
        }
    }

    async dropIndex(table: Table | string, indexOrName: TableIndex | string): Promise<void> {
        const name = typeof indexOrName === "string" ? indexOrName : indexOrName.name;
        await this.query(`DROP INDEX "${name}"`);
    }

    async dropIndices(table: Table | string, indices: TableIndex[]): Promise<void> {
        for (const idx of indices) {
            await this.dropIndex(table, idx);
        }
    }

    async createView(view: View, oldView?: View): Promise<void> {
        if (oldView) {
            await this.dropView(oldView);
        }
        const materializedStr = view.materialized ? "MATERIALIZED " : "";
        await this.query(`CREATE ${materializedStr}VIEW ${this._escapeTableName(view.name)} AS ${view.expression}`);
    }

    async dropView(view: View | string): Promise<void> {
        const viewName = typeof view === "string" ? view : view.name;
        const materializedStr = (typeof view !== "string" && view.materialized) ? "MATERIALIZED " : "";
        await this.query(`DROP ${materializedStr}VIEW IF EXISTS ${this._escapeTableName(viewName)}`);
    }

    async clearTable(tableName: string): Promise<void> {
        await this.query(`TRUNCATE TABLE ${this._escapeTableName(tableName)}`);
    }

    async getTables(tableNames?: string[]): Promise<Table[]> {
        let sql = `SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'`;
        if (tableNames && tableNames.length > 0) {
            const names = tableNames.map((n) => `'${this._escape(n)}'`).join(", ");
            sql += ` AND table_name IN (${names})`;
        }
        const rows = await this.query(sql);
        return rows.map((row: any) =>
            new Table({
                name: row.table_schema === "public" ? row.table_name : `${row.table_schema}.${row.table_name}`,
                columns: [],
            })
        );
    }

    async getViews(viewNames?: string[]): Promise<View[]> {
        let sql = `SELECT table_schema, table_name, view_definition FROM information_schema.views WHERE 1=1`;
        if (viewNames && viewNames.length > 0) {
            const names = viewNames.map((n) => `'${this._escape(n)}'`).join(", ");
            sql += ` AND table_name IN (${names})`;
        }
        const rows = await this.query(sql);
        return rows.map((row: any) =>
            new View({
                name: row.table_schema === "public" ? row.table_name : `${row.table_schema}.${row.table_name}`,
                expression: row.view_definition,
            })
        );
    }

    async getTable(tableName: string): Promise<Table | undefined> {
        const tables = await this.getTables([tableName]);
        return tables.length > 0 ? tables[0] : undefined;
    }

    async getView(viewName: string): Promise<View | undefined> {
        const views = await this.getViews([viewName]);
        return views.length > 0 ? views[0] : undefined;
    }

    async getChangedColumns(): Promise<TableColumn[]> {
        return [];
    }

    // ---------------------------------------------------------------
    // Connection / Lifecycle
    // ---------------------------------------------------------------

    async connect(): Promise<any> {
        return this._client;
    }

    async release(): Promise<void> {
        this.isReleased = true;
    }

    async clearDatabase(database?: string): Promise<void> {
        const tables = await this.getTables();
        for (const table of tables) {
            await this.dropTable(table, true, true, true);
        }
    }

    getDatabases(): Promise<string[]> {
        return this.query(`SELECT datname FROM pg_database WHERE datistemplate = false`).then(
            (rows: any[]) => rows.map((r: any) => r.datname)
        );
    }

    getSchemas(schema?: string): Promise<string[]> {
        let sql = `SELECT schema_name FROM information_schema.schemata`;
        if (schema) sql += ` WHERE schema_name = '${this._escape(schema)}'`;
        return this.query(sql).then((rows: any[]) => rows.map((r: any) => r.schema_name));
    }

    // ---------------------------------------------------------------
    // Internal Helpers
    // ---------------------------------------------------------------

    private _buildSql(sql: string, params?: any[]): string {
        if (!params || params.length === 0) return sql;
        let result = sql;
        for (let i = params.length - 1; i >= 0; i--) {
            const placeholder = `$${i + 1}`;
            const val = params[i];
            let literal: string;
            if (val === null || val === undefined) {
                literal = "NULL";
            } else if (typeof val === "boolean") {
                literal = val ? "TRUE" : "FALSE";
            } else if (typeof val === "number") {
                literal = String(val);
            } else if (typeof val === "string") {
                literal = `'${val.replace(/'/g, "''")}'`;
            } else if (val instanceof Date) {
                literal = `'${val.toISOString()}'`;
            } else {
                literal = `'${JSON.stringify(val).replace(/'/g, "''")}'`;
            }
            result = result.split(placeholder).join(literal);
        }
        return result;
    }

    private _mapRows(columns: string[], rows: any[][]): Record<string, any>[] {
        return rows.map((row) => {
            const record: Record<string, any> = {};
            for (let i = 0; i < columns.length; i++) {
                record[columns[i]] = row[i];
            }
            return record;
        });
    }

    private _buildColumnDef(column: TableColumn): string {
        const parts: string[] = [];
        parts.push(`"${column.name}"`);

        if (column.isGenerated && column.generationStrategy === "increment") {
            parts.push(column.type === "bigint" ? "BIGSERIAL" : "SERIAL");
        } else {
            parts.push(this._getColumnType(column));
        }

        if (!column.isNullable) {
            parts.push("NOT NULL");
        }

        if (column.isUnique) {
            parts.push("UNIQUE");
        }

        if (column.default !== undefined && column.default !== null) {
            parts.push(`DEFAULT ${column.default}`);
        }

        if (column.isGenerated && column.generationStrategy === "uuid") {
            parts.push("DEFAULT gen_random_uuid()");
        }

        return parts.join(" ");
    }

    private _getColumnType(column: TableColumn): string {
        const baseType = column.type.toLowerCase();
        let pyroType = TYPEORM_TO_PYROSQL[baseType] || column.type.toUpperCase();

        if (column.length && (baseType === "varchar" || baseType === "character varying" || baseType === "char" || baseType === "character")) {
            pyroType = `${pyroType}(${column.length})`;
        } else if (column.precision !== undefined && column.precision !== null) {
            if (column.scale !== undefined && column.scale !== null) {
                pyroType = `${pyroType}(${column.precision}, ${column.scale})`;
            } else {
                pyroType = `${pyroType}(${column.precision})`;
            }
        }

        if (column.isArray) {
            pyroType = `${pyroType}[]`;
        }

        return pyroType;
    }

    private _escapeTableName(name: string): string {
        const parts = name.split(".");
        return parts.map((p) => `"${p}"`).join(".");
    }

    private _parseTableName(name: string): { schema: string; name: string } {
        const parts = name.split(".");
        if (parts.length >= 2) {
            return { schema: parts[0], name: parts[1] };
        }
        return { schema: "public", name: parts[0] };
    }

    private _escape(val: string): string {
        return val.replace(/'/g, "''");
    }
}
