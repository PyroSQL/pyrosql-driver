import { entityKind } from "drizzle-orm";

/**
 * PyroSQL dialect for Drizzle ORM.
 *
 * PyroSQL uses PostgreSQL-compatible SQL syntax with $1, $2, ... placeholders,
 * RETURNING clauses, and standard DDL. This dialect provides SQL generation
 * utilities for building queries against a PyroSQL server.
 */
export class PyroSqlDialect {
    static readonly [entityKind] = "PyroSqlDialect";

    /**
     * Escape an identifier with double-quotes.
     */
    escapeName(name: string): string {
        return `"${name}"`;
    }

    /**
     * Escape a string value by wrapping in single quotes and doubling embedded quotes.
     */
    escapeString(value: string): string {
        return `'${value.replace(/'/g, "''")}'`;
    }

    /**
     * Escape a parameter reference -- PyroSQL uses $N placeholders.
     */
    escapeParam(num: number): string {
        return `$${num + 1}`;
    }

    /**
     * Build a migration SQL for "CREATE TABLE IF NOT EXISTS".
     */
    buildCreateTableIfNotExists(tableName: string, columns: string, constraints: string): string {
        const constraintsSql = constraints ? `, ${constraints}` : "";
        return `CREATE TABLE IF NOT EXISTS ${this.escapeName(tableName)} (${columns}${constraintsSql})`;
    }

    /**
     * Build the SQL for dropping a table.
     */
    buildDropTable(tableName: string, ifExists: boolean = true, cascade: boolean = true): string {
        const ifExistsStr = ifExists ? " IF EXISTS" : "";
        const cascadeStr = cascade ? " CASCADE" : "";
        return `DROP TABLE${ifExistsStr} ${this.escapeName(tableName)}${cascadeStr}`;
    }

    /**
     * Build ALTER TABLE ADD COLUMN.
     */
    buildAddColumn(tableName: string, columnDef: string): string {
        return `ALTER TABLE ${this.escapeName(tableName)} ADD COLUMN ${columnDef}`;
    }

    /**
     * Build ALTER TABLE DROP COLUMN.
     */
    buildDropColumn(tableName: string, columnName: string): string {
        return `ALTER TABLE ${this.escapeName(tableName)} DROP COLUMN ${this.escapeName(columnName)}`;
    }

    /**
     * Build ALTER TABLE RENAME COLUMN.
     */
    buildRenameColumn(tableName: string, oldName: string, newName: string): string {
        return `ALTER TABLE ${this.escapeName(tableName)} RENAME COLUMN ${this.escapeName(oldName)} TO ${this.escapeName(newName)}`;
    }

    /**
     * Build CREATE INDEX.
     */
    buildCreateIndex(indexName: string, tableName: string, columns: string[], unique: boolean = false, where?: string): string {
        const uniqueStr = unique ? " UNIQUE" : "";
        const cols = columns.map((c) => this.escapeName(c)).join(", ");
        const whereStr = where ? ` WHERE ${where}` : "";
        return `CREATE${uniqueStr} INDEX ${this.escapeName(indexName)} ON ${this.escapeName(tableName)} (${cols})${whereStr}`;
    }

    /**
     * Build DROP INDEX.
     */
    buildDropIndex(indexName: string): string {
        return `DROP INDEX IF EXISTS ${this.escapeName(indexName)}`;
    }

    /**
     * Build CREATE SCHEMA.
     */
    buildCreateSchema(schemaName: string, ifNotExists: boolean = true): string {
        const ifNotExistsStr = ifNotExists ? " IF NOT EXISTS" : "";
        return `CREATE SCHEMA${ifNotExistsStr} ${this.escapeName(schemaName)}`;
    }

    /**
     * Build DROP SCHEMA.
     */
    buildDropSchema(schemaName: string, ifExists: boolean = true, cascade: boolean = false): string {
        const ifExistsStr = ifExists ? " IF EXISTS" : "";
        const cascadeStr = cascade ? " CASCADE" : "";
        return `DROP SCHEMA${ifExistsStr} ${this.escapeName(schemaName)}${cascadeStr}`;
    }

    /**
     * Build ALTER TABLE ALTER COLUMN TYPE.
     */
    buildAlterColumnType(tableName: string, columnName: string, newType: string): string {
        return `ALTER TABLE ${this.escapeName(tableName)} ALTER COLUMN ${this.escapeName(columnName)} TYPE ${newType}`;
    }

    /**
     * Build ALTER TABLE ADD CONSTRAINT FOREIGN KEY.
     */
    buildAddForeignKey(
        tableName: string,
        constraintName: string,
        columns: string[],
        refTable: string,
        refColumns: string[],
        onDelete?: string,
        onUpdate?: string,
    ): string {
        const cols = columns.map((c) => this.escapeName(c)).join(", ");
        const refCols = refColumns.map((c) => this.escapeName(c)).join(", ");
        let sql = `ALTER TABLE ${this.escapeName(tableName)} ADD CONSTRAINT ${this.escapeName(constraintName)} FOREIGN KEY (${cols}) REFERENCES ${this.escapeName(refTable)} (${refCols})`;
        if (onDelete) sql += ` ON DELETE ${onDelete}`;
        if (onUpdate) sql += ` ON UPDATE ${onUpdate}`;
        return sql;
    }

    /**
     * Build ALTER TABLE DROP CONSTRAINT.
     */
    buildDropConstraint(tableName: string, constraintName: string): string {
        return `ALTER TABLE ${this.escapeName(tableName)} DROP CONSTRAINT ${this.escapeName(constraintName)}`;
    }
}
