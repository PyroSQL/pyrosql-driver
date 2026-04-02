import { PyroSqlDriver } from "../src/PyroSqlDriver";
import { PyroSqlQueryRunner } from "../src/PyroSqlQueryRunner";
import { PyroSqlConnectionOptions } from "../src/PyroSqlConnectionOptions";
import { Table, TableColumn, TableIndex, TableForeignKey, TableUnique, TableCheck, DataSource } from "typeorm";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

const mockQueryResult = {
    columns: ["id", "name", "active"],
    rows: [
        [1, "Alice", true],
        [2, "Bob", false],
    ],
    rows_affected: 0,
};

const mockClient = {
    query: jest.fn().mockReturnValue(mockQueryResult),
    execute: jest.fn().mockReturnValue(1),
    begin: jest.fn().mockReturnValue({
        id: "tx-001",
        query: jest.fn().mockReturnValue(mockQueryResult),
        execute: jest.fn().mockReturnValue(1),
        commit: jest.fn(),
        rollback: jest.fn(),
    }),
    close: jest.fn(),
    _interpolate: jest.fn((sql: string, _params: any[]) => sql),
    _checkOpen: jest.fn(),
};

const mockPool = {
    get: jest.fn(),
    destroy: jest.fn(),
};

jest.mock("pyrosql", () => ({
    Client: jest.fn().mockImplementation(() => mockClient),
    Pool: jest.fn().mockImplementation(() => mockPool),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockDataSource(options?: Partial<PyroSqlConnectionOptions>): DataSource {
    const ds = {
        options: {
            type: "pyrosql" as const,
            url: "vsql://localhost:12520/testdb",
            ...options,
        },
        createEntityManager: jest.fn().mockReturnValue({}),
    } as unknown as DataSource;
    return ds;
}

// ---------------------------------------------------------------------------
// PyroSqlConnectionOptions
// ---------------------------------------------------------------------------

describe("PyroSqlConnectionOptions", () => {
    it("should define the correct type", () => {
        const opts: PyroSqlConnectionOptions = {
            type: "pyrosql",
            url: "vsql://localhost:12520/mydb",
            database: "mydb",
            poolSize: 5,
            usePool: true,
        };
        expect(opts.type).toBe("pyrosql");
        expect(opts.url).toBe("vsql://localhost:12520/mydb");
        expect(opts.database).toBe("mydb");
        expect(opts.poolSize).toBe(5);
        expect(opts.usePool).toBe(true);
    });
});

// ---------------------------------------------------------------------------
// PyroSqlDriver
// ---------------------------------------------------------------------------

describe("PyroSqlDriver", () => {
    let driver: PyroSqlDriver;

    beforeEach(() => {
        jest.clearAllMocks();
        const ds = createMockDataSource();
        driver = new PyroSqlDriver(ds);
    });

    describe("connect / disconnect", () => {
        it("should connect and create client", async () => {
            await driver.connect();
            expect(driver.getClient()).toBeDefined();
        });

        it("should disconnect and clean up", async () => {
            await driver.connect();
            await driver.disconnect();
            expect(() => driver.getClient()).toThrow("not connected");
        });
    });

    describe("database extraction from URL", () => {
        it("should extract database from URL when not provided", () => {
            const ds = createMockDataSource({ url: "vsql://host:12520/extracted_db" });
            const d = new PyroSqlDriver(ds);
            expect(d.database).toBe("extracted_db");
        });

        it("should prefer explicit database", () => {
            const ds = createMockDataSource({ database: "explicit_db" });
            const d = new PyroSqlDriver(ds);
            expect(d.database).toBe("explicit_db");
        });
    });

    describe("pool configuration", () => {
        it("should create pool by default", async () => {
            await driver.connect();
            expect(driver.getPool()).toBeDefined();
        });

        it("should skip pool when usePool is false", async () => {
            const ds = createMockDataSource({ usePool: false });
            const d = new PyroSqlDriver(ds);
            await d.connect();
            expect(d.getPool()).toBeNull();
            await d.disconnect();
        });
    });

    describe("type normalization", () => {
        it("should normalize integer types", () => {
            expect(driver.normalizeType({ type: "int4" })).toBe("int");
            expect(driver.normalizeType({ type: "int8" })).toBe("bigint");
            expect(driver.normalizeType({ type: "int2" })).toBe("smallint");
            expect(driver.normalizeType({ type: "integer" })).toBe("int");
        });

        it("should normalize boolean types", () => {
            expect(driver.normalizeType({ type: "bool" })).toBe("boolean");
        });

        it("should normalize timestamp types", () => {
            expect(driver.normalizeType({ type: "timestamp without time zone" })).toBe("timestamp");
            expect(driver.normalizeType({ type: "timestamp with time zone" })).toBe("timestamptz");
        });

        it("should normalize character types", () => {
            expect(driver.normalizeType({ type: "character varying" })).toBe("varchar");
            expect(driver.normalizeType({ type: "character" })).toBe("char");
        });

        it("should pass unknown types through", () => {
            expect(driver.normalizeType({ type: "jsonb" })).toBe("jsonb");
        });
    });

    describe("createFullType", () => {
        it("should add length", () => {
            const col = new TableColumn({ name: "col", type: "varchar", length: "255" });
            expect(driver.createFullType(col)).toBe("varchar(255)");
        });

        it("should add precision and scale", () => {
            const col = new TableColumn({ name: "col", type: "numeric", precision: 10, scale: 2 });
            expect(driver.createFullType(col)).toBe("numeric(10, 2)");
        });

        it("should handle arrays", () => {
            const col = new TableColumn({ name: "col", type: "int", isArray: true });
            expect(driver.createFullType(col)).toBe("int[]");
        });
    });

    describe("parameter creation", () => {
        it("should create PostgreSQL-style parameters", () => {
            expect(driver.createParameter("p", 0)).toBe("$1");
            expect(driver.createParameter("p", 4)).toBe("$5");
        });
    });

    describe("RETURNING support", () => {
        it("should support returning for insert/update/delete", () => {
            expect(driver.isReturningSqlSupported("insert")).toBe(true);
            expect(driver.isReturningSqlSupported("update")).toBe(true);
            expect(driver.isReturningSqlSupported("delete")).toBe(true);
        });

        it("should not support returning for select", () => {
            expect(driver.isReturningSqlSupported("select")).toBe(false);
        });
    });

    describe("UUID generation", () => {
        it("should support UUID generation", () => {
            expect(driver.isUUIDGenerationSupported()).toBe(true);
        });
    });

    describe("escape", () => {
        it("should wrap identifiers in double quotes", () => {
            expect(driver.escape("users")).toBe('"users"');
            expect(driver.escape("my table")).toBe('"my table"');
        });
    });

    describe("buildTableName", () => {
        it("should build simple table name", () => {
            expect(driver.buildTableName("users")).toBe("users");
        });

        it("should prepend schema", () => {
            expect(driver.buildTableName("users", "public")).toBe("public.users");
        });
    });

    describe("parseTableName", () => {
        it("should parse simple table name", () => {
            expect(driver.parseTableName("users")).toEqual({ tableName: "users" });
        });

        it("should parse schema-qualified table name", () => {
            expect(driver.parseTableName("public.users")).toEqual({
                schema: "public",
                tableName: "users",
            });
        });
    });

    describe("value preparation", () => {
        it("should serialize JSON values", () => {
            const meta = { type: "jsonb" } as any;
            const result = driver.preparePersistentValue({ key: "value" }, meta);
            expect(result).toBe('{"key":"value"}');
        });

        it("should format dates for timestamp", () => {
            const meta = { type: "timestamp" } as any;
            const date = new Date("2025-06-15T10:30:00Z");
            const result = driver.preparePersistentValue(date, meta);
            expect(result).toBe("2025-06-15T10:30:00.000Z");
        });

        it("should format dates for date column", () => {
            const meta = { type: "date" } as any;
            const date = new Date("2025-06-15T10:30:00Z");
            const result = driver.preparePersistentValue(date, meta);
            expect(result).toBe("2025-06-15");
        });

        it("should pass null through", () => {
            const meta = { type: "int" } as any;
            expect(driver.preparePersistentValue(null, meta)).toBeNull();
        });
    });

    describe("value hydration", () => {
        it("should parse JSON strings", () => {
            const meta = { type: "jsonb" } as any;
            expect(driver.prepareHydratedValue('{"a":1}', meta)).toEqual({ a: 1 });
        });

        it("should parse integer strings", () => {
            const meta = { type: "int" } as any;
            expect(driver.prepareHydratedValue("42", meta)).toBe(42);
        });

        it("should parse float strings", () => {
            const meta = { type: "float" } as any;
            expect(driver.prepareHydratedValue("3.14", meta)).toBeCloseTo(3.14);
        });

        it("should parse boolean strings", () => {
            const meta = { type: "boolean" } as any;
            expect(driver.prepareHydratedValue("true", meta)).toBe(true);
            expect(driver.prepareHydratedValue("false", meta)).toBe(false);
            expect(driver.prepareHydratedValue("t", meta)).toBe(true);
        });

        it("should parse date strings", () => {
            const meta = { type: "timestamp" } as any;
            const result = driver.prepareHydratedValue("2025-06-15T10:30:00.000Z", meta);
            expect(result).toBeInstanceOf(Date);
        });

        it("should pass null through", () => {
            const meta = { type: "int" } as any;
            expect(driver.prepareHydratedValue(null, meta)).toBeNull();
        });
    });

    describe("createQueryRunner", () => {
        it("should create a PyroSqlQueryRunner", async () => {
            await driver.connect();
            const runner = driver.createQueryRunner();
            expect(runner).toBeInstanceOf(PyroSqlQueryRunner);
        });
    });
});

// ---------------------------------------------------------------------------
// PyroSqlQueryRunner
// ---------------------------------------------------------------------------

describe("PyroSqlQueryRunner", () => {
    let driver: PyroSqlDriver;
    let runner: PyroSqlQueryRunner;

    beforeEach(async () => {
        jest.clearAllMocks();
        const ds = createMockDataSource();
        driver = new PyroSqlDriver(ds);
        await driver.connect();
        runner = driver.createQueryRunner();
    });

    describe("query execution", () => {
        it("should execute SELECT queries and return mapped rows", async () => {
            const result = await runner.query("SELECT id, name, active FROM users");
            expect(result).toEqual([
                { id: 1, name: "Alice", active: true },
                { id: 2, name: "Bob", active: false },
            ]);
        });

        it("should return structured results when requested", async () => {
            const result = await runner.query("SELECT id FROM users", [], true);
            expect(result).toHaveProperty("records");
            expect(result).toHaveProperty("raw");
            expect(result).toHaveProperty("affected");
            expect(result.records).toHaveLength(2);
        });

        it("should handle INSERT with RETURNING", async () => {
            mockClient.query.mockReturnValueOnce({
                columns: ["id"],
                rows: [[42]],
                rows_affected: 1,
            });
            const result = await runner.query("INSERT INTO users (name) VALUES ('Charlie') RETURNING id");
            expect(result).toEqual([{ id: 42 }]);
        });

        it("should handle DML without RETURNING", async () => {
            const result = await runner.query("UPDATE users SET name = 'Charlie' WHERE id = 1");
            expect(mockClient.execute).toHaveBeenCalled();
        });
    });

    describe("transactions", () => {
        it("should start a transaction", async () => {
            await runner.startTransaction();
            expect(runner.isTransactionActive).toBe(true);
            expect(mockClient.begin).toHaveBeenCalled();
        });

        it("should commit a transaction", async () => {
            await runner.startTransaction();
            await runner.commitTransaction();
            expect(runner.isTransactionActive).toBe(false);
        });

        it("should rollback a transaction", async () => {
            await runner.startTransaction();
            await runner.rollbackTransaction();
            expect(runner.isTransactionActive).toBe(false);
        });

        it("should throw when starting transaction twice", async () => {
            await runner.startTransaction();
            await expect(runner.startTransaction()).rejects.toThrow("already started");
        });

        it("should throw when committing without active transaction", async () => {
            await expect(runner.commitTransaction()).rejects.toThrow("No active transaction");
        });

        it("should throw when rolling back without active transaction", async () => {
            await expect(runner.rollbackTransaction()).rejects.toThrow("No active transaction");
        });

        it("should execute queries within transaction context", async () => {
            await runner.startTransaction();
            const tx = mockClient.begin.mock.results[0].value;
            await runner.query("SELECT 1");
            expect(tx.query).toHaveBeenCalled();
        });
    });

    describe("schema DDL - createTable", () => {
        it("should generate CREATE TABLE DDL", async () => {
            const table = new Table({
                name: "test_table",
                columns: [
                    {
                        name: "id",
                        type: "int",
                        isPrimary: true,
                        isGenerated: true,
                        generationStrategy: "increment",
                    },
                    {
                        name: "name",
                        type: "varchar",
                        length: "100",
                        isNullable: false,
                    },
                    {
                        name: "email",
                        type: "varchar",
                        length: "255",
                        isUnique: true,
                        isNullable: true,
                    },
                    {
                        name: "active",
                        type: "boolean",
                        default: "true",
                    },
                ],
            });

            await runner.createTable(table);

            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("CREATE TABLE");
            expect(executedSql).toContain('"test_table"');
            expect(executedSql).toContain("SERIAL");
            expect(executedSql).toContain("VARCHAR(100)");
            expect(executedSql).toContain("NOT NULL");
            expect(executedSql).toContain("UNIQUE");
            expect(executedSql).toContain("BOOLEAN");
            expect(executedSql).toContain("PRIMARY KEY");
        });

        it("should support IF NOT EXISTS", async () => {
            const table = new Table({
                name: "test_table",
                columns: [{ name: "id", type: "int", isPrimary: true }],
            });
            await runner.createTable(table, true);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("IF NOT EXISTS");
        });
    });

    describe("schema DDL - dropTable", () => {
        it("should generate DROP TABLE DDL", async () => {
            await runner.dropTable("test_table", true);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP TABLE");
            expect(executedSql).toContain("IF EXISTS");
            expect(executedSql).toContain('"test_table"');
            expect(executedSql).toContain("CASCADE");
        });
    });

    describe("schema DDL - columns", () => {
        it("should add a column", async () => {
            const col = new TableColumn({
                name: "age",
                type: "int",
                isNullable: true,
            });
            await runner.addColumn("users", col);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("ADD COLUMN");
            expect(executedSql).toContain('"age"');
            expect(executedSql).toContain("INT");
        });

        it("should drop a column", async () => {
            await runner.dropColumn("users", "age");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP COLUMN");
            expect(executedSql).toContain('"age"');
        });

        it("should rename a column", async () => {
            await runner.renameColumn("users", "old_name", "new_name");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("RENAME COLUMN");
            expect(executedSql).toContain('"old_name"');
            expect(executedSql).toContain('"new_name"');
        });
    });

    describe("schema DDL - indices", () => {
        it("should create an index", async () => {
            const index = new TableIndex({
                name: "idx_users_name",
                columnNames: ["name"],
            });
            await runner.createIndex("users", index);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("CREATE");
            expect(executedSql).toContain("INDEX");
            expect(executedSql).toContain('"idx_users_name"');
            expect(executedSql).toContain('"name"');
        });

        it("should create a unique index", async () => {
            const index = new TableIndex({
                name: "idx_users_email",
                columnNames: ["email"],
                isUnique: true,
            });
            await runner.createIndex("users", index);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("UNIQUE");
        });

        it("should create a partial index", async () => {
            const index = new TableIndex({
                name: "idx_users_active",
                columnNames: ["name"],
                where: "active = true",
            });
            await runner.createIndex("users", index);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("WHERE active = true");
        });

        it("should drop an index", async () => {
            await runner.dropIndex("users", "idx_users_name");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP INDEX");
        });
    });

    describe("schema DDL - foreign keys", () => {
        it("should create a foreign key", async () => {
            const fk = new TableForeignKey({
                name: "fk_orders_user",
                columnNames: ["user_id"],
                referencedTableName: "users",
                referencedColumnNames: ["id"],
                onDelete: "CASCADE",
                onUpdate: "NO ACTION",
            });
            await runner.createForeignKey("orders", fk);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("FOREIGN KEY");
            expect(executedSql).toContain('"user_id"');
            expect(executedSql).toContain("REFERENCES");
            expect(executedSql).toContain('"users"');
            expect(executedSql).toContain("ON DELETE CASCADE");
            expect(executedSql).toContain("ON UPDATE NO ACTION");
        });

        it("should drop a foreign key", async () => {
            await runner.dropForeignKey("orders", "fk_orders_user");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP CONSTRAINT");
            expect(executedSql).toContain('"fk_orders_user"');
        });
    });

    describe("schema DDL - unique constraints", () => {
        it("should create a unique constraint", async () => {
            const uc = new TableUnique({
                name: "uq_users_email",
                columnNames: ["email"],
            });
            await runner.createUniqueConstraint("users", uc);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("UNIQUE");
            expect(executedSql).toContain('"uq_users_email"');
        });
    });

    describe("schema DDL - check constraints", () => {
        it("should create a check constraint", async () => {
            const cc = new TableCheck({
                name: "chk_users_age",
                expression: "age >= 0",
            });
            await runner.createCheckConstraint("users", cc);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("CHECK");
            expect(executedSql).toContain("age >= 0");
        });
    });

    describe("schema introspection", () => {
        it("should check if table exists", async () => {
            mockClient.query.mockReturnValueOnce({
                columns: ["?column?"],
                rows: [[1]],
                rows_affected: 0,
            });
            const exists = await runner.hasTable("users");
            expect(exists).toBe(true);
        });

        it("should check if column exists", async () => {
            mockClient.query.mockReturnValueOnce({
                columns: ["?column?"],
                rows: [],
                rows_affected: 0,
            });
            const exists = await runner.hasColumn("users", "nonexistent");
            expect(exists).toBe(false);
        });
    });

    describe("schema DDL - views", () => {
        it("should create a view", async () => {
            const view = new (await import("typeorm")).View({
                name: "active_users",
                expression: "SELECT * FROM users WHERE active = true",
            });
            await runner.createView(view);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("CREATE");
            expect(executedSql).toContain("VIEW");
            expect(executedSql).toContain('"active_users"');
        });

        it("should drop a view", async () => {
            await runner.dropView("active_users");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP");
            expect(executedSql).toContain("VIEW");
        });
    });

    describe("schema DDL - database/schema", () => {
        it("should create a database", async () => {
            await runner.createDatabase("newdb", true);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("CREATE DATABASE");
            expect(executedSql).toContain("IF NOT EXISTS");
        });

        it("should drop a database", async () => {
            await runner.dropDatabase("olddb", true);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP DATABASE");
            expect(executedSql).toContain("IF EXISTS");
        });

        it("should create a schema", async () => {
            await runner.createSchema("myschema", true);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("CREATE SCHEMA");
        });

        it("should drop a schema with cascade", async () => {
            await runner.dropSchema("myschema", true, true);
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("DROP SCHEMA");
            expect(executedSql).toContain("CASCADE");
        });
    });

    describe("lifecycle", () => {
        it("should connect", async () => {
            const result = await runner.connect();
            expect(result).toBeDefined();
        });

        it("should release", async () => {
            await runner.release();
            expect(runner.isReleased).toBe(true);
        });

        it("should clear table", async () => {
            await runner.clearTable("users");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("TRUNCATE TABLE");
        });

        it("should rename table", async () => {
            await runner.renameTable("old_table", "new_table");
            const executedSql = mockClient.execute.mock.calls[0][0];
            expect(executedSql).toContain("RENAME TO");
        });
    });

    describe("stream (unsupported)", () => {
        it("should throw for stream", () => {
            expect(() => runner.stream("SELECT 1")).toThrow("not supported");
        });
    });

    describe("parameter interpolation", () => {
        it("should interpolate null parameters", async () => {
            await runner.query("SELECT $1", [null]);
            const executedSql = mockClient.query.mock.calls[0][0];
            expect(executedSql).toContain("NULL");
        });

        it("should interpolate boolean parameters", async () => {
            await runner.query("SELECT $1", [true]);
            const executedSql = mockClient.query.mock.calls[0][0];
            expect(executedSql).toContain("TRUE");
        });

        it("should interpolate numeric parameters", async () => {
            await runner.query("SELECT $1", [42]);
            const executedSql = mockClient.query.mock.calls[0][0];
            expect(executedSql).toContain("42");
        });

        it("should interpolate string parameters with escaping", async () => {
            await runner.query("SELECT $1", ["O'Brien"]);
            const executedSql = mockClient.query.mock.calls[0][0];
            expect(executedSql).toContain("O''Brien");
        });

        it("should interpolate Date parameters", async () => {
            const date = new Date("2025-01-15T00:00:00.000Z");
            await runner.query("SELECT $1", [date]);
            const executedSql = mockClient.query.mock.calls[0][0];
            expect(executedSql).toContain("2025-01-15");
        });

        it("should interpolate object parameters as JSON", async () => {
            await runner.query("SELECT $1", [{ key: "value" }]);
            const executedSql = mockClient.query.mock.calls[0][0];
            expect(executedSql).toContain('"key"');
        });
    });
});
