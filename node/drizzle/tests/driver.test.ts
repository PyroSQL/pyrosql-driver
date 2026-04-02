import { PyroSqlDialect } from "../src/dialect";
import { PyroSqlSession, PyroSqlPreparedQuery } from "../src/session";
import { PyroSqlDatabase } from "../src/driver";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

const mockQueryResult = {
    columns: ["id", "name", "email", "active"],
    rows: [
        [1, "Alice", "alice@example.com", true],
        [2, "Bob", "bob@example.com", false],
    ],
    rows_affected: 0,
};

const mockTransaction = {
    id: "tx-001",
    query: jest.fn().mockReturnValue(mockQueryResult),
    execute: jest.fn().mockReturnValue(1),
    commit: jest.fn(),
    rollback: jest.fn(),
};

const mockClient = {
    query: jest.fn().mockReturnValue(mockQueryResult),
    execute: jest.fn().mockReturnValue(1),
    begin: jest.fn().mockReturnValue(mockTransaction),
    close: jest.fn(),
    _interpolate: jest.fn((sql: string) => sql),
    _checkOpen: jest.fn(),
};

jest.mock("pyrosql", () => ({
    Client: jest.fn().mockImplementation(() => mockClient),
    Pool: jest.fn(),
    Transaction: jest.fn(),
}));

// ---------------------------------------------------------------------------
// PyroSqlDialect
// ---------------------------------------------------------------------------

describe("PyroSqlDialect", () => {
    let dialect: PyroSqlDialect;

    beforeEach(() => {
        dialect = new PyroSqlDialect();
    });

    describe("escapeName", () => {
        it("should wrap identifiers in double quotes", () => {
            expect(dialect.escapeName("users")).toBe('"users"');
            expect(dialect.escapeName("my table")).toBe('"my table"');
            expect(dialect.escapeName("ORDER")).toBe('"ORDER"');
        });
    });

    describe("escapeString", () => {
        it("should wrap strings in single quotes", () => {
            expect(dialect.escapeString("hello")).toBe("'hello'");
        });

        it("should double embedded single quotes", () => {
            expect(dialect.escapeString("O'Brien")).toBe("'O''Brien'");
            expect(dialect.escapeString("it's a 'test'")).toBe("'it''s a ''test'''");
        });
    });

    describe("escapeParam", () => {
        it("should generate $N-style parameters (1-indexed)", () => {
            expect(dialect.escapeParam(0)).toBe("$1");
            expect(dialect.escapeParam(1)).toBe("$2");
            expect(dialect.escapeParam(9)).toBe("$10");
        });
    });

    describe("DDL builders", () => {
        it("should build CREATE TABLE IF NOT EXISTS", () => {
            const sql = dialect.buildCreateTableIfNotExists(
                "users",
                '"id" SERIAL NOT NULL, "name" VARCHAR(100)',
                'PRIMARY KEY ("id")'
            );
            expect(sql).toBe(
                'CREATE TABLE IF NOT EXISTS "users" ("id" SERIAL NOT NULL, "name" VARCHAR(100), PRIMARY KEY ("id"))'
            );
        });

        it("should build CREATE TABLE without constraints", () => {
            const sql = dialect.buildCreateTableIfNotExists(
                "simple",
                '"id" INT',
                ""
            );
            expect(sql).toBe('CREATE TABLE IF NOT EXISTS "simple" ("id" INT)');
        });

        it("should build DROP TABLE", () => {
            expect(dialect.buildDropTable("users")).toBe(
                'DROP TABLE IF EXISTS "users" CASCADE'
            );
            expect(dialect.buildDropTable("users", false, false)).toBe(
                'DROP TABLE "users"'
            );
        });

        it("should build ADD COLUMN", () => {
            const sql = dialect.buildAddColumn("users", '"age" INT NOT NULL DEFAULT 0');
            expect(sql).toBe('ALTER TABLE "users" ADD COLUMN "age" INT NOT NULL DEFAULT 0');
        });

        it("should build DROP COLUMN", () => {
            expect(dialect.buildDropColumn("users", "age")).toBe(
                'ALTER TABLE "users" DROP COLUMN "age"'
            );
        });

        it("should build RENAME COLUMN", () => {
            expect(dialect.buildRenameColumn("users", "old_name", "new_name")).toBe(
                'ALTER TABLE "users" RENAME COLUMN "old_name" TO "new_name"'
            );
        });

        it("should build CREATE INDEX", () => {
            expect(dialect.buildCreateIndex("idx_users_name", "users", ["name"])).toBe(
                'CREATE INDEX "idx_users_name" ON "users" ("name")'
            );
        });

        it("should build CREATE UNIQUE INDEX", () => {
            expect(dialect.buildCreateIndex("idx_users_email", "users", ["email"], true)).toBe(
                'CREATE UNIQUE INDEX "idx_users_email" ON "users" ("email")'
            );
        });

        it("should build CREATE INDEX with WHERE", () => {
            expect(
                dialect.buildCreateIndex("idx_active", "users", ["name"], false, "active = true")
            ).toBe('CREATE INDEX "idx_active" ON "users" ("name") WHERE active = true');
        });

        it("should build multi-column CREATE INDEX", () => {
            expect(dialect.buildCreateIndex("idx_composite", "users", ["first_name", "last_name"])).toBe(
                'CREATE INDEX "idx_composite" ON "users" ("first_name", "last_name")'
            );
        });

        it("should build DROP INDEX", () => {
            expect(dialect.buildDropIndex("idx_users_name")).toBe(
                'DROP INDEX IF EXISTS "idx_users_name"'
            );
        });

        it("should build CREATE SCHEMA", () => {
            expect(dialect.buildCreateSchema("myschema")).toBe(
                'CREATE SCHEMA IF NOT EXISTS "myschema"'
            );
        });

        it("should build DROP SCHEMA with CASCADE", () => {
            expect(dialect.buildDropSchema("myschema", true, true)).toBe(
                'DROP SCHEMA IF EXISTS "myschema" CASCADE'
            );
        });

        it("should build ALTER COLUMN TYPE", () => {
            expect(dialect.buildAlterColumnType("users", "age", "BIGINT")).toBe(
                'ALTER TABLE "users" ALTER COLUMN "age" TYPE BIGINT'
            );
        });

        it("should build ADD FOREIGN KEY", () => {
            expect(
                dialect.buildAddForeignKey("orders", "fk_user", ["user_id"], "users", ["id"], "CASCADE", "NO ACTION")
            ).toBe(
                'ALTER TABLE "orders" ADD CONSTRAINT "fk_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE ON UPDATE NO ACTION'
            );
        });

        it("should build DROP CONSTRAINT", () => {
            expect(dialect.buildDropConstraint("orders", "fk_user")).toBe(
                'ALTER TABLE "orders" DROP CONSTRAINT "fk_user"'
            );
        });
    });
});

// ---------------------------------------------------------------------------
// PyroSqlPreparedQuery
// ---------------------------------------------------------------------------

describe("PyroSqlPreparedQuery", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe("execute", () => {
        it("should execute a SELECT query and return mapped rows", async () => {
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT id, name FROM users",
                [],
            );
            const result: any = await pq.execute();
            expect(result.rows).toHaveLength(2);
            expect(result.rows[0]).toEqual({ id: 1, name: "Alice", email: "alice@example.com", active: true });
        });

        it("should execute a DML query and return rowCount", async () => {
            mockClient.execute.mockReturnValueOnce(3);
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "UPDATE users SET active = FALSE",
                [],
            );
            const result: any = await pq.execute();
            expect(result.rowCount).toBe(3);
            expect(result.rows).toEqual([]);
        });

        it("should handle INSERT with RETURNING", async () => {
            mockClient.query.mockReturnValueOnce({
                columns: ["id"],
                rows: [[99]],
                rows_affected: 1,
            });
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "INSERT INTO users (name) VALUES ('X') RETURNING id",
                [],
            );
            const result: any = await pq.execute();
            expect(result.rows).toEqual([{ id: 99 }]);
        });

        it("should interpolate parameters", async () => {
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT * FROM users WHERE id = $1 AND name = $2",
                [42, "Alice"],
            );
            await pq.execute();
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("42")
            );
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("'Alice'")
            );
        });

        it("should interpolate null values", async () => {
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT * FROM users WHERE name = $1",
                [null],
            );
            await pq.execute();
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("NULL")
            );
        });

        it("should interpolate boolean values", async () => {
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT * FROM users WHERE active = $1",
                [true],
            );
            await pq.execute();
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("TRUE")
            );
        });

        it("should interpolate Date values", async () => {
            const date = new Date("2025-06-15T10:30:00Z");
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT * FROM events WHERE date = $1",
                [date],
            );
            await pq.execute();
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("2025-06-15")
            );
        });

        it("should use custom result mapper", async () => {
            const mapper = (rows: any[][]) => rows.map((r) => ({ userId: r[0] }));
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT id FROM users",
                [],
                mapper,
            );
            const result = await pq.execute();
            expect(result).toEqual([{ userId: 1 }, { userId: 2 }]);
        });
    });

    describe("all", () => {
        it("should return all rows as mapped objects", async () => {
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT id, name FROM users",
                [],
            );
            const result: any = await pq.all();
            expect(result).toHaveLength(2);
            expect(result[0]).toHaveProperty("id");
            expect(result[0]).toHaveProperty("name");
        });
    });

    describe("values", () => {
        it("should return raw row arrays", async () => {
            const pq = new PyroSqlPreparedQuery(
                mockClient,
                "SELECT id, name FROM users",
                [],
            );
            const result: any = await pq.values();
            expect(result).toHaveLength(2);
            expect(result[0]).toEqual([1, "Alice", "alice@example.com", true]);
        });
    });
});

// ---------------------------------------------------------------------------
// PyroSqlSession
// ---------------------------------------------------------------------------

describe("PyroSqlSession", () => {
    let session: PyroSqlSession;
    let dialect: PyroSqlDialect;

    beforeEach(() => {
        jest.clearAllMocks();
        dialect = new PyroSqlDialect();
        session = new PyroSqlSession(mockClient as any, dialect, undefined);
    });

    describe("execute", () => {
        it("should execute a SELECT query", async () => {
            const result: any = await session.execute("SELECT * FROM users");
            expect(result.rows).toHaveLength(2);
            expect(mockClient.query).toHaveBeenCalledWith("SELECT * FROM users");
        });

        it("should execute a DML statement", async () => {
            mockClient.execute.mockReturnValueOnce(5);
            const result: any = await session.execute("DELETE FROM users WHERE active = FALSE");
            expect(result.rowCount).toBe(5);
            expect(mockClient.execute).toHaveBeenCalled();
        });
    });

    describe("prepareQuery", () => {
        it("should create a PyroSqlPreparedQuery", () => {
            const pq = session.prepareQuery({
                sql: "SELECT * FROM users WHERE id = $1",
                params: [42],
            });
            expect(pq).toBeInstanceOf(PyroSqlPreparedQuery);
        });
    });

    describe("transaction", () => {
        it("should execute a transaction and commit on success", async () => {
            const result = await session.transaction(async (tx) => {
                return "tx-result";
            });
            expect(result).toBe("tx-result");
            expect(mockClient.begin).toHaveBeenCalled();
            expect(mockTransaction.commit).toHaveBeenCalled();
        });

        it("should rollback on error", async () => {
            await expect(
                session.transaction(async () => {
                    throw new Error("oops");
                })
            ).rejects.toThrow("oops");
            expect(mockTransaction.rollback).toHaveBeenCalled();
        });

        it("should set isolation level when configured", async () => {
            await session.transaction(
                async () => "done",
                { isolationLevel: "serializable" },
            );
            expect(mockTransaction.execute).toHaveBeenCalledWith(
                expect.stringContaining("ISOLATION LEVEL serializable")
            );
        });

        it("should set access mode when configured", async () => {
            await session.transaction(
                async () => "done",
                { accessMode: "read only" },
            );
            expect(mockTransaction.execute).toHaveBeenCalledWith(
                expect.stringContaining("read only")
            );
        });

        it("should execute queries within transaction context", async () => {
            await session.transaction(async (tx) => {
                const pq = tx.prepareQuery({
                    sql: "SELECT * FROM users",
                    params: [],
                });
                await pq.execute();
            });
            expect(mockTransaction.query).toHaveBeenCalled();
        });

        it("should execute DML within transaction context", async () => {
            await session.transaction(async (tx) => {
                await tx.execute("UPDATE users SET active = TRUE");
            });
            expect(mockTransaction.execute).toHaveBeenCalledWith(
                expect.stringContaining("UPDATE users")
            );
        });

        it("should support nested transactions via savepoints", async () => {
            await session.transaction(async (tx) => {
                await tx.transaction(async (nestedTx) => {
                    await nestedTx.execute("INSERT INTO users (name) VALUES ('nested')");
                });
            });
            expect(mockTransaction.execute).toHaveBeenCalledWith(
                expect.stringContaining("SAVEPOINT")
            );
            expect(mockTransaction.execute).toHaveBeenCalledWith(
                expect.stringContaining("RELEASE SAVEPOINT")
            );
        });

        it("should rollback to savepoint on nested transaction error", async () => {
            await session.transaction(async (tx) => {
                try {
                    await tx.transaction(async () => {
                        throw new Error("nested fail");
                    });
                } catch (e) {
                    // expected
                }
            });
            expect(mockTransaction.execute).toHaveBeenCalledWith(
                expect.stringContaining("ROLLBACK TO SAVEPOINT")
            );
        });
    });
});

// ---------------------------------------------------------------------------
// PyroSqlDatabase (drizzle wrapper)
// ---------------------------------------------------------------------------

describe("PyroSqlDatabase", () => {
    let db: PyroSqlDatabase;

    beforeEach(() => {
        jest.clearAllMocks();
        const dialect = new PyroSqlDialect();
        const session = new PyroSqlSession(mockClient as any, dialect, undefined);
        db = new PyroSqlDatabase(mockClient as any, dialect, session);
    });

    describe("execute", () => {
        it("should execute a SELECT query", async () => {
            const result: any = await db.execute("SELECT * FROM users");
            expect(result.rows).toHaveLength(2);
        });

        it("should execute a DML statement", async () => {
            mockClient.execute.mockReturnValueOnce(3);
            const result: any = await db.execute("DELETE FROM users");
            expect(result.rowCount).toBe(3);
        });
    });

    describe("select", () => {
        it("should return mapped rows", async () => {
            const rows = await db.select("SELECT * FROM users");
            expect(rows).toHaveLength(2);
            expect(rows[0]).toHaveProperty("id");
            expect(rows[0]).toHaveProperty("name");
        });

        it("should support parameters", async () => {
            await db.select("SELECT * FROM users WHERE id = $1", [1]);
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("1")
            );
        });
    });

    describe("run", () => {
        it("should return affected row count", async () => {
            mockClient.execute.mockReturnValueOnce(5);
            const count = await db.run("DELETE FROM users WHERE active = FALSE");
            expect(count).toBe(5);
        });
    });

    describe("transaction", () => {
        it("should run function inside transaction", async () => {
            const result = await db.transaction(async (tx) => {
                return "value";
            });
            expect(result).toBe("value");
            expect(mockClient.begin).toHaveBeenCalled();
            expect(mockTransaction.commit).toHaveBeenCalled();
        });
    });

    describe("close", () => {
        it("should close the client", () => {
            db.close();
            expect(mockClient.close).toHaveBeenCalled();
        });
    });
});

// ---------------------------------------------------------------------------
// drizzle() and drizzleFromClient()
// ---------------------------------------------------------------------------

describe("drizzle()", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it("should create a PyroSqlDatabase instance", () => {
        const { drizzle } = require("../src/driver");
        const db = drizzle({ url: "vsql://localhost:12520/testdb" });
        expect(db).toBeInstanceOf(PyroSqlDatabase);
        expect(db.session).toBeInstanceOf(PyroSqlSession);
        expect(db.dialect).toBeInstanceOf(PyroSqlDialect);
    });

    it("should accept schema parameter", () => {
        const { drizzle } = require("../src/driver");
        const schema = {};
        const db = drizzle({ url: "vsql://localhost:12520/testdb", schema });
        expect(db).toBeDefined();
    });
});

describe("drizzleFromClient()", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it("should create a PyroSqlDatabase from an existing client", () => {
        const { drizzleFromClient } = require("../src/driver");
        const db = drizzleFromClient(mockClient);
        expect(db).toBeInstanceOf(PyroSqlDatabase);
    });
});

// ---------------------------------------------------------------------------
// Parameter interpolation edge cases
// ---------------------------------------------------------------------------

describe("Parameter interpolation edge cases", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it("should handle $10+ parameters without confusion", async () => {
        const args = Array.from({ length: 12 }, (_, i) => i * 10);
        const placeholders = args.map((_, i) => `$${i + 1}`).join(", ");
        const pq = new PyroSqlPreparedQuery(
            mockClient,
            `SELECT ${placeholders}`,
            args,
        );
        await pq.execute();
        const calledSql = mockClient.query.mock.calls[0][0];
        expect(calledSql).toContain("90");
        expect(calledSql).toContain("100");
        expect(calledSql).toContain("110");
    });

    it("should handle string values containing dollar signs", async () => {
        const pq = new PyroSqlPreparedQuery(
            mockClient,
            "INSERT INTO t (price) VALUES ($1)",
            ["$99.99"],
        );
        await pq.execute();
        const calledSql = mockClient.execute.mock.calls[0][0];
        expect(calledSql).toContain("'$99.99'");
    });

    it("should handle empty params", async () => {
        const pq = new PyroSqlPreparedQuery(
            mockClient,
            "SELECT 1",
            [],
        );
        await pq.execute();
        expect(mockClient.query).toHaveBeenCalledWith("SELECT 1");
    });

    it("should handle object params as JSON", async () => {
        const pq = new PyroSqlPreparedQuery(
            mockClient,
            "INSERT INTO t (data) VALUES ($1)",
            [{ key: "value" }],
        );
        await pq.execute();
        const calledSql = mockClient.execute.mock.calls[0][0];
        expect(calledSql).toContain('"key"');
    });

    it("should handle Buffer params as hex", async () => {
        const buf = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF]);
        const pq = new PyroSqlPreparedQuery(
            mockClient,
            "INSERT INTO t (data) VALUES ($1)",
            [buf],
        );
        await pq.execute();
        const calledSql = mockClient.execute.mock.calls[0][0];
        expect(calledSql).toContain("deadbeef");
    });
});
