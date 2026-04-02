import { PyroSqlAdapter } from "../src/adapter";
import { PyroSqlConnector } from "../src/connector";

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

const mockPool = {
    get: jest.fn(),
    destroy: jest.fn(),
};

jest.mock("pyrosql", () => ({
    Client: jest.fn().mockImplementation(() => mockClient),
    Pool: jest.fn().mockImplementation(() => mockPool),
    Transaction: jest.fn(),
}));

// Helper to create a SqlQuery
function sqlQuery(sql: string, args: any[] = []) {
    return {
        sql,
        args,
        argTypes: args.map(() => ({ scalarType: "unknown" as const, arity: "scalar" as const })),
    };
}

// ---------------------------------------------------------------------------
// PyroSqlConnector
// ---------------------------------------------------------------------------

describe("PyroSqlConnector", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it("should connect and expose a client", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        connector.connect();
        expect(connector.isConnected).toBe(true);
        expect(connector.getClient()).toBeDefined();
    });

    it("should not reconnect if already connected", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        connector.connect();
        connector.connect();
        const { Client } = require("pyrosql");
        expect(Client).toHaveBeenCalledTimes(1);
    });

    it("should close connections", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        connector.connect();
        connector.close();
        expect(connector.isConnected).toBe(false);
        expect(mockClient.close).toHaveBeenCalled();
        expect(mockPool.destroy).toHaveBeenCalled();
    });

    it("should auto-connect on getClient", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        const client = connector.getClient();
        expect(client).toBeDefined();
        expect(connector.isConnected).toBe(true);
    });

    it("should execute raw queries", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        const result = connector.queryRaw("SELECT 1");
        expect(result.columns).toBeDefined();
        expect(result.rows).toBeDefined();
    });

    it("should execute raw DML", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        const affected = connector.executeRaw("UPDATE users SET name = 'X'");
        expect(affected).toBe(1);
    });

    it("should begin a transaction", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
        });
        const tx = connector.beginTransaction();
        expect(tx).toBeDefined();
        expect(mockClient.begin).toHaveBeenCalled();
    });

    it("should skip pool when usePool is false", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
            usePool: false,
        });
        connector.connect();
        const { Pool } = require("pyrosql");
        expect(Pool).not.toHaveBeenCalled();
    });

    it("should configure pool size", () => {
        const connector = new PyroSqlConnector({
            url: "vsql://localhost:12520/testdb",
            poolSize: 20,
        });
        connector.connect();
        const { Pool } = require("pyrosql");
        expect(Pool).toHaveBeenCalledWith("vsql://localhost:12520/testdb", 20);
    });
});

// ---------------------------------------------------------------------------
// PyroSqlAdapter - Queryable
// ---------------------------------------------------------------------------

describe("PyroSqlAdapter", () => {
    let adapter: PyroSqlAdapter;

    beforeEach(() => {
        jest.clearAllMocks();
        adapter = new PyroSqlAdapter({
            url: "vsql://localhost:12520/testdb",
        });
    });

    afterEach(async () => {
        await adapter.dispose();
    });

    describe("queryRaw", () => {
        it("should execute a SELECT query and return SqlResultSet", async () => {
            const result = await adapter.queryRaw(
                sqlQuery("SELECT id, name, active FROM users")
            );
            expect(result.columnNames).toEqual(["id", "name", "active"]);
            expect(result.rows).toHaveLength(2);
            expect(result.rows[0][0]).toBe(1);
            expect(result.rows[0][1]).toBe("Alice");
            expect(result.rows[0][2]).toBe(true);
        });

        it("should handle SELECT with parameters", async () => {
            await adapter.queryRaw(
                sqlQuery("SELECT * FROM users WHERE id = $1", [42])
            );
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("42")
            );
        });

        it("should handle INSERT with RETURNING", async () => {
            mockClient.query.mockReturnValueOnce({
                columns: ["id"],
                rows: [[99]],
                rows_affected: 1,
            });
            const result = await adapter.queryRaw(
                sqlQuery("INSERT INTO users (name) VALUES ($1) RETURNING id", ["Charlie"])
            );
            expect(result.rows[0][0]).toBe(99);
        });

        it("should handle DML without RETURNING", async () => {
            const result = await adapter.queryRaw(
                sqlQuery("UPDATE users SET active = TRUE")
            );
            expect(result.columnNames).toEqual([]);
            expect(result.rows).toEqual([]);
            expect(mockClient.execute).toHaveBeenCalled();
        });

        it("should handle errors by throwing", async () => {
            mockClient.query.mockImplementationOnce(() => {
                throw new Error("connection lost");
            });
            await expect(
                adapter.queryRaw(sqlQuery("SELECT * FROM nonexistent"))
            ).rejects.toThrow("connection lost");
        });

        it("should interpolate null parameters", async () => {
            await adapter.queryRaw(
                sqlQuery("SELECT * FROM users WHERE name = $1", [null])
            );
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("NULL")
            );
        });

        it("should interpolate boolean parameters", async () => {
            await adapter.queryRaw(
                sqlQuery("SELECT * FROM users WHERE active = $1", [true])
            );
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("TRUE")
            );
        });

        it("should interpolate string parameters with escaping", async () => {
            await adapter.queryRaw(
                sqlQuery("SELECT * FROM users WHERE name = $1", ["O'Brien"])
            );
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("O''Brien")
            );
        });

        it("should interpolate Date parameters", async () => {
            const date = new Date("2025-06-15T10:30:00Z");
            await adapter.queryRaw(
                sqlQuery("SELECT * FROM events WHERE created_at > $1", [date])
            );
            expect(mockClient.query).toHaveBeenCalledWith(
                expect.stringContaining("2025-06-15")
            );
        });
    });

    describe("executeRaw", () => {
        it("should execute DML and return affected rows", async () => {
            const result = await adapter.executeRaw(
                sqlQuery("UPDATE users SET active = $1 WHERE id = $2", [false, 1])
            );
            expect(result).toBe(1);
        });

        it("should handle DELETE", async () => {
            mockClient.execute.mockReturnValueOnce(3);
            const result = await adapter.executeRaw(
                sqlQuery("DELETE FROM users WHERE active = FALSE")
            );
            expect(result).toBe(3);
        });

        it("should handle INSERT with RETURNING via executeRaw", async () => {
            mockClient.query.mockReturnValueOnce({
                columns: ["id"],
                rows: [[10]],
                rows_affected: 1,
            });
            const result = await adapter.executeRaw(
                sqlQuery("INSERT INTO users (name) VALUES ('X') RETURNING id")
            );
            expect(result).toBe(1);
        });

        it("should throw on errors", async () => {
            mockClient.execute.mockImplementationOnce(() => {
                throw new Error("syntax error");
            });
            await expect(
                adapter.executeRaw(sqlQuery("INVALID SQL"))
            ).rejects.toThrow("syntax error");
        });
    });

    describe("executeScript", () => {
        it("should execute multiple statements", async () => {
            await adapter.executeScript(
                "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)"
            );
            expect(mockClient.execute).toHaveBeenCalledTimes(2);
            expect(mockClient.execute).toHaveBeenCalledWith("CREATE TABLE t1 (id INT)");
            expect(mockClient.execute).toHaveBeenCalledWith("CREATE TABLE t2 (id INT)");
        });

        it("should skip empty statements", async () => {
            await adapter.executeScript("CREATE TABLE t1 (id INT); ; ;");
            expect(mockClient.execute).toHaveBeenCalledTimes(1);
        });
    });

    describe("connectionInfo", () => {
        it("should return schema name and relation join support", () => {
            const info = adapter.getConnectionInfo();
            expect(info.schemaName).toBe("public");
            expect(info.supportsRelationJoins).toBe(true);
        });
    });

    describe("dispose", () => {
        it("should dispose the adapter cleanly", async () => {
            await adapter.dispose();
            expect(mockClient.close).toHaveBeenCalled();
        });
    });

    describe("adapterName and provider", () => {
        it("should have correct adapter name", () => {
            expect(adapter.adapterName).toBe("prisma-pyrosql");
        });

        it("should have postgres provider", () => {
            expect(adapter.provider).toBe("postgres");
        });
    });
});

// ---------------------------------------------------------------------------
// PyroSqlAdapter - Transactions
// ---------------------------------------------------------------------------

describe("PyroSqlAdapter Transactions", () => {
    let adapter: PyroSqlAdapter;

    beforeEach(() => {
        jest.clearAllMocks();
        adapter = new PyroSqlAdapter({
            url: "vsql://localhost:12520/testdb",
        });
    });

    afterEach(async () => {
        await adapter.dispose();
    });

    it("should start a transaction", async () => {
        const tx = await adapter.startTransaction();
        expect(tx).toBeDefined();
        expect(mockClient.begin).toHaveBeenCalled();
    });

    it("should execute queries within a transaction", async () => {
        const tx = await adapter.startTransaction();
        const result = await tx.queryRaw(
            sqlQuery("SELECT * FROM users")
        );
        expect(result.columnNames).toEqual(["id", "name", "active"]);
        expect(mockTransaction.query).toHaveBeenCalled();
    });

    it("should execute DML within a transaction", async () => {
        const tx = await adapter.startTransaction();
        const affected = await tx.executeRaw(
            sqlQuery("UPDATE users SET name = 'X' WHERE id = 1")
        );
        expect(affected).toBe(1);
    });

    it("should commit a transaction", async () => {
        const tx = await adapter.startTransaction();
        await tx.commit();
        expect(mockTransaction.commit).toHaveBeenCalled();
    });

    it("should rollback a transaction", async () => {
        const tx = await adapter.startTransaction();
        await tx.rollback();
        expect(mockTransaction.rollback).toHaveBeenCalled();
    });

    it("should handle commit failure", async () => {
        mockTransaction.commit.mockImplementationOnce(() => {
            throw new Error("commit failed");
        });
        const tx = await adapter.startTransaction();
        await expect(tx.commit()).rejects.toThrow("commit failed");
    });

    it("should handle rollback failure", async () => {
        mockTransaction.rollback.mockImplementationOnce(() => {
            throw new Error("rollback failed");
        });
        const tx = await adapter.startTransaction();
        await expect(tx.rollback()).rejects.toThrow("rollback failed");
    });

    it("should handle transaction start failure", async () => {
        mockClient.begin.mockImplementationOnce(() => {
            throw new Error("cannot begin");
        });
        await expect(adapter.startTransaction()).rejects.toThrow("cannot begin");
    });

    it("should set isolation level when provided", async () => {
        await adapter.startTransaction("SERIALIZABLE");
        expect(mockTransaction.execute).toHaveBeenCalledWith(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        );
    });

    it("should have correct adapter name and provider on transaction", async () => {
        const tx = await adapter.startTransaction();
        expect(tx.adapterName).toBe("prisma-pyrosql");
        expect(tx.provider).toBe("postgres");
    });
});

// ---------------------------------------------------------------------------
// Column type mapping
// ---------------------------------------------------------------------------

describe("Column type mapping", () => {
    it("should correctly handle various column types in query results", async () => {
        jest.clearAllMocks();
        const adapter = new PyroSqlAdapter({
            url: "vsql://localhost:12520/testdb",
        });

        mockClient.query.mockReturnValueOnce({
            columns: ["int_col", "text_col", "bool_col", "json_col"],
            rows: [
                [42, "hello", true, '{"key":"val"}'],
                [null, null, null, null],
            ],
            rows_affected: 0,
        });

        const result = await adapter.queryRaw(
            sqlQuery("SELECT int_col, text_col, bool_col, json_col FROM typed_table")
        );
        expect(result.columnNames).toEqual(["int_col", "text_col", "bool_col", "json_col"]);
        expect(result.rows).toHaveLength(2);
        expect(result.rows[0][0]).toBe(42);
        expect(result.rows[0][1]).toBe("hello");
        expect(result.rows[0][2]).toBe(true);
        expect(result.rows[1][0]).toBeNull();
        expect(result.rows[1][1]).toBeNull();

        await adapter.dispose();
    });
});

// ---------------------------------------------------------------------------
// Parameter interpolation edge cases
// ---------------------------------------------------------------------------

describe("Parameter interpolation edge cases", () => {
    let adapter: PyroSqlAdapter;

    beforeEach(() => {
        jest.clearAllMocks();
        adapter = new PyroSqlAdapter({
            url: "vsql://localhost:12520/testdb",
        });
    });

    afterEach(async () => {
        await adapter.dispose();
    });

    it("should handle multiple parameters in correct order", async () => {
        await adapter.queryRaw(
            sqlQuery("SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1", [10, "hello"])
        );
        const calledSql = mockClient.query.mock.calls[0][0];
        expect(calledSql).toContain("a = 10");
        expect(calledSql).toContain("b = 'hello'");
        expect(calledSql).toContain("c = 10");
    });

    it("should handle $10+ parameters without confusion", async () => {
        const args = Array.from({ length: 12 }, (_, i) => i);
        await adapter.queryRaw(
            sqlQuery(
                "SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12",
                args
            )
        );
        const calledSql = mockClient.query.mock.calls[0][0];
        expect(calledSql).toContain("9");
        expect(calledSql).toContain("11");
    });

    it("should handle object parameters as JSON", async () => {
        await adapter.queryRaw(
            sqlQuery("INSERT INTO t (data) VALUES ($1)", [{ nested: { key: "value" } }])
        );
        const calledSql = mockClient.execute.mock.calls[0][0];
        expect(calledSql).toContain('"nested"');
    });
});
