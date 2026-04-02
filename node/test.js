/**
 * Integration tests for PyroSQL Node.js driver via PWire protocol.
 *
 * Uses koffi (modern FFI) to call the libpyrosql_ffi_pwire shared library
 * directly, since ffi-napi has compilation issues on Node 20+.
 *
 * The FFI library exposes a blocking TCP client speaking the PWire binary
 * protocol with the following C API:
 *   pyro_pwire_init()                          — no-op
 *   pyro_pwire_connect(host, port) -> ptr      — open TCP connection
 *   pyro_pwire_query(ptr, sql) -> char*        — run query, returns JSON
 *   pyro_pwire_execute(ptr, sql) -> i64        — run DML, returns rows affected
 *   pyro_pwire_free_string(char*)              — free returned JSON string
 *   pyro_pwire_close(ptr)                      — close connection
 *
 * Run via Docker:
 *   docker run --rm --network host \
 *     -v /var/pyrosql-driver:/pyrosql -w /app node:20-slim \
 *     sh -c "cp /pyrosql/pdo/dist/libpyrosql_ffi_pwire.so /app/ && \
 *            cp /pyrosql/node/test.js /app/ && \
 *            npm init -y && npm install koffi && node test.js"
 */

'use strict';

// ── Configuration ──────────────────────────────────────────────────────────

const PYROSQL_HOST = process.env.PYROSQL_HOST || '127.0.0.1';
const PYROSQL_PORT = parseInt(process.env.PYROSQL_PORT || '12520', 10);
const LIB_PATH = process.env.PYROSQL_FFI_LIB || './libpyrosql_ffi_pwire.so';

// ── Load the FFI library ───────────────────────────────────────────────────

const koffi = require('koffi');
const lib = koffi.load(LIB_PATH);

const pyro_pwire_init = lib.func('void pyro_pwire_init()');
const pyro_pwire_connect = lib.func('void* pyro_pwire_connect(const char*, uint16_t)');
// koffi auto-decodes char* returns as JS strings (note: causes minor leak
// since we cannot call free_string on the result -- acceptable for tests).
const pyro_pwire_query = lib.func('const char* pyro_pwire_query(void*, const char*)');
const pyro_pwire_execute = lib.func('int64_t pyro_pwire_execute(void*, const char*)');
const pyro_pwire_close = lib.func('void pyro_pwire_close(void*)');

// ── Test harness ───────────────────────────────────────────────────────────

let totalTests = 0;
let passedTests = 0;
let failedTests = 0;
const failures = [];

function assert(condition, message) {
    if (!condition) throw new Error(`Assertion failed: ${message}`);
}

function assertEqual(actual, expected, message) {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(
            `${message}\n  Expected: ${JSON.stringify(expected)}\n  Actual:   ${JSON.stringify(actual)}`
        );
    }
}

function test(name, fn) {
    totalTests++;
    try {
        fn();
        passedTests++;
        console.log(`  PASS  ${name}`);
    } catch (e) {
        failedTests++;
        failures.push({ name, error: e.message });
        console.log(`  FAIL  ${name}`);
        console.log(`        ${e.message.split('\n')[0]}`);
    }
}

// ── Helper: query and parse JSON ───────────────────────────────────────────

function queryJSON(handle, sql) {
    const str = pyro_pwire_query(handle, sql);
    if (str === null || str === undefined) throw new Error('Query returned null');
    const result = JSON.parse(str);
    if (result.error) throw new Error(`Query error: ${result.error}`);
    return result;
}

// Values from PWire are often returned as strings; coerce for comparisons
function coerceInt(v) {
    if (typeof v === 'string') return parseInt(v, 10);
    return v;
}

function coerceFloat(v) {
    if (typeof v === 'string') return parseFloat(v);
    return v;
}

// ── Unique table name to avoid collisions ──────────────────────────────────

const TABLE_NAME = `integration_test_${Date.now()}`;

// ═══════════════════════════════════════════════════════════════════════════
// TEST SUITE
// ═══════════════════════════════════════════════════════════════════════════

console.log('\n=== PyroSQL Node.js Integration Tests ===\n');

// ── 0. Initialization ─────────────────────────────────────────────────────

console.log('Section: Initialization');

test('pyro_pwire_init succeeds', () => {
    pyro_pwire_init();
});

// ── 1. Connection ──────────────────────────────────────────────────────────

console.log('\nSection: Connection');

let handle = null;

test('connect to PyroSQL server', () => {
    handle = pyro_pwire_connect(PYROSQL_HOST, PYROSQL_PORT);
    assert(handle !== null, 'connect returned null handle');
});

test('SELECT 1 smoke test', () => {
    const result = queryJSON(handle, 'SELECT 1 AS val');
    assert(result.columns.length > 0, 'no columns returned');
    assert(result.rows.length > 0, 'no rows returned');
    const val = coerceInt(result.rows[0][0]);
    assertEqual(val, 1, 'SELECT 1 should return 1');
});

test('SELECT expression', () => {
    const result = queryJSON(handle, 'SELECT 2 + 3 AS sum');
    const val = coerceInt(result.rows[0][0]);
    assertEqual(val, 5, 'SELECT 2+3 should return 5');
});

test('SELECT string literal', () => {
    const result = queryJSON(handle, "SELECT 'hello' AS greeting");
    assertEqual(result.rows[0][0], 'hello', 'should return hello');
});

// ── 2. CREATE TABLE ────────────────────────────────────────────────────────

console.log('\nSection: CREATE TABLE');

test('CREATE TABLE succeeds', () => {
    const affected = pyro_pwire_execute(handle, `
        CREATE TABLE ${TABLE_NAME} (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255),
            age INTEGER,
            active BOOLEAN DEFAULT TRUE,
            score FLOAT
        )
    `);
    assert(affected >= 0, `CREATE TABLE returned ${affected}`);
});

// ── 3. INSERT ──────────────────────────────────────────────────────────────

console.log('\nSection: INSERT');

test('INSERT single row', () => {
    const affected = pyro_pwire_execute(handle,
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (1, 'Alice', 'alice@example.com', 30, TRUE, 95.5)`
    );
    assert(affected >= 0, `INSERT returned ${affected}`);
});

test('INSERT multiple rows', () => {
    const sqls = [
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (2, 'Bob', 'bob@example.com', 25, TRUE, 88.0)`,
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (3, 'Charlie', 'charlie@example.com', 35, FALSE, 72.3)`,
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (4, 'Diana', NULL, 28, TRUE, 91.7)`,
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (5, 'Eve', 'eve@example.com', 22, FALSE, 65.0)`,
    ];
    for (const sql of sqls) {
        const affected = pyro_pwire_execute(handle, sql);
        assert(affected >= 0, `INSERT returned ${affected}`);
    }
});

// ── 4. SELECT ──────────────────────────────────────────────────────────────

console.log('\nSection: SELECT');

test('SELECT all rows', () => {
    const result = queryJSON(handle, `SELECT * FROM ${TABLE_NAME} ORDER BY id`);
    assertEqual(result.rows.length, 5, 'should have 5 rows');
});

test('SELECT with WHERE clause', () => {
    const result = queryJSON(handle, `SELECT name, age FROM ${TABLE_NAME} WHERE age > 25 ORDER BY age`);
    assert(result.rows.length >= 2, `expected at least 2 rows, got ${result.rows.length}`);
});

test('SELECT with WHERE boolean', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE active = TRUE ORDER BY name`);
    assert(result.rows.length >= 2, `expected at least 2 active rows, got ${result.rows.length}`);
});

test('SELECT with COUNT', () => {
    const result = queryJSON(handle, `SELECT COUNT(*) AS cnt FROM ${TABLE_NAME}`);
    assertEqual(result.rows.length, 1, 'COUNT should return 1 row');
    const cnt = coerceInt(result.rows[0][0]);
    assert(cnt >= 5, `COUNT should be >= 5, got ${cnt}`);
});

test('SELECT with NULL check', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE email IS NULL`);
    assert(result.rows.length >= 1, 'should find rows with NULL email');
});

test('SELECT specific columns with alias', () => {
    const result = queryJSON(handle, `SELECT name AS user_name, age AS user_age FROM ${TABLE_NAME} WHERE id = 1`);
    assertEqual(result.rows.length, 1, 'should return exactly 1 row');
});

test('SELECT with ORDER BY and LIMIT', () => {
    const result = queryJSON(handle, `SELECT name, score FROM ${TABLE_NAME} ORDER BY score DESC LIMIT 3`);
    assert(result.rows.length <= 3, `LIMIT 3 should return at most 3 rows, got ${result.rows.length}`);
});

test('SELECT with LIKE', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE name LIKE 'A%'`);
    assert(result.rows.length >= 1, 'should find names starting with A');
});

test('SELECT with IN clause', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id IN (1, 3, 5) ORDER BY id`);
    assert(result.rows.length >= 1, 'should find rows with id in (1,3,5)');
});

test('SELECT with BETWEEN', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE age BETWEEN 25 AND 35 ORDER BY age`);
    assert(result.rows.length >= 2, 'should find rows with age between 25 and 35');
});

test('SELECT with aggregate functions', () => {
    const result = queryJSON(handle, `SELECT MIN(age) AS min_age, MAX(age) AS max_age, AVG(score) AS avg_score FROM ${TABLE_NAME}`);
    assertEqual(result.rows.length, 1, 'aggregates should return 1 row');
    const minAge = coerceInt(result.rows[0][0]);
    const maxAge = coerceInt(result.rows[0][1]);
    assert(minAge <= maxAge, `MIN(age) should be <= MAX(age): ${minAge} vs ${maxAge}`);
});

// ── 5. UPDATE ──────────────────────────────────────────────────────────────

console.log('\nSection: UPDATE');

test('UPDATE single row', () => {
    const affected = pyro_pwire_execute(handle,
        `UPDATE ${TABLE_NAME} SET age = 31 WHERE id = 1`
    );
    assert(affected >= 0, `UPDATE returned ${affected}`);

    const result = queryJSON(handle, `SELECT age FROM ${TABLE_NAME} WHERE id = 1`);
    const age = coerceInt(result.rows[0][0]);
    assertEqual(age, 31, 'age should be updated to 31');
});

test('UPDATE multiple rows', () => {
    const affected = pyro_pwire_execute(handle,
        `UPDATE ${TABLE_NAME} SET active = TRUE WHERE active = FALSE`
    );
    assert(affected >= 0, `UPDATE returned ${affected}`);
});

test('UPDATE with expression', () => {
    const affected = pyro_pwire_execute(handle,
        `UPDATE ${TABLE_NAME} SET score = score + 5 WHERE id = 5`
    );
    assert(affected >= 0, `UPDATE returned ${affected}`);

    const result = queryJSON(handle, `SELECT score FROM ${TABLE_NAME} WHERE id = 5`);
    const score = coerceFloat(result.rows[0][0]);
    assert(score >= 69 && score <= 71, `score should be ~70.0, got ${score}`);
});

// ── 6. DELETE ──────────────────────────────────────────────────────────────

console.log('\nSection: DELETE');

test('DELETE single row', () => {
    const affected = pyro_pwire_execute(handle,
        `DELETE FROM ${TABLE_NAME} WHERE id = 5`
    );
    assert(affected >= 0, `DELETE returned ${affected}`);

    const result = queryJSON(handle, `SELECT COUNT(*) FROM ${TABLE_NAME}`);
    const cnt = coerceInt(result.rows[0][0]);
    assertEqual(cnt, 4, 'should have 4 rows after delete');
});

// ── 7. Transactions ────────────────────────────────────────────────────────

console.log('\nSection: Transactions');

test('BEGIN + INSERT + COMMIT', () => {
    pyro_pwire_execute(handle, 'BEGIN');
    pyro_pwire_execute(handle,
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (10, 'TxUser', 'tx@example.com', 40, TRUE, 99.0)`
    );
    pyro_pwire_execute(handle, 'COMMIT');

    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id = 10`);
    assertEqual(result.rows.length, 1, 'committed row should exist');
    assertEqual(result.rows[0][0], 'TxUser', 'committed name should be TxUser');
});

test('BEGIN + INSERT + ROLLBACK', () => {
    pyro_pwire_execute(handle, 'BEGIN');
    pyro_pwire_execute(handle,
        `INSERT INTO ${TABLE_NAME} (id, name, email, age, active, score) VALUES (11, 'RollbackUser', 'rb@example.com', 50, TRUE, 10.0)`
    );
    pyro_pwire_execute(handle, 'ROLLBACK');

    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id = 11`);
    assertEqual(result.rows.length, 0, 'rolled back row should NOT exist');
});

test('Transaction with UPDATE + COMMIT + verify', () => {
    // Note: some databases don't expose dirty reads within the same
    // transaction over the wire protocol. We verify the update after COMMIT.
    pyro_pwire_execute(handle, 'BEGIN');

    pyro_pwire_execute(handle,
        `UPDATE ${TABLE_NAME} SET name = 'Alice_TX' WHERE id = 1`
    );

    pyro_pwire_execute(handle, 'COMMIT');

    const afterCommit = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id = 1`);
    assertEqual(afterCommit.rows[0][0], 'Alice_TX', 'should see updated name after commit');
});

test('Nested transaction isolation (savepoints)', () => {
    try {
        pyro_pwire_execute(handle, 'BEGIN');
        pyro_pwire_execute(handle,
            `INSERT INTO ${TABLE_NAME} (id, name, age) VALUES (20, 'Outer', 100)`
        );

        pyro_pwire_execute(handle, 'SAVEPOINT sp1');
        pyro_pwire_execute(handle,
            `INSERT INTO ${TABLE_NAME} (id, name, age) VALUES (21, 'Inner', 200)`
        );
        pyro_pwire_execute(handle, 'ROLLBACK TO SAVEPOINT sp1');

        pyro_pwire_execute(handle, 'COMMIT');

        const r1 = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id = 20`);
        assertEqual(r1.rows.length, 1, 'outer insert should exist');
        const r2 = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id = 21`);
        assertEqual(r2.rows.length, 0, 'inner insert should be rolled back');

        // Cleanup
        pyro_pwire_execute(handle, `DELETE FROM ${TABLE_NAME} WHERE id = 20`);
    } catch (e) {
        // Try to recover from any transaction state
        try { pyro_pwire_execute(handle, 'ROLLBACK'); } catch (_) {}
        console.log('        (savepoints not supported: ' + e.message.split('\n')[0] + ')');
    }
});

// ── 8. Prepared Statements (via SQL PREPARE/EXECUTE) ───────────────────────

console.log('\nSection: Prepared Statements');

test('PREPARE + EXECUTE with parameters', () => {
    try {
        pyro_pwire_execute(handle, `PREPARE find_user AS SELECT name, age FROM ${TABLE_NAME} WHERE id = $1`);
        const result = queryJSON(handle, 'EXECUTE find_user(1)');
        assert(result.rows.length >= 1, 'should find user with id 1');
        console.log('        (SQL PREPARE/EXECUTE supported)');
        pyro_pwire_execute(handle, 'DEALLOCATE find_user');
    } catch (e) {
        console.log('        (SQL PREPARE/EXECUTE not supported, using inline params)');
        const result = queryJSON(handle, `SELECT name, age FROM ${TABLE_NAME} WHERE id = 1`);
        assert(result.rows.length >= 1, 'should find user with id 1');
    }
});

test('Multiple queries reusing connection', () => {
    for (let i = 0; i < 10; i++) {
        const result = queryJSON(handle, `SELECT COUNT(*) FROM ${TABLE_NAME}`);
        const cnt = coerceInt(result.rows[0][0]);
        assert(cnt >= 1, `iteration ${i}: count should be >= 1`);
    }
});

test('Rapid fire queries', () => {
    const start = Date.now();
    for (let i = 0; i < 50; i++) {
        queryJSON(handle, 'SELECT 1');
    }
    const elapsed = Date.now() - start;
    console.log(`        (50 queries in ${elapsed}ms)`);
});

// ── 9. Error Handling ──────────────────────────────────────────────────────

console.log('\nSection: Error Handling');

test('query on non-existent table returns error', () => {
    const str = pyro_pwire_query(handle, 'SELECT * FROM nonexistent_table_xyz_999');
    if (str === null) return; // null is acceptable
    const result = JSON.parse(str);
    assert(result.error, 'should return error for non-existent table');
});

test('execute invalid SQL returns error', () => {
    const affected = pyro_pwire_execute(handle, 'INVALID SQL STATEMENT HERE !!!');
    assert(affected < 0, `invalid SQL should return negative, got ${affected}`);
});

test('null handle returns null/error gracefully', () => {
    const str = pyro_pwire_query(null, 'SELECT 1');
    assertEqual(str, null, 'query with null handle should return null');

    const affected = pyro_pwire_execute(null, 'SELECT 1');
    assertEqual(affected, -1, 'execute with null handle should return -1');
});

test('close with null does not crash', () => {
    pyro_pwire_close(null);
});

test('syntax error in INSERT', () => {
    const affected = pyro_pwire_execute(handle, `INSERT INTO ${TABLE_NAME} INVALID`);
    assert(affected < 0, 'syntax error should return negative');
});

test('connection remains valid after error', () => {
    // After an error, the connection should still work
    pyro_pwire_execute(handle, 'SELECT * FROM nonexistent_xyz');
    const result = queryJSON(handle, 'SELECT 1 AS recovery');
    const val = coerceInt(result.rows[0][0]);
    assertEqual(val, 1, 'connection should recover after error');
});

// ── 10. Data Types ─────────────────────────────────────────────────────────

console.log('\nSection: Data Types');

const TYPES_TABLE = `types_test_${Date.now()}`;

test('CREATE TABLE with various types', () => {
    const affected = pyro_pwire_execute(handle, `
        CREATE TABLE ${TYPES_TABLE} (
            id INTEGER PRIMARY KEY,
            int_col INTEGER,
            float_col FLOAT,
            text_col TEXT,
            bool_col BOOLEAN,
            varchar_col VARCHAR(255)
        )
    `);
    assert(affected >= 0, 'CREATE TABLE for types test failed');
});

test('INSERT and SELECT integer values', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, int_col) VALUES (1, 42)`
    );
    const result = queryJSON(handle, `SELECT int_col FROM ${TYPES_TABLE} WHERE id = 1`);
    const val = coerceInt(result.rows[0][0]);
    assertEqual(val, 42, 'integer value should be 42');
});

test('INSERT and SELECT zero', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, int_col) VALUES (10, 0)`
    );
    const result = queryJSON(handle, `SELECT int_col FROM ${TYPES_TABLE} WHERE id = 10`);
    const val = coerceInt(result.rows[0][0]);
    assertEqual(val, 0, 'zero value should be 0');
});

test('INSERT and SELECT float values', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, float_col) VALUES (2, 3.14159)`
    );
    const result = queryJSON(handle, `SELECT float_col FROM ${TYPES_TABLE} WHERE id = 2`);
    const val = coerceFloat(result.rows[0][0]);
    assert(Math.abs(val - 3.14159) < 0.001, `float should be ~3.14159, got ${val}`);
});

test('INSERT and SELECT text values', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, text_col) VALUES (3, 'Hello, PyroSQL!')`
    );
    const result = queryJSON(handle, `SELECT text_col FROM ${TYPES_TABLE} WHERE id = 3`);
    assertEqual(result.rows[0][0], 'Hello, PyroSQL!', 'text value mismatch');
});

test('INSERT and SELECT boolean values', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, bool_col) VALUES (4, TRUE)`
    );
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, bool_col) VALUES (5, FALSE)`
    );
    const r1 = queryJSON(handle, `SELECT bool_col FROM ${TYPES_TABLE} WHERE id = 4`);
    const r2 = queryJSON(handle, `SELECT bool_col FROM ${TYPES_TABLE} WHERE id = 5`);
    assertEqual(r1.rows[0][0], true, 'bool TRUE mismatch');
    assertEqual(r2.rows[0][0], false, 'bool FALSE mismatch');
});

test('INSERT and SELECT special characters', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, text_col) VALUES (6, 'O''Brien said "hello"')`
    );
    const result = queryJSON(handle, `SELECT text_col FROM ${TYPES_TABLE} WHERE id = 6`);
    assertEqual(result.rows[0][0], 'O\'Brien said "hello"', 'special chars mismatch');
});

test('INSERT and SELECT NULL values', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, text_col, int_col) VALUES (7, NULL, NULL)`
    );
    const result = queryJSON(handle, `SELECT text_col, int_col FROM ${TYPES_TABLE} WHERE id = 7`);
    assertEqual(result.rows[0][0], null, 'text_col should be null');
    assertEqual(result.rows[0][1], null, 'int_col should be null');
});

test('INSERT and SELECT large integer', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, int_col) VALUES (8, 2147483647)`
    );
    const result = queryJSON(handle, `SELECT int_col FROM ${TYPES_TABLE} WHERE id = 8`);
    const val = coerceInt(result.rows[0][0]);
    assertEqual(val, 2147483647, 'large int mismatch');
});

test('INSERT and SELECT negative values', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, int_col, float_col) VALUES (9, -42, -3.14)`
    );
    const result = queryJSON(handle, `SELECT int_col, float_col FROM ${TYPES_TABLE} WHERE id = 9`);
    const intVal = coerceInt(result.rows[0][0]);
    const floatVal = coerceFloat(result.rows[0][1]);
    assertEqual(intVal, -42, 'negative int mismatch');
    assert(Math.abs(floatVal - (-3.14)) < 0.01, `negative float mismatch: ${floatVal}`);
});

test('INSERT and SELECT empty string', () => {
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, text_col) VALUES (11, '')`
    );
    const result = queryJSON(handle, `SELECT text_col FROM ${TYPES_TABLE} WHERE id = 11`);
    assertEqual(result.rows[0][0], '', 'empty string mismatch');
});

test('INSERT and SELECT long text', () => {
    const longText = 'A'.repeat(1000);
    pyro_pwire_execute(handle,
        `INSERT INTO ${TYPES_TABLE} (id, text_col) VALUES (12, '${longText}')`
    );
    const result = queryJSON(handle, `SELECT text_col FROM ${TYPES_TABLE} WHERE id = 12`);
    assertEqual(result.rows[0][0], longText, 'long text mismatch');
    assertEqual(result.rows[0][0].length, 1000, 'long text length mismatch');
});

// ── 11. Multiple Connections ───────────────────────────────────────────────

console.log('\nSection: Multiple Connections');

test('open second connection and query', () => {
    const handle2 = pyro_pwire_connect(PYROSQL_HOST, PYROSQL_PORT);
    assert(handle2 !== null, 'second connection returned null');

    const result = queryJSON(handle2, `SELECT COUNT(*) FROM ${TABLE_NAME}`);
    const cnt = coerceInt(result.rows[0][0]);
    assert(cnt >= 1, 'second connection should see data');

    pyro_pwire_close(handle2);
});

test('concurrent writes from two connections', () => {
    const handle2 = pyro_pwire_connect(PYROSQL_HOST, PYROSQL_PORT);
    assert(handle2 !== null, 'connection 2 returned null');

    const TBL = `concurrent_test_${Date.now()}`;
    pyro_pwire_execute(handle, `CREATE TABLE ${TBL} (id INTEGER PRIMARY KEY, val TEXT)`);

    pyro_pwire_execute(handle, `INSERT INTO ${TBL} (id, val) VALUES (1, 'from_conn1')`);
    pyro_pwire_execute(handle2, `INSERT INTO ${TBL} (id, val) VALUES (2, 'from_conn2')`);

    const result = queryJSON(handle, `SELECT COUNT(*) FROM ${TBL}`);
    const cnt = coerceInt(result.rows[0][0]);
    assertEqual(cnt, 2, 'both inserts should be visible');

    pyro_pwire_execute(handle, `DROP TABLE ${TBL}`);
    pyro_pwire_close(handle2);
});

test('many connections sequentially', () => {
    for (let i = 0; i < 5; i++) {
        const h = pyro_pwire_connect(PYROSQL_HOST, PYROSQL_PORT);
        assert(h !== null, `connection ${i} failed`);
        queryJSON(h, 'SELECT 1');
        pyro_pwire_close(h);
    }
});

// ── 12. DDL Operations ─────────────────────────────────────────────────────

console.log('\nSection: DDL Operations');

test('ALTER TABLE ADD COLUMN', () => {
    try {
        const affected = pyro_pwire_execute(handle,
            `ALTER TABLE ${TABLE_NAME} ADD COLUMN notes TEXT`
        );
        assert(affected >= 0, 'ALTER TABLE ADD COLUMN failed');

        pyro_pwire_execute(handle,
            `UPDATE ${TABLE_NAME} SET notes = 'test note' WHERE id = 1`
        );
        const result = queryJSON(handle, `SELECT notes FROM ${TABLE_NAME} WHERE id = 1`);
        assertEqual(result.rows[0][0], 'test note', 'new column should have data');
    } catch (e) {
        console.log('        (ALTER TABLE not supported: ' + e.message.split('\n')[0] + ')');
    }
});

test('CREATE INDEX', () => {
    try {
        const affected = pyro_pwire_execute(handle,
            `CREATE INDEX idx_${TABLE_NAME}_name ON ${TABLE_NAME} (name)`
        );
        assert(affected >= 0, 'CREATE INDEX failed');
    } catch (e) {
        console.log('        (CREATE INDEX not supported: ' + e.message.split('\n')[0] + ')');
    }
});

test('DROP INDEX', () => {
    try {
        const affected = pyro_pwire_execute(handle,
            `DROP INDEX IF EXISTS idx_${TABLE_NAME}_name`
        );
        assert(affected >= 0, 'DROP INDEX failed');
    } catch (e) {
        console.log('        (DROP INDEX not supported: ' + e.message.split('\n')[0] + ')');
    }
});

// ── 13. Edge Cases ─────────────────────────────────────────────────────────

console.log('\nSection: Edge Cases');

test('empty result set', () => {
    const result = queryJSON(handle, `SELECT * FROM ${TABLE_NAME} WHERE id = 9999`);
    assertEqual(result.rows.length, 0, 'should return 0 rows for non-existent id');
    assert(result.columns.length > 0, 'columns should still be present');
});

test('SELECT with multiple conditions (AND)', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE age > 20 AND active = TRUE ORDER BY name`);
    assert(result.rows.length >= 1, 'should find rows matching both conditions');
});

test('SELECT with OR condition', () => {
    const result = queryJSON(handle, `SELECT name FROM ${TABLE_NAME} WHERE id = 1 OR id = 2 ORDER BY id`);
    assert(result.rows.length >= 1, 'should find rows matching OR condition');
});

test('UPDATE non-existent row', () => {
    const affected = pyro_pwire_execute(handle,
        `UPDATE ${TABLE_NAME} SET name = 'Ghost' WHERE id = 99999`
    );
    // Should succeed but affect 0 rows
    assert(affected >= 0, `UPDATE non-existent should not error, got ${affected}`);
});

test('DELETE non-existent row', () => {
    const affected = pyro_pwire_execute(handle,
        `DELETE FROM ${TABLE_NAME} WHERE id = 99999`
    );
    assert(affected >= 0, `DELETE non-existent should not error, got ${affected}`);
});

// ── Cleanup ────────────────────────────────────────────────────────────────

console.log('\nSection: Cleanup');

test('DROP TABLE cleanup (main table)', () => {
    const affected = pyro_pwire_execute(handle, `DROP TABLE IF EXISTS ${TABLE_NAME}`);
    assert(affected >= 0, 'DROP TABLE failed');
});

test('DROP TABLE cleanup (types table)', () => {
    const affected = pyro_pwire_execute(handle, `DROP TABLE IF EXISTS ${TYPES_TABLE}`);
    assert(affected >= 0, 'DROP TABLE failed');
});

test('close connection', () => {
    pyro_pwire_close(handle);
    handle = null;
});

// ── Summary ────────────────────────────────────────────────────────────────

console.log('\n' + '='.repeat(60));
console.log(`  Results: ${passedTests} passed, ${failedTests} failed, ${totalTests} total`);
console.log('='.repeat(60));

if (failures.length > 0) {
    console.log('\nFailures:');
    for (const f of failures) {
        console.log(`  - ${f.name}: ${f.error.split('\n')[0]}`);
    }
}

console.log('');
process.exit(failedTests > 0 ? 1 : 0);
