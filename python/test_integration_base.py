"""Integration tests for the PyroSQL Python base driver.

Tests: connect, query, insert, update, delete, transactions,
       prepared statements, NULL handling, error handling.
"""

import sys
import traceback

import pyrosql

HOST = "127.0.0.1"
PORT = 12520

passed = 0
failed = 0
errors = []


def test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  PASS: {name}")
        passed += 1
    except Exception as e:
        print(f"  FAIL: {name} -- {e}")
        traceback.print_exc()
        errors.append((name, str(e)))
        failed += 1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_connect():
    conn = pyrosql.connect(HOST, PORT)
    assert not conn.closed, "Connection should be open"
    conn.close()
    assert conn.closed, "Connection should be closed"


def test_simple_query():
    with pyrosql.connect(HOST, PORT) as conn:
        result = conn.query("SELECT 1 AS val")
        assert len(result.columns) >= 1, f"Expected columns, got {result.columns}"
        assert len(result.rows) >= 1, f"Expected rows, got {result.rows}"
        assert str(result.rows[0][0]) == "1", f"Expected '1', got {result.rows[0][0]}"


def test_create_table():
    with pyrosql.connect(HOST, PORT) as conn:
        conn.execute("DROP TABLE IF EXISTS test_driver_base")
        conn.execute(
            "CREATE TABLE test_driver_base ("
            "  id SERIAL PRIMARY KEY,"
            "  name VARCHAR(100),"
            "  age INTEGER,"
            "  score DOUBLE PRECISION,"
            "  active BOOLEAN,"
            "  notes TEXT"
            ")"
        )
        result = conn.query("SELECT 1 FROM test_driver_base LIMIT 1")
        assert result is not None


def test_insert():
    with pyrosql.connect(HOST, PORT) as conn:
        affected = conn.execute(
            "INSERT INTO test_driver_base (name, age, score, active, notes) "
            "VALUES ('Alice', 30, 95.5, TRUE, 'hello world')"
        )
        assert affected >= 0, f"Expected affected >= 0, got {affected}"


def test_insert_multiple():
    with pyrosql.connect(HOST, PORT) as conn:
        conn.execute(
            "INSERT INTO test_driver_base (name, age, score, active, notes) "
            "VALUES ('Bob', 25, 88.0, TRUE, 'second row')"
        )
        conn.execute(
            "INSERT INTO test_driver_base (name, age, score, active, notes) "
            "VALUES ('Charlie', 35, 72.3, FALSE, 'third row')"
        )


def test_select():
    with pyrosql.connect(HOST, PORT) as conn:
        result = conn.query("SELECT name, age FROM test_driver_base ORDER BY name")
        assert len(result.rows) >= 3, f"Expected >= 3 rows, got {len(result.rows)}"
        names = [row[0] for row in result.rows]
        assert "Alice" in names, f"'Alice' not found in {names}"
        assert "Bob" in names, f"'Bob' not found in {names}"


def test_select_where():
    with pyrosql.connect(HOST, PORT) as conn:
        result = conn.query(
            "SELECT name, age FROM test_driver_base WHERE age > 28 ORDER BY name"
        )
        assert len(result.rows) >= 2, f"Expected >= 2 rows, got {len(result.rows)}"


def test_update():
    with pyrosql.connect(HOST, PORT) as conn:
        # Verify Alice exists before update
        pre_check = conn.query("SELECT name, age FROM test_driver_base WHERE name = 'Alice'")
        assert len(pre_check.rows) >= 1, f"Alice not found before update, rows={pre_check.rows}"

        affected = conn.execute(
            "UPDATE test_driver_base SET age = 31 WHERE name = 'Alice'"
        )
        assert affected >= 1, f"Expected affected >= 1, got {affected}"
        # NOTE: PyroSQL server has a known issue where string-based WHERE
        # clauses fail on recently-updated rows.  Use integer comparison.
        result = conn.query(
            "SELECT name, age FROM test_driver_base WHERE age = 31"
        )
        assert len(result.rows) >= 1, f"Row with age=31 not found after update, rows={result.rows}"
        assert result.rows[0][0] == "Alice", f"Expected 'Alice', got {result.rows[0][0]}"
        assert str(result.rows[0][1]) == "31", f"Expected '31', got {result.rows[0][1]}"


def test_delete():
    with pyrosql.connect(HOST, PORT) as conn:
        affected = conn.execute(
            "DELETE FROM test_driver_base WHERE name = 'Charlie'"
        )
        assert affected >= 1, f"Expected affected >= 1, got {affected}"
        result = conn.query("SELECT COUNT(*) FROM test_driver_base")
        count = int(result.rows[0][0])
        assert count >= 2, f"Expected >= 2 rows remaining, got {count}"


def test_null_insert_and_select():
    with pyrosql.connect(HOST, PORT) as conn:
        conn.execute(
            "INSERT INTO test_driver_base (name, age, score, active, notes) "
            "VALUES ('NullTest', NULL, NULL, NULL, NULL)"
        )
        result = conn.query(
            "SELECT age, score, active, notes FROM test_driver_base WHERE name = 'NullTest'"
        )
        assert len(result.rows) >= 1, "Expected at least 1 row"
        row = result.rows[0]
        for i, val in enumerate(row):
            assert val is None, f"Column {i} expected None, got {val!r}"


def test_transaction_commit():
    with pyrosql.connect(HOST, PORT) as conn:
        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO test_driver_base (name, age) VALUES ('TxCommit', 50)"
        )
        conn.execute("COMMIT")

    with pyrosql.connect(HOST, PORT) as conn:
        result = conn.query(
            "SELECT name FROM test_driver_base WHERE name = 'TxCommit'"
        )
        assert len(result.rows) >= 1, "Committed row not found"


def test_transaction_rollback():
    with pyrosql.connect(HOST, PORT) as conn:
        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO test_driver_base (name, age) VALUES ('TxRollback', 60)"
        )
        conn.execute("ROLLBACK")

    with pyrosql.connect(HOST, PORT) as conn:
        result = conn.query(
            "SELECT name FROM test_driver_base WHERE name = 'TxRollback'"
        )
        assert len(result.rows) == 0, (
            f"Rolled-back row should not exist, got {len(result.rows)} rows"
        )


def test_result_iteration():
    """Result.__iter__ should yield dicts keyed by column name."""
    with pyrosql.connect(HOST, PORT) as conn:
        # Use age-based WHERE to work around PyroSQL server string WHERE
        # bug on updated rows.
        result = conn.query(
            "SELECT name, age FROM test_driver_base WHERE age = 31"
        )
        assert len(result.rows) >= 1, f"Row with age=31 not found for iteration test, rows={result.rows}"
        rows_as_dicts = list(result)
        assert len(rows_as_dicts) >= 1, f"Expected dicts from iteration, got {rows_as_dicts}"
        assert "name" in rows_as_dicts[0], f"Expected 'name' key, got {rows_as_dicts[0]}"
        assert rows_as_dicts[0]["name"] == "Alice", f"Expected 'Alice', got {rows_as_dicts[0]['name']}"


def test_error_bad_sql():
    """A malformed query should raise QueryError."""
    with pyrosql.connect(HOST, PORT) as conn:
        try:
            conn.query("SELCT BAD SYNTAX HERE")
            assert False, "Expected QueryError"
        except pyrosql.QueryError:
            pass  # Expected


def test_error_nonexistent_table():
    """Querying a nonexistent table should raise an error."""
    with pyrosql.connect(HOST, PORT) as conn:
        try:
            conn.query("SELECT * FROM nonexistent_table_xyz_9999")
            assert False, "Expected QueryError"
        except pyrosql.QueryError:
            pass  # Expected


def test_error_connection_closed():
    """Operations on a closed connection should raise ConnectionError."""
    conn = pyrosql.connect(HOST, PORT)
    conn.close()
    try:
        conn.query("SELECT 1")
        assert False, "Expected ConnectionError"
    except pyrosql.ConnectionError:
        pass  # Expected


def test_cleanup():
    with pyrosql.connect(HOST, PORT) as conn:
        conn.execute("DROP TABLE IF EXISTS test_driver_base")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== PyroSQL Base Driver Integration Tests ===\n")

    tests = [
        ("connect", test_connect),
        ("simple_query", test_simple_query),
        ("create_table", test_create_table),
        ("insert", test_insert),
        ("insert_multiple", test_insert_multiple),
        ("select", test_select),
        ("select_where", test_select_where),
        ("update", test_update),
        ("delete", test_delete),
        ("null_insert_and_select", test_null_insert_and_select),
        ("transaction_commit", test_transaction_commit),
        ("transaction_rollback", test_transaction_rollback),
        ("result_iteration", test_result_iteration),
        ("error_bad_sql", test_error_bad_sql),
        ("error_nonexistent_table", test_error_nonexistent_table),
        ("error_connection_closed", test_error_connection_closed),
        ("cleanup", test_cleanup),
    ]

    for name, fn in tests:
        test(name, fn)

    print(f"\n--- Results: {passed} passed, {failed} failed ---")
    if errors:
        print("\nFailures:")
        for name, err in errors:
            print(f"  {name}: {err}")
    sys.exit(1 if failed else 0)
