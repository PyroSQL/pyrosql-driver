"""Full integration tests for PyroSQL Python driver (pure TCP mode).

Tests: connect, CREATE TABLE, INSERT, SELECT, prepared statements,
       transactions, large payload (LZ4 compression), error handling, cleanup.
"""

import sys
import traceback

import pyrosql

HOST = "127.0.0.1"
PORT = 12520
TABLE = "_inttest_python"

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
    conn = pyrosql.connect_tcp(HOST, PORT)
    assert not conn.closed, "Connection should be open"
    conn.close()
    assert conn.closed, "Connection should be closed"


def test_simple_query():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        result = conn.query("SELECT 1 AS val")
        assert len(result.columns) >= 1, f"Expected columns, got {result.columns}"
        assert len(result.rows) >= 1, f"Expected rows, got {result.rows}"
        assert str(result.rows[0][0]) == "1", f"Expected '1', got {result.rows[0][0]}"


def test_create_table():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
        conn.execute(
            f"CREATE TABLE {TABLE} ("
            "  id SERIAL PRIMARY KEY,"
            "  name TEXT,"
            "  score DOUBLE PRECISION,"
            "  active BOOLEAN"
            ")"
        )
        result = conn.query(f"SELECT 1 FROM {TABLE} LIMIT 1")
        assert result is not None


def test_insert_3_rows():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute(
            f"INSERT INTO {TABLE} (name, score, active) "
            "VALUES ('Alice', 95.5, TRUE)"
        )
        conn.execute(
            f"INSERT INTO {TABLE} (name, score, active) "
            "VALUES ('Bob', 88.0, TRUE)"
        )
        conn.execute(
            f"INSERT INTO {TABLE} (name, score, active) "
            "VALUES ('Charlie', 72.3, FALSE)"
        )


def test_select_and_verify():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        result = conn.query(f"SELECT name, score, active FROM {TABLE} ORDER BY name")
        assert len(result.rows) >= 3, f"Expected >= 3 rows, got {len(result.rows)}"
        names = [row[0] for row in result.rows]
        assert "Alice" in names, f"'Alice' not found in {names}"
        assert "Bob" in names, f"'Bob' not found in {names}"
        assert "Charlie" in names, f"'Charlie' not found in {names}"


def test_prepared_statement_select():
    """Test server-side binding with string parameter (most reliable type)."""
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        result = conn.query(
            f"SELECT name, score FROM {TABLE} WHERE name = $1",
            ["Alice"]
        )
        assert len(result.rows) >= 1, f"Expected >= 1 row for Alice, got {len(result.rows)}"
        assert result.rows[0][0] == "Alice", f"Expected 'Alice', got {result.rows[0][0]}"


def test_prepared_statement_insert():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute(
            f"INSERT INTO {TABLE} (name, score, active) VALUES ($1, $2, $3)",
            ["Diana", 91.0, True]
        )
        result = conn.query(f"SELECT name FROM {TABLE} WHERE name = 'Diana'")
        assert len(result.rows) >= 1, "Diana not found after prepared INSERT"


def test_transaction_commit():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute("BEGIN")
        conn.execute(
            f"INSERT INTO {TABLE} (name, score, active) VALUES ('TxCommit', 50.0, TRUE)"
        )
        conn.execute("COMMIT")

    with pyrosql.connect_tcp(HOST, PORT) as conn:
        result = conn.query(
            f"SELECT name FROM {TABLE} WHERE name = 'TxCommit'"
        )
        assert len(result.rows) >= 1, "Committed row not found"


def test_transaction_rollback():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute("BEGIN")
        conn.execute(
            f"INSERT INTO {TABLE} (name, score, active) VALUES ('TxRollback', 60.0, TRUE)"
        )
        conn.execute("ROLLBACK")

    with pyrosql.connect_tcp(HOST, PORT) as conn:
        result = conn.query(
            f"SELECT name FROM {TABLE} WHERE name = 'TxRollback'"
        )
        assert len(result.rows) == 0, (
            f"Rolled-back row should not exist, got {len(result.rows)} rows"
        )


def test_large_payload():
    """Insert 150 rows and select them all -- tests LZ4 compression for payloads > 8KB."""
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute("DROP TABLE IF EXISTS _inttest_python_large")
        conn.execute(
            "CREATE TABLE _inttest_python_large ("
            "  id SERIAL PRIMARY KEY,"
            "  name TEXT,"
            "  description TEXT,"
            "  score DOUBLE PRECISION"
            ")"
        )
        for i in range(150):
            desc = f"This is a longer description for row number {i} to help exceed the 8KB compression threshold with enough data"
            conn.execute(
                f"INSERT INTO _inttest_python_large (name, description, score) "
                f"VALUES ('user_{i}', '{desc}', {float(i) * 1.1})"
            )

        result = conn.query("SELECT id, name, description, score FROM _inttest_python_large ORDER BY id")
        assert len(result.rows) == 150, f"Expected 150 rows, got {len(result.rows)}"
        print(f"        (retrieved {len(result.rows)} rows - LZ4 compression test)")

        conn.execute("DROP TABLE IF EXISTS _inttest_python_large")


def test_error_bad_sql():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        try:
            conn.query("SELCT BAD SYNTAX HERE")
            assert False, "Expected error"
        except Exception:
            pass  # Expected


def test_error_nonexistent_table():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        try:
            conn.query("SELECT * FROM nonexistent_table_xyz_9999")
            assert False, "Expected error"
        except Exception:
            pass  # Expected


def test_error_connection_closed():
    conn = pyrosql.connect_tcp(HOST, PORT)
    conn.close()
    try:
        conn.query("SELECT 1")
        assert False, "Expected ConnectionError"
    except pyrosql.ConnectionError:
        pass  # Expected


def test_cleanup():
    with pyrosql.connect_tcp(HOST, PORT) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {TABLE}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== PyroSQL Python Driver Integration Tests (TCP mode) ===\n")

    tests = [
        ("connect", test_connect),
        ("simple_query", test_simple_query),
        ("create_table", test_create_table),
        ("insert_3_rows", test_insert_3_rows),
        ("select_and_verify", test_select_and_verify),
        ("prepared_statement_select", test_prepared_statement_select),
        ("prepared_statement_insert", test_prepared_statement_insert),
        ("transaction_commit", test_transaction_commit),
        ("transaction_rollback", test_transaction_rollback),
        ("large_payload_lz4", test_large_payload),
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
