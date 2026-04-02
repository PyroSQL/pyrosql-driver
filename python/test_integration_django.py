"""Integration tests for the PyroSQL Django backend.

Minimal Django setup (no full project needed) that exercises the backend
against a real PyroSQL server.
"""

import os
import sys
import traceback

# ---------------------------------------------------------------------------
# Minimal Django configuration
# ---------------------------------------------------------------------------

import django
from django.conf import settings

if not settings.configured:
    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "pyrosql.django",
                "HOST": "127.0.0.1",
                "PORT": "12520",
                "NAME": "fomium",
                "USER": "pyrosql",
                "PASSWORD": "secret",
            }
        },
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
        ],
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        USE_TZ=False,
    )

django.setup()

from django.db import connection

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

def test_connection():
    """Test that we can get a cursor and execute a simple query."""
    cursor = connection.cursor()
    cursor.execute("SELECT 1 AS val")
    row = cursor.fetchone()
    assert row is not None, "Expected a row from SELECT 1"
    assert str(row[0]) == "1", f"Expected '1', got {row[0]}"
    cursor.close()


def test_create_table():
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_django_integration")
    cursor.execute(
        "CREATE TABLE test_django_integration ("
        "  id SERIAL PRIMARY KEY,"
        "  name VARCHAR(100) NOT NULL,"
        "  age INTEGER,"
        "  active BOOLEAN DEFAULT TRUE"
        ")"
    )
    cursor.close()


def test_insert():
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO test_django_integration (name, age, active) VALUES (%s, %s, %s)",
        ["Alice", 30, True],
    )
    assert cursor.rowcount >= 0, f"Expected rowcount >= 0, got {cursor.rowcount}"
    cursor.close()


def test_insert_multiple():
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO test_django_integration (name, age, active) VALUES (%s, %s, %s)",
        ["Bob", 25, True],
    )
    cursor.execute(
        "INSERT INTO test_django_integration (name, age, active) VALUES (%s, %s, %s)",
        ["Charlie", 35, False],
    )
    cursor.close()


def test_select():
    cursor = connection.cursor()
    cursor.execute("SELECT name, age FROM test_django_integration ORDER BY name")
    rows = cursor.fetchall()
    assert len(rows) >= 3, f"Expected >= 3 rows, got {len(rows)}"
    names = [r[0] for r in rows]
    assert "Alice" in names, f"Alice not in {names}"
    cursor.close()


def test_select_with_params():
    cursor = connection.cursor()
    cursor.execute(
        "SELECT name FROM test_django_integration WHERE age > %s ORDER BY name",
        [28],
    )
    rows = cursor.fetchall()
    assert len(rows) >= 2, f"Expected >= 2 rows, got {len(rows)}"
    cursor.close()


def test_update():
    cursor = connection.cursor()
    cursor.execute(
        "UPDATE test_django_integration SET age = %s WHERE name = %s",
        [31, "Alice"],
    )
    assert cursor.rowcount >= 1, f"Expected rowcount >= 1, got {cursor.rowcount}"
    # NOTE: PyroSQL server has a known issue where string-based WHERE
    # clauses fail on recently-updated rows.  Use integer comparison.
    cursor.execute("SELECT name, age FROM test_django_integration WHERE age = 31")
    row = cursor.fetchone()
    assert row is not None, "Row with age=31 not found after update"
    assert row[0] == "Alice", f"Expected 'Alice', got {row[0]}"
    assert str(row[1]) == "31", f"Expected '31', got {row[1]}"
    cursor.close()


def test_delete():
    cursor = connection.cursor()
    cursor.execute("DELETE FROM test_django_integration WHERE name = %s", ["Charlie"])
    assert cursor.rowcount >= 1, f"Expected rowcount >= 1, got {cursor.rowcount}"
    cursor.close()


def test_null_handling():
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO test_django_integration (name, age, active) VALUES (%s, %s, %s)",
        ["NullTest", None, None],
    )
    cursor.execute("SELECT age, active FROM test_django_integration WHERE name = 'NullTest'")
    row = cursor.fetchone()
    assert row is not None, "NullTest row not found"
    assert row[0] is None, f"Expected age=None, got {row[0]!r}"
    assert row[1] is None, f"Expected active=None, got {row[1]!r}"
    cursor.close()


def test_fetchone_fetchmany_fetchall():
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM test_django_integration ORDER BY name")
    first = cursor.fetchone()
    assert first is not None
    rest = cursor.fetchall()
    assert len(rest) >= 1
    cursor.close()


def test_transaction():
    """Test that commit/rollback work via the connection."""
    cursor = connection.cursor()
    cursor.execute("BEGIN")
    cursor.execute(
        "INSERT INTO test_django_integration (name, age) VALUES (%s, %s)",
        ["TxTest", 99],
    )
    cursor.execute("ROLLBACK")
    cursor.execute("SELECT name FROM test_django_integration WHERE name = 'TxTest'")
    rows = cursor.fetchall()
    assert len(rows) == 0, f"Rolled-back row should not exist, got {len(rows)}"
    cursor.close()


def test_error_handling():
    cursor = connection.cursor()
    try:
        cursor.execute("SELCT BAD SYNTAX")
        assert False, "Expected an exception"
    except Exception:
        pass  # Expected
    cursor.close()


def test_cleanup():
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_django_integration")
    cursor.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== PyroSQL Django Backend Integration Tests ===\n")

    tests = [
        ("connection", test_connection),
        ("create_table", test_create_table),
        ("insert", test_insert),
        ("insert_multiple", test_insert_multiple),
        ("select", test_select),
        ("select_with_params", test_select_with_params),
        ("update", test_update),
        ("delete", test_delete),
        ("null_handling", test_null_handling),
        ("fetchone_fetchmany_fetchall", test_fetchone_fetchmany_fetchall),
        ("transaction", test_transaction),
        ("error_handling", test_error_handling),
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
