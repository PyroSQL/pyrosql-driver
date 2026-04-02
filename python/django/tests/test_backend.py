"""Comprehensive tests for the PyroSQL Django database backend.

These tests verify the backend's functionality using mocking to avoid
requiring a live PyroSQL server.
"""

import datetime
import decimal
import json
import uuid
from collections import namedtuple
from unittest import TestCase, mock

import django
from django.conf import settings

# Configure Django settings before importing any Django modules
if not settings.configured:
    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "pyrosql.django",
                "HOST": "localhost",
                "PORT": "12520",
                "NAME": "testdb",
                "USER": "pyrosql",
                "PASSWORD": "secret",
            }
        },
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        USE_TZ=True,
        TIME_ZONE="UTC",
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.auth",
        ],
    )
    django.setup()

from django.db import models
from django.db.models import F, Q, Avg, Count, Max, Min, Sum, Value
from django.db.models.functions import Concat, Lower, Upper

from pyrosql.django.base import (
    CursorWrapper,
    DatabaseWrapper,
    _format_sql,
    _quote_param,
)
from pyrosql.django.client import DatabaseClient
from pyrosql.django.compiler import SQLCompiler, SQLInsertCompiler
from pyrosql.django.creation import DatabaseCreation
from pyrosql.django.features import DatabaseFeatures
from pyrosql.django.introspection import DatabaseIntrospection
from pyrosql.django.operations import DatabaseOperations
from pyrosql.django.schema import DatabaseSchemaEditor


# ---------------------------------------------------------------------------
# Mock pyrosql driver objects
# ---------------------------------------------------------------------------

class MockResult:
    def __init__(self, columns=None, rows=None, rows_affected=0):
        self.columns = columns or []
        self.rows = rows or []
        self.rows_affected = rows_affected


class MockConnection:
    def __init__(self):
        self.closed = False
        self._queries = []
        self._executes = []

    def query(self, sql):
        self._queries.append(sql)
        return MockResult(columns=["result"], rows=[[1]], rows_affected=0)

    def execute(self, sql):
        self._executes.append(sql)
        return 0

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# Test: Parameter quoting
# ---------------------------------------------------------------------------

class TestQuoteParam(TestCase):
    def test_none(self):
        self.assertEqual(_quote_param(None), "NULL")

    def test_bool_true(self):
        self.assertEqual(_quote_param(True), "TRUE")

    def test_bool_false(self):
        self.assertEqual(_quote_param(False), "FALSE")

    def test_int(self):
        self.assertEqual(_quote_param(42), "42")
        self.assertEqual(_quote_param(-1), "-1")
        self.assertEqual(_quote_param(0), "0")

    def test_float(self):
        result = _quote_param(3.14)
        self.assertIn("3.14", result)

    def test_decimal(self):
        self.assertEqual(_quote_param(decimal.Decimal("99.95")), "99.95")

    def test_string(self):
        self.assertEqual(_quote_param("hello"), "'hello'")

    def test_string_with_single_quote(self):
        self.assertEqual(_quote_param("it's"), "'it''s'")

    def test_string_with_backslash(self):
        self.assertEqual(_quote_param("a\\b"), "'a\\\\b'")

    def test_bytes(self):
        result = _quote_param(b"\xde\xad")
        self.assertEqual(result, "'\\xdead'")

    def test_datetime(self):
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        result = _quote_param(dt)
        self.assertEqual(result, "'2024-01-15 10:30:00.000000'")

    def test_datetime_with_tz(self):
        dt = datetime.datetime(
            2024, 1, 15, 10, 30, 0,
            tzinfo=datetime.timezone.utc,
        )
        result = _quote_param(dt)
        self.assertIn("2024-01-15", result)

    def test_date(self):
        d = datetime.date(2024, 6, 15)
        self.assertEqual(_quote_param(d), "'2024-06-15'")

    def test_time(self):
        t = datetime.time(14, 30, 0)
        self.assertEqual(_quote_param(t), "'14:30:00'")

    def test_uuid(self):
        u = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.assertEqual(_quote_param(u), "'12345678-1234-5678-1234-567812345678'")

    def test_list(self):
        result = _quote_param([1, 2, 3])
        self.assertEqual(result, "'[1, 2, 3]'")

    def test_dict(self):
        result = _quote_param({"key": "value"})
        self.assertIn("key", result)

    def test_timedelta(self):
        td = datetime.timedelta(hours=1, minutes=30)
        result = _quote_param(td)
        self.assertIn("INTERVAL", result)
        self.assertIn("5400000000", result)

    def test_memoryview(self):
        mv = memoryview(b"\xca\xfe")
        result = _quote_param(mv)
        self.assertEqual(result, "'\\xcafe'")


# ---------------------------------------------------------------------------
# Test: SQL formatting
# ---------------------------------------------------------------------------

class TestFormatSQL(TestCase):
    def test_no_params(self):
        result = _format_sql("SELECT 1")
        self.assertEqual(result, "SELECT 1")

    def test_empty_params(self):
        result = _format_sql("SELECT 1", [])
        self.assertEqual(result, "SELECT 1")

    def test_single_param(self):
        result = _format_sql("SELECT * FROM t WHERE id = %s", [42])
        self.assertEqual(result, "SELECT * FROM t WHERE id = 42")

    def test_multiple_params(self):
        result = _format_sql(
            "INSERT INTO t (a, b) VALUES (%s, %s)",
            [1, "hello"],
        )
        self.assertEqual(result, "INSERT INTO t (a, b) VALUES (1, 'hello')")

    def test_none_param(self):
        result = _format_sql("SELECT * FROM t WHERE x = %s", [None])
        self.assertEqual(result, "SELECT * FROM t WHERE x = NULL")

    def test_escaped_percent(self):
        result = _format_sql("SELECT '%%' || %s", ["test"])
        self.assertEqual(result, "SELECT '%' || 'test'")

    def test_like_pattern(self):
        result = _format_sql(
            "SELECT * FROM t WHERE name LIKE %s",
            ["%foo%"],
        )
        self.assertEqual(result, "SELECT * FROM t WHERE name LIKE '%foo%'")


# ---------------------------------------------------------------------------
# Test: CursorWrapper
# ---------------------------------------------------------------------------

class TestCursorWrapper(TestCase):
    def setUp(self):
        self.mock_conn = MockConnection()
        self.cursor = CursorWrapper(self.mock_conn)

    def test_execute_select(self):
        self.cursor.execute("SELECT 1")
        self.assertIsNotNone(self.cursor.description)
        row = self.cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)

    def test_execute_with_params(self):
        self.cursor.execute("SELECT * FROM t WHERE id = %s", [42])
        self.assertIn("id = 42", self.mock_conn._queries[-1])

    def test_execute_insert(self):
        self.mock_conn.execute = mock.Mock(return_value=1)
        self.cursor.execute("INSERT INTO t (a) VALUES (%s)", [1])
        self.assertEqual(self.cursor.rowcount, 1)
        self.assertIsNone(self.cursor.description)

    def test_fetchone_empty(self):
        self.mock_conn.query = mock.Mock(
            return_value=MockResult(columns=["a"], rows=[], rows_affected=0)
        )
        self.cursor.execute("SELECT * FROM empty_table")
        row = self.cursor.fetchone()
        self.assertIsNone(row)

    def test_fetchmany(self):
        self.mock_conn.query = mock.Mock(
            return_value=MockResult(
                columns=["id"],
                rows=[[1], [2], [3], [4], [5]],
                rows_affected=0,
            )
        )
        self.cursor.execute("SELECT id FROM t")
        rows = self.cursor.fetchmany(3)
        self.assertEqual(len(rows), 3)
        remaining = self.cursor.fetchmany(10)
        self.assertEqual(len(remaining), 2)

    def test_fetchall(self):
        self.mock_conn.query = mock.Mock(
            return_value=MockResult(
                columns=["id", "name"],
                rows=[[1, "a"], [2, "b"]],
                rows_affected=0,
            )
        )
        self.cursor.execute("SELECT id, name FROM t")
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], (1, "a"))
        self.assertEqual(rows[1], (2, "b"))

    def test_executemany(self):
        call_count = 0

        def mock_execute(sql):
            nonlocal call_count
            call_count += 1
            return 1

        self.mock_conn.execute = mock_execute
        self.cursor.executemany(
            "INSERT INTO t (a) VALUES (%s)",
            [[1], [2], [3]],
        )
        self.assertEqual(call_count, 3)
        self.assertEqual(self.cursor.rowcount, 3)

    def test_close(self):
        self.cursor.close()
        self.assertTrue(self.cursor.closed)

    def test_execute_after_close_raises(self):
        self.cursor.close()
        from django.db import DatabaseError
        with self.assertRaises(DatabaseError):
            self.cursor.execute("SELECT 1")

    def test_context_manager(self):
        with CursorWrapper(self.mock_conn) as c:
            c.execute("SELECT 1")
        self.assertTrue(c.closed)

    def test_returning_sets_lastrowid(self):
        self.mock_conn.query = mock.Mock(
            return_value=MockResult(
                columns=["id"],
                rows=[[42]],
                rows_affected=1,
            )
        )
        self.cursor.execute("INSERT INTO t (a) VALUES (1) RETURNING id")
        self.assertEqual(self.cursor.lastrowid, 42)

    def test_iterator(self):
        self.mock_conn.query = mock.Mock(
            return_value=MockResult(
                columns=["x"],
                rows=[[1], [2], [3]],
                rows_affected=0,
            )
        )
        self.cursor.execute("SELECT x FROM t")
        results = list(self.cursor)
        self.assertEqual(len(results), 3)

    def test_integrity_error_on_unique_violation(self):
        from pyrosql import QueryError
        self.mock_conn.query = mock.Mock(
            side_effect=QueryError(
                "unique constraint violation on key"
            )
        )
        from django.db import IntegrityError
        with self.assertRaises(IntegrityError):
            self.cursor.execute("SELECT 1")

    def test_database_error_on_generic_query_error(self):
        from pyrosql import QueryError
        self.mock_conn.query = mock.Mock(
            side_effect=QueryError("syntax error")
        )
        from django.db import DatabaseError
        with self.assertRaises(DatabaseError):
            self.cursor.execute("SELECT 1")


# ---------------------------------------------------------------------------
# Test: DatabaseOperations
# ---------------------------------------------------------------------------

class TestDatabaseOperations(TestCase):
    def setUp(self):
        self.ops = DatabaseOperations(connection=mock.MagicMock())

    def test_quote_name(self):
        self.assertEqual(self.ops.quote_name("my_table"), '"my_table"')

    def test_quote_name_already_quoted(self):
        self.assertEqual(self.ops.quote_name('"my_table"'), '"my_table"')

    def test_no_limit_value(self):
        self.assertIsNone(self.ops.no_limit_value())

    def test_max_name_length(self):
        self.assertEqual(self.ops.max_name_length(), 63)

    def test_date_extract_sql_year(self):
        sql, params = self.ops.date_extract_sql("year", "col", [])
        self.assertIn("EXTRACT", sql)
        self.assertIn("YEAR", sql)

    def test_date_extract_sql_week_day(self):
        sql, params = self.ops.date_extract_sql("week_day", "col", [])
        self.assertIn("DOW", sql)
        self.assertIn("+ 1", sql)

    def test_date_extract_sql_iso_week_day(self):
        sql, params = self.ops.date_extract_sql("iso_week_day", "col", [])
        self.assertIn("ISODOW", sql)

    def test_date_extract_sql_iso_year(self):
        sql, params = self.ops.date_extract_sql("iso_year", "col", [])
        self.assertIn("ISOYEAR", sql)

    def test_date_trunc_sql(self):
        sql, params = self.ops.date_trunc_sql("month", "col", [])
        self.assertIn("DATE_TRUNC", sql)

    def test_datetime_cast_date_sql(self):
        sql, params = self.ops.datetime_cast_date_sql("col", [], None)
        self.assertIn("::DATE", sql)

    def test_datetime_cast_time_sql(self):
        sql, params = self.ops.datetime_cast_time_sql("col", [], None)
        self.assertIn("::TIME", sql)

    def test_datetime_cast_date_with_tz(self):
        sql, params = self.ops.datetime_cast_date_sql("col", [], "US/Eastern")
        self.assertIn("AT TIME ZONE", sql)
        self.assertIn("::DATE", sql)

    def test_distinct_sql_no_fields(self):
        result, params = self.ops.distinct_sql([], [])
        self.assertEqual(result, ["DISTINCT"])

    def test_distinct_sql_with_fields(self):
        result, params = self.ops.distinct_sql(["a", "b"], [])
        self.assertEqual(result, ["DISTINCT ON (a, b)"])

    def test_regex_lookup(self):
        self.assertEqual(self.ops.regex_lookup("regex"), "%s ~ %s")
        self.assertEqual(self.ops.regex_lookup("iregex"), "%s ~* %s")

    def test_prep_for_like_query(self):
        self.assertEqual(self.ops.prep_for_like_query("a%b_c\\d"), "a\\%b\\_c\\\\d")

    def test_return_insert_columns(self):
        field = mock.MagicMock()
        field.model._meta.db_table = "my_table"
        field.column = "id"
        sql, params = self.ops.return_insert_columns([field])
        self.assertIn("RETURNING", sql)
        self.assertIn('"my_table"', sql)
        self.assertIn('"id"', sql)

    def test_return_insert_columns_empty(self):
        sql, params = self.ops.return_insert_columns([])
        self.assertEqual(sql, "")

    def test_bulk_insert_sql(self):
        result = self.ops.bulk_insert_sql(
            ["a", "b"],
            [("%s", "%s"), ("%s", "%s")],
        )
        self.assertIn("VALUES", result)
        self.assertEqual(result.count("("), 2)

    def test_sql_flush_with_reset(self):
        style = mock.MagicMock()
        style.SQL_KEYWORD = lambda x: x
        style.SQL_FIELD = lambda x: x
        result = self.ops.sql_flush(
            style, ["t1", "t2"], reset_sequences=True
        )
        self.assertTrue(len(result) > 0)
        self.assertIn("TRUNCATE", result[0])
        self.assertIn("RESTART IDENTITY", result[0])

    def test_sql_flush_delete(self):
        style = mock.MagicMock()
        style.SQL_KEYWORD = lambda x: x
        style.SQL_FIELD = lambda x: x
        result = self.ops.sql_flush(style, ["t1", "t2"])
        self.assertEqual(len(result), 2)
        for stmt in result:
            self.assertIn("DELETE", stmt)

    def test_sql_flush_cascade(self):
        style = mock.MagicMock()
        style.SQL_KEYWORD = lambda x: x
        style.SQL_FIELD = lambda x: x
        result = self.ops.sql_flush(style, ["t1"], allow_cascade=True)
        self.assertIn("CASCADE", result[0])

    def test_sql_flush_empty(self):
        style = mock.MagicMock()
        result = self.ops.sql_flush(style, [])
        self.assertEqual(result, [])

    def test_adapt_datefield_value(self):
        d = datetime.date(2024, 1, 15)
        self.assertEqual(self.ops.adapt_datefield_value(d), d)

    def test_adapt_datetimefield_value(self):
        dt = datetime.datetime(2024, 1, 15, 10, 0)
        self.assertEqual(self.ops.adapt_datetimefield_value(dt), dt)

    def test_adapt_timefield_value(self):
        t = datetime.time(10, 30)
        self.assertEqual(self.ops.adapt_timefield_value(t), t)

    def test_adapt_decimalfield_value(self):
        d = decimal.Decimal("3.14")
        self.assertEqual(self.ops.adapt_decimalfield_value(d), d)

    def test_adapt_json_value(self):
        result = self.ops.adapt_json_value({"key": "val"}, None)
        self.assertEqual(json.loads(result), {"key": "val"})

    def test_adapt_ipaddressfield_value(self):
        self.assertEqual(self.ops.adapt_ipaddressfield_value("127.0.0.1"), "127.0.0.1")
        self.assertIsNone(self.ops.adapt_ipaddressfield_value(None))
        self.assertIsNone(self.ops.adapt_ipaddressfield_value(""))

    def test_subtract_temporals_date(self):
        sql, params = self.ops.subtract_temporals(
            "DateField", ("lhs", []), ("rhs", [])
        )
        self.assertIn("::DATE", sql)

    def test_subtract_temporals_datetime(self):
        sql, params = self.ops.subtract_temporals(
            "DateTimeField", ("lhs", []), ("rhs", [])
        )
        self.assertIn("lhs", sql)
        self.assertIn("rhs", sql)

    def test_explain_query_prefix(self):
        result = self.ops.explain_query_prefix()
        self.assertEqual(result, "EXPLAIN")

    def test_explain_query_prefix_with_format(self):
        result = self.ops.explain_query_prefix(format="JSON")
        self.assertIn("FORMAT JSON", result)

    def test_prepare_sql_script(self):
        result = self.ops.prepare_sql_script("SELECT 1; SELECT 2;")
        self.assertEqual(result, ["SELECT 1; SELECT 2;"])

    def test_set_time_zone_sql(self):
        self.assertEqual(self.ops.set_time_zone_sql(), "SET TIME ZONE %s")

    def test_format_for_duration_arithmetic(self):
        result = self.ops.format_for_duration_arithmetic("1000")
        self.assertIn("INTERVAL", result)
        self.assertIn("MICROSECONDS", result)

    def test_last_executed_query_with_params(self):
        result = self.ops.last_executed_query(
            mock.MagicMock(), "SELECT %s, %s", [1, "abc"]
        )
        self.assertIn("1", result)
        self.assertIn("abc", result)

    def test_last_executed_query_no_params(self):
        result = self.ops.last_executed_query(
            mock.MagicMock(), "SELECT 1", None
        )
        self.assertEqual(result, "SELECT 1")

    def test_convert_durationfield_value(self):
        td = datetime.timedelta(hours=1, minutes=30)
        result = self.ops.convert_durationfield_value(td, None, None)
        self.assertEqual(result, 5400000000)  # microseconds

    def test_convert_durationfield_value_none(self):
        result = self.ops.convert_durationfield_value(None, None, None)
        self.assertIsNone(result)

    def test_convert_durationfield_value_int(self):
        result = self.ops.convert_durationfield_value(5000000, None, None)
        self.assertEqual(result, 5000000)

    def test_deferrable_sql(self):
        self.assertEqual(
            self.ops.deferrable_sql(), " DEFERRABLE INITIALLY DEFERRED"
        )

    def test_sequence_reset_sql(self):
        model = mock.MagicMock()
        field = mock.MagicMock(spec=models.AutoField)
        field.column = "id"
        model._meta.local_fields = [field]
        model._meta.db_table = "my_table"
        result = self.ops.sequence_reset_sql(mock.MagicMock(), [model])
        self.assertTrue(len(result) > 0)
        self.assertIn("setval", result[0])
        self.assertIn("my_table", result[0])


# ---------------------------------------------------------------------------
# Test: DatabaseFeatures
# ---------------------------------------------------------------------------

class TestDatabaseFeatures(TestCase):
    def setUp(self):
        self.features = DatabaseFeatures(connection=mock.MagicMock())

    def test_can_return_columns_from_insert(self):
        self.assertTrue(self.features.can_return_columns_from_insert)

    def test_can_return_rows_from_bulk_insert(self):
        self.assertTrue(self.features.can_return_rows_from_bulk_insert)

    def test_has_bulk_insert(self):
        self.assertTrue(self.features.has_bulk_insert)

    def test_has_native_uuid_field(self):
        self.assertTrue(self.features.has_native_uuid_field)

    def test_has_native_json_field(self):
        self.assertTrue(self.features.has_native_json_field)

    def test_uses_savepoints(self):
        self.assertTrue(self.features.uses_savepoints)

    def test_can_rollback_ddl(self):
        self.assertTrue(self.features.can_rollback_ddl)

    def test_supports_over_clause(self):
        self.assertTrue(self.features.supports_over_clause)

    def test_supports_aggregate_filter_clause(self):
        self.assertTrue(self.features.supports_aggregate_filter_clause)

    def test_supports_partial_indexes(self):
        self.assertTrue(self.features.supports_partial_indexes)

    def test_supports_expression_indexes(self):
        self.assertTrue(self.features.supports_expression_indexes)

    def test_can_distinct_on_fields(self):
        self.assertTrue(self.features.can_distinct_on_fields)

    def test_has_select_for_update(self):
        self.assertTrue(self.features.has_select_for_update)

    def test_has_select_for_update_nowait(self):
        self.assertTrue(self.features.has_select_for_update_nowait)

    def test_has_select_for_update_skip_locked(self):
        self.assertTrue(self.features.has_select_for_update_skip_locked)

    def test_has_select_for_update_of(self):
        self.assertTrue(self.features.has_select_for_update_of)

    def test_supports_order_by_nulls_modifier(self):
        self.assertTrue(self.features.supports_order_by_nulls_modifier)

    def test_supports_json_field_contains(self):
        self.assertTrue(self.features.supports_json_field_contains)

    def test_supports_update_conflicts(self):
        self.assertTrue(self.features.supports_update_conflicts)

    def test_supports_boolean_expr_in_select_clause(self):
        self.assertTrue(self.features.supports_boolean_expr_in_select_clause)

    def test_supports_explaining_query_execution(self):
        self.assertTrue(self.features.supports_explaining_query_execution)

    def test_can_defer_constraint_checks(self):
        self.assertTrue(self.features.can_defer_constraint_checks)

    def test_supports_deferrable_unique_constraints(self):
        self.assertTrue(self.features.supports_deferrable_unique_constraints)

    def test_supports_comments(self):
        self.assertTrue(self.features.supports_comments)

    def test_can_introspect_autofield(self):
        self.assertTrue(self.features.can_introspect_autofield)

    def test_can_introspect_small_integer_field(self):
        self.assertTrue(self.features.can_introspect_small_integer_field)

    def test_introspected_field_types(self):
        types = self.features.introspected_field_types
        self.assertEqual(types["AutoField"], "AutoField")
        self.assertEqual(types["BigAutoField"], "BigAutoField")
        self.assertEqual(types["UUIDField"], "UUIDField")
        self.assertEqual(types["JSONField"], "JSONField")

    def test_django_test_skips(self):
        self.assertIsInstance(self.features.django_test_skips, dict)


# ---------------------------------------------------------------------------
# Test: DatabaseSchemaEditor
# ---------------------------------------------------------------------------

class TestDatabaseSchemaEditor(TestCase):
    def test_data_types(self):
        self.assertEqual(DatabaseSchemaEditor.data_types["AutoField"], "SERIAL")
        self.assertEqual(DatabaseSchemaEditor.data_types["BigAutoField"], "BIGSERIAL")
        self.assertEqual(DatabaseSchemaEditor.data_types["SmallAutoField"], "SMALLSERIAL")
        self.assertEqual(DatabaseSchemaEditor.data_types["BooleanField"], "BOOLEAN")
        self.assertEqual(DatabaseSchemaEditor.data_types["UUIDField"], "UUID")
        self.assertEqual(DatabaseSchemaEditor.data_types["JSONField"], "JSONB")
        self.assertEqual(DatabaseSchemaEditor.data_types["TextField"], "TEXT")
        self.assertEqual(DatabaseSchemaEditor.data_types["BinaryField"], "BYTEA")
        self.assertEqual(
            DatabaseSchemaEditor.data_types["DateTimeField"],
            "TIMESTAMP WITH TIME ZONE",
        )
        self.assertEqual(
            DatabaseSchemaEditor.data_types["DecimalField"],
            "NUMERIC(%(max_digits)s, %(decimal_places)s)",
        )
        self.assertEqual(
            DatabaseSchemaEditor.data_types["CharField"],
            "VARCHAR(%(max_length)s)",
        )

    def test_data_type_check_constraints(self):
        checks = DatabaseSchemaEditor.data_type_check_constraints
        self.assertIn("PositiveIntegerField", checks)
        self.assertIn(">= 0", checks["PositiveIntegerField"])

    def test_quote_value_string(self):
        editor = DatabaseSchemaEditor.__new__(DatabaseSchemaEditor)
        self.assertEqual(editor.quote_value("hello"), "'hello'")
        self.assertEqual(editor.quote_value("it's"), "'it''s'")

    def test_quote_value_bool(self):
        editor = DatabaseSchemaEditor.__new__(DatabaseSchemaEditor)
        self.assertEqual(editor.quote_value(True), "TRUE")
        self.assertEqual(editor.quote_value(False), "FALSE")

    def test_quote_value_none(self):
        editor = DatabaseSchemaEditor.__new__(DatabaseSchemaEditor)
        self.assertEqual(editor.quote_value(None), "NULL")

    def test_quote_value_int(self):
        editor = DatabaseSchemaEditor.__new__(DatabaseSchemaEditor)
        self.assertEqual(editor.quote_value(42), "42")

    def test_quote_value_bytes(self):
        editor = DatabaseSchemaEditor.__new__(DatabaseSchemaEditor)
        result = editor.quote_value(b"\xca\xfe")
        self.assertIn("cafe", result)


# ---------------------------------------------------------------------------
# Test: DatabaseClient
# ---------------------------------------------------------------------------

class TestDatabaseClient(TestCase):
    def test_settings_to_cmd_args(self):
        settings_dict = {
            "HOST": "myhost",
            "PORT": "12520",
            "NAME": "mydb",
            "USER": "admin",
            "PASSWORD": "secret",
        }
        args, env = DatabaseClient.settings_to_cmd_args_env(settings_dict, [])
        self.assertEqual(args[0], "pyrosql")
        self.assertIn("--host", args)
        self.assertIn("myhost", args)
        self.assertIn("--port", args)
        self.assertIn("12520", args)
        self.assertIn("--user", args)
        self.assertIn("admin", args)
        self.assertIn("mydb", args)
        self.assertIsNotNone(env)
        self.assertEqual(env["PYROSQL_PASSWORD"], "secret")

    def test_settings_to_cmd_args_no_password(self):
        settings_dict = {
            "HOST": "myhost",
            "PORT": "12520",
            "NAME": "mydb",
            "USER": "admin",
            "PASSWORD": "",
        }
        args, env = DatabaseClient.settings_to_cmd_args_env(settings_dict, [])
        self.assertIsNone(env)

    def test_settings_to_cmd_args_extra_parameters(self):
        settings_dict = {
            "HOST": "myhost",
            "PORT": "12520",
            "NAME": "mydb",
            "USER": "",
            "PASSWORD": "",
        }
        args, env = DatabaseClient.settings_to_cmd_args_env(
            settings_dict, ["--verbose"]
        )
        self.assertIn("--verbose", args)

    def test_settings_to_cmd_args_minimal(self):
        settings_dict = {}
        args, env = DatabaseClient.settings_to_cmd_args_env(settings_dict, [])
        self.assertEqual(args, ["pyrosql"])
        self.assertIsNone(env)


# ---------------------------------------------------------------------------
# Test: DatabaseCreation
# ---------------------------------------------------------------------------

class TestDatabaseCreation(TestCase):
    def setUp(self):
        self.mock_conn = mock.MagicMock()
        self.mock_conn.ops.quote_name = lambda name: '"%s"' % name
        self.mock_conn.settings_dict = {
            "HOST": "localhost",
            "PORT": "12520",
            "NAME": "mydb",
            "USER": "",
            "PASSWORD": "",
            "TEST": {},
        }
        self.creation = DatabaseCreation(self.mock_conn)

    def test_sql_table_creation_suffix_default(self):
        result = self.creation.sql_table_creation_suffix()
        self.assertEqual(result, "")

    def test_sql_table_creation_suffix_with_encoding(self):
        self.mock_conn.settings_dict["TEST"] = {"CHARSET": "UTF8"}
        result = self.creation.sql_table_creation_suffix()
        self.assertIn("ENCODING", result)
        self.assertIn("UTF8", result)

    def test_sql_table_creation_suffix_with_template(self):
        self.mock_conn.settings_dict["TEST"] = {"TEMPLATE": "template0"}
        result = self.creation.sql_table_creation_suffix()
        self.assertIn("TEMPLATE", result)

    def test_quote_name(self):
        result = self.creation._quote_name("my_table")
        self.assertEqual(result, '"my_table"')


# ---------------------------------------------------------------------------
# Test: DatabaseIntrospection
# ---------------------------------------------------------------------------

class TestDatabaseIntrospection(TestCase):
    def setUp(self):
        self.mock_conn = mock.MagicMock()
        self.introspection = DatabaseIntrospection(self.mock_conn)

    def test_data_types_reverse(self):
        dtypes = self.introspection.data_types_reverse
        self.assertEqual(dtypes["boolean"], "BooleanField")
        self.assertEqual(dtypes["integer"], "IntegerField")
        self.assertEqual(dtypes["bigint"], "BigIntegerField")
        self.assertEqual(dtypes["smallint"], "SmallIntegerField")
        self.assertEqual(dtypes["text"], "TextField")
        self.assertEqual(dtypes["varchar"], "CharField")
        self.assertEqual(dtypes["character varying"], "CharField")
        self.assertEqual(dtypes["date"], "DateField")
        self.assertEqual(dtypes["timestamp with time zone"], "DateTimeField")
        self.assertEqual(dtypes["uuid"], "UUIDField")
        self.assertEqual(dtypes["jsonb"], "JSONField")
        self.assertEqual(dtypes["bytea"], "BinaryField")
        self.assertEqual(dtypes["serial"], "AutoField")
        self.assertEqual(dtypes["bigserial"], "BigAutoField")
        self.assertEqual(dtypes["smallserial"], "SmallAutoField")
        self.assertEqual(dtypes["numeric"], "DecimalField")
        self.assertEqual(dtypes["double precision"], "FloatField")
        self.assertEqual(dtypes["inet"], "GenericIPAddressField")

    def test_get_field_type_autofield(self):
        desc = mock.MagicMock()
        desc.is_autofield = True
        result = self.introspection.get_field_type("integer", desc)
        self.assertEqual(result, "AutoField")

    def test_get_field_type_big_autofield(self):
        desc = mock.MagicMock()
        desc.is_autofield = True
        result = self.introspection.get_field_type("bigint", desc)
        self.assertEqual(result, "BigAutoField")

    def test_get_field_type_small_autofield(self):
        desc = mock.MagicMock()
        desc.is_autofield = True
        result = self.introspection.get_field_type("smallint", desc)
        self.assertEqual(result, "SmallAutoField")

    def test_get_field_type_non_autofield(self):
        desc = mock.MagicMock()
        desc.is_autofield = False
        result = self.introspection.get_field_type("integer", desc)
        self.assertEqual(result, "IntegerField")

    def test_get_table_list(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("auth_user", "BASE TABLE"),
            ("my_view", "VIEW"),
        ]
        result = self.introspection.get_table_list(mock_cursor)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "auth_user")
        self.assertEqual(result[0].type, "t")
        self.assertEqual(result[1].name, "my_view")
        self.assertEqual(result[1].type, "v")

    def test_get_table_description(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("id", "integer", None, None, None, None, 0, "nextval('t_id_seq')", 1, None),
            ("name", "character varying", None, 255, None, None, 1, None, 0, None),
        ]
        result = self.introspection.get_table_description(mock_cursor, "my_table")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "id")
        self.assertTrue(result[0].is_autofield)
        self.assertEqual(result[1].name, "name")
        self.assertFalse(result[1].is_autofield)

    def test_get_relations(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("author_id", "auth_user", "id"),
        ]
        result = self.introspection.get_relations(mock_cursor, "books")
        self.assertIn("author_id", result)
        self.assertEqual(result["author_id"], ("id", "auth_user"))

    def test_get_primary_key_column(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchone.return_value = ("id",)
        result = self.introspection.get_primary_key_column(mock_cursor, "my_table")
        self.assertEqual(result, "id")

    def test_get_primary_key_column_none(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchone.return_value = None
        result = self.introspection.get_primary_key_column(mock_cursor, "my_table")
        self.assertIsNone(result)

    def test_get_sequences(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [("id",)]
        result = self.introspection.get_sequences(mock_cursor, "my_table")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table"], "my_table")
        self.assertEqual(result[0]["column"], "id")

    def test_get_constraints(self):
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.side_effect = [
            [
                ("pk_my_table", "PRIMARY KEY", "id", 1),
                ("uq_name", "UNIQUE", "name", 1),
            ],
            [
                ("fk_author", "author_id", "auth_user", "id"),
            ],
            [
                ("idx_name", "name", False, False),
            ],
        ]
        result = self.introspection.get_constraints(mock_cursor, "my_table")
        self.assertIn("pk_my_table", result)
        self.assertTrue(result["pk_my_table"]["primary_key"])
        self.assertTrue(result["pk_my_table"]["unique"])
        self.assertIn("uq_name", result)
        self.assertTrue(result["uq_name"]["unique"])
        self.assertIn("fk_author", result)
        self.assertIsNotNone(result["fk_author"]["foreign_key"])
        self.assertIn("idx_name", result)
        self.assertTrue(result["idx_name"]["index"])


# ---------------------------------------------------------------------------
# Test: DatabaseWrapper
# ---------------------------------------------------------------------------

class TestDatabaseWrapper(TestCase):
    def _make_wrapper(self, settings_dict=None):
        wrapper = DatabaseWrapper.__new__(DatabaseWrapper)
        wrapper.settings_dict = settings_dict or {
            "HOST": "localhost",
            "PORT": "12520",
            "NAME": "testdb",
            "USER": "pyrosql",
            "PASSWORD": "secret",
        }
        return wrapper

    def test_get_connection_params(self):
        wrapper = self._make_wrapper({
            "HOST": "myhost",
            "PORT": "5432",
            "NAME": "mydb",
            "USER": "admin",
            "PASSWORD": "pw",
        })
        params = wrapper.get_connection_params()
        self.assertEqual(params["host"], "myhost")
        self.assertEqual(params["port"], 5432)
        self.assertEqual(params["dbname"], "mydb")
        self.assertEqual(params["user"], "admin")
        self.assertEqual(params["password"], "pw")

    def test_get_connection_params_defaults(self):
        wrapper = self._make_wrapper({})
        params = wrapper.get_connection_params()
        self.assertEqual(params["host"], "localhost")
        self.assertEqual(params["port"], 12520)

    def test_vendor(self):
        self.assertEqual(DatabaseWrapper.vendor, "pyrosql")

    def test_display_name(self):
        self.assertEqual(DatabaseWrapper.display_name, "PyroSQL")

    def test_data_types(self):
        self.assertIn("AutoField", DatabaseWrapper.data_types)
        self.assertIn("BigAutoField", DatabaseWrapper.data_types)
        self.assertIn("CharField", DatabaseWrapper.data_types)
        self.assertIn("JSONField", DatabaseWrapper.data_types)
        self.assertIn("UUIDField", DatabaseWrapper.data_types)

    def test_operators(self):
        self.assertIn("exact", DatabaseWrapper.operators)
        self.assertIn("contains", DatabaseWrapper.operators)
        self.assertIn("regex", DatabaseWrapper.operators)
        self.assertIn("iregex", DatabaseWrapper.operators)
        self.assertIn("startswith", DatabaseWrapper.operators)

    def test_pattern_ops(self):
        self.assertIn("contains", DatabaseWrapper.pattern_ops)
        self.assertIn("icontains", DatabaseWrapper.pattern_ops)
        self.assertIn("startswith", DatabaseWrapper.pattern_ops)
        self.assertIn("endswith", DatabaseWrapper.pattern_ops)

    def test_is_usable_true(self):
        wrapper = self._make_wrapper()
        wrapper.connection = MockConnection()
        self.assertTrue(wrapper.is_usable())

    def test_is_usable_false(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        mock_conn.query = mock.Mock(side_effect=Exception("down"))
        wrapper.connection = mock_conn
        self.assertFalse(wrapper.is_usable())

    def test_is_usable_no_connection(self):
        wrapper = self._make_wrapper()
        wrapper.connection = None
        self.assertFalse(wrapper.is_usable())

    def test_commit(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._commit()
        self.assertIn("COMMIT", mock_conn._executes)

    def test_rollback(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._rollback()
        self.assertIn("ROLLBACK", mock_conn._executes)

    def test_commit_no_connection(self):
        wrapper = self._make_wrapper()
        wrapper.connection = None
        # Should not raise
        wrapper._commit()

    def test_rollback_no_connection(self):
        wrapper = self._make_wrapper()
        wrapper.connection = None
        # Should not raise
        wrapper._rollback()

    def test_savepoint(self):
        wrapper = self._make_wrapper()
        wrapper.ops = DatabaseOperations(connection=mock.MagicMock())
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._savepoint("sp1")
        all_cmds = mock_conn._queries + mock_conn._executes
        self.assertTrue(
            any("SAVEPOINT" in q for q in all_cmds)
        )

    def test_savepoint_rollback(self):
        wrapper = self._make_wrapper()
        wrapper.ops = DatabaseOperations(connection=mock.MagicMock())
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._savepoint_rollback("sp1")
        all_cmds = mock_conn._queries + mock_conn._executes
        self.assertTrue(
            any("ROLLBACK TO SAVEPOINT" in q for q in all_cmds)
        )

    def test_savepoint_commit(self):
        wrapper = self._make_wrapper()
        wrapper.ops = DatabaseOperations(connection=mock.MagicMock())
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._savepoint_commit("sp1")
        all_cmds = mock_conn._queries + mock_conn._executes
        self.assertTrue(
            any("RELEASE SAVEPOINT" in q for q in all_cmds)
        )

    def test_close(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._close()
        self.assertTrue(mock_conn.closed)

    def test_create_cursor(self):
        wrapper = self._make_wrapper()
        wrapper.connection = MockConnection()
        cursor = wrapper.create_cursor()
        self.assertIsInstance(cursor, CursorWrapper)

    def test_start_transaction(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._start_transaction_under_autocommit()
        all_cmds = mock_conn._queries + mock_conn._executes
        self.assertTrue(
            any("BEGIN" in q for q in all_cmds)
        )

    def test_set_autocommit_on(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._set_autocommit(True)
        self.assertTrue(
            any("AUTOCOMMIT ON" in q for q in mock_conn._executes)
        )

    def test_set_autocommit_off(self):
        wrapper = self._make_wrapper()
        mock_conn = MockConnection()
        wrapper.connection = mock_conn
        wrapper._set_autocommit(False)
        self.assertTrue(
            any("AUTOCOMMIT OFF" in q for q in mock_conn._executes)
        )

    def test_set_autocommit_no_connection(self):
        wrapper = self._make_wrapper()
        wrapper.connection = None
        # Should not raise
        wrapper._set_autocommit(True)


# ---------------------------------------------------------------------------
# Test: Compiler
# ---------------------------------------------------------------------------

class TestCompiler(TestCase):
    def test_compiler_classes_exist(self):
        from pyrosql.django.compiler import (
            SQLAggregateCompiler,
            SQLCompiler,
            SQLDeleteCompiler,
            SQLInsertCompiler,
            SQLUpdateCompiler,
        )
        from django.db.models.sql import compiler as base_compiler
        self.assertTrue(issubclass(SQLCompiler, base_compiler.SQLCompiler))
        self.assertTrue(issubclass(SQLInsertCompiler, base_compiler.SQLInsertCompiler))
        self.assertTrue(issubclass(SQLDeleteCompiler, base_compiler.SQLDeleteCompiler))
        self.assertTrue(issubclass(SQLUpdateCompiler, base_compiler.SQLUpdateCompiler))
        self.assertTrue(
            issubclass(SQLAggregateCompiler, base_compiler.SQLAggregateCompiler)
        )


# ---------------------------------------------------------------------------
# Test: Edge cases in SQL formatting
# ---------------------------------------------------------------------------

class TestSQLFormattingEdgeCases(TestCase):
    def test_sql_injection_prevention(self):
        result = _format_sql(
            "SELECT * FROM t WHERE name = %s",
            ["Robert'; DROP TABLE students;--"],
        )
        self.assertIn("Robert''; DROP TABLE students;--", result)
        self.assertTrue(result.startswith("SELECT"))

    def test_unicode_params(self):
        result = _format_sql(
            "INSERT INTO t (name) VALUES (%s)",
            ["\u00e9\u00e8\u00ea"],
        )
        self.assertIn("\u00e9\u00e8\u00ea", result)

    def test_large_integer(self):
        result = _format_sql("SELECT %s", [2**63])
        self.assertIn(str(2**63), result)

    def test_negative_decimal(self):
        result = _format_sql("SELECT %s", [decimal.Decimal("-999.99")])
        self.assertIn("-999.99", result)

    def test_empty_string_param(self):
        result = _format_sql("SELECT %s", [""])
        self.assertIn("''", result)

    def test_multiple_percent_s_in_string_literal(self):
        result = _format_sql("SELECT '%%s' || %s", ["test"])
        self.assertIn("%s", result)
        self.assertIn("'test'", result)

    def test_bool_in_where(self):
        result = _format_sql(
            "SELECT * FROM t WHERE active = %s AND deleted = %s",
            [True, False],
        )
        self.assertIn("TRUE", result)
        self.assertIn("FALSE", result)

    def test_none_in_insert(self):
        result = _format_sql(
            "INSERT INTO t (a, b) VALUES (%s, %s)",
            [1, None],
        )
        self.assertIn("1", result)
        self.assertIn("NULL", result)


# ---------------------------------------------------------------------------
# Test: Type mappings are complete
# ---------------------------------------------------------------------------

class TestTypeMappings(TestCase):
    def test_all_django_field_types_have_sql_type(self):
        required_types = [
            "AutoField",
            "BigAutoField",
            "SmallAutoField",
            "BinaryField",
            "BooleanField",
            "CharField",
            "DateField",
            "DateTimeField",
            "DecimalField",
            "FloatField",
            "IntegerField",
            "BigIntegerField",
            "SmallIntegerField",
            "TextField",
            "TimeField",
            "UUIDField",
            "JSONField",
            "SlugField",
            "FilePathField",
            "IPAddressField",
            "GenericIPAddressField",
            "PositiveIntegerField",
            "PositiveBigIntegerField",
            "PositiveSmallIntegerField",
            "DurationField",
        ]
        for field_type in required_types:
            self.assertIn(
                field_type,
                DatabaseSchemaEditor.data_types,
                f"Missing SQL type for {field_type}",
            )

    def test_all_reverse_types_are_valid(self):
        valid_fields = {
            "AutoField",
            "BigAutoField",
            "SmallAutoField",
            "BooleanField",
            "SmallIntegerField",
            "IntegerField",
            "BigIntegerField",
            "FloatField",
            "DecimalField",
            "CharField",
            "TextField",
            "DateField",
            "TimeField",
            "DateTimeField",
            "UUIDField",
            "JSONField",
            "BinaryField",
            "GenericIPAddressField",
        }
        for sql_type, django_type in DatabaseIntrospection.data_types_reverse.items():
            self.assertIn(
                django_type,
                valid_fields,
                f"Reverse mapping {sql_type} -> {django_type} not valid",
            )


# ---------------------------------------------------------------------------
# Test: Query detection in CursorWrapper
# ---------------------------------------------------------------------------

class TestQueryDetection(TestCase):
    def test_with_cte_detected_as_query(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("WITH cte AS (SELECT 1) SELECT * FROM cte")
        self.assertTrue(len(mock_conn._queries) > 0)

    def test_explain_detected_as_query(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("EXPLAIN SELECT 1")
        self.assertTrue(len(mock_conn._queries) > 0)

    def test_show_detected_as_query(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("SHOW TABLES")
        self.assertTrue(len(mock_conn._queries) > 0)

    def test_insert_returning_detected_as_query(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("INSERT INTO t (a) VALUES (1) RETURNING id")
        self.assertTrue(len(mock_conn._queries) > 0)

    def test_plain_insert_detected_as_execute(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("INSERT INTO t (a) VALUES (1)")
        self.assertTrue(len(mock_conn._executes) > 0)
        self.assertEqual(len(mock_conn._queries), 0)

    def test_update_detected_as_execute(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("UPDATE t SET a = 1")
        self.assertTrue(len(mock_conn._executes) > 0)

    def test_delete_detected_as_execute(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("DELETE FROM t WHERE id = 1")
        self.assertTrue(len(mock_conn._executes) > 0)

    def test_create_table_detected_as_execute(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        self.assertTrue(len(mock_conn._executes) > 0)

    def test_values_clause_detected_as_query(self):
        mock_conn = MockConnection()
        cursor = CursorWrapper(mock_conn)
        cursor.execute("VALUES (1), (2), (3)")
        self.assertTrue(len(mock_conn._queries) > 0)
