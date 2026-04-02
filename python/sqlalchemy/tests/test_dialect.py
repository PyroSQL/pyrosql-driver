"""Tests for the PyroSQL SQLAlchemy dialect.

These tests exercise the dialect, compiler, type mapping, and DBAPI wrapper
in isolation (without a running PyroSQL server) using mocks where necessary,
plus unit tests for the pure-logic components.
"""

import unittest
from datetime import date, datetime, time
from decimal import Decimal
from unittest import mock
from uuid import UUID

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    SmallInteger,
    BigInteger,
    String,
    Text,
    Boolean,
    Float,
    Numeric,
    DateTime,
    Date,
    Time,
    LargeBinary,
    Table,
    Uuid,
    create_engine,
    select,
    insert,
    text,
    literal,
)
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateTable, DropTable

from pyrosql_dialect import PyroSQLDialect
from pyrosql_dialect.base import PyroSQLDialect as BaseDialect
from pyrosql_dialect.compiler import (
    PyroSQLCompiler,
    PyroSQLDDLCompiler,
    PyroSQLTypeCompiler,
)
from pyrosql_dialect.types import (
    SERIAL,
    BIGSERIAL,
    SMALLSERIAL,
    JSONB,
    JSON,
    UUID as PyroUUID,
    TIMESTAMP,
    TIMESTAMPTZ,
    BYTEA,
    INET,
    CIDR,
    MACADDR,
    INTERVAL,
    TEXT,
    lookup_type,
)
from pyrosql_dialect.dbapi import (
    _quote_value,
    _bind_parameters,
    Connection as DBAPIConnection,
    Cursor,
    Error,
    DatabaseError,
    OperationalError,
    ProgrammingError,
    InterfaceError,
)


# ---------------------------------------------------------------------------
# Type mapping tests
# ---------------------------------------------------------------------------

class TestLookupType(unittest.TestCase):
    """Test the type name -> SQLAlchemy type resolution."""

    def test_integer_types(self):
        for name in ("integer", "int", "int4"):
            typ = lookup_type(name)
            self.assertIsInstance(typ, Integer)

    def test_smallint(self):
        typ = lookup_type("smallint")
        self.assertIsInstance(typ, SmallInteger)

    def test_bigint(self):
        typ = lookup_type("bigint")
        self.assertIsInstance(typ, BigInteger)

    def test_serial(self):
        typ = lookup_type("serial")
        self.assertIsInstance(typ, SERIAL)

    def test_bigserial(self):
        typ = lookup_type("bigserial")
        self.assertIsInstance(typ, BIGSERIAL)

    def test_boolean(self):
        typ = lookup_type("boolean")
        self.assertIsInstance(typ, Boolean)

    def test_text(self):
        typ = lookup_type("text")
        self.assertIsInstance(typ, Text)

    def test_varchar_with_length(self):
        typ = lookup_type("varchar(255)")
        self.assertIsInstance(typ, String)
        self.assertEqual(typ.length, 255)

    def test_varchar_without_length(self):
        typ = lookup_type("varchar")
        self.assertIsInstance(typ, String)

    def test_character_varying_with_length(self):
        typ = lookup_type("character varying(100)")
        self.assertIsInstance(typ, String)
        self.assertEqual(typ.length, 100)

    def test_numeric_with_precision_and_scale(self):
        typ = lookup_type("numeric(10, 2)")
        self.assertIsInstance(typ, Numeric)
        self.assertEqual(typ.precision, 10)
        self.assertEqual(typ.scale, 2)

    def test_numeric_without_args(self):
        typ = lookup_type("numeric")
        self.assertIsInstance(typ, Numeric)

    def test_timestamp(self):
        typ = lookup_type("timestamp")
        self.assertIsInstance(typ, DateTime)

    def test_timestamptz(self):
        typ = lookup_type("timestamptz")
        self.assertIsInstance(typ, DateTime)

    def test_jsonb(self):
        typ = lookup_type("jsonb")
        self.assertIsInstance(typ, JSONB)

    def test_uuid(self):
        typ = lookup_type("uuid")
        self.assertIsInstance(typ, Uuid)

    def test_bytea(self):
        typ = lookup_type("bytea")
        self.assertIsInstance(typ, LargeBinary)

    def test_inet(self):
        typ = lookup_type("inet")
        self.assertIsInstance(typ, INET)

    def test_unknown_returns_nulltype(self):
        from sqlalchemy.types import NullType
        typ = lookup_type("some_exotic_type")
        self.assertIsInstance(typ, NullType)

    def test_case_insensitive(self):
        typ = lookup_type("INTEGER")
        self.assertIsInstance(typ, Integer)

    def test_whitespace_handling(self):
        typ = lookup_type("  text  ")
        self.assertIsInstance(typ, Text)


# ---------------------------------------------------------------------------
# DBAPI _quote_value tests
# ---------------------------------------------------------------------------

class TestQuoteValue(unittest.TestCase):
    """Test literal value quoting for parameter binding."""

    def test_none(self):
        self.assertEqual(_quote_value(None), "NULL")

    def test_bool_true(self):
        self.assertEqual(_quote_value(True), "TRUE")

    def test_bool_false(self):
        self.assertEqual(_quote_value(False), "FALSE")

    def test_int(self):
        self.assertEqual(_quote_value(42), "42")

    def test_float(self):
        result = _quote_value(3.14)
        self.assertIn("3.14", result)

    def test_decimal(self):
        self.assertEqual(_quote_value(Decimal("99.99")), "99.99")

    def test_string(self):
        self.assertEqual(_quote_value("hello"), "'hello'")

    def test_string_with_quotes(self):
        self.assertEqual(_quote_value("it's"), "'it''s'")

    def test_bytes(self):
        result = _quote_value(b"\x00\xff")
        self.assertEqual(result, "'\\x00ff'")

    def test_datetime(self):
        dt = datetime(2025, 1, 15, 10, 30, 0)
        result = _quote_value(dt)
        self.assertIn("2025-01-15", result)

    def test_date(self):
        d = date(2025, 6, 1)
        result = _quote_value(d)
        self.assertIn("2025-06-01", result)

    def test_time(self):
        t = time(14, 30, 0)
        result = _quote_value(t)
        self.assertIn("14:30:00", result)

    def test_uuid(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        result = _quote_value(u)
        self.assertIn("12345678-1234-5678-1234-567812345678", result)

    def test_list(self):
        result = _quote_value([1, 2, 3])
        self.assertIn("[1, 2, 3]", result)

    def test_dict(self):
        result = _quote_value({"key": "val"})
        self.assertIn("key", result)
        self.assertIn("val", result)


# ---------------------------------------------------------------------------
# DBAPI _bind_parameters tests
# ---------------------------------------------------------------------------

class TestBindParameters(unittest.TestCase):
    """Test parameter binding into SQL strings."""

    def test_no_params(self):
        sql = "SELECT 1"
        self.assertEqual(_bind_parameters(sql), "SELECT 1")

    def test_positional_params(self):
        sql = "SELECT * FROM t WHERE id = ? AND name = ?"
        result = _bind_parameters(sql, [42, "alice"])
        self.assertIn("42", result)
        self.assertIn("'alice'", result)
        self.assertNotIn("?", result)

    def test_named_params(self):
        sql = "SELECT * FROM t WHERE id = :id AND name = :name"
        result = _bind_parameters(sql, {"id": 1, "name": "bob"})
        self.assertIn("1", result)
        self.assertIn("'bob'", result)
        self.assertNotIn(":id", result)
        self.assertNotIn(":name", result)

    def test_named_missing_raises(self):
        sql = "SELECT * FROM t WHERE id = :id"
        with self.assertRaises(ProgrammingError):
            _bind_parameters(sql, {"wrong_key": 1})


# ---------------------------------------------------------------------------
# DBAPI Cursor tests (mocked connection)
# ---------------------------------------------------------------------------

class TestCursor(unittest.TestCase):
    """Test the PEP 249 Cursor using a mocked raw connection."""

    def _make_cursor(self, query_result=None, execute_result=0):
        """Create a Cursor backed by a mocked DBAPI connection."""
        mock_raw = mock.MagicMock()

        if query_result is not None:
            mock_raw.query.return_value = query_result
        else:
            # Default: empty result
            result_obj = mock.MagicMock()
            result_obj.columns = []
            result_obj.rows = []
            result_obj.rows_affected = 0
            mock_raw.query.return_value = result_obj

        mock_raw.execute.return_value = execute_result

        conn = mock.MagicMock(spec=DBAPIConnection)
        conn._raw_conn = mock_raw
        cursor = Cursor(conn)
        return cursor, mock_raw

    def test_execute_select(self):
        result_obj = mock.MagicMock()
        result_obj.columns = ["id", "name"]
        result_obj.rows = [[1, "alice"], [2, "bob"]]
        result_obj.rows_affected = 0

        cursor, raw = self._make_cursor(query_result=result_obj)
        cursor.execute("SELECT id, name FROM users")

        self.assertIsNotNone(cursor.description)
        self.assertEqual(len(cursor.description), 2)
        self.assertEqual(cursor.description[0][0], "id")
        self.assertEqual(cursor.description[1][0], "name")

    def test_fetchone(self):
        result_obj = mock.MagicMock()
        result_obj.columns = ["x"]
        result_obj.rows = [[10], [20]]
        result_obj.rows_affected = 0

        cursor, _ = self._make_cursor(query_result=result_obj)
        cursor.execute("SELECT x FROM t")

        row1 = cursor.fetchone()
        self.assertEqual(row1, (10, ))

        row2 = cursor.fetchone()
        self.assertEqual(row2, (20, ))

        row3 = cursor.fetchone()
        self.assertIsNone(row3)

    def test_fetchall(self):
        result_obj = mock.MagicMock()
        result_obj.columns = ["v"]
        result_obj.rows = [[1], [2], [3]]
        result_obj.rows_affected = 0

        cursor, _ = self._make_cursor(query_result=result_obj)
        cursor.execute("SELECT v FROM t")
        rows = cursor.fetchall()
        self.assertEqual(rows, [(1,), (2,), (3,)])

    def test_fetchmany(self):
        result_obj = mock.MagicMock()
        result_obj.columns = ["v"]
        result_obj.rows = [[1], [2], [3], [4], [5]]
        result_obj.rows_affected = 0

        cursor, _ = self._make_cursor(query_result=result_obj)
        cursor.execute("SELECT v FROM t")
        batch = cursor.fetchmany(2)
        self.assertEqual(batch, [(1,), (2,)])
        batch = cursor.fetchmany(2)
        self.assertEqual(batch, [(3,), (4,)])
        batch = cursor.fetchmany(2)
        self.assertEqual(batch, [(5,)])

    def test_execute_insert(self):
        cursor, raw = self._make_cursor(execute_result=1)
        cursor.execute("INSERT INTO t (name) VALUES ('x')")
        self.assertEqual(cursor.rowcount, 1)
        self.assertIsNone(cursor.description)

    def test_execute_returning(self):
        result_obj = mock.MagicMock()
        result_obj.columns = ["id"]
        result_obj.rows = [[42]]
        result_obj.rows_affected = 1

        cursor, _ = self._make_cursor(query_result=result_obj)
        cursor.execute("INSERT INTO t (name) VALUES ('x') RETURNING id")
        self.assertEqual(cursor.lastrowid, 42)
        row = cursor.fetchone()
        self.assertEqual(row, (42,))

    def test_close_prevents_fetch(self):
        cursor, _ = self._make_cursor()
        cursor.close()
        with self.assertRaises(InterfaceError):
            cursor.fetchone()

    def test_iter(self):
        result_obj = mock.MagicMock()
        result_obj.columns = ["n"]
        result_obj.rows = [[1], [2]]
        result_obj.rows_affected = 0

        cursor, _ = self._make_cursor(query_result=result_obj)
        cursor.execute("SELECT n FROM t")
        collected = list(cursor)
        self.assertEqual(collected, [(1,), (2,)])

    def test_executemany(self):
        cursor, raw = self._make_cursor(execute_result=1)
        cursor.executemany(
            "INSERT INTO t (v) VALUES (?)",
            [[1], [2], [3]],
        )
        self.assertEqual(raw.execute.call_count, 3)


# ---------------------------------------------------------------------------
# Dialect unit tests
# ---------------------------------------------------------------------------

class TestDialectConfig(unittest.TestCase):
    """Test basic dialect configuration and URL parsing."""

    def test_name(self):
        d = PyroSQLDialect()
        self.assertEqual(d.name, "pyrosql")

    def test_create_connect_args_full_url(self):
        d = PyroSQLDialect()
        url = make_url("pyrosql://myuser:mypass@dbhost:5555/mydb")
        args, kwargs = d.create_connect_args(url)
        self.assertEqual(args, [])
        self.assertEqual(kwargs["host"], "dbhost")
        self.assertEqual(kwargs["port"], 5555)
        self.assertEqual(kwargs["database"], "mydb")
        self.assertEqual(kwargs["user"], "myuser")
        self.assertEqual(kwargs["password"], "mypass")

    def test_create_connect_args_defaults(self):
        d = PyroSQLDialect()
        url = make_url("pyrosql://")
        args, kwargs = d.create_connect_args(url)
        self.assertEqual(kwargs["host"], "127.0.0.1")
        self.assertEqual(kwargs["port"], 12520)

    def test_supports_returning(self):
        d = PyroSQLDialect()
        self.assertTrue(d.implicit_returning)

    def test_supports_native_boolean(self):
        d = PyroSQLDialect()
        self.assertTrue(d.supports_native_boolean)

    def test_dbapi_module(self):
        mod = PyroSQLDialect.dbapi()
        self.assertEqual(mod.apilevel, "2.0")
        self.assertEqual(mod.paramstyle, "qmark")

    def test_import_dbapi(self):
        mod = PyroSQLDialect.import_dbapi()
        self.assertIs(mod, PyroSQLDialect.dbapi())

    def test_default_schema(self):
        d = PyroSQLDialect()
        self.assertEqual(d.default_schema_name, "public")

    def test_server_version(self):
        d = PyroSQLDialect()
        version = d._get_server_version_info(None)
        self.assertEqual(version, (1, 0, 0))


# ---------------------------------------------------------------------------
# Compiler tests (DDL rendering)
# ---------------------------------------------------------------------------

class TestTypeCompiler(unittest.TestCase):
    """Test DDL type compilation."""

    def _compiler(self):
        d = PyroSQLDialect()
        return d.type_compiler_instance

    def test_boolean(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Boolean()), "BOOLEAN")

    def test_integer(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Integer()), "INTEGER")

    def test_bigint(self):
        tc = self._compiler()
        self.assertEqual(tc.process(BigInteger()), "BIGINT")

    def test_smallint(self):
        tc = self._compiler()
        self.assertEqual(tc.process(SmallInteger()), "SMALLINT")

    def test_float(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Float()), "REAL")

    def test_numeric_with_precision(self):
        tc = self._compiler()
        result = tc.process(Numeric(precision=8, scale=2))
        self.assertEqual(result, "NUMERIC(8, 2)")

    def test_numeric_without_precision(self):
        tc = self._compiler()
        result = tc.process(Numeric())
        self.assertEqual(result, "NUMERIC")

    def test_varchar_with_length(self):
        tc = self._compiler()
        result = tc.process(String(50))
        self.assertEqual(result, "VARCHAR(50)")

    def test_text(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Text()), "TEXT")

    def test_datetime(self):
        tc = self._compiler()
        self.assertEqual(tc.process(DateTime()), "TIMESTAMP")

    def test_date(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Date()), "DATE")

    def test_time(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Time()), "TIME")

    def test_large_binary(self):
        tc = self._compiler()
        self.assertEqual(tc.process(LargeBinary()), "BYTEA")

    def test_uuid(self):
        tc = self._compiler()
        self.assertEqual(tc.process(Uuid()), "UUID")

    def test_jsonb(self):
        tc = self._compiler()
        self.assertEqual(tc.process(JSONB()), "JSONB")

    def test_timestamp_tz(self):
        tc = self._compiler()
        result = tc.process(TIMESTAMPTZ())
        self.assertEqual(result, "TIMESTAMP WITH TIME ZONE")

    def test_inet(self):
        tc = self._compiler()
        self.assertEqual(tc.process(INET()), "INET")

    def test_serial(self):
        tc = self._compiler()
        self.assertEqual(tc.process(SERIAL()), "SERIAL")


class TestDDLCompiler(unittest.TestCase):
    """Test DDL statement generation."""

    def _compile_table(self, table):
        d = PyroSQLDialect()
        create = CreateTable(table)
        compiled = create.compile(dialect=d)
        return str(compiled).strip()

    def test_create_table_simple(self):
        meta = MetaData()
        t = Table("users", meta,
                   Column("id", Integer, primary_key=True),
                   Column("name", String(100), nullable=False),
                   Column("active", Boolean))
        ddl = self._compile_table(t)
        self.assertIn("CREATE TABLE users", ddl)
        self.assertIn("SERIAL", ddl)  # autoincrement primary key
        self.assertIn("VARCHAR(100)", ddl)
        self.assertIn("NOT NULL", ddl)
        self.assertIn("BOOLEAN", ddl)

    def test_create_table_bigserial_pk(self):
        meta = MetaData()
        t = Table("big_table", meta,
                   Column("id", BigInteger, primary_key=True))
        ddl = self._compile_table(t)
        self.assertIn("BIGSERIAL", ddl)

    def test_create_table_no_autoincrement(self):
        meta = MetaData()
        t = Table("manual_table", meta,
                   Column("id", Integer, primary_key=True, autoincrement=False),
                   Column("val", Text))
        ddl = self._compile_table(t)
        self.assertIn("INTEGER", ddl)
        self.assertNotIn("SERIAL", ddl)
        self.assertIn("TEXT", ddl)

    def test_drop_table(self):
        meta = MetaData()
        t = Table("to_drop", meta, Column("id", Integer, primary_key=True))
        d = PyroSQLDialect()
        drop = DropTable(t)
        compiled = str(drop.compile(dialect=d)).strip()
        self.assertIn("DROP TABLE to_drop", compiled)


class TestSQLCompiler(unittest.TestCase):
    """Test SQL statement compilation."""

    def _dialect(self):
        return PyroSQLDialect()

    def test_select_with_limit_offset(self):
        meta = MetaData()
        t = Table("items", meta, Column("id", Integer))
        stmt = select(t.c.id).limit(10).offset(5)
        compiled = stmt.compile(dialect=self._dialect())
        sql = str(compiled)
        self.assertIn("LIMIT", sql)
        self.assertIn("OFFSET", sql)

    def test_boolean_literal_true(self):
        stmt = select(literal(True))
        compiled = stmt.compile(
            dialect=self._dialect(), compile_kwargs={"literal_binds": True}
        )
        sql = str(compiled)
        # Should contain "true" (case-insensitive check).
        self.assertIn("true", sql.lower())

    def test_boolean_literal_false(self):
        stmt = select(literal(False))
        compiled = stmt.compile(
            dialect=self._dialect(), compile_kwargs={"literal_binds": True}
        )
        sql = str(compiled)
        self.assertIn("false", sql.lower())

    def test_insert_returning(self):
        meta = MetaData()
        t = Table("users", meta,
                   Column("id", Integer, primary_key=True),
                   Column("name", String(50)))
        stmt = insert(t).values(name="alice").returning(t.c.id)
        compiled = stmt.compile(dialect=self._dialect())
        sql = str(compiled)
        self.assertIn("RETURNING", sql)


# ---------------------------------------------------------------------------
# Custom type tests
# ---------------------------------------------------------------------------

class TestCustomTypes(unittest.TestCase):
    """Test custom PyroSQL type objects."""

    def test_serial_is_integer(self):
        self.assertIsInstance(SERIAL(), Integer)

    def test_bigserial_is_biginteger(self):
        self.assertIsInstance(BIGSERIAL(), BigInteger)

    def test_smallserial_is_smallinteger(self):
        self.assertIsInstance(SMALLSERIAL(), SmallInteger)

    def test_jsonb_visit_name(self):
        self.assertEqual(JSONB.__visit_name__, "JSONB")

    def test_timestamptz_has_timezone(self):
        t = TIMESTAMPTZ()
        self.assertTrue(t.timezone)

    def test_bytea_is_largebinary(self):
        self.assertIsInstance(BYTEA(), LargeBinary)


# ---------------------------------------------------------------------------
# DBAPI exception hierarchy tests
# ---------------------------------------------------------------------------

class TestExceptionHierarchy(unittest.TestCase):
    """Verify PEP 249 exception hierarchy."""

    def test_database_error_is_error(self):
        self.assertTrue(issubclass(DatabaseError, Error))

    def test_operational_error(self):
        self.assertTrue(issubclass(OperationalError, DatabaseError))

    def test_programming_error(self):
        self.assertTrue(issubclass(ProgrammingError, DatabaseError))

    def test_interface_error(self):
        self.assertTrue(issubclass(InterfaceError, Error))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
