"""PyroSQL database backend for Django.

Provides DatabaseWrapper, CursorWrapper, and connection management using
the pyrosql Python driver (which talks PWire protocol via FFI).
"""

from __future__ import annotations

import datetime
import decimal
import json
import re
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, Tuple

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError, DatabaseError, InternalError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property

try:
    import pyrosql as pyrosql_driver
except ImportError:
    raise ImproperlyConfigured(
        "Error loading pyrosql module. "
        "Did you install the pyrosql package?"
    )

from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PARAMSTYLE_RE = re.compile(r"(?<!%)%s")


def _format_sql(sql: str, params: Optional[Sequence] = None) -> str:
    """Convert Django-style %s placeholders into inline literals.

    The PyroSQL FFI layer does not support server-side parameter binding,
    so we do safe quoting client-side (similar to how sqlite3 works in
    Django).
    """
    if params is None or len(params) == 0:
        return sql.replace("%%", "%")

    converted = []
    for p in params:
        converted.append(_quote_param(p))

    result = _PARAMSTYLE_RE.sub(lambda m: converted.pop(0) if converted else "%s", sql)
    result = result.replace("%%", "%")
    return result


def _quote_param(value: Any) -> str:
    """Quote a single parameter value for safe SQL interpolation."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return "'\\x%s'" % value.hex()
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            return "'%s'" % value.isoformat()
        return "'%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, datetime.date):
        return "'%s'" % value.isoformat()
    if isinstance(value, datetime.time):
        return "'%s'" % value.isoformat()
    if isinstance(value, datetime.timedelta):
        total_seconds = value.total_seconds()
        return "'%d microseconds'::INTERVAL" % (total_seconds * 1_000_000)
    if isinstance(value, uuid.UUID):
        return "'%s'" % str(value)
    if isinstance(value, (list, dict)):
        json_str = json.dumps(value).replace("'", "''")
        return "'%s'" % json_str
    if isinstance(value, memoryview):
        return "'\\x%s'" % bytes(value).hex()
    # Default: treat as string
    s = str(value)
    s = s.replace("'", "''")
    s = s.replace("\\", "\\\\")
    return "'%s'" % s


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------

class CursorWrapper:
    """DB-API 2.0-like cursor wrapping a pyrosql.Connection.

    Since the PyroSQL FFI layer uses ``query()`` and ``execute()``
    directly on the connection handle, this cursor simply delegates to
    those methods while maintaining description/rowcount state.
    """

    def __init__(self, connection: pyrosql_driver.Connection):
        self.connection = connection
        self.description: Optional[List[Tuple]] = None
        self.rowcount: int = -1
        self.lastrowid: Optional[int] = None
        self._rows: List[List[Any]] = []
        self._row_index: int = 0
        self.arraysize: int = 1
        self._closed: bool = False

    def close(self):
        self._closed = True
        self._rows = []
        self.description = None

    @property
    def closed(self):
        return self._closed

    def _check_closed(self):
        if self._closed:
            raise DatabaseError("Cursor is closed")

    def execute(self, sql: str, params: Optional[Sequence] = None):
        self._check_closed()
        formatted_sql = _format_sql(sql, params)
        self._rows = []
        self._row_index = 0
        self.description = None
        self.lastrowid = None

        try:
            stripped = formatted_sql.lstrip().upper()
            is_query = (
                stripped.startswith("SELECT")
                or stripped.startswith("WITH")
                or stripped.startswith("EXPLAIN")
                or stripped.startswith("SHOW")
                or stripped.startswith("VALUES")
                or "RETURNING" in stripped
            )

            if is_query:
                result = self.connection.query(formatted_sql)
                self._rows = result.rows if result.rows else []
                self.rowcount = result.rows_affected if result.rows_affected else len(self._rows)

                if result.columns:
                    self.description = [
                        (col, None, None, None, None, None, None)
                        for col in result.columns
                    ]
                else:
                    self.description = None

                if "RETURNING" in stripped and self._rows:
                    try:
                        self.lastrowid = self._rows[0][0]
                    except (IndexError, TypeError):
                        pass
            else:
                rows_affected = self.connection.execute(formatted_sql)
                self.rowcount = rows_affected
                self.description = None
                self._rows = []
        except pyrosql_driver.QueryError as e:
            error_msg = str(e)
            if any(
                kw in error_msg.lower()
                for kw in (
                    "unique", "duplicate", "violates", "foreign key",
                    "not null", "constraint",
                )
            ):
                raise IntegrityError(error_msg) from e
            raise DatabaseError(error_msg) from e
        except pyrosql_driver.ConnectionError as e:
            raise DatabaseError(str(e)) from e
        except pyrosql_driver.PyroSQLError as e:
            raise DatabaseError(str(e)) from e

    def executemany(self, sql: str, param_list: Sequence[Sequence]):
        self._check_closed()
        rowcount = 0
        for params in param_list:
            self.execute(sql, params)
            if self.rowcount > 0:
                rowcount += self.rowcount
        self.rowcount = rowcount

    def fetchone(self) -> Optional[List[Any]]:
        self._check_closed()
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return _coerce_row(row)

    def fetchmany(self, size: Optional[int] = None) -> List[List[Any]]:
        self._check_closed()
        if size is None:
            size = self.arraysize
        end = min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index:end]
        self._row_index = end
        return [_coerce_row(r) for r in rows]

    def fetchall(self) -> List[List[Any]]:
        self._check_closed()
        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows)
        return [_coerce_row(r) for r in rows]

    def __iter__(self):
        return iter(self.fetchall())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def _coerce_row(row):
    """Convert row values to tuples for DB-API compliance."""
    if isinstance(row, (list, tuple)):
        return tuple(row)
    return row


# ---------------------------------------------------------------------------
# DatabaseWrapper
# ---------------------------------------------------------------------------

class DatabaseWrapper(BaseDatabaseWrapper):
    """Django database backend for PyroSQL.

    Uses Django's standard ``self.connection`` attribute (set by
    BaseDatabaseWrapper) to hold the underlying pyrosql.Connection.
    """

    vendor = "pyrosql"
    display_name = "PyroSQL"

    data_types = DatabaseSchemaEditor.data_types
    data_types_suffix = {}
    data_type_check_constraints = DatabaseSchemaEditor.data_type_check_constraints

    operators = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE %s",
        "icontains": "LIKE UPPER(%s)",
        "regex": "~ %s",
        "iregex": "~* %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "LIKE UPPER(%s)",
        "iendswith": "LIKE UPPER(%s)",
    }

    pattern_esc = (
        r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'),"
        r" E'%%', E'\\%%'), E'_', E'\\_')"
    )

    pattern_ops = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "LIKE '%%' || UPPER({}) || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "LIKE UPPER({}) || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "LIKE '%%' || UPPER({})",
    }

    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_connection_params(self) -> Dict[str, Any]:
        s = self.settings_dict
        host = s.get("HOST") or "localhost"
        port = int(s.get("PORT") or 12520)
        dbname = s.get("NAME") or ""
        user = s.get("USER") or ""
        password = s.get("PASSWORD") or ""
        return {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    @async_unsafe
    def get_new_connection(self, conn_params: Dict[str, Any]):
        host = conn_params["host"]
        port = conn_params["port"]
        conn = pyrosql_driver.connect(host, port)

        dbname = conn_params.get("dbname")
        if dbname:
            try:
                conn.execute("USE %s" % dbname)
            except pyrosql_driver.PyroSQLError:
                pass

        user = conn_params.get("user")
        password = conn_params.get("password")
        if user and password:
            try:
                conn.execute(
                    "AUTHENTICATE '%s' '%s'"
                    % (
                        user.replace("'", "''"),
                        password.replace("'", "''"),
                    )
                )
            except pyrosql_driver.PyroSQLError:
                pass

        return conn

    def init_connection_state(self):
        tz = self.timezone_name
        if tz and self.connection is not None:
            try:
                self.connection.query(
                    "SET TIME ZONE '%s'" % tz.replace("'", "''")
                )
            except (pyrosql_driver.PyroSQLError, AttributeError):
                pass

    def create_cursor(self, name=None):
        return CursorWrapper(self.connection)

    def _set_autocommit(self, autocommit):
        if self.connection is None:
            return
        try:
            if autocommit:
                self.connection.execute("SET AUTOCOMMIT ON")
            else:
                self.connection.execute("SET AUTOCOMMIT OFF")
        except (pyrosql_driver.PyroSQLError, AttributeError):
            pass

    def is_usable(self):
        try:
            if self.connection is None:
                return False
            self.connection.query("SELECT 1")
            return True
        except Exception:
            return False

    @async_unsafe
    def ensure_connection(self):
        if self.connection is None:
            with self.wrap_database_errors:
                self.connect()

    @cached_property
    def pg_version(self):
        return (15, 0, 0)

    def _commit(self):
        if self.connection is not None:
            try:
                self.connection.execute("COMMIT")
            except pyrosql_driver.PyroSQLError:
                pass

    def _rollback(self):
        if self.connection is not None:
            try:
                self.connection.execute("ROLLBACK")
            except pyrosql_driver.PyroSQLError:
                pass

    def _savepoint_allowed(self):
        return self.features.uses_savepoints

    def _savepoint(self, sid):
        cursor = self.create_cursor()
        cursor.execute("SAVEPOINT %s" % self.ops.quote_name(sid))

    def _savepoint_commit(self, sid):
        cursor = self.create_cursor()
        cursor.execute("RELEASE SAVEPOINT %s" % self.ops.quote_name(sid))

    def _savepoint_rollback(self, sid):
        cursor = self.create_cursor()
        cursor.execute("ROLLBACK TO SAVEPOINT %s" % self.ops.quote_name(sid))

    def _start_transaction_under_autocommit(self):
        cursor = self.create_cursor()
        cursor.execute("BEGIN")

    def _close(self):
        if self.connection is not None:
            try:
                if not self.connection.closed:
                    self.connection.close()
            except Exception:
                pass

    @contextmanager
    def _nodb_cursor(self):
        conn_params = self.get_connection_params()
        conn_params["dbname"] = ""
        nodb_conn = None
        try:
            nodb_conn = self.get_new_connection(conn_params)
            cursor = CursorWrapper(nodb_conn)
            yield cursor
            cursor.close()
        finally:
            if nodb_conn is not None:
                try:
                    if not nodb_conn.closed:
                        nodb_conn.close()
                except Exception:
                    pass

    @cached_property
    def timezone_name(self):
        if settings.USE_TZ:
            return self.settings_dict.get("TIME_ZONE") or settings.TIME_ZONE
        return None
