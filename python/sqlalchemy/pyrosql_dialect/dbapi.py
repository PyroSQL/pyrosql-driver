"""PEP 249 (DB-API 2.0) wrapper around the PyroSQL Python driver.

This module provides the thin DB-API compatibility layer that SQLAlchemy
expects from ``dialect.dbapi()``.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import UUID

import pyrosql as _pyrosql_driver


# ---------------------------------------------------------------------------
# PEP 249 module-level attributes
# ---------------------------------------------------------------------------

apilevel = "2.0"
threadsafety = 1  # Connections may not be shared between threads.
paramstyle = "qmark"  # Use ``?`` as the parameter placeholder.


# ---------------------------------------------------------------------------
# Exceptions  (PEP 249 hierarchy)
# ---------------------------------------------------------------------------

class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class InterfaceError(Error):
    pass


class DataError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class Warning(Exception):  # noqa: A001
    pass


# ---------------------------------------------------------------------------
# Type objects (PEP 249)
# ---------------------------------------------------------------------------

STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime
ROWID = int


# ---------------------------------------------------------------------------
# Helper — parameter binding
# ---------------------------------------------------------------------------

def _quote_value(value: Any) -> str:
    """Serialise a Python value into a SQL literal for embedding in a query."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bytes):
        hex_str = value.hex()
        return f"'\\x{hex_str}'"
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, time):
        return f"'{value.isoformat()}'"
    if isinstance(value, UUID):
        return f"'{value}'"
    if isinstance(value, (list, dict)):
        import json
        escaped = json.dumps(value).replace("'", "''")
        return f"'{escaped}'"
    # Fallback — str representation
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _bind_parameters(sql: str, parameters: Optional[Sequence] = None) -> str:
    """Replace ``?`` placeholders with literal values from *parameters*.

    Also handles ``:name`` style named parameters when *parameters* is a dict.
    """
    if parameters is None:
        return sql

    if isinstance(parameters, dict):
        # Named parameters — :name style
        def _replace_named(match):
            key = match.group(1)
            if key not in parameters:
                raise ProgrammingError(f"missing named parameter: {key}")
            return _quote_value(parameters[key])

        return re.sub(r":([A-Za-z_]\w*)", _replace_named, sql)

    # Positional parameters — ``?`` style
    parts: list[str] = []
    param_iter = iter(parameters)
    for segment in sql.split("?"):
        parts.append(segment)
        try:
            parts.append(_quote_value(next(param_iter)))
        except StopIteration:
            break
    return "".join(parts)


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------

class Cursor:
    """PEP 249 Cursor implementation backed by a PyroSQL :class:`Connection`."""

    arraysize: int = 1

    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._raw_conn = connection._raw_conn
        self.description: Optional[List[Tuple]] = None
        self.rowcount: int = -1
        self._rows: List[Tuple] = []
        self._pos: int = 0
        self._closed: bool = False
        self.lastrowid: Optional[int] = None

    # -- Core PEP 249 methods -----------------------------------------------

    def execute(self, operation: str, parameters: Optional[Sequence] = None) -> None:
        self._check_open()
        sql = _bind_parameters(operation, parameters)

        # Decide whether this is a query (returns rows) or a DML statement.
        stripped = sql.lstrip().upper()
        is_query = stripped.startswith(("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"))
        # RETURNING clauses also produce result rows.
        has_returning = "RETURNING" in stripped

        if is_query or has_returning:
            try:
                result = self._raw_conn.query(sql)
            except _pyrosql_driver.QueryError as exc:
                raise DatabaseError(str(exc)) from exc
            except _pyrosql_driver.ConnectionError as exc:
                raise OperationalError(str(exc)) from exc

            self.description = [
                (col, None, None, None, None, None, None)
                for col in result.columns
            ]
            self._rows = [tuple(row) for row in result.rows]
            self.rowcount = result.rows_affected if result.rows_affected else len(self._rows)
            self._pos = 0

            # Try to extract lastrowid from RETURNING results.
            if has_returning and self._rows:
                first_val = self._rows[0][0]
                if first_val is not None:
                    try:
                        self.lastrowid = int(first_val)
                    except (ValueError, TypeError):
                        self.lastrowid = None
        else:
            try:
                affected = self._raw_conn.execute(sql)
            except _pyrosql_driver.QueryError as exc:
                raise DatabaseError(str(exc)) from exc
            except _pyrosql_driver.ConnectionError as exc:
                raise OperationalError(str(exc)) from exc

            self.description = None
            self._rows = []
            self.rowcount = affected
            self._pos = 0
            self.lastrowid = None

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence]) -> None:
        self._check_open()
        total_affected = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self.rowcount > 0:
                total_affected += self.rowcount
        self.rowcount = total_affected

    def fetchone(self) -> Optional[Tuple]:
        self._check_open()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        self._check_open()
        if size is None:
            size = self.arraysize
        end = min(self._pos + size, len(self._rows))
        rows = self._rows[self._pos:end]
        self._pos = end
        return rows

    def fetchall(self) -> List[Tuple]:
        self._check_open()
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    def close(self) -> None:
        self._closed = True
        self._rows = []
        self.description = None

    def setinputsizes(self, sizes) -> None:
        pass  # Not applicable.

    def setoutputsize(self, size, column=None) -> None:
        pass  # Not applicable.

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class Connection:
    """PEP 249 Connection backed by a PyroSQL low-level connection."""

    def __init__(self, host: str, port: int = 12520, database: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None) -> None:
        try:
            self._raw_conn = _pyrosql_driver.connect(host, port)
        except _pyrosql_driver.ConnectionError as exc:
            raise OperationalError(str(exc)) from exc

        self._closed = False
        self._database = database
        self._user = user

        # If a database is specified, switch to it.
        if database:
            try:
                self._raw_conn.execute(f"USE {database}")
            except _pyrosql_driver.QueryError:
                # Server might not support USE — ignore gracefully.
                pass

        # If user/password provided, attempt auth.
        if user:
            auth_sql = f"SET SESSION AUTHORIZATION '{user}'"
            try:
                self._raw_conn.execute(auth_sql)
            except _pyrosql_driver.QueryError:
                pass

    def cursor(self) -> Cursor:
        self._check_open()
        return Cursor(self)

    def commit(self) -> None:
        self._check_open()
        try:
            self._raw_conn.execute("COMMIT")
        except _pyrosql_driver.QueryError:
            pass

    def rollback(self) -> None:
        self._check_open()
        try:
            self._raw_conn.execute("ROLLBACK")
        except _pyrosql_driver.QueryError:
            pass

    def close(self) -> None:
        if not self._closed:
            self._raw_conn.close()
            self._closed = True

    def begin(self) -> None:
        self._check_open()
        try:
            self._raw_conn.execute("BEGIN")
        except _pyrosql_driver.QueryError:
            pass

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("connection is closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


# ---------------------------------------------------------------------------
# Module-level connect()
# ---------------------------------------------------------------------------

def connect(host: str = "127.0.0.1", port: int = 12520,
            database: Optional[str] = None, user: Optional[str] = None,
            password: Optional[str] = None, **kwargs) -> Connection:
    """PEP 249 ``connect()`` entry point."""
    return Connection(host=host, port=port, database=database,
                      user=user, password=password)
