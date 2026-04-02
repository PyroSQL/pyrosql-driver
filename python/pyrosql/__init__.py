"""PyroSQL Python driver — pure-Python ctypes binding to libpyrosql_ffi_pwire.

Supports two backends:
1. FFI: Uses libpyrosql_ffi_pwire.so via ctypes (default when available).
2. Pure-Python TCP: Direct PWire protocol with LZ4 compression and
   server-side PREPARE/EXECUTE/CLOSE binding.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import platform
import struct
import sys
from pathlib import Path
from typing import Any, List, Optional, Sequence


__version__ = "0.3.0"
__all__ = ["connect", "connect_tcp", "Connection", "TCPConnection", "Result", "PyroSQLError"]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PyroSQLError(Exception):
    """Base exception for all PyroSQL driver errors."""


class ConnectionError(PyroSQLError):  # noqa: A001 — shadows builtin intentionally
    """Raised when a connection cannot be established or has been lost."""


class QueryError(PyroSQLError):
    """Raised when a query or execute call fails."""


# ---------------------------------------------------------------------------
# FFI library loading
# ---------------------------------------------------------------------------

def _lib_name() -> str:
    """Return the platform-specific shared library file name."""
    system = platform.system()
    if system == "Linux":
        return "libpyrosql_ffi_pwire.so"
    elif system == "Darwin":
        return "libpyrosql_ffi_pwire.dylib"
    elif system == "Windows":
        return "pyrosql_ffi_pwire.dll"
    else:
        return "libpyrosql_ffi_pwire.so"


def _find_library() -> str:
    """Locate the FFI shared library.

    Search order:
    1. ``PYROSQL_FFI_LIB`` environment variable (explicit path).
    2. Sibling ``ffi-pwire/target/release/`` directory (development layout).
    3. Same directory as this Python file.
    4. System library search via ctypes.util.find_library.
    """
    # 1. Explicit env var
    env_path = os.environ.get("PYROSQL_FFI_LIB")
    if env_path:
        if os.path.isfile(env_path):
            return env_path
        raise PyroSQLError(
            f"PYROSQL_FFI_LIB points to '{env_path}' which does not exist"
        )

    lib_name = _lib_name()

    # 2. Development layout: python/ is next to ffi-pwire/
    dev_path = (
        Path(__file__).resolve().parent.parent.parent
        / "ffi-pwire"
        / "target"
        / "release"
        / lib_name
    )
    if dev_path.is_file():
        return str(dev_path)

    # Also check debug build
    dev_debug = (
        Path(__file__).resolve().parent.parent.parent
        / "ffi-pwire"
        / "target"
        / "debug"
        / lib_name
    )
    if dev_debug.is_file():
        return str(dev_debug)

    # 3. Same directory as this file (bundled wheel)
    local_path = Path(__file__).resolve().parent / lib_name
    if local_path.is_file():
        return str(local_path)

    # 4. System search
    found = ctypes.util.find_library("pyrosql_ffi_pwire")
    if found:
        return found

    raise PyroSQLError(
        f"Cannot find {lib_name}. Set the PYROSQL_FFI_LIB environment "
        f"variable to the full path of the shared library, or place it "
        f"next to this package."
    )


def _load_lib() -> ctypes.CDLL:
    """Load the shared library and declare C function signatures."""
    path = _find_library()
    lib = ctypes.CDLL(path)

    # void pyro_pwire_init(void);
    lib.pyro_pwire_init.argtypes = []
    lib.pyro_pwire_init.restype = None

    # void* pyro_pwire_connect(const char* host, uint16_t port);
    lib.pyro_pwire_connect.argtypes = [ctypes.c_char_p, ctypes.c_uint16]
    lib.pyro_pwire_connect.restype = ctypes.c_void_p

    # char* pyro_pwire_query(void* conn, const char* sql);
    # NOTE: restype must be c_void_p (not c_char_p) to preserve the
    # original pointer for pyro_pwire_free_string.  ctypes auto-converts
    # c_char_p return values into Python bytes, losing the C pointer.
    lib.pyro_pwire_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.pyro_pwire_query.restype = ctypes.c_void_p

    # int64_t pyro_pwire_execute(void* conn, const char* sql);
    lib.pyro_pwire_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.pyro_pwire_execute.restype = ctypes.c_int64

    # void pyro_pwire_free_string(char* ptr);
    lib.pyro_pwire_free_string.argtypes = [ctypes.c_void_p]
    lib.pyro_pwire_free_string.restype = None

    # void pyro_pwire_close(void* conn);
    lib.pyro_pwire_close.argtypes = [ctypes.c_void_p]
    lib.pyro_pwire_close.restype = None

    # char* pyro_pwire_prepare(void* conn, const char* sql);
    # Returns JSON with handle info, or NULL on failure.
    try:
        lib.pyro_pwire_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        lib.pyro_pwire_prepare.restype = ctypes.c_void_p
    except AttributeError:
        pass  # Symbol not available in this build

    # char* pyro_pwire_execute_prepared(void* conn, const char* prepared_json, const char* params_json);
    try:
        lib.pyro_pwire_execute_prepared.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        lib.pyro_pwire_execute_prepared.restype = ctypes.c_void_p
    except AttributeError:
        pass  # Symbol not available in this build

    lib.pyro_pwire_init()
    return lib


# Module-level singleton — loaded on first use.
_lib: Optional[ctypes.CDLL] = None


def _get_lib() -> ctypes.CDLL:
    global _lib
    if _lib is None:
        _lib = _load_lib()
    return _lib


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

class Result:
    """Query result containing columns, rows, and rows_affected.

    Attributes:
        columns: List of column name strings.
        rows: List of rows, where each row is a list of string values
              (or ``None`` for SQL NULL).
        rows_affected: Number of rows affected (for DML statements).
    """

    __slots__ = ("columns", "rows", "rows_affected")

    def __init__(
        self,
        columns: List[str],
        rows: List[List[Any]],
        rows_affected: int,
    ) -> None:
        self.columns = columns
        self.rows = rows
        self.rows_affected = rows_affected

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self):
        """Iterate over rows as dicts keyed by column name."""
        for row in self.rows:
            yield dict(zip(self.columns, row))

    def __repr__(self) -> str:
        return (
            f"Result(columns={self.columns!r}, "
            f"rows={len(self.rows)}, "
            f"rows_affected={self.rows_affected})"
        )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class Connection:
    """A connection to a PyroSQL server via the PWire protocol.

    Use :func:`connect` to create instances. Supports the context manager
    protocol (``with`` statement) for automatic cleanup.

    Example::

        with pyrosql.connect("127.0.0.1", 12520) as conn:
            result = conn.query("SELECT id, name FROM users")
            for row in result:
                print(row["name"])
    """

    def __init__(self, handle: int, lib: ctypes.CDLL) -> None:
        self._handle = handle
        self._lib = lib
        self._closed = False

    # -- public API --------------------------------------------------------

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> Result:
        """Execute a SQL query and return the result set.

        When *params* is provided, uses server-side prepared statement binding
        (MSG_PREPARE + MSG_EXECUTE) instead of client-side interpolation.

        Args:
            sql: SQL query string (e.g. ``"SELECT * FROM users WHERE id = $1"``).
            params: Optional sequence of parameter values for ``$1, $2, ...``
                    placeholders.

        Returns:
            A :class:`Result` with columns, rows, and rows_affected.

        Raises:
            QueryError: If the server returns an error.
            ConnectionError: If the connection is closed.
        """
        self._check_open()

        if params is not None and len(params) > 0:
            return self._query_prepared(sql, params)

        sql_bytes = sql.encode("utf-8")
        raw_ptr = self._lib.pyro_pwire_query(self._handle, sql_bytes)
        if raw_ptr is None or raw_ptr == 0:
            raise QueryError("pyro_pwire_query returned NULL")
        try:
            json_str = ctypes.string_at(raw_ptr).decode("utf-8")
        except Exception:
            raise QueryError("failed to decode query response as UTF-8")
        finally:
            # Free the C-allocated string using the original pointer.
            self._lib.pyro_pwire_free_string(raw_ptr)

        return self._parse_json_response(json_str)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        """Execute a DML statement (INSERT, UPDATE, DELETE, etc.).

        When *params* is provided, uses server-side prepared statement binding.

        Args:
            sql: SQL statement string.
            params: Optional sequence of parameter values.

        Returns:
            The number of rows affected.

        Raises:
            QueryError: If the server returns an error (rows_affected == -1).
            ConnectionError: If the connection is closed.
        """
        self._check_open()

        if params is not None and len(params) > 0:
            result = self._query_prepared(sql, params)
            return result.rows_affected

        sql_bytes = sql.encode("utf-8")
        result = self._lib.pyro_pwire_execute(self._handle, sql_bytes)
        if result < 0:
            raise QueryError(
                f"execute failed (returned {result}); "
                f"use query() to see the error message"
            )
        return result

    def _query_prepared(self, sql: str, params: Sequence[Any]) -> Result:
        """Execute via FFI prepare + execute_prepared for server-side binding.

        Falls back to client-side interpolation if FFI prepare is not available.
        """
        if not hasattr(self._lib, "pyro_pwire_prepare"):
            # Fall back to client-side interpolation
            final_sql = self._interpolate(sql, params)
            return self.query(final_sql)

        sql_bytes = sql.encode("utf-8")
        prep_ptr = self._lib.pyro_pwire_prepare(self._handle, sql_bytes)
        if prep_ptr is None or prep_ptr == 0:
            raise QueryError("pyro_pwire_prepare returned NULL")
        try:
            prep_json = ctypes.string_at(prep_ptr).decode("utf-8")
        except Exception:
            raise QueryError("failed to decode prepare response")
        finally:
            self._lib.pyro_pwire_free_string(prep_ptr)

        # Check for error in prepare response
        if prep_json.startswith('{"error"'):
            try:
                err = json.loads(prep_json)
                raise QueryError(err.get("error", prep_json))
            except (json.JSONDecodeError, QueryError):
                raise

        params_json = json.dumps(
            [None if v is None else v for v in params]
        ).encode("utf-8")
        prep_bytes = prep_json.encode("utf-8")

        exec_ptr = self._lib.pyro_pwire_execute_prepared(
            self._handle, prep_bytes, params_json
        )
        if exec_ptr is None or exec_ptr == 0:
            raise QueryError("pyro_pwire_execute_prepared returned NULL")
        try:
            exec_json = ctypes.string_at(exec_ptr).decode("utf-8")
        except Exception:
            raise QueryError("failed to decode execute_prepared response")
        finally:
            self._lib.pyro_pwire_free_string(exec_ptr)

        return self._parse_json_response(exec_json)

    @staticmethod
    def _parse_json_response(json_str: str) -> "Result":
        """Parse a JSON response string into a Result."""
        # Check for error responses
        if json_str.startswith('{"error"'):
            try:
                err = json.loads(json_str)
                raise QueryError(err.get("error", json_str))
            except (json.JSONDecodeError, QueryError):
                raise
            except Exception:
                raise QueryError(json_str)

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise QueryError(f"invalid JSON response: {exc}") from exc

        return Result(
            columns=data.get("columns", []),
            rows=data.get("rows", []),
            rows_affected=data.get("rows_affected", 0),
        )

    def close(self) -> None:
        """Close the connection and free resources.

        Safe to call multiple times.
        """
        if not self._closed and self._handle:
            self._lib.pyro_pwire_close(self._handle)
            self._closed = True
            self._handle = None

    @property
    def closed(self) -> bool:
        """Whether this connection has been closed."""
        return self._closed

    # -- context manager ---------------------------------------------------

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -- destructor --------------------------------------------------------

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- internals ---------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise ConnectionError("connection is closed")

    def _interpolate(self, sql: str, params: Sequence[Any]) -> str:
        """Replace $N placeholders with escaped literal values (client-side)."""
        result = sql
        for i in range(len(params) - 1, -1, -1):
            placeholder = f"${i + 1}"
            val = params[i]
            if val is None:
                literal = "NULL"
            elif isinstance(val, bool):
                literal = "TRUE" if val else "FALSE"
            elif isinstance(val, (int, float)):
                literal = str(val)
            elif isinstance(val, str):
                literal = "'" + val.replace("'", "''") + "'"
            else:
                literal = "'" + str(val).replace("'", "''") + "'"
            result = result.replace(placeholder, literal)
        return result

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<pyrosql.Connection [{state}]>"


# ---------------------------------------------------------------------------
# TCPConnection — pure-Python TCP with LZ4 and server-side binding
# ---------------------------------------------------------------------------

class TCPConnection:
    """A pure-Python TCP connection to PyroSQL with LZ4 compression
    and server-side PREPARE/EXECUTE/CLOSE binding.

    Does not require the FFI shared library.

    Example::

        with pyrosql.connect_tcp("127.0.0.1", 12520) as conn:
            result = conn.query("SELECT id, name FROM users WHERE age > $1", [25])
            for row in result:
                print(row["name"])
    """

    def __init__(self, wire: "pyrosql.pwire.PWireConnection") -> None:
        self._wire = wire
        self._closed = False

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> Result:
        """Execute a SQL query and return the result set.

        When *params* is provided, uses server-side prepared statement binding
        (MSG_PREPARE + MSG_EXECUTE + MSG_CLOSE).
        """
        self._check_open()

        if params is not None and len(params) > 0:
            return self._query_prepared(sql, params)

        data = self._wire.query(sql)
        return Result(
            columns=data.get("columns", []),
            rows=data.get("rows", []),
            rows_affected=data.get("rows_affected", 0),
        )

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        """Execute a DML statement. Returns number of rows affected."""
        self._check_open()

        if params is not None and len(params) > 0:
            result = self._query_prepared(sql, params)
            return result.rows_affected

        data = self._wire.execute(sql)
        return data.get("rows_affected", 0)

    def _query_prepared(self, sql: str, params: Sequence[Any]) -> Result:
        """Use MSG_PREPARE + MSG_EXECUTE + MSG_CLOSE for server-side binding."""
        from . import pwire

        wire_params = []
        for v in params:
            if v is None:
                wire_params.append("NULL")
            elif isinstance(v, bool):
                wire_params.append("true" if v else "false")
            elif isinstance(v, (int, float)):
                wire_params.append(str(v))
            else:
                wire_params.append(str(v))

        handle = self._wire.prepare(sql)
        try:
            data = self._wire.execute_prepared(handle, wire_params)
            return Result(
                columns=data.get("columns", []),
                rows=data.get("rows", []),
                rows_affected=data.get("rows_affected", 0),
            )
        finally:
            try:
                self._wire.close_prepared(handle)
            except Exception:
                pass  # best effort

    def close(self) -> None:
        if not self._closed:
            self._wire.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "TCPConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _check_open(self) -> None:
        if self._closed:
            raise ConnectionError("connection is closed")

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<pyrosql.TCPConnection [{state}]>"


# ---------------------------------------------------------------------------
# Module-level connect functions
# ---------------------------------------------------------------------------

def connect(host: str, port: int = 12520) -> Connection:
    """Connect to a PyroSQL server via FFI.

    Args:
        host: Hostname or IP address (e.g. ``"127.0.0.1"``).
        port: TCP port number (default: 12520).

    Returns:
        A :class:`Connection` instance.

    Raises:
        ConnectionError: If the connection cannot be established.
    """
    lib = _get_lib()
    host_bytes = host.encode("utf-8")
    handle = lib.pyro_pwire_connect(host_bytes, port)
    if handle is None or handle == 0:
        raise ConnectionError(f"failed to connect to {host}:{port}")
    return Connection(handle, lib)


def connect_tcp(
    host: str,
    port: int = 12520,
    user: str = "",
    password: str = "",
    timeout: float = 30.0,
) -> TCPConnection:
    """Connect to a PyroSQL server via pure-Python TCP with LZ4 + server-side binding.

    Does not require the FFI shared library.

    Args:
        host: Hostname or IP address.
        port: TCP port number (default: 12520).
        user: Username for authentication.
        password: Password for authentication.
        timeout: Connection timeout in seconds.

    Returns:
        A :class:`TCPConnection` instance.
    """
    from . import pwire

    wire = pwire.PWireConnection(host, port, timeout)
    wire.authenticate(user, password)
    return TCPConnection(wire)
