"""PyroSQL Python driver — pure-Python ctypes binding to libpyrosql_ffi_pwire."""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import platform
import sys
from pathlib import Path
from typing import Any, List, Optional, Sequence


__version__ = "0.2.0"
__all__ = ["connect", "Connection", "Result", "PyroSQLError"]


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
    lib.pyro_pwire_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.pyro_pwire_query.restype = ctypes.c_char_p

    # int64_t pyro_pwire_execute(void* conn, const char* sql);
    lib.pyro_pwire_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.pyro_pwire_execute.restype = ctypes.c_int64

    # void pyro_pwire_free_string(char* ptr);
    lib.pyro_pwire_free_string.argtypes = [ctypes.c_char_p]
    lib.pyro_pwire_free_string.restype = None

    # void pyro_pwire_close(void* conn);
    lib.pyro_pwire_close.argtypes = [ctypes.c_void_p]
    lib.pyro_pwire_close.restype = None

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

    def query(self, sql: str) -> Result:
        """Execute a SQL query and return the result set.

        Args:
            sql: SQL query string (e.g. ``"SELECT * FROM users"``).

        Returns:
            A :class:`Result` with columns, rows, and rows_affected.

        Raises:
            QueryError: If the server returns an error.
            ConnectionError: If the connection is closed.
        """
        self._check_open()
        sql_bytes = sql.encode("utf-8")
        raw = self._lib.pyro_pwire_query(self._handle, sql_bytes)
        if raw is None:
            raise QueryError("pyro_pwire_query returned NULL")
        try:
            json_str = raw.decode("utf-8")
        except Exception:
            raise QueryError("failed to decode query response as UTF-8")

        # Check for error responses (the FFI layer may return JSON with an
        # "error" key, or a bare error string).
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
        finally:
            # Free the C-allocated string.
            self._lib.pyro_pwire_free_string(raw)

        return Result(
            columns=data.get("columns", []),
            rows=data.get("rows", []),
            rows_affected=data.get("rows_affected", 0),
        )

    def execute(self, sql: str) -> int:
        """Execute a DML statement (INSERT, UPDATE, DELETE, etc.).

        Args:
            sql: SQL statement string.

        Returns:
            The number of rows affected.

        Raises:
            QueryError: If the server returns an error (rows_affected == -1).
            ConnectionError: If the connection is closed.
        """
        self._check_open()
        sql_bytes = sql.encode("utf-8")
        result = self._lib.pyro_pwire_execute(self._handle, sql_bytes)
        if result < 0:
            raise QueryError(
                f"execute failed (returned {result}); "
                f"use query() to see the error message"
            )
        return result

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

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<pyrosql.Connection [{state}]>"


# ---------------------------------------------------------------------------
# Module-level connect function
# ---------------------------------------------------------------------------

def connect(host: str, port: int = 12520) -> Connection:
    """Connect to a PyroSQL server.

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
