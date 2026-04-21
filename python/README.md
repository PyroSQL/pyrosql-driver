# pyrosql

Pure-Python driver for [PyroSQL](https://github.com/PyroSQL/pyrosql) using ctypes FFI.

Loads the prebuilt `libpyrosql_ffi_pwire` shared library (built by `ffi-pwire/`) and communicates with the server over the PWire binary protocol. No Rust toolchain or compilation required at install time.

## Installation

```bash
pip install pyrosql
```

## Prerequisites

The shared library `libpyrosql_ffi_pwire.so` (Linux), `.dylib` (macOS), or `.dll` (Windows) must be available. The driver searches for it in this order:

1. Path specified in the `PYROSQL_FFI_LIB` environment variable.
2. `../ffi-pwire/target/release/` relative to the package (development layout).
3. Same directory as the installed `pyrosql` package.
4. System library path (`ldconfig`, `DYLD_LIBRARY_PATH`, etc.).

## Usage

### Basic query

```python
import pyrosql

conn = pyrosql.connect("127.0.0.1", 12520)

result = conn.query("SELECT id, name FROM users WHERE active = true")
print(result.columns)  # ['id', 'name']

for row in result:
    print(row)  # {'id': '1', 'name': 'alice'}

conn.close()
```

### Context manager

```python
import pyrosql

with pyrosql.connect("127.0.0.1", 12520) as conn:
    conn.execute("INSERT INTO users (name) VALUES ('bob')")
    result = conn.query("SELECT count(*) AS cnt FROM users")
    print(result.rows[0][0])  # '2'
```

### Execute (DML)

```python
with pyrosql.connect("127.0.0.1", 12520) as conn:
    rows_affected = conn.execute("DELETE FROM users WHERE active = false")
    print(f"Deleted {rows_affected} rows")
```

### Error handling

```python
import pyrosql

try:
    conn = pyrosql.connect("127.0.0.1", 12520)
    result = conn.query("SELECT * FROM nonexistent_table")
except pyrosql.ConnectionError as e:
    print(f"Connection failed: {e}")
except pyrosql.QueryError as e:
    print(f"Query failed: {e}")
except pyrosql.PyroSQLError as e:
    print(f"Driver error: {e}")
```

## API Reference

### `pyrosql.connect(host, port=12520) -> Connection`

Open a connection to a PyroSQL server.

### `Connection.query(sql) -> Result`

Execute a SQL query and return the result set.

### `Connection.execute(sql) -> int`

Execute a DML statement and return the number of rows affected.

### `Connection.close()`

Close the connection. Safe to call multiple times.

### `Result`

- `.columns` -- list of column name strings
- `.rows` -- list of rows (each row is a list of values)
- `.rows_affected` -- number of rows affected
- Iterable: yields dicts keyed by column name
- `len(result)` -- number of rows

## License

MIT — see [LICENSE](../LICENSE) for details.
