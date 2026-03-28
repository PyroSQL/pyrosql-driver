# PyroSQL Driver

Official drivers for [PyroSQL](https://github.com/pyrosql/pyrosql). All drivers use the **PWire** binary protocol — no text parsing, no PG wire overhead.

## Drivers

| Language | Directory | Status | Install |
|----------|-----------|--------|---------|
| Rust | [`rust/`](rust/) | Ready | `cargo add pyrosql` |
| Node.js | [`node/`](node/) | Beta | `npm install pyrosql` |
| PHP | [`php/`](php/) | Beta | `composer require pyrosql/pyrosql` |
| Python | [`python/`](python/) | Alpha | `pip install pyrosql` |
| Go | — | Planned | `go get github.com/pyrosql/pyrosql-driver/go` |
| Java | — | Planned | Maven: `com.pyrosql:pyrosql-driver` |
| .NET | — | Planned | `dotnet add package PyroSQL` |
| Ruby | — | Planned | `gem install pyrosql` |
| Swift | — | Planned | Swift Package Manager |
| Elixir | — | Planned | `{:pyrosql, "~> 0.1"}` |

## Feature matrix

| Feature | Rust | Node.js | PHP | Python |
|---------|------|---------|-----|--------|
| Query / Execute | ✓ | ✓ | ✓ | ✓ |
| Transactions | ✓ | ✓ | ✓ | — |
| Prepared statements | ✓ | ✓ | ✓ | — |
| Connection pool | ✓ | ✓ | ✓ | — |
| Cursors | ✓ | ✓ | ✓ | — |
| Bulk insert | ✓ | ✓ | ✓ | — |
| COPY IN / OUT | ✓ | — | ✓ | — |
| CDC streaming | ✓ | ✓ | ✓ | — |
| LISTEN / NOTIFY | ✓ | — | ✓ | — |
| WATCH (live queries) | ✓ | — | ✓ | — |
| Retry on transient error | ✓ | ✓ | ✓ | — |
| SCRAM auth | ✓ | — | — | — |
| Async / non-blocking | ✓ | — | — | — |

## Architecture

```
┌─────────────┐     PWire (binary, TCP)     ┌──────────────┐
│  Your App   │ ──────────────────────────── │  PyroSQL     │
│  + Driver   │                              │  Server      │
└─────────────┘                              └──────────────┘
```

- **Rust driver** — async (Tokio), speaks PWire natively.
- **Node.js and PHP** — load the `libpyrosql_ffi_pwire` shared library via FFI (`ffi-napi` / `ext-ffi`). Synchronous, full-featured.
- **Python** — loads `libpyrosql_ffi_pwire` via `ctypes`. Synchronous, basic query/execute only.
- **Go, Java (planned)** — will implement PWire natively.

The shared library is provided by the [`ffi-pwire/`](ffi-pwire/) crate. At install time no Rust toolchain is needed — the `.so`/`.dylib`/`.dll` ships with the package.

## Why PWire instead of PG wire?

| | PG wire | PWire |
|---|---------|-------|
| Format | Text (parse ints from ASCII) | Binary (native LE bytes) |
| Overhead | Type OIDs, format codes, row descriptions per query | Schema-aware, no per-value tags |
| Latency | ~150 µs/query | ~70 µs/query |
| Compatibility | Every PG tool works | PyroSQL drivers only |

You can still connect with any PostgreSQL driver over PG wire. PWire is for when you need maximum performance.

## License

MIT — see [LICENSE](LICENSE) for details.
