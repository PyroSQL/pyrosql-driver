# Changelog

## [1.1.0] - 2026-04-02

### Added
- **Connection pooler** (`tools/pyrosql-pooler/`) — transaction-level and session-level pooling with health checks
- **LZ4 auto-compression** — transparent compression for payloads >8KB, negotiated during auth
- **Server-side prepared statements** — PREPARE/EXECUTE/CLOSE binary protocol for 2-5x faster repeated queries
- **Unified PHP extension** (`pdo/`) — single `pyrosql.so` with PDO driver + native functions (listen, notify, copy, watch, CDC)
- **Doctrine DBAL driver** (`php/doctrine-dbal/`) — for Symfony/Doctrine projects
- **Laravel driver** (`php/laravel/`) — Illuminate Database driver with migrations support
- **Django backend** (`python/django/`) — full Django database backend
- **SQLAlchemy dialect** (`python/sqlalchemy/`) — with PEP 249 DBAPI wrapper
- **TypeORM driver** (`node/typeorm/`)
- **Prisma adapter** (`node/prisma/`)
- **Drizzle driver** (`node/drizzle/`)
- **Go driver** (`go/`) — database/sql compatible
- **GORM driver** (`go/gorm/`)
- **JDBC driver** (`jdbc/`) — with SPI auto-registration
- **Hibernate dialect** (`jdbc/hibernate/`)
- **.NET ADO.NET driver** (`dotnet/PyroSQL.Data/`)
- **Entity Framework Core provider** (`dotnet/efcore/`)
- **Ruby driver + ActiveRecord adapter** (`ruby/`)
- **Diesel backend** (`rust/diesel/`)
- **sqlx driver** (`rust/sqlx/`)
- **pyrosql-dump** (`tools/pyrosql-dump/`) — backup/restore tool

### Fixed
- PHP PDO: segfault on prepared statements with bound parameters
- PHP PDO: segfault on ERRMODE_EXCEPTION error handling (fetch_err format)
- Go: auth response handling (accept RESP_READY)
- Java JDBC: auth response + prepared statement protocol
- .NET: auth response + prepared statement protocol
- Ruby: auth response + prepared statement protocol
- Python: munmap crash in FFI string handling
- SQLAlchemy: sqltypes.text() → sqlalchemy.text()

### Changed
- All drivers now prefer server-side binding over client-side interpolation
- LZ4 compression is automatic and transparent (no app code changes)
- PHP extension renamed from `pdo_pyrosql` to `pyrosql` (unified)

## [1.0.0] - 2026-03-29

### Added
- Initial release with PHP, Python, Node.js, and Rust drivers
- PWire binary protocol over TCP
- FFI shared library (`libpyrosql_ffi_pwire.so`)
