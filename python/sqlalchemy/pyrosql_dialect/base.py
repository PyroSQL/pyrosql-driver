"""PyroSQL SQLAlchemy dialect — main dialect class."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import pool, text as sa_text, types as sqltypes, util
from sqlalchemy.engine import default, interfaces
from sqlalchemy.engine.reflection import Inspector

from . import dbapi as pyrosql_dbapi
from .compiler import PyroSQLCompiler, PyroSQLDDLCompiler, PyroSQLTypeCompiler
from .types import lookup_type


class PyroSQLDialect(default.DefaultDialect):
    """SQLAlchemy dialect for PyroSQL."""

    name = "pyrosql"
    driver = "pyrosql"

    # -- Feature flags -------------------------------------------------------
    supports_alter = True
    supports_native_boolean = True
    supports_native_enum = True
    supports_statement_cache = True
    supports_sequences = True
    supports_default_values = True
    supports_default_metavalue = True
    postfetch_lastrowid = False
    implicit_returning = True
    preexecute_autoincrement_sequences = False
    supports_empty_insert = False
    supports_multivalues_insert = True

    # Isolation level support.
    _isolation_lookup = {
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
    }

    default_paramstyle = "qmark"
    default_schema_name = "public"

    # -- Compiler classes ----------------------------------------------------
    statement_compiler = PyroSQLCompiler
    ddl_compiler = PyroSQLDDLCompiler
    type_compiler_cls = PyroSQLTypeCompiler

    # -- Column type mapping -------------------------------------------------
    colspecs: Dict = {}

    # -- DBAPI ---------------------------------------------------------------

    @classmethod
    def dbapi(cls):
        """Return the DB-API 2.0 module."""
        return pyrosql_dbapi

    @classmethod
    def import_dbapi(cls):
        return pyrosql_dbapi

    # -- Connection creation -------------------------------------------------

    def create_connect_args(self, url):
        """Translate a SQLAlchemy URL into ``(args, kwargs)`` for the DBAPI
        ``connect()`` call.

        URL format: ``pyrosql://user:pass@host:port/dbname``
        """
        opts: Dict[str, Any] = {}
        opts["host"] = url.host or "127.0.0.1"
        opts["port"] = url.port or 12520
        if url.database:
            opts["database"] = url.database
        if url.username:
            opts["user"] = url.username
        if url.password:
            opts["password"] = url.password
        return ([], opts)

    # -- Connection initialisation -------------------------------------------

    def on_connect(self):
        """Return a callable invoked on every new raw DBAPI connection."""
        def _on_connect(dbapi_conn):
            pass
        return _on_connect

    # -- Transaction / isolation level ---------------------------------------

    def do_begin(self, dbapi_connection):
        dbapi_connection.begin()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def get_isolation_level(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SHOW transaction_isolation")
            row = cursor.fetchone()
            if row:
                return row[0].upper()
        except Exception:
            pass
        finally:
            cursor.close()
        return "READ COMMITTED"

    def set_isolation_level(self, dbapi_connection, level):
        dbapi_connection.cursor().execute(
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"
        )

    def get_default_isolation_level(self, dbapi_connection):
        return "READ COMMITTED"

    # -- Identifier handling -------------------------------------------------

    def _get_server_version_info(self, connection):
        return (1, 0, 0)

    def _get_default_schema_name(self, connection):
        return self.default_schema_name

    # -- Reflection / introspection ------------------------------------------

    def has_table(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = :table"
            ),
            {"schema": schema, "table": table_name},
        )
        return result.scalar() is not None

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT 1 FROM information_schema.sequences "
                "WHERE sequence_schema = :schema AND sequence_name = :seq"
            ),
            {"schema": schema, "seq": sequence_name},
        )
        return result.scalar() is not None

    def get_schema_names(self, connection, **kw):
        result = connection.execute(
            sa_text(
                "SELECT schema_name FROM information_schema.schemata "
                "ORDER BY schema_name"
            )
        )
        return [row[0] for row in result]

    def get_table_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
                "ORDER BY table_name"
            ),
            {"schema": schema},
        )
        return [row[0] for row in result]

    def get_view_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_type = 'VIEW' "
                "ORDER BY table_name"
            ),
            {"schema": schema},
        )
        return [row[0] for row in result]

    def get_columns(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT column_name, data_type, is_nullable, column_default, "
                "character_maximum_length, numeric_precision, numeric_scale "
                "FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table "
                "ORDER BY ordinal_position"
            ),
            {"schema": schema, "table": table_name},
        )

        columns = []
        for row in result:
            col_name = row[0]
            raw_type = row[1]
            nullable = row[2].upper() == "YES" if row[2] else True
            default_val = row[3]
            char_len = row[4]
            num_prec = row[5]
            num_scale = row[6]

            col_type = lookup_type(raw_type)

            # Refine the type with length/precision info from the catalog.
            if char_len is not None and isinstance(col_type, sqltypes.String):
                col_type = sqltypes.String(length=int(char_len))
            if num_prec is not None and isinstance(col_type, sqltypes.Numeric):
                col_type = sqltypes.Numeric(
                    precision=int(num_prec),
                    scale=int(num_scale) if num_scale is not None else 0,
                )

            # Detect autoincrement from default containing nextval().
            autoincrement = False
            if default_val and "nextval" in str(default_val).lower():
                autoincrement = True

            columns.append({
                "name": col_name,
                "type": col_type,
                "nullable": nullable,
                "default": default_val,
                "autoincrement": autoincrement,
            })

        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT kcu.column_name, tc.constraint_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "WHERE tc.table_schema = :schema "
                "  AND tc.table_name = :table "
                "  AND tc.constraint_type = 'PRIMARY KEY' "
                "ORDER BY kcu.ordinal_position"
            ),
            {"schema": schema, "table": table_name},
        )
        cols = []
        pk_name = None
        for row in result:
            cols.append(row[0])
            pk_name = row[1]

        return {"constrained_columns": cols, "name": pk_name}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT tc.constraint_name, kcu.column_name, "
                "  ccu.table_schema AS foreign_table_schema, "
                "  ccu.table_name AS foreign_table_name, "
                "  ccu.column_name AS foreign_column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "JOIN information_schema.constraint_column_usage ccu "
                "  ON ccu.constraint_name = tc.constraint_name "
                "  AND ccu.table_schema = tc.table_schema "
                "WHERE tc.table_schema = :schema "
                "  AND tc.table_name = :table "
                "  AND tc.constraint_type = 'FOREIGN KEY'"
            ),
            {"schema": schema, "table": table_name},
        )

        fkeys: Dict[str, dict] = {}
        for row in result:
            fk_name = row[0]
            if fk_name not in fkeys:
                fkeys[fk_name] = {
                    "name": fk_name,
                    "constrained_columns": [],
                    "referred_schema": row[2] if row[2] != self.default_schema_name else None,
                    "referred_table": row[3],
                    "referred_columns": [],
                }
            fkeys[fk_name]["constrained_columns"].append(row[1])
            fkeys[fk_name]["referred_columns"].append(row[4])

        return list(fkeys.values())

    def get_indexes(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT indexname, indexdef "
                "FROM pg_indexes "
                "WHERE schemaname = :schema AND tablename = :table"
            ),
            {"schema": schema, "table": table_name},
        )

        indexes = []
        for row in result:
            idx_name = row[0]
            idx_def = row[1] if row[1] else ""
            unique = "UNIQUE" in idx_def.upper()

            # Extract column names from CREATE INDEX ... ON table (col1, col2).
            import re
            col_match = re.search(r"\((.+?)\)", idx_def)
            cols = []
            if col_match:
                cols = [c.strip().strip('"') for c in col_match.group(1).split(",")]

            indexes.append({
                "name": idx_name,
                "column_names": cols,
                "unique": unique,
            })

        return indexes

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT tc.constraint_name, kcu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "WHERE tc.table_schema = :schema "
                "  AND tc.table_name = :table "
                "  AND tc.constraint_type = 'UNIQUE' "
                "ORDER BY tc.constraint_name, kcu.ordinal_position"
            ),
            {"schema": schema, "table": table_name},
        )

        constraints: Dict[str, dict] = {}
        for row in result:
            name = row[0]
            if name not in constraints:
                constraints[name] = {"name": name, "column_names": []}
            constraints[name]["column_names"].append(row[1])

        return list(constraints.values())

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT tc.constraint_name, cc.check_clause "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.check_constraints cc "
                "  ON tc.constraint_name = cc.constraint_name "
                "  AND tc.constraint_schema = cc.constraint_schema "
                "WHERE tc.table_schema = :schema "
                "  AND tc.table_name = :table "
                "  AND tc.constraint_type = 'CHECK'"
            ),
            {"schema": schema, "table": table_name},
        )

        checks = []
        for row in result:
            checks.append({
                "name": row[0],
                "sqltext": row[1],
            })
        return checks

    def get_sequence_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        result = connection.execute(
            sa_text(
                "SELECT sequence_name FROM information_schema.sequences "
                "WHERE sequence_schema = :schema ORDER BY sequence_name"
            ),
            {"schema": schema},
        )
        return [row[0] for row in result]

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return {"text": None}

    def get_multi_columns(self, connection, schema=None, filter_names=None, **kw):
        """Multi-table column reflection (SQLAlchemy 2.0+)."""
        result = {}
        tables = filter_names or self.get_table_names(connection, schema=schema)
        for table_name in tables:
            cols = self.get_columns(connection, table_name, schema=schema, **kw)
            key = (schema or self.default_schema_name, table_name)
            result[key] = cols
        return result


# Alias for entry point registration.
dialect = PyroSQLDialect
