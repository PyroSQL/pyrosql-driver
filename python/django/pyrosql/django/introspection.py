"""DatabaseIntrospection for PyroSQL.

Queries information_schema and pg_catalog to discover database objects.
"""

from collections import namedtuple

from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.introspection import FieldInfo as BaseFieldInfo
from django.db.backends.base.introspection import TableInfo
from django.db.models import Index


FieldInfo = namedtuple(
    "FieldInfo",
    BaseFieldInfo._fields + ("is_autofield", "comment"),
)


class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = {
        "boolean": "BooleanField",
        "bool": "BooleanField",
        "smallint": "SmallIntegerField",
        "int2": "SmallIntegerField",
        "integer": "IntegerField",
        "int": "IntegerField",
        "int4": "IntegerField",
        "bigint": "BigIntegerField",
        "int8": "BigIntegerField",
        "serial": "AutoField",
        "bigserial": "BigAutoField",
        "smallserial": "SmallAutoField",
        "real": "FloatField",
        "float4": "FloatField",
        "double precision": "FloatField",
        "float8": "FloatField",
        "numeric": "DecimalField",
        "decimal": "DecimalField",
        "character varying": "CharField",
        "varchar": "CharField",
        "character": "CharField",
        "char": "CharField",
        "text": "TextField",
        "date": "DateField",
        "time": "TimeField",
        "time without time zone": "TimeField",
        "time with time zone": "TimeField",
        "timestamp": "DateTimeField",
        "timestamp without time zone": "DateTimeField",
        "timestamp with time zone": "DateTimeField",
        "timestamptz": "DateTimeField",
        "uuid": "UUIDField",
        "json": "JSONField",
        "jsonb": "JSONField",
        "bytea": "BinaryField",
        "inet": "GenericIPAddressField",
        "cidr": "GenericIPAddressField",
    }

    ignored_tables = []

    def get_field_type(self, data_type, description):
        field_type = super().get_field_type(data_type, description)
        if description.is_autofield:
            if field_type == "IntegerField":
                return "AutoField"
            elif field_type == "BigIntegerField":
                return "BigAutoField"
            elif field_type == "SmallIntegerField":
                return "SmallAutoField"
        return field_type

    def get_table_list(self, cursor):
        cursor.execute(
            "SELECT table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_schema = 'public' "
            "ORDER BY table_name"
        )
        return [
            TableInfo(row[0], {"BASE TABLE": "t", "VIEW": "v"}.get(row[1], "t"))
            for row in cursor.fetchall()
        ]

    def get_table_description(self, cursor, table_name):
        cursor.execute(
            """
            SELECT
                c.column_name,
                c.data_type,
                NULL AS display_size,
                CASE
                    WHEN c.character_maximum_length IS NOT NULL
                        THEN c.character_maximum_length
                    WHEN c.numeric_precision IS NOT NULL
                        THEN c.numeric_precision
                    ELSE NULL
                END AS internal_size,
                c.numeric_precision AS precision,
                c.numeric_scale AS scale,
                CASE WHEN c.is_nullable = 'YES' THEN 1 ELSE 0 END AS nullable,
                c.column_default,
                CASE
                    WHEN c.column_default LIKE 'nextval%%' THEN 1
                    ELSE 0
                END AS is_autofield,
                NULL AS comment
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = %s
            ORDER BY c.ordinal_position
            """,
            [table_name],
        )

        results = []
        for row in cursor.fetchall():
            col_name = row[0]
            data_type = row[1]
            display_size = row[2]
            internal_size = row[3]
            precision = row[4]
            scale = row[5]
            null_ok = bool(row[6])
            default = row[7]
            is_autofield = bool(row[8])
            comment = row[9]

            results.append(
                FieldInfo(
                    name=col_name,
                    type_code=data_type,
                    display_size=display_size,
                    internal_size=internal_size if internal_size else None,
                    precision=precision,
                    scale=scale,
                    null_ok=null_ok,
                    default=default,
                    collation=None,
                    is_autofield=is_autofield,
                    comment=comment,
                )
            )
        return results

    def get_sequences(self, cursor, table_name, table_fields=()):
        cursor.execute(
            """
            SELECT c.column_name
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
              AND c.table_name = %s
              AND c.column_default LIKE 'nextval%%'
            """,
            [table_name],
        )
        return [
            {"table": table_name, "column": row[0]} for row in cursor.fetchall()
        ]

    def get_relations(self, cursor, table_name):
        cursor.execute(
            """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s
            """,
            [table_name],
        )
        return {
            row[0]: (row[2], row[1]) for row in cursor.fetchall()
        }

    def get_primary_key_column(self, cursor, table_name):
        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            LIMIT 1
            """,
            [table_name],
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def get_key_columns(self, cursor, table_name):
        cursor.execute(
            """
            SELECT
                kcu.column_name,
                ccu.table_name,
                ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s
            """,
            [table_name],
        )
        return [row for row in cursor.fetchall()]

    def get_constraints(self, cursor, table_name):
        constraints = {}

        cursor.execute(
            """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = %s
              AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK')
            ORDER BY tc.constraint_name, kcu.ordinal_position
            """,
            [table_name],
        )
        for row in cursor.fetchall():
            constraint_name = row[0]
            constraint_type = row[1]
            column_name = row[2]

            if constraint_name not in constraints:
                constraints[constraint_name] = {
                    "columns": [],
                    "primary_key": constraint_type == "PRIMARY KEY",
                    "unique": constraint_type in ("PRIMARY KEY", "UNIQUE"),
                    "foreign_key": None,
                    "check": constraint_type == "CHECK",
                    "index": False,
                    "definition": None,
                    "options": None,
                    "orders": [],
                    "type": "",
                }
            if column_name:
                constraints[constraint_name]["columns"].append(column_name)

        cursor.execute(
            """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name AS ref_table,
                ccu.column_name AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s
            ORDER BY tc.constraint_name, kcu.ordinal_position
            """,
            [table_name],
        )
        for row in cursor.fetchall():
            constraint_name = row[0]
            column_name = row[1]
            ref_table = row[2]
            ref_column = row[3]

            if constraint_name not in constraints:
                constraints[constraint_name] = {
                    "columns": [],
                    "primary_key": False,
                    "unique": False,
                    "foreign_key": (ref_table, ref_column),
                    "check": False,
                    "index": False,
                    "definition": None,
                    "options": None,
                    "orders": [],
                    "type": "",
                }
            else:
                constraints[constraint_name]["foreign_key"] = (ref_table, ref_column)
            if column_name:
                constraints[constraint_name]["columns"].append(column_name)

        cursor.execute(
            """
            SELECT
                ic.relname AS index_name,
                a.attname AS column_name,
                ix.indisunique,
                ix.indisprimary
            FROM pg_catalog.pg_class t
            JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_class ic ON ic.oid = ix.indexrelid
            JOIN pg_catalog.pg_attribute a
                ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = %s
              AND n.nspname = 'public'
            ORDER BY ic.relname, a.attnum
            """,
            [table_name],
        )
        for row in cursor.fetchall():
            index_name = row[0]
            column_name = row[1]
            is_unique = row[2]
            is_primary = row[3]

            if index_name not in constraints:
                constraints[index_name] = {
                    "columns": [],
                    "primary_key": is_primary,
                    "unique": is_unique,
                    "foreign_key": None,
                    "check": False,
                    "index": True,
                    "definition": None,
                    "options": None,
                    "orders": [],
                    "type": Index.suffix,
                }
            if column_name and column_name not in constraints[index_name]["columns"]:
                constraints[index_name]["columns"].append(column_name)

        return constraints
