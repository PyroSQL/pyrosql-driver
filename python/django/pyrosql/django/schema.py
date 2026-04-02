"""DatabaseSchemaEditor for PyroSQL.

Generates DDL statements for creating, altering, and dropping database objects.
Uses PostgreSQL-compatible syntax.
"""

from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_sequence = 'CREATE SEQUENCE %(sequence)s'
    sql_delete_sequence = 'DROP SEQUENCE IF EXISTS %(sequence)s CASCADE'
    sql_set_sequence_max = (
        "SELECT setval(%(sequence)s, MAX(%(column)s)) FROM %(table)s"
    )
    sql_set_sequence_owner = (
        "ALTER SEQUENCE %(sequence)s OWNED BY %(table)s.%(column)s"
    )

    sql_create_index = (
        "CREATE INDEX %(name)s ON %(table)s%(using)s "
        "(%(columns)s)%(include)s%(condition)s"
    )
    sql_create_index_concurrently = (
        "CREATE INDEX CONCURRENTLY %(name)s ON %(table)s%(using)s "
        "(%(columns)s)%(include)s%(condition)s"
    )
    sql_delete_index = "DROP INDEX IF EXISTS %(name)s"
    sql_delete_index_concurrently = "DROP INDEX CONCURRENTLY IF EXISTS %(name)s"

    sql_alter_column_type = "ALTER COLUMN %(column)s TYPE %(type)s USING %(column)s::%(type)s"
    sql_alter_column_collate = "ALTER COLUMN %(column)s TYPE %(type)s%(collation)s USING %(column)s::%(type)s"

    sql_create_column_inline_fk = None

    sql_delete_fk = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"
    sql_delete_procedure = "DROP FUNCTION IF EXISTS %(procedure)s(%(param_types)s)"

    sql_alter_sequence_type = "ALTER SEQUENCE IF EXISTS %(sequence)s AS %(type)s"

    data_types = {
        "AutoField": "SERIAL",
        "BigAutoField": "BIGSERIAL",
        "SmallAutoField": "SMALLSERIAL",
        "BinaryField": "BYTEA",
        "BooleanField": "BOOLEAN",
        "CharField": "VARCHAR(%(max_length)s)",
        "DateField": "DATE",
        "DateTimeField": "TIMESTAMP WITH TIME ZONE",
        "DecimalField": "NUMERIC(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "INTERVAL",
        "FilePathField": "VARCHAR(%(max_length)s)",
        "FloatField": "DOUBLE PRECISION",
        "IntegerField": "INTEGER",
        "BigIntegerField": "BIGINT",
        "IPAddressField": "INET",
        "GenericIPAddressField": "INET",
        "JSONField": "JSONB",
        "OneToOneField": "INTEGER",
        "PositiveBigIntegerField": "BIGINT",
        "PositiveIntegerField": "INTEGER",
        "PositiveSmallIntegerField": "SMALLINT",
        "SlugField": "VARCHAR(%(max_length)s)",
        "SmallIntegerField": "SMALLINT",
        "TextField": "TEXT",
        "TimeField": "TIME",
        "UUIDField": "UUID",
    }

    data_type_check_constraints = {
        "PositiveBigIntegerField": '"%(column)s" >= 0',
        "PositiveIntegerField": '"%(column)s" >= 0',
        "PositiveSmallIntegerField": '"%(column)s" >= 0',
    }

    def quote_value(self, value):
        if isinstance(value, str):
            return "'%s'" % value.replace("\\", "\\\\").replace("'", "''")
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, bytes):
            hex_str = value.hex()
            return "'\\x%s'" % hex_str
        if value is None:
            return "NULL"
        return str(value)

    def _field_data_type(self, field):
        if field.is_relation:
            return field.rel_db_type(self.connection)
        return self.data_types.get(field.get_internal_type(), "TEXT")

    def _alter_column_type_sql(self, model, old_field, new_field, new_type):
        self.sql_alter_column_type = (
            "ALTER COLUMN %(column)s TYPE %(type)s USING %(column)s::%(type)s"
        )
        return super()._alter_column_type_sql(model, old_field, new_field, new_type)

    def _create_index_sql(
        self,
        model,
        *,
        fields=None,
        name=None,
        suffix="",
        using="",
        db_tablespace=None,
        col_suffixes=(),
        sql=None,
        opclasses=(),
        condition=None,
        include=None,
        expressions=None,
        **kwargs,
    ):
        return super()._create_index_sql(
            model,
            fields=fields,
            name=name,
            suffix=suffix,
            using=using,
            db_tablespace=db_tablespace,
            col_suffixes=col_suffixes,
            sql=sql or self.sql_create_index,
            opclasses=opclasses,
            condition=condition,
            include=include,
            expressions=expressions,
            **kwargs,
        )

    def _model_indexes_sql(self, model):
        return super()._model_indexes_sql(model)

    def _alter_field(
        self,
        model,
        old_field,
        new_field,
        old_type,
        new_type,
        old_db_params,
        new_db_params,
        strict=False,
    ):
        super()._alter_field(
            model,
            old_field,
            new_field,
            old_type,
            new_type,
            old_db_params,
            new_db_params,
            strict,
        )

        old_is_auto = old_field.get_internal_type() in (
            "AutoField",
            "BigAutoField",
            "SmallAutoField",
        )
        new_is_auto = new_field.get_internal_type() in (
            "AutoField",
            "BigAutoField",
            "SmallAutoField",
        )
        if new_is_auto and not old_is_auto:
            seq_name = "%s_%s_seq" % (model._meta.db_table, new_field.column)
            self.execute(
                self.sql_create_sequence % {"sequence": self.quote_name(seq_name)}
            )
            self.execute(
                "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT nextval('%s')"
                % (
                    self.quote_name(model._meta.db_table),
                    self.quote_name(new_field.column),
                    seq_name,
                )
            )
            self.execute(
                self.sql_set_sequence_max
                % {
                    "sequence": "'%s'" % seq_name,
                    "column": self.quote_name(new_field.column),
                    "table": self.quote_name(model._meta.db_table),
                }
            )
            self.execute(
                self.sql_set_sequence_owner
                % {
                    "sequence": self.quote_name(seq_name),
                    "table": self.quote_name(model._meta.db_table),
                    "column": self.quote_name(new_field.column),
                }
            )
        elif old_is_auto and not new_is_auto:
            self.execute(
                "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT"
                % (
                    self.quote_name(model._meta.db_table),
                    self.quote_name(new_field.column),
                )
            )
            seq_name = "%s_%s_seq" % (model._meta.db_table, old_field.column)
            self.execute(
                self.sql_delete_sequence % {"sequence": self.quote_name(seq_name)}
            )

    def _field_indexes_sql(self, model, field):
        return super()._field_indexes_sql(model, field)

    def _create_fk_sql(self, model, field, suffix):
        return super()._create_fk_sql(model, field, suffix)

    def prepare_default(self, value):
        return self.quote_value(value)
