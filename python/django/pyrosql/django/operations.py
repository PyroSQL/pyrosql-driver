"""DatabaseOperations for PyroSQL.

Handles SQL generation, type conversions, and database-specific operations
for PyroSQL's PostgreSQL-compatible dialect.
"""

import datetime
import decimal
import json
import uuid

from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from django.utils.duration import duration_microseconds


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "pyrosql.django.compiler"
    cast_char_field_without_max_length = "VARCHAR"
    explain_prefix = "EXPLAIN"
    cast_data_types = {
        "AutoField": "INTEGER",
        "BigAutoField": "BIGINT",
        "SmallAutoField": "SMALLINT",
    }

    def unification_cast_sql(self, output_field):
        internal_type = output_field.get_internal_type()
        if internal_type in (
            "GenericIPAddressField",
            "IPAddressField",
            "TimeField",
            "UUIDField",
        ):
            return "CAST(%%s AS %s)" % output_field.db_type(self.connection)
        return "%s"

    def date_extract_sql(self, lookup_type, sql, params):
        if lookup_type == "week_day":
            return f"EXTRACT(DOW FROM {sql}) + 1", params
        if lookup_type == "iso_week_day":
            return f"EXTRACT(ISODOW FROM {sql})", params
        if lookup_type == "iso_year":
            return f"EXTRACT(ISOYEAR FROM {sql})", params
        return f"EXTRACT({lookup_type.upper()} FROM {sql})", params

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        if tzname:
            sql = f"{sql} AT TIME ZONE %s"
            params = (*params, tzname)
        return f"DATE_TRUNC(%s, {sql})", ("%s", lookup_type, *params)

    def datetime_cast_date_sql(self, sql, params, tzname):
        if tzname:
            sql = f"{sql} AT TIME ZONE %s"
            params = (*params, tzname)
        return f"({sql})::DATE", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        if tzname:
            sql = f"{sql} AT TIME ZONE %s"
            params = (*params, tzname)
        return f"({sql})::TIME", params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        if tzname:
            sql = f"{sql} AT TIME ZONE %s"
            params = (*params, tzname)
        return self.date_extract_sql(lookup_type, sql, params)

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname):
        if tzname:
            sql = f"{sql} AT TIME ZONE %s"
            params = (*params, tzname)
        return f"DATE_TRUNC(%s, {sql})", ("%s", lookup_type, *params)

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        if tzname:
            sql = f"{sql} AT TIME ZONE %s"
            params = (*params, tzname)
        return f"DATE_TRUNC(%s, {sql})", ("%s", lookup_type, *params)

    def deferrable_sql(self):
        return " DEFERRABLE INITIALLY DEFERRED"

    def distinct_sql(self, fields, params):
        if fields:
            field_sql = ", ".join(fields)
            return [f"DISTINCT ON ({field_sql})"], params
        return ["DISTINCT"], []

    def fetch_returned_insert_columns(self, cursor, returning_params):
        return cursor.fetchone()

    def lookup_cast(self, lookup_type, internal_type=None):
        lookup = "%s"
        if lookup_type == "iexact":
            lookup = "%s::TEXT"
        elif lookup_type in ("contains", "icontains"):
            lookup = "%s::TEXT"
        return lookup

    def no_limit_value(self):
        return None

    def prepare_sql_script(self, sql):
        return [sql]

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name
        return '"%s"' % name

    def regex_lookup(self, lookup_type):
        if lookup_type == "regex":
            return "%s ~ %s"
        return "%s ~* %s"

    def set_time_zone_sql(self):
        return "SET TIME ZONE %s"

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []
        sql = []
        if reset_sequences:
            sql.append(
                "%s %s %s %s;"
                % (
                    style.SQL_KEYWORD("TRUNCATE"),
                    ", ".join(style.SQL_FIELD(self.quote_name(t)) for t in tables),
                    style.SQL_KEYWORD("RESTART IDENTITY"),
                    style.SQL_KEYWORD("CASCADE") if allow_cascade else "",
                )
            )
        else:
            if allow_cascade:
                sql.append(
                    "%s %s %s;"
                    % (
                        style.SQL_KEYWORD("TRUNCATE"),
                        ", ".join(
                            style.SQL_FIELD(self.quote_name(t)) for t in tables
                        ),
                        style.SQL_KEYWORD("CASCADE"),
                    )
                )
            else:
                for table in tables:
                    sql.append(
                        "%s %s %s;"
                        % (
                            style.SQL_KEYWORD("DELETE"),
                            style.SQL_KEYWORD("FROM"),
                            style.SQL_FIELD(self.quote_name(table)),
                        )
                    )
        return sql

    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        for seq_info in sequences:
            table_name = seq_info["table"]
            column_name = seq_info.get("column") or "id"
            sql.append(
                "SELECT setval(pg_get_serial_sequence('%s','%s'), "
                "COALESCE(MAX(%s), 1), MAX(%s) IS NOT NULL) FROM %s;"
                % (
                    table_name,
                    column_name,
                    self.quote_name(column_name),
                    self.quote_name(column_name),
                    self.quote_name(table_name),
                )
            )
        return sql

    def sequence_reset_sql(self, style, model_list):
        from django.db import models

        output = []
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append(
                        "SELECT setval(pg_get_serial_sequence("
                        "'%s','%s'), COALESCE(MAX(%s), 1), "
                        "MAX(%s) IS NOT NULL) FROM %s;"
                        % (
                            model._meta.db_table,
                            f.column,
                            self.quote_name(f.column),
                            self.quote_name(f.column),
                            self.quote_name(model._meta.db_table),
                        )
                    )
                    break
        return output

    def tablespace_sql(self, tablespace, inline=False):
        if inline:
            return "USING INDEX TABLESPACE %s" % self.quote_name(tablespace)
        return "TABLESPACE %s" % self.quote_name(tablespace)

    def prep_for_like_query(self, x):
        return str(x).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def adapt_datefield_value(self, value):
        return value

    def adapt_datetimefield_value(self, value):
        return value

    def adapt_timefield_value(self, value):
        return value

    def adapt_decimalfield_value(self, value, max_digits=None, decimal_places=None):
        return value

    def adapt_ipaddressfield_value(self, value):
        if value:
            return value
        return None

    def adapt_json_value(self, value, encoder):
        return json.dumps(value, cls=encoder)

    def subtract_temporals(self, internal_type, lhs, rhs):
        lhs_sql, lhs_params = lhs
        rhs_sql, rhs_params = rhs
        params = (*lhs_params, *rhs_params)
        if internal_type == "DateField":
            return f"(({lhs_sql})::DATE - ({rhs_sql})::DATE)", params
        return f"({lhs_sql} - {rhs_sql})", params

    def explain_query_prefix(self, format=None, **options):
        prefix = self.explain_prefix
        extra = {}
        if format:
            extra["FORMAT"] = format
        extra.update(options)
        if extra:
            prefix += " (%s)" % ", ".join(
                f"{key} {value}" if not isinstance(value, bool) else key
                for key, value in extra.items()
                if value is not False
            )
        return prefix

    def on_conflict_suffix_sql(self, fields, on_conflict, update_fields, unique_fields):
        return ""

    def return_insert_columns(self, fields):
        if not fields:
            return "", ()
        columns = [
            "%s.%s"
            % (
                self.quote_name(field.model._meta.db_table),
                self.quote_name(field.column),
            )
            for field in fields
        ]
        return "RETURNING %s" % ", ".join(columns), ()

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def max_name_length(self):
        return 63

    def last_insert_id(self, cursor, table_name, pk_name):
        sql = "SELECT CURRVAL(pg_get_serial_sequence('%s','%s'))" % (
            table_name,
            pk_name,
        )
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] if row else None

    def last_executed_query(self, cursor, sql, params):
        if params:
            try:
                return sql % tuple(
                    "'%s'" % str(p).replace("'", "''") if p is not None else "NULL"
                    for p in params
                )
            except (TypeError, ValueError):
                return sql
        return sql

    def convert_durationfield_value(self, value, expression, connection):
        if value is not None:
            if isinstance(value, datetime.timedelta):
                return duration_microseconds(value)
            if isinstance(value, (int, float)):
                return int(value)
        return value

    def format_for_duration_arithmetic(self, sql):
        return "INTERVAL '%s MICROSECONDS'" % sql
