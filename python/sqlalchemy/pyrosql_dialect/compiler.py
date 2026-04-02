"""SQLAlchemy SQL and DDL compilers for the PyroSQL dialect."""

from sqlalchemy.sql import compiler
from sqlalchemy import types as sqltypes


class PyroSQLCompiler(compiler.SQLCompiler):
    """SQL statement compiler for PyroSQL.

    Handles PyroSQL-specific SQL generation including LIMIT/OFFSET,
    boolean literals, RETURNING, and string concatenation.
    """

    def limit_clause(self, select, **kw):
        """Generate LIMIT / OFFSET clause.

        PyroSQL uses the standard ``LIMIT n OFFSET m`` syntax.
        """
        text = ""
        if select._limit_clause is not None:
            text += " \n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text += " \n OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def visit_true(self, element, **kw):
        return "TRUE"

    def visit_false(self, element, **kw):
        return "FALSE"

    def returning_clause(self, stmt, returning_cols, **kw):
        """Compile a RETURNING clause."""
        columns = [
            self._label_returning_column(stmt, c, fallback_label_name=c.key, **kw)
            for c in returning_cols
        ]
        return "RETURNING " + ", ".join(columns)

    def visit_now_func(self, fn, **kw):
        return "NOW()"

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "%s || %s" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw),
        )

    def render_literal_value(self, value, type_):
        if isinstance(type_, sqltypes.Boolean):
            return "TRUE" if value else "FALSE"
        return super().render_literal_value(value, type_)

    def for_update_clause(self, select, **kw):
        if select._for_update_arg is None:
            return ""
        if select._for_update_arg.read:
            return " FOR SHARE"
        return " FOR UPDATE"


class PyroSQLDDLCompiler(compiler.DDLCompiler):
    """DDL compiler for PyroSQL.

    Handles CREATE TABLE, ALTER TABLE, DROP TABLE, type compilation, etc.
    """

    def visit_create_table(self, create, **kw):
        """Compile a CREATE TABLE statement."""
        return super().visit_create_table(create, **kw)

    def visit_drop_table(self, drop, **kw):
        """Compile a DROP TABLE statement."""
        return super().visit_drop_table(drop, **kw)

    def get_column_specification(self, column, **kw):
        """Render a column definition for CREATE TABLE."""
        # Check for auto-increment / serial columns.
        if column.primary_key and column.autoincrement is not False:
            if isinstance(column.type, sqltypes.SmallInteger):
                colspec = self.preparer.format_column(column) + " SMALLSERIAL"
            elif isinstance(column.type, sqltypes.BigInteger):
                colspec = self.preparer.format_column(column) + " BIGSERIAL"
            elif isinstance(column.type, sqltypes.Integer):
                colspec = self.preparer.format_column(column) + " SERIAL"
            else:
                colspec = self.preparer.format_column(column) + " " + self.dialect.type_compiler_instance.process(column.type)
        else:
            colspec = self.preparer.format_column(column) + " " + self.dialect.type_compiler_instance.process(column.type)

        # Default value.
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        # NULL / NOT NULL.
        if not column.nullable:
            colspec += " NOT NULL"
        elif column.nullable and not column.primary_key:
            colspec += " NULL"

        return colspec

    def visit_create_index(self, create, **kw):
        """Compile a CREATE INDEX statement."""
        return super().visit_create_index(create, **kw)

    def visit_drop_index(self, drop, **kw):
        """Compile a DROP INDEX statement."""
        return super().visit_drop_index(drop, **kw)

    def post_create_table(self, table):
        """Return any text to append after CREATE TABLE body (before the closing paren)."""
        return ""


class PyroSQLTypeCompiler(compiler.GenericTypeCompiler):
    """Compiles SQLAlchemy type objects into PyroSQL DDL type strings."""

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_FLOAT(self, type_, **kw):
        return "REAL"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE PRECISION"

    def visit_DOUBLE_PRECISION(self, type_, **kw):
        return "DOUBLE PRECISION"

    def visit_NUMERIC(self, type_, **kw):
        if type_.precision is not None and type_.scale is not None:
            return f"NUMERIC({type_.precision}, {type_.scale})"
        if type_.precision is not None:
            return f"NUMERIC({type_.precision})"
        return "NUMERIC"

    def visit_DECIMAL(self, type_, **kw):
        return self.visit_NUMERIC(type_, **kw)

    def visit_TEXT(self, type_, **kw):
        return "TEXT"

    def visit_VARCHAR(self, type_, **kw):
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "VARCHAR"

    def visit_CHAR(self, type_, **kw):
        if type_.length:
            return f"CHAR({type_.length})"
        return "CHAR"

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_NCHAR(self, type_, **kw):
        return self.visit_CHAR(type_, **kw)

    def visit_BLOB(self, type_, **kw):
        return "BYTEA"

    def visit_BINARY(self, type_, **kw):
        return "BYTEA"

    def visit_VARBINARY(self, type_, **kw):
        return "BYTEA"

    def visit_CLOB(self, type_, **kw):
        return "TEXT"

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        if getattr(type_, "timezone", False):
            return "TIME WITH TIME ZONE"
        return "TIME"

    def visit_DATETIME(self, type_, **kw):
        return "TIMESTAMP"

    def visit_TIMESTAMP(self, type_, **kw):
        if getattr(type_, "timezone", False):
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"

    def visit_INTERVAL(self, type_, **kw):
        return "INTERVAL"

    def visit_JSON(self, type_, **kw):
        return "JSON"

    def visit_JSONB(self, type_, **kw):
        return "JSONB"

    def visit_UUID(self, type_, **kw):
        return "UUID"

    def visit_Uuid(self, type_, **kw):
        return "UUID"

    def visit_uuid(self, type_, **kw):
        return "UUID"

    def visit_BYTEA(self, type_, **kw):
        return "BYTEA"

    def visit_INET(self, type_, **kw):
        return "INET"

    def visit_CIDR(self, type_, **kw):
        return "CIDR"

    def visit_MACADDR(self, type_, **kw):
        return "MACADDR"

    def visit_SERIAL(self, type_, **kw):
        return "SERIAL"

    def visit_BIGSERIAL(self, type_, **kw):
        return "BIGSERIAL"

    def visit_SMALLSERIAL(self, type_, **kw):
        return "SMALLSERIAL"

    def visit_TIMESTAMPTZ(self, type_, **kw):
        return "TIMESTAMP WITH TIME ZONE"

    def visit_ARRAY(self, type_, **kw):
        inner = self.process(type_.item_type, **kw)
        return f"{inner}[]"

    def visit_large_binary(self, type_, **kw):
        return "BYTEA"

    def visit_string(self, type_, **kw):
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "VARCHAR"

    def visit_unicode(self, type_, **kw):
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "VARCHAR"

    def visit_unicode_text(self, type_, **kw):
        return "TEXT"
