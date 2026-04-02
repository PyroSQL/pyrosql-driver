"""SQLCompiler for PyroSQL.

Extends Django's default SQL compilers with PyroSQL-specific behavior.
"""

from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        return super().as_sql(
            with_limits=with_limits,
            with_col_aliases=with_col_aliases,
        )


class SQLInsertCompiler(compiler.SQLInsertCompiler):
    def as_sql(self):
        return super().as_sql()


class SQLDeleteCompiler(compiler.SQLDeleteCompiler):
    def as_sql(self):
        return super().as_sql()


class SQLUpdateCompiler(compiler.SQLUpdateCompiler):
    def as_sql(self):
        return super().as_sql()


class SQLAggregateCompiler(compiler.SQLAggregateCompiler):
    pass
