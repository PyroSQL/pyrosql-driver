"""DatabaseCreation for PyroSQL.

Handles creating and destroying test databases.
"""

from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):

    def _quote_name(self, name):
        return self.connection.ops.quote_name(name)

    def _get_database_create_suffix(self, encoding=None, template=None):
        suffix = ""
        if encoding:
            suffix += " ENCODING '%s'" % encoding
        if template:
            suffix += " TEMPLATE %s" % self._quote_name(template)
        return suffix

    def sql_table_creation_suffix(self):
        test_settings = self.connection.settings_dict.get("TEST", {})
        encoding = test_settings.get("CHARSET")
        template = test_settings.get("TEMPLATE")
        return self._get_database_create_suffix(encoding=encoding, template=template)

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        try:
            super()._execute_create_test_db(cursor, parameters, keepdb)
        except Exception:
            if keepdb:
                return
            raise

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        test_database_name = self._get_test_db_name()
        test_db_params = {
            "dbname": self._quote_name(test_database_name),
            "suffix": self.sql_table_creation_suffix(),
        }
        with self._nodb_cursor() as cursor:
            try:
                self._execute_create_test_db(cursor, test_db_params, keepdb)
            except Exception as e:
                if not autoclobber:
                    raise
                self._destroy_test_db(test_database_name, verbosity)
                self._execute_create_test_db(cursor, test_db_params, keepdb)
        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        with self._nodb_cursor() as cursor:
            cursor.execute(
                "DROP DATABASE IF EXISTS %s" % self._quote_name(test_database_name)
            )

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        source_database_name = self.connection.settings_dict["NAME"]
        target_database_name = self.get_test_db_clone_settings(suffix)["NAME"]
        test_db_params = {
            "dbname": self._quote_name(target_database_name),
            "suffix": self._get_database_create_suffix(
                template=source_database_name,
            ),
        }
        with self._nodb_cursor() as cursor:
            try:
                self._execute_create_test_db(cursor, test_db_params, keepdb)
            except Exception:
                if keepdb:
                    return
                try:
                    self._destroy_test_db(target_database_name, verbosity)
                except Exception:
                    pass
                self._execute_create_test_db(cursor, test_db_params, keepdb)
