"""DatabaseClient for PyroSQL.

Provides the ``runshell`` method used by Django's ``dbshell`` management command.
"""

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "pyrosql"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]
        host = settings_dict.get("HOST")
        port = settings_dict.get("PORT")
        dbname = settings_dict.get("NAME")
        user = settings_dict.get("USER")

        if host:
            args += ["--host", host]
        if port:
            args += ["--port", str(port)]
        if user:
            args += ["--user", user]
        if dbname:
            args.append(dbname)
        args.extend(parameters)

        env = None
        password = settings_dict.get("PASSWORD")
        if password:
            env = {"PYROSQL_PASSWORD": password}

        return args, env
