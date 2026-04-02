"""PyroSQL SQLAlchemy dialect package.

Usage::

    from sqlalchemy import create_engine
    engine = create_engine('pyrosql://user:pass@host:12520/dbname')
"""

from .base import PyroSQLDialect

dialect = PyroSQLDialect

__all__ = ["PyroSQLDialect", "dialect"]
