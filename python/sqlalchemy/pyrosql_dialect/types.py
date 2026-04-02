"""PyroSQL custom type mappings for SQLAlchemy."""

from sqlalchemy import types as sqltypes
from sqlalchemy import util


class SERIAL(sqltypes.Integer):
    """PyroSQL SERIAL type — auto-incrementing integer."""
    __visit_name__ = "SERIAL"


class BIGSERIAL(sqltypes.BigInteger):
    """PyroSQL BIGSERIAL type — auto-incrementing big integer."""
    __visit_name__ = "BIGSERIAL"


class SMALLSERIAL(sqltypes.SmallInteger):
    """PyroSQL SMALLSERIAL type — auto-incrementing small integer."""
    __visit_name__ = "SMALLSERIAL"


class JSONB(sqltypes.JSON):
    """PyroSQL JSONB type — binary JSON storage."""
    __visit_name__ = "JSONB"


class JSON(sqltypes.JSON):
    """PyroSQL JSON type."""
    __visit_name__ = "JSON"


class UUID(sqltypes.Uuid):
    """PyroSQL UUID type."""
    __visit_name__ = "UUID"


class TIMESTAMP(sqltypes.TIMESTAMP):
    """PyroSQL TIMESTAMP type."""
    __visit_name__ = "TIMESTAMP"


class TIMESTAMPTZ(sqltypes.TIMESTAMP):
    """PyroSQL TIMESTAMP WITH TIME ZONE type."""
    __visit_name__ = "TIMESTAMPTZ"

    def __init__(self):
        super().__init__(timezone=True)


class BYTEA(sqltypes.LargeBinary):
    """PyroSQL BYTEA type — binary data."""
    __visit_name__ = "BYTEA"


class INET(sqltypes.TypeEngine):
    """PyroSQL INET type — IP address."""
    __visit_name__ = "INET"


class CIDR(sqltypes.TypeEngine):
    """PyroSQL CIDR type — network address."""
    __visit_name__ = "CIDR"


class MACADDR(sqltypes.TypeEngine):
    """PyroSQL MACADDR type — MAC address."""
    __visit_name__ = "MACADDR"


class INTERVAL(sqltypes.Interval):
    """PyroSQL INTERVAL type."""
    __visit_name__ = "INTERVAL"


class ARRAY(sqltypes.ARRAY):
    """PyroSQL ARRAY type."""
    __visit_name__ = "ARRAY"


class TEXT(sqltypes.Text):
    """PyroSQL TEXT type."""
    __visit_name__ = "TEXT"


# Mapping from PyroSQL type names (as returned by introspection) to SQLAlchemy types.
PYROSQL_TYPE_MAP = {
    "integer": sqltypes.Integer,
    "int": sqltypes.Integer,
    "int4": sqltypes.Integer,
    "smallint": sqltypes.SmallInteger,
    "int2": sqltypes.SmallInteger,
    "bigint": sqltypes.BigInteger,
    "int8": sqltypes.BigInteger,
    "serial": SERIAL,
    "bigserial": BIGSERIAL,
    "smallserial": SMALLSERIAL,
    "real": sqltypes.Float,
    "float4": sqltypes.Float,
    "double precision": sqltypes.Float,
    "float8": sqltypes.Float,
    "numeric": sqltypes.Numeric,
    "decimal": sqltypes.Numeric,
    "boolean": sqltypes.Boolean,
    "bool": sqltypes.Boolean,
    "text": sqltypes.Text,
    "varchar": sqltypes.String,
    "character varying": sqltypes.String,
    "char": sqltypes.String,
    "character": sqltypes.String,
    "bytea": sqltypes.LargeBinary,
    "date": sqltypes.Date,
    "time": sqltypes.Time,
    "timestamp": sqltypes.DateTime,
    "timestamp without time zone": sqltypes.DateTime,
    "timestamp with time zone": sqltypes.DateTime,
    "timestamptz": sqltypes.DateTime,
    "interval": sqltypes.Interval,
    "json": sqltypes.JSON,
    "jsonb": JSONB,
    "uuid": sqltypes.Uuid,
    "inet": INET,
    "cidr": CIDR,
    "macaddr": MACADDR,
}


def lookup_type(type_name):
    """Resolve a PyroSQL type name string to a SQLAlchemy type instance.

    Args:
        type_name: The type name as returned by the database introspection
                   queries (e.g. ``"varchar"``, ``"int4"``).

    Returns:
        An instantiated SQLAlchemy type object.
    """
    type_name_lower = type_name.lower().strip()

    # Handle varchar(N) / char(N)
    if type_name_lower.startswith("character varying") or type_name_lower.startswith("varchar"):
        # Extract length if present
        import re
        m = re.search(r"\((\d+)\)", type_name_lower)
        if m:
            return sqltypes.String(length=int(m.group(1)))
        return sqltypes.String()

    if type_name_lower.startswith("char") or type_name_lower.startswith("character"):
        import re
        m = re.search(r"\((\d+)\)", type_name_lower)
        if m:
            return sqltypes.String(length=int(m.group(1)))
        return sqltypes.String()

    if type_name_lower.startswith("numeric") or type_name_lower.startswith("decimal"):
        import re
        m = re.search(r"\((\d+)(?:,\s*(\d+))?\)", type_name_lower)
        if m:
            precision = int(m.group(1))
            scale = int(m.group(2)) if m.group(2) else 0
            return sqltypes.Numeric(precision=precision, scale=scale)
        return sqltypes.Numeric()

    type_cls = PYROSQL_TYPE_MAP.get(type_name_lower)
    if type_cls is not None:
        return type_cls()

    # Fallback: treat unknown types as NullType
    return sqltypes.NullType()
