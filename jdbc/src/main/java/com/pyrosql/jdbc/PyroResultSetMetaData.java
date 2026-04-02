package com.pyrosql.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * ResultSetMetaData implementation for PyroSQL result sets.
 */
public class PyroResultSetMetaData implements ResultSetMetaData {

    private final PWireCodec.ColumnDef[] columns;

    public PyroResultSetMetaData(PWireCodec.ColumnDef[] columns) {
        this.columns = columns;
    }

    private void checkIndex(int column) throws SQLException {
        if (column < 1 || column > columns.length) {
            throw new SQLException("Column index out of range: " + column + ", columns: " + columns.length);
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkIndex(column);
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkIndex(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkIndex(column);
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkIndex(column);
        int typeTag = columns[column - 1].typeTag;
        return typeTag == PWireCodec.TYPE_I64 || typeTag == PWireCodec.TYPE_F64;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkIndex(column);
        switch (columns[column - 1].typeTag) {
            case PWireCodec.TYPE_I64: return 20;
            case PWireCodec.TYPE_F64: return 25;
            case PWireCodec.TYPE_BOOL: return 5;
            case PWireCodec.TYPE_TEXT: return 65535;
            case PWireCodec.TYPE_BYTES: return 65535;
            default: return 65535;
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkIndex(column);
        return columns[column - 1].name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        checkIndex(column);
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkIndex(column);
        switch (columns[column - 1].typeTag) {
            case PWireCodec.TYPE_I64: return 19;
            case PWireCodec.TYPE_F64: return 15;
            default: return 0;
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkIndex(column);
        if (columns[column - 1].typeTag == PWireCodec.TYPE_F64) return 6;
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkIndex(column);
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkIndex(column);
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkIndex(column);
        switch (columns[column - 1].typeTag) {
            case PWireCodec.TYPE_I64: return Types.BIGINT;
            case PWireCodec.TYPE_F64: return Types.DOUBLE;
            case PWireCodec.TYPE_TEXT: return Types.VARCHAR;
            case PWireCodec.TYPE_BOOL: return Types.BOOLEAN;
            case PWireCodec.TYPE_BYTES: return Types.VARBINARY;
            case PWireCodec.TYPE_NULL: return Types.NULL;
            default: return Types.OTHER;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkIndex(column);
        switch (columns[column - 1].typeTag) {
            case PWireCodec.TYPE_I64: return "I64";
            case PWireCodec.TYPE_F64: return "F64";
            case PWireCodec.TYPE_TEXT: return "TEXT";
            case PWireCodec.TYPE_BOOL: return "BOOL";
            case PWireCodec.TYPE_BYTES: return "BYTES";
            case PWireCodec.TYPE_NULL: return "NULL";
            default: return "UNKNOWN";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkIndex(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        checkIndex(column);
        switch (columns[column - 1].typeTag) {
            case PWireCodec.TYPE_I64: return Long.class.getName();
            case PWireCodec.TYPE_F64: return Double.class.getName();
            case PWireCodec.TYPE_TEXT: return String.class.getName();
            case PWireCodec.TYPE_BOOL: return Boolean.class.getName();
            case PWireCodec.TYPE_BYTES: return byte[].class.getName();
            default: return Object.class.getName();
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
