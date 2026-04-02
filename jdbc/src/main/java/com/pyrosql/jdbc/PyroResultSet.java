package com.pyrosql.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * ResultSet implementation for PyroSQL.
 * Backed by an in-memory list of rows received from the PWire protocol.
 */
public class PyroResultSet implements ResultSet {

    private final PyroStatement ownerStatement;
    private final PWireCodec.ColumnDef[] columns;
    private final List<Object[]> rows;
    private final PyroResultSetMetaData metaData;
    private int cursor = -1; // before first
    private boolean closed = false;
    private boolean wasNull = false;

    public PyroResultSet(PyroStatement ownerStatement, PWireCodec.ResultSet rs) {
        this.ownerStatement = ownerStatement;
        this.columns = rs.columns;
        this.rows = rs.rows;
        this.metaData = new PyroResultSetMetaData(columns);
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }

    private void checkRow() throws SQLException {
        checkOpen();
        if (cursor < 0 || cursor >= rows.size()) {
            throw new SQLException("No current row");
        }
    }

    private int resolveColumn(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columns.length) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        return columnIndex - 1;
    }

    private Object getValue(int columnIndex) throws SQLException {
        checkRow();
        int idx = resolveColumn(columnIndex);
        Object val = rows.get(cursor)[idx];
        wasNull = (val == null);
        return val;
    }

    // ---- Navigation ----

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        if (cursor < rows.size() - 1) {
            cursor++;
            return true;
        }
        cursor = rows.size();
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    // ---- Getters by index ----

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return null;
        if (val instanceof byte[]) return new String((byte[]) val, StandardCharsets.UTF_8);
        return val.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return false;
        if (val instanceof Boolean) return (Boolean) val;
        if (val instanceof Number) return ((Number) val).longValue() != 0;
        if (val instanceof String) return Boolean.parseBoolean((String) val);
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).byteValue();
        if (val instanceof String) return Byte.parseByte((String) val);
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).shortValue();
        if (val instanceof String) return Short.parseShort((String) val);
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).intValue();
        if (val instanceof Boolean) return ((Boolean) val) ? 1 : 0;
        if (val instanceof String) return Integer.parseInt((String) val);
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).longValue();
        if (val instanceof Boolean) return ((Boolean) val) ? 1L : 0L;
        if (val instanceof String) return Long.parseLong((String) val);
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return 0.0f;
        if (val instanceof Number) return ((Number) val).floatValue();
        if (val instanceof String) return Float.parseFloat((String) val);
        return 0.0f;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return 0.0;
        if (val instanceof Number) return ((Number) val).doubleValue();
        if (val instanceof String) return Double.parseDouble((String) val);
        return 0.0;
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        if (bd != null) bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
        return bd;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return null;
        if (val instanceof byte[]) return (byte[]) val;
        if (val instanceof String) return ((String) val).getBytes(StandardCharsets.UTF_8);
        return val.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        return Date.valueOf(s);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        return Time.valueOf(s);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        return Timestamp.valueOf(s);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        byte[] b = getBytes(columnIndex);
        return b == null ? null : new ByteArrayInputStream(b);
    }

    @Override
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndex);
    }

    // ---- Getters by label ----

    @Override public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
    @Override public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
    @Override public byte getByte(String columnLabel) throws SQLException { return getByte(findColumn(columnLabel)); }
    @Override public short getShort(String columnLabel) throws SQLException { return getShort(findColumn(columnLabel)); }
    @Override public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
    @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
    @Override public float getFloat(String columnLabel) throws SQLException { return getFloat(findColumn(columnLabel)); }
    @Override public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
    @Override @SuppressWarnings("deprecation") public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { return getBigDecimal(findColumn(columnLabel), scale); }
    @Override public byte[] getBytes(String columnLabel) throws SQLException { return getBytes(findColumn(columnLabel)); }
    @Override public Date getDate(String columnLabel) throws SQLException { return getDate(findColumn(columnLabel)); }
    @Override public Time getTime(String columnLabel) throws SQLException { return getTime(findColumn(columnLabel)); }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { return getTimestamp(findColumn(columnLabel)); }
    @Override public InputStream getAsciiStream(String columnLabel) throws SQLException { return getAsciiStream(findColumn(columnLabel)); }
    @Override @SuppressWarnings("deprecation") public InputStream getUnicodeStream(String columnLabel) throws SQLException { return getUnicodeStream(findColumn(columnLabel)); }
    @Override public InputStream getBinaryStream(String columnLabel) throws SQLException { return getBinaryStream(findColumn(columnLabel)); }

    // ---- Metadata ----

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name.equalsIgnoreCase(columnLabel)) return i + 1;
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        return s == null ? null : new StringReader(s);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return null;
        if (val instanceof BigDecimal) return (BigDecimal) val;
        if (val instanceof Long) return BigDecimal.valueOf((Long) val);
        if (val instanceof Double) return BigDecimal.valueOf((Double) val);
        if (val instanceof String) return new BigDecimal((String) val);
        return new BigDecimal(val.toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    // ---- Positioning ----

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkOpen();
        return cursor == -1 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkOpen();
        return cursor >= rows.size() && !rows.isEmpty();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkOpen();
        return cursor == 0 && !rows.isEmpty();
    }

    @Override
    public boolean isLast() throws SQLException {
        checkOpen();
        return cursor == rows.size() - 1 && !rows.isEmpty();
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkOpen();
        cursor = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkOpen();
        cursor = rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkOpen();
        if (rows.isEmpty()) return false;
        cursor = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkOpen();
        if (rows.isEmpty()) return false;
        cursor = rows.size() - 1;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkOpen();
        if (cursor < 0 || cursor >= rows.size()) return 0;
        return cursor + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkOpen();
        if (row > 0) {
            cursor = row - 1;
            return cursor < rows.size();
        } else if (row < 0) {
            cursor = rows.size() + row;
            return cursor >= 0;
        }
        cursor = -1;
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return absolute(getRow() + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkOpen();
        if (cursor > 0) {
            cursor--;
            return true;
        }
        cursor = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        checkOpen();
        return TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkOpen();
        return CONCUR_READ_ONLY;
    }

    // ---- Update operations (not supported) ----

    @Override public boolean rowUpdated() throws SQLException { return false; }
    @Override public boolean rowInserted() throws SQLException { return false; }
    @Override public boolean rowDeleted() throws SQLException { return false; }
    @Override public void updateNull(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateByte(int columnIndex, byte x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateShort(int columnIndex, short x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateInt(int columnIndex, int x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateLong(int columnIndex, long x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateFloat(int columnIndex, float x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateDouble(int columnIndex, double x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateString(int columnIndex, String x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateDate(int columnIndex, Date x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateTime(int columnIndex, Time x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateObject(int columnIndex, Object x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override public void updateNull(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBoolean(String columnLabel, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateByte(String columnLabel, byte x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateShort(String columnLabel, short x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateInt(String columnLabel, int x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateLong(String columnLabel, long x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateFloat(String columnLabel, float x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateDouble(String columnLabel, double x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateString(String columnLabel, String x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBytes(String columnLabel, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateDate(String columnLabel, Date x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateTime(String columnLabel, Time x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateObject(String columnLabel, Object x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void refreshRow() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void cancelRowUpdates() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void moveToCurrentRow() throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override
    public Statement getStatement() throws SQLException {
        return ownerStatement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("getRef"); }
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        byte[] b = getBytes(columnIndex);
        if (b == null) return null;
        return new javax.sql.rowset.serial.SerialBlob(b);
    }
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        return new javax.sql.rowset.serial.SerialClob(s.toCharArray());
    }
    @Override public Array getArray(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("getArray"); }

    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { return getObject(findColumn(columnLabel)); }
    @Override public Ref getRef(String columnLabel) throws SQLException { return getRef(findColumn(columnLabel)); }
    @Override public Blob getBlob(String columnLabel) throws SQLException { return getBlob(findColumn(columnLabel)); }
    @Override public Clob getClob(String columnLabel) throws SQLException { return getClob(findColumn(columnLabel)); }
    @Override public Array getArray(String columnLabel) throws SQLException { return getArray(findColumn(columnLabel)); }

    @Override public Date getDate(int columnIndex, Calendar cal) throws SQLException { return getDate(columnIndex); }
    @Override public Date getDate(String columnLabel, Calendar cal) throws SQLException { return getDate(columnLabel); }
    @Override public Time getTime(int columnIndex, Calendar cal) throws SQLException { return getTime(columnIndex); }
    @Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { return getTime(columnLabel); }
    @Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { return getTimestamp(columnIndex); }
    @Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { return getTimestamp(columnLabel); }

    @Override public URL getURL(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("getURL"); }
    @Override public URL getURL(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException("getURL"); }

    @Override public void updateRef(int columnIndex, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateRef(String columnLabel, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBlob(int columnIndex, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBlob(String columnLabel, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateClob(int columnIndex, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateClob(String columnLabel, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateArray(int columnIndex, Array x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateArray(String columnLabel, Array x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override public RowId getRowId(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("getRowId"); }
    @Override public RowId getRowId(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException("getRowId"); }
    @Override public void updateRowId(int columnIndex, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateRowId(String columnLabel, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override public void updateNString(int columnIndex, String nString) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNString(String columnLabel, String nString) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override public NClob getNClob(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("getNClob"); }
    @Override public NClob getNClob(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException("getNClob"); }
    @Override public SQLXML getSQLXML(int columnIndex) throws SQLException { throw new SQLFeatureNotSupportedException("getSQLXML"); }
    @Override public SQLXML getSQLXML(String columnLabel) throws SQLException { throw new SQLFeatureNotSupportedException("getSQLXML"); }
    @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateClob(int columnIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateClob(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNClob(int columnIndex, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }
    @Override public void updateNClob(String columnLabel, Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("Updates not supported"); }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object val = getValue(columnIndex);
        if (val == null) return null;
        if (type.isInstance(val)) return type.cast(val);
        if (type == String.class) return type.cast(getString(columnIndex));
        if (type == Long.class || type == long.class) return type.cast(getLong(columnIndex));
        if (type == Integer.class || type == int.class) return type.cast(getInt(columnIndex));
        if (type == Double.class || type == double.class) return type.cast(getDouble(columnIndex));
        if (type == Float.class || type == float.class) return type.cast(getFloat(columnIndex));
        if (type == Boolean.class || type == boolean.class) return type.cast(getBoolean(columnIndex));
        if (type == BigDecimal.class) return type.cast(getBigDecimal(columnIndex));
        if (type == byte[].class) return type.cast(getBytes(columnIndex));
        throw new SQLException("Cannot convert to " + type.getName());
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
