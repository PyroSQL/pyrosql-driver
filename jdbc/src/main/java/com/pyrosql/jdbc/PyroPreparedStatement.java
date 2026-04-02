package com.pyrosql.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * PreparedStatement implementation for PyroSQL.
 * Uses the PWire binary PREPARE + EXECUTE + CLOSE messages for server-side binding.
 */
public class PyroPreparedStatement extends PyroStatement implements PreparedStatement {

    private final String sql;
    private final int handle;
    private boolean handleClosed = false;
    private final Map<Integer, String> parameters = new TreeMap<>();
    private final Map<Integer, Boolean> isNull = new TreeMap<>();
    private final List<Map<Integer, String>> batchParamValues = new ArrayList<>();
    private final List<Map<Integer, Boolean>> batchParamNulls = new ArrayList<>();

    public PyroPreparedStatement(PyroConnection connection, PWireConnection wire, String sql) throws SQLException {
        super(connection, wire);
        this.sql = rewritePlaceholders(sql);
        this.handle = wire.prepare(this.sql);
    }

    /**
     * Rewrite JDBC-style ? placeholders to $1, $2, ... placeholders that
     * the server expects for binary PREPARE/EXECUTE.
     */
    private static String rewritePlaceholders(String sql) {
        StringBuilder result = new StringBuilder(sql.length());
        int paramIndex = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                result.append(c);
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                result.append(c);
            } else if (c == '?' && !inSingleQuote && !inDoubleQuote) {
                paramIndex++;
                result.append('$').append(paramIndex);
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    /**
     * Build the ordered list of parameter string values for MSG_EXECUTE.
     */
    private List<String> buildParamList() {
        int maxParam = parameters.isEmpty() ? 0 : Collections.max(parameters.keySet());
        List<String> params = new ArrayList<>(maxParam);
        for (int i = 1; i <= maxParam; i++) {
            Boolean nullFlag = isNull.get(i);
            if (nullFlag != null && nullFlag) {
                params.add("NULL");
            } else {
                String val = parameters.get(i);
                params.add(val != null ? val : "NULL");
            }
        }
        return params;
    }

    private void setParam(int parameterIndex, String value) {
        parameters.put(parameterIndex, value);
        isNull.put(parameterIndex, false);
    }

    private void setParamBytes(int parameterIndex, byte[] value) {
        // Convert bytes to hex literal
        StringBuilder hex = new StringBuilder("X'");
        for (byte b : value) {
            hex.append(String.format("%02x", b & 0xFF));
        }
        hex.append("'");
        parameters.put(parameterIndex, hex.toString());
        isNull.put(parameterIndex, false);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        PWireCodec.Frame frame = wire.execute(handle, buildParamList());
        processFrame(frame);
        ResultSet rs = getResultSet();
        if (rs == null) throw new SQLException("Query did not return a result set");
        return rs;
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkOpen();
        PWireCodec.Frame frame = wire.execute(handle, buildParamList());
        processFrame(frame);
        return getUpdateCount();
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();
        PWireCodec.Frame frame = wire.execute(handle, buildParamList());
        processFrame(frame);
        return getResultSet() != null;
    }

    @Override
    public void close() throws SQLException {
        if (!handleClosed) {
            handleClosed = true;
            try {
                wire.closeHandle(handle);
            } catch (SQLException e) {
                // Best effort — connection may already be closed
            }
        }
        super.close();
    }

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();
        parameters.clear();
        isNull.clear();
    }

    // ---- Parameter setters ----

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, "NULL");
        isNull.put(parameterIndex, true);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, x ? "true" : "false");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkOpen();
        setParam(parameterIndex, String.valueOf(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.DECIMAL); return; }
        setParam(parameterIndex, x.toPlainString());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.VARCHAR); return; }
        // Server-side binding: send the raw string value. The server handles quoting.
        setParam(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.VARBINARY); return; }
        setParamBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.DATE); return; }
        setParam(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.TIME); return; }
        setParam(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.TIMESTAMP); return; }
        setParam(parameterIndex, "'" + x.toString() + "'");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.VARCHAR); return; }
        try {
            byte[] buf = new byte[length];
            int read = 0;
            while (read < length) {
                int n = x.read(buf, read, length - read);
                if (n < 0) break;
                read += n;
            }
            setParamBytes(parameterIndex, Arrays.copyOf(buf, read));
        } catch (IOException e) {
            throw new SQLException("Error reading stream", e);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else {
            setString(parameterIndex, x.toString());
        }
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
        batchParamValues.add(new TreeMap<>(parameters));
        batchParamNulls.add(new TreeMap<>(isNull));
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        int[] results = new int[batchParamValues.size()];
        for (int i = 0; i < batchParamValues.size(); i++) {
            parameters.clear();
            isNull.clear();
            parameters.putAll(batchParamValues.get(i));
            isNull.putAll(batchParamNulls.get(i));
            try {
                results[i] = executeUpdate();
            } catch (SQLException e) {
                throw new BatchUpdateException("Batch failed at index " + i + ": " + e.getMessage(),
                        e.getSQLState(), results);
            }
        }
        batchParamValues.clear();
        batchParamNulls.clear();
        parameters.clear();
        isNull.clear();
        return results;
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        batchParamValues.clear();
        batchParamNulls.clear();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        checkOpen();
        if (reader == null) { setNull(parameterIndex, Types.VARCHAR); return; }
        try {
            char[] buf = new char[length];
            int read = 0;
            while (read < length) {
                int n = reader.read(buf, read, length - read);
                if (n < 0) break;
                read += n;
            }
            setParam(parameterIndex, new String(buf, 0, read));
        } catch (IOException e) {
            throw new SQLException("Error reading reader", e);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.BLOB); return; }
        setBytes(parameterIndex, x.getBytes(1, (int) x.length()));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.CLOB); return; }
        setString(parameterIndex, x.getSubString(1, (int) x.length()));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null; // metadata is available after execution
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.VARCHAR); return; }
        setParam(parameterIndex, x.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, (int) length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, (int) length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkOpen();
        if (x == null) { setNull(parameterIndex, Types.VARCHAR); return; }
        try {
            setParamBytes(parameterIndex, readAll(x));
        } catch (IOException e) {
            throw new SQLException("Error reading stream", e);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        checkOpen();
        if (reader == null) { setNull(parameterIndex, Types.VARCHAR); return; }
        try {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[4096];
            int n;
            while ((n = reader.read(buf)) >= 0) sb.append(buf, 0, n);
            setParam(parameterIndex, sb.toString());
        } catch (IOException e) {
            throw new SQLException("Error reading reader", e);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    private byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) bos.write(buf, 0, n);
        return bos.toByteArray();
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
