package com.pyrosql.jdbc;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Connection implementation for PyroSQL.
 * Wraps a PWireConnection and provides JDBC-standard connection management.
 */
public class PyroConnection implements Connection {

    private PWireConnection wire;
    private boolean closed = false;
    private boolean autoCommit = true;
    private int transactionIsolation = TRANSACTION_READ_COMMITTED;
    private boolean readOnly = false;
    private String catalog;
    private String schema;
    private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private SQLWarning warnings;

    public PyroConnection(String host, int port, String database, String user, String password,
                          Properties info) throws SQLException {
        int timeout = 30000;
        String timeoutStr = info != null ? info.getProperty("loginTimeout") : null;
        if (timeoutStr != null) {
            timeout = Integer.parseInt(timeoutStr) * 1000;
        }

        this.wire = new PWireConnection(host, port, timeout);
        this.wire.authenticate(user, password);
        this.catalog = database;

        // Select the database
        if (database != null && !database.isEmpty()) {
            wire.query("USE " + database);
        }
    }

    PWireConnection getWire() {
        return wire;
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("Connection is closed", "08003");
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new PyroStatement(this, wire);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new PyroPreparedStatement(this, wire, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        if (this.autoCommit != autoCommit) {
            this.autoCommit = autoCommit;
            if (autoCommit) {
                wire.query("COMMIT");
            } else {
                wire.query("BEGIN");
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("Cannot commit in auto-commit mode");
        PWireCodec.Frame frame = wire.query("COMMIT");
        if (frame.type == PWireCodec.RESP_ERROR) {
            PWireCodec.ErrorResult err = PWireCodec.decodeError(frame.payload);
            throw new SQLException(err.message, err.sqlState);
        }
        // Start a new transaction
        wire.query("BEGIN");
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("Cannot rollback in auto-commit mode");
        PWireCodec.Frame frame = wire.query("ROLLBACK");
        if (frame.type == PWireCodec.RESP_ERROR) {
            PWireCodec.ErrorResult err = PWireCodec.decodeError(frame.payload);
            throw new SQLException(err.message, err.sqlState);
        }
        // Start a new transaction
        wire.query("BEGIN");
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            if (wire != null) {
                wire.close();
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return new PyroDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
        warnings = null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkOpen();
        return java.util.Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkOpen();
        String name = "sp_" + System.nanoTime();
        wire.query("SAVEPOINT " + name);
        return new PyroSavepoint(name);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        wire.query("SAVEPOINT " + name);
        return new PyroSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        wire.query("ROLLBACK TO SAVEPOINT " + savepoint.getSavepointName());
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        wire.query("RELEASE SAVEPOINT " + savepoint.getSavepointName());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed) return false;
        try {
            wire.ping();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // silently ignore
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // silently ignore
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkOpen();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkOpen();
        return 0;
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

    // ---- Inner classes ----

    private static class PyroSavepoint implements Savepoint {
        private final String name;
        PyroSavepoint(String name) { this.name = name; }
        @Override public int getSavepointId() throws SQLException { throw new SQLException("Named savepoint"); }
        @Override public String getSavepointName() throws SQLException { return name; }
    }
}
