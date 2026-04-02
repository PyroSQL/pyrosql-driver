package com.pyrosql.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Statement implementation for PyroSQL.
 * Sends SQL directly to the server via the PWire QUERY message.
 */
public class PyroStatement implements Statement {

    protected PyroConnection connection;
    protected PWireConnection wire;
    private boolean closed = false;
    private PyroResultSet currentResultSet;
    private long updateCount = -1;
    private int maxRows = 0;
    private int queryTimeout = 0;
    private int fetchSize = 0;
    private final List<String> batch = new ArrayList<>();
    private boolean closeOnCompletion = false;

    public PyroStatement(PyroConnection connection, PWireConnection wire) {
        this.connection = connection;
        this.wire = wire;
    }

    protected void checkOpen() throws SQLException {
        if (closed) throw new SQLException("Statement is closed");
        if (connection.isClosed()) throw new SQLException("Connection is closed");
    }

    private void clearResults() {
        if (currentResultSet != null) {
            try { currentResultSet.close(); } catch (SQLException ignored) {}
            currentResultSet = null;
        }
        updateCount = -1;
    }

    protected void processFrame(PWireCodec.Frame frame) throws SQLException {
        switch (frame.type) {
            case PWireCodec.RESP_RESULT_SET:
                PWireCodec.ResultSet rs = PWireCodec.decodeResultSet(frame.payload);
                currentResultSet = new PyroResultSet(this, rs);
                updateCount = -1;
                break;
            case PWireCodec.RESP_OK:
                PWireCodec.OkResult ok = PWireCodec.decodeOk(frame.payload);
                updateCount = ok.rowsAffected;
                currentResultSet = null;
                break;
            case PWireCodec.RESP_ERROR:
                PWireCodec.ErrorResult err = PWireCodec.decodeError(frame.payload);
                throw new SQLException(err.message, err.sqlState);
            default:
                throw new SQLException("Unexpected response type: " + frame.type);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        clearResults();
        PWireCodec.Frame frame = wire.query(sql);
        processFrame(frame);
        if (currentResultSet == null) {
            throw new SQLException("Query did not return a result set");
        }
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        clearResults();
        PWireCodec.Frame frame = wire.query(sql);
        processFrame(frame);
        if (currentResultSet != null) {
            throw new SQLException("executeUpdate returned a result set");
        }
        return (int) updateCount;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            clearResults();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkOpen();
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkOpen();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkOpen();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkOpen();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkOpen();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkOpen();
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancel");
    }

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
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCursorName");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        clearResults();
        PWireCodec.Frame frame = wire.query(sql);
        processFrame(frame);
        return currentResultSet != null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        return (int) updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkOpen();
        clearResults();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkOpen();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        int[] results = new int[batch.size()];
        for (int i = 0; i < batch.size(); i++) {
            try {
                results[i] = executeUpdate(batch.get(i));
            } catch (SQLException e) {
                throw new BatchUpdateException("Batch failed at statement " + i + ": " + e.getMessage(),
                        e.getSQLState(), results);
            }
        }
        batch.clear();
        return results;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("getGeneratedKeys");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkOpen();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkOpen();
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkOpen();
        return closeOnCompletion;
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
