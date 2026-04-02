package com.pyrosql.jdbc;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.*;

import java.io.*;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.ResultSet;
import java.util.*;

/**
 * Tests for the PyroSQL JDBC driver.
 * Uses a mock PWire server to verify protocol encoding/decoding,
 * connection management, query execution, prepared statements,
 * transactions, NULL handling, all data types, error handling,
 * ResultSetMetaData, and batch operations.
 */
public class DriverTest {

    private ServerSocket serverSocket;
    private int port;
    private Thread serverThread;
    private volatile byte[] lastReceivedPayload;
    private volatile int lastReceivedType;
    private volatile List<byte[]> receivedMessages;

    // ---- Codec unit tests (no server needed) ----

    @Test
    public void testEncodeAuth() {
        byte[] frame = PWireCodec.encodeAuth("admin", "secret");
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_AUTH, buf.get() & 0xFF);
        int len = buf.getInt();
        assertEquals(frame.length - 5, len);
        // user: 1 byte len + "admin" + 1 byte len + "secret"
        assertEquals(5, buf.get() & 0xFF);
        byte[] user = new byte[5];
        buf.get(user);
        assertEquals("admin", new String(user, StandardCharsets.UTF_8));
        assertEquals(6, buf.get() & 0xFF);
        byte[] pass = new byte[6];
        buf.get(pass);
        assertEquals("secret", new String(pass, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeQuery() {
        byte[] frame = PWireCodec.encodeQuery("SELECT 1");
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_QUERY, buf.get() & 0xFF);
        int len = buf.getInt();
        byte[] sql = new byte[len];
        buf.get(sql);
        assertEquals("SELECT 1", new String(sql, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodePrepare() {
        byte[] frame = PWireCodec.encodePrepare("SELECT ?");
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_PREPARE, buf.get() & 0xFF);
        int len = buf.getInt();
        byte[] sql = new byte[len];
        buf.get(sql);
        assertEquals("SELECT ?", new String(sql, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeExecute() {
        List<String> params = new ArrayList<>();
        params.add("hello");
        params.add("42");
        byte[] frame = PWireCodec.encodeExecute(7, params);
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_EXECUTE, buf.get() & 0xFF);
        int totalLen = buf.getInt();
        assertTrue(totalLen > 0);
        // handle: u32 LE
        int handle = buf.getInt();
        assertEquals(7, handle);
        // param_count: u16 LE
        int paramCount = buf.getShort() & 0xFFFF;
        assertEquals(2, paramCount);
        // First param
        int p1Len = buf.getShort() & 0xFFFF;
        assertEquals(5, p1Len);
        byte[] p1 = new byte[p1Len];
        buf.get(p1);
        assertEquals("hello", new String(p1, StandardCharsets.UTF_8));
        // Second param
        int p2Len = buf.getShort() & 0xFFFF;
        assertEquals(2, p2Len);
        byte[] p2 = new byte[p2Len];
        buf.get(p2);
        assertEquals("42", new String(p2, StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeClose() {
        byte[] frame = PWireCodec.encodeClose(42);
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_CLOSE, buf.get() & 0xFF);
        int len = buf.getInt();
        assertEquals(4, len); // 4 bytes for u32 handle
        int handle = buf.getInt();
        assertEquals(42, handle);
    }

    @Test
    public void testEncodePing() {
        byte[] frame = PWireCodec.encodePing();
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_PING, buf.get() & 0xFF);
        assertEquals(0, buf.getInt());
    }

    @Test
    public void testEncodeQuit() {
        byte[] frame = PWireCodec.encodeQuit();
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_QUIT & 0xFF, buf.get() & 0xFF);
        assertEquals(0, buf.getInt());
    }

    @Test
    public void testDecodeFrame() {
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(5 + payload.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 0x01);
        buf.putInt(payload.length);
        buf.put(payload);
        byte[] data = buf.array();

        PWireCodec.Frame frame = PWireCodec.decodeFrame(data, 0, data.length);
        assertEquals(0x01, frame.type);
        assertArrayEquals(payload, frame.payload);
        assertEquals(9, frame.totalBytes);
    }

    @Test(expected = PWireException.class)
    public void testDecodeFrameIncomplete() {
        PWireCodec.decodeFrame(new byte[]{0x01, 0x00}, 0, 2);
    }

    @Test
    public void testDecodeResultSet() {
        // Build a result set payload: 2 columns (name TEXT, age I64), 2 rows
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        writeLE16(bos, 2); // col count

        // Col 1: "name" TEXT
        bos.write(4); // name len
        bos.write("name".getBytes(StandardCharsets.UTF_8), 0, 4);
        bos.write(PWireCodec.TYPE_TEXT);

        // Col 2: "age" I64
        bos.write(3);
        bos.write("age".getBytes(StandardCharsets.UTF_8), 0, 3);
        bos.write(PWireCodec.TYPE_I64);

        writeLE32(bos, 2); // row count

        // Row 1: name="Alice", age=30
        bos.write(0); // null bitmap: no nulls
        writeLE16(bos, 5);
        bos.write("Alice".getBytes(StandardCharsets.UTF_8), 0, 5);
        writeLE64(bos, 30);

        // Row 2: name=null, age=25
        bos.write(1); // null bitmap: bit 0 set (col 0 is null)
        // col 0 is null, skip
        writeLE64(bos, 25);

        byte[] payload = bos.toByteArray();
        PWireCodec.ResultSet rs = PWireCodec.decodeResultSet(payload);

        assertEquals(2, rs.columns.length);
        assertEquals("name", rs.columns[0].name);
        assertEquals(PWireCodec.TYPE_TEXT, rs.columns[0].typeTag);
        assertEquals("age", rs.columns[1].name);
        assertEquals(PWireCodec.TYPE_I64, rs.columns[1].typeTag);

        assertEquals(2, rs.rows.size());
        assertEquals("Alice", rs.rows.get(0)[0]);
        assertEquals(30L, rs.rows.get(0)[1]);
        assertNull(rs.rows.get(1)[0]);
        assertEquals(25L, rs.rows.get(1)[1]);
    }

    @Test
    public void testDecodeResultSetAllTypes() {
        // Build a result set with all 5 types + NULL
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        writeLE16(bos, 5); // col count: I64, F64, TEXT, BOOL, BYTES

        bos.write(2); bos.write("id".getBytes(), 0, 2); bos.write(PWireCodec.TYPE_I64);
        bos.write(5); bos.write("score".getBytes(), 0, 5); bos.write(PWireCodec.TYPE_F64);
        bos.write(4); bos.write("name".getBytes(), 0, 4); bos.write(PWireCodec.TYPE_TEXT);
        bos.write(6); bos.write("active".getBytes(), 0, 6); bos.write(PWireCodec.TYPE_BOOL);
        bos.write(4); bos.write("data".getBytes(), 0, 4); bos.write(PWireCodec.TYPE_BYTES);

        writeLE32(bos, 1); // 1 row

        // Row: no nulls
        bos.write(0); // null bitmap
        writeLE64(bos, 42L); // I64
        writeLE64Double(bos, 3.14); // F64
        writeLE16(bos, 3); bos.write("foo".getBytes(), 0, 3); // TEXT
        bos.write(1); // BOOL true
        byte[] binData = new byte[]{0x01, 0x02, 0x03};
        writeLE16(bos, 3); bos.write(binData, 0, 3); // BYTES

        PWireCodec.ResultSet rs = PWireCodec.decodeResultSet(bos.toByteArray());
        assertEquals(5, rs.columns.length);
        assertEquals(1, rs.rows.size());

        Object[] row = rs.rows.get(0);
        assertEquals(42L, row[0]);
        assertEquals(3.14, (double) row[1], 0.001);
        assertEquals("foo", row[2]);
        assertEquals(true, row[3]);
        assertArrayEquals(binData, (byte[]) row[4]);
    }

    @Test
    public void testDecodeOk() {
        ByteBuffer buf = ByteBuffer.allocate(9 + 6).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(5L);       // rows_affected
        buf.put((byte) 6);    // tag_len
        buf.put("INSERT".getBytes(StandardCharsets.UTF_8));
        PWireCodec.OkResult ok = PWireCodec.decodeOk(buf.array());
        assertEquals(5, ok.rowsAffected);
        assertEquals("INSERT", ok.tag);
    }

    @Test
    public void testDecodeError() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write("42S02".getBytes(StandardCharsets.UTF_8), 0, 5); // sqlstate
        writeLE16(bos, 15);
        bos.write("Table not found".getBytes(StandardCharsets.UTF_8), 0, 15);
        PWireCodec.ErrorResult err = PWireCodec.decodeError(bos.toByteArray());
        assertEquals("42S02", err.sqlState);
        assertEquals("Table not found", err.message);
    }

    // ---- ResultSetMetaData tests ----

    @Test
    public void testResultSetMetaData() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("id", PWireCodec.TYPE_I64),
            new PWireCodec.ColumnDef("score", PWireCodec.TYPE_F64),
            new PWireCodec.ColumnDef("name", PWireCodec.TYPE_TEXT),
            new PWireCodec.ColumnDef("active", PWireCodec.TYPE_BOOL),
            new PWireCodec.ColumnDef("data", PWireCodec.TYPE_BYTES)
        };
        PyroResultSetMetaData meta = new PyroResultSetMetaData(cols);

        assertEquals(5, meta.getColumnCount());

        assertEquals("id", meta.getColumnName(1));
        assertEquals(Types.BIGINT, meta.getColumnType(1));
        assertEquals("I64", meta.getColumnTypeName(1));
        assertTrue(meta.isSigned(1));
        assertEquals(Long.class.getName(), meta.getColumnClassName(1));

        assertEquals("score", meta.getColumnName(2));
        assertEquals(Types.DOUBLE, meta.getColumnType(2));
        assertEquals("F64", meta.getColumnTypeName(2));

        assertEquals("name", meta.getColumnName(3));
        assertEquals(Types.VARCHAR, meta.getColumnType(3));
        assertEquals("TEXT", meta.getColumnTypeName(3));
        assertFalse(meta.isSigned(3));

        assertEquals("active", meta.getColumnName(4));
        assertEquals(Types.BOOLEAN, meta.getColumnType(4));
        assertEquals("BOOL", meta.getColumnTypeName(4));
        assertEquals(Boolean.class.getName(), meta.getColumnClassName(4));

        assertEquals("data", meta.getColumnName(5));
        assertEquals(Types.VARBINARY, meta.getColumnType(5));
        assertEquals("BYTES", meta.getColumnTypeName(5));
        assertEquals(byte[].class.getName(), meta.getColumnClassName(5));
    }

    @Test(expected = SQLException.class)
    public void testResultSetMetaDataInvalidIndex() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("x", PWireCodec.TYPE_I64)
        };
        new PyroResultSetMetaData(cols).getColumnName(0);
    }

    @Test(expected = SQLException.class)
    public void testResultSetMetaDataIndexTooHigh() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("x", PWireCodec.TYPE_I64)
        };
        new PyroResultSetMetaData(cols).getColumnName(2);
    }

    // ---- ResultSet tests (in-memory, no server) ----

    @Test
    public void testResultSetNavigation() throws SQLException {
        PWireCodec.ResultSet data = buildSimpleResultSet(3);
        PyroResultSet rs = new PyroResultSet(null, data);

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());

        assertTrue(rs.next());
        assertTrue(rs.isFirst());
        assertEquals(1, rs.getRow());

        assertTrue(rs.next());
        assertEquals(2, rs.getRow());

        assertTrue(rs.next());
        assertTrue(rs.isLast());
        assertEquals(3, rs.getRow());

        assertFalse(rs.next());
        assertTrue(rs.isAfterLast());

        // Scroll back
        assertTrue(rs.first());
        assertEquals(1, rs.getRow());

        assertTrue(rs.last());
        assertEquals(3, rs.getRow());

        assertTrue(rs.absolute(2));
        assertEquals(2, rs.getRow());

        assertTrue(rs.previous());
        assertEquals(1, rs.getRow());

        assertFalse(rs.previous());
        assertTrue(rs.isBeforeFirst());
    }

    @Test
    public void testResultSetGetters() throws SQLException {
        // Build a result set with various types
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("i", PWireCodec.TYPE_I64),
            new PWireCodec.ColumnDef("f", PWireCodec.TYPE_F64),
            new PWireCodec.ColumnDef("s", PWireCodec.TYPE_TEXT),
            new PWireCodec.ColumnDef("b", PWireCodec.TYPE_BOOL),
        };
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{100L, 2.718, "hello", true});
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, rows);
        PyroResultSet rs = new PyroResultSet(null, data);

        assertTrue(rs.next());

        assertEquals(100L, rs.getLong(1));
        assertEquals(100, rs.getInt(1));
        assertEquals((short) 100, rs.getShort(1));
        assertEquals((byte) 100, rs.getByte(1));
        assertEquals("100", rs.getString(1));
        assertEquals(100.0f, rs.getFloat(1), 0.01);
        assertEquals(100.0, rs.getDouble(1), 0.01);
        assertEquals(new BigDecimal(100), rs.getBigDecimal(1));
        assertFalse(rs.wasNull());

        assertEquals(2.718, rs.getDouble(2), 0.001);
        assertEquals(2.718f, rs.getFloat(2), 0.01);

        assertEquals("hello", rs.getString(3));
        assertEquals("hello", rs.getString("s"));
        assertFalse(rs.wasNull());

        assertTrue(rs.getBoolean(4));
        assertEquals(1, rs.getInt(4));
        assertTrue(rs.getBoolean("b"));
    }

    @Test
    public void testResultSetNullHandling() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("val", PWireCodec.TYPE_TEXT),
            new PWireCodec.ColumnDef("num", PWireCodec.TYPE_I64),
        };
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{null, null});
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, rows);
        PyroResultSet rs = new PyroResultSet(null, data);

        assertTrue(rs.next());

        assertNull(rs.getString(1));
        assertTrue(rs.wasNull());

        assertNull(rs.getObject(1));
        assertTrue(rs.wasNull());

        assertEquals(0L, rs.getLong(2));
        assertTrue(rs.wasNull());

        assertEquals(0, rs.getInt(2));
        assertTrue(rs.wasNull());

        assertEquals(0.0, rs.getDouble(2), 0.0);
        assertTrue(rs.wasNull());

        assertFalse(rs.getBoolean(2));
        assertTrue(rs.wasNull());

        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
    }

    @Test
    public void testResultSetFindColumn() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("Alpha", PWireCodec.TYPE_TEXT),
            new PWireCodec.ColumnDef("Beta", PWireCodec.TYPE_I64),
        };
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, Collections.emptyList());
        PyroResultSet rs = new PyroResultSet(null, data);

        assertEquals(1, rs.findColumn("Alpha"));
        assertEquals(1, rs.findColumn("alpha")); // case insensitive
        assertEquals(2, rs.findColumn("BETA"));
    }

    @Test(expected = SQLException.class)
    public void testResultSetFindColumnNotFound() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("x", PWireCodec.TYPE_TEXT),
        };
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, Collections.emptyList());
        new PyroResultSet(null, data).findColumn("notexist");
    }

    @Test
    public void testResultSetClosed() throws SQLException {
        PWireCodec.ResultSet data = buildSimpleResultSet(1);
        PyroResultSet rs = new PyroResultSet(null, data);
        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());
        try {
            rs.next();
            fail("Should throw on closed ResultSet");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testResultSetGetObjectByType() throws SQLException {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("v", PWireCodec.TYPE_I64),
        };
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{99L});
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, rows);
        PyroResultSet rs = new PyroResultSet(null, data);
        rs.next();

        assertEquals(Long.valueOf(99), rs.getObject(1, Long.class));
        assertEquals("99", rs.getObject(1, String.class));
        assertEquals(Integer.valueOf(99), rs.getObject(1, Integer.class));
        assertEquals(Double.valueOf(99.0), rs.getObject(1, Double.class));
    }

    @Test
    public void testResultSetBytesColumn() throws SQLException {
        byte[] expected = new byte[]{0x0A, 0x0B, 0x0C};
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("bin", PWireCodec.TYPE_BYTES),
        };
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{expected});
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, rows);
        PyroResultSet rs = new PyroResultSet(null, data);
        rs.next();
        assertArrayEquals(expected, rs.getBytes(1));
        assertArrayEquals(expected, rs.getBytes("bin"));
    }

    @Test
    public void testResultSetStreams() throws Exception {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("s", PWireCodec.TYPE_TEXT),
        };
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"streaming"});
        PWireCodec.ResultSet data = new PWireCodec.ResultSet(cols, rows);
        PyroResultSet rs = new PyroResultSet(null, data);
        rs.next();

        InputStream ais = rs.getAsciiStream(1);
        assertNotNull(ais);
        byte[] buf = new byte[100];
        int n = ais.read(buf);
        assertEquals("streaming", new String(buf, 0, n, StandardCharsets.UTF_8));

        java.io.Reader reader = rs.getCharacterStream(1);
        assertNotNull(reader);
        char[] cbuf = new char[100];
        int cn = reader.read(cbuf);
        assertEquals("streaming", new String(cbuf, 0, cn));
    }

    // ---- Driver URL parsing tests ----

    @Test
    public void testDriverAcceptsUrl() throws SQLException {
        PyroDriver driver = new PyroDriver();
        assertTrue(driver.acceptsURL("jdbc:pyrosql://localhost:12520/mydb"));
        assertTrue(driver.acceptsURL("jdbc:pyrosql://host/db"));
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost/db"));
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    public void testDriverVersionInfo() {
        PyroDriver driver = new PyroDriver();
        assertEquals(1, driver.getMajorVersion());
        assertEquals(0, driver.getMinorVersion());
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    public void testDriverPropertyInfo() throws SQLException {
        PyroDriver driver = new PyroDriver();
        DriverPropertyInfo[] props = driver.getPropertyInfo("jdbc:pyrosql://localhost/db", new Properties());
        assertTrue(props.length >= 2);
        assertEquals("user", props[0].name);
        assertEquals("password", props[1].name);
    }

    @Test
    public void testDriverRejectsNonPyroUrl() throws SQLException {
        PyroDriver driver = new PyroDriver();
        assertNull(driver.connect("jdbc:mysql://localhost/test", new Properties()));
    }

    // ---- Integration tests with mock server ----

    @Test
    public void testConnectAndAuth() throws Exception {
        startMockServer((in, out) -> {
            // Expect AUTH message
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_AUTH, frame.type);
            // Send READY response
            sendFrame(out, PWireCodec.RESP_READY, new byte[0]);
            // Expect USE database query
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            String sql = new String(frame.payload, StandardCharsets.UTF_8);
            assertEquals("USE testdb", sql);
            sendOk(out, 0, "OK");
            // Expect QUIT
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUIT, frame.type);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        serverThread.join(5000);
    }

    @Test
    public void testQueryExecution() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // Expect QUERY
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("SELECT id, name FROM users", new String(frame.payload, StandardCharsets.UTF_8));

            // Send result set: 2 cols, 1 row
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeLE16(bos, 2);
            bos.write(2); bos.write("id".getBytes(), 0, 2); bos.write(PWireCodec.TYPE_I64);
            bos.write(4); bos.write("name".getBytes(), 0, 4); bos.write(PWireCodec.TYPE_TEXT);
            writeLE32(bos, 1);
            bos.write(0); // null bitmap
            writeLE64(bos, 1L);
            writeLE16(bos, 5); bos.write("Alice".getBytes(), 0, 5);
            sendFrame(out, PWireCodec.RESP_RESULT_SET, bos.toByteArray());

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, name FROM users");

        assertTrue(rs.next());
        assertEquals(1L, rs.getLong("id"));
        assertEquals("Alice", rs.getString("name"));
        assertFalse(rs.next());

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("id", meta.getColumnName(1));
        assertEquals("name", meta.getColumnName(2));

        rs.close();
        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            sendOk(out, 3, "DELETE");

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        Statement stmt = conn.createStatement();
        int affected = stmt.executeUpdate("DELETE FROM users WHERE active = false");
        assertEquals(3, affected);

        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testPreparedStatement() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // PREPARE: receive MSG_PREPARE, respond with RESP_READY(handle=1)
            handlePrepare(in, out, 1);

            // EXECUTE: receive MSG_EXECUTE with handle + params
            readExecute(in);
            sendOk(out, 1, "INSERT");

            // CLOSE: receive MSG_CLOSE
            handleClose(in, out);

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t(a,b) VALUES(?,?)");
        ps.setString(1, "hello");
        ps.setInt(2, 42);
        int affected = ps.executeUpdate();
        assertEquals(1, affected);

        ps.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testPreparedStatementQuery() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // PREPARE
            handlePrepare(in, out, 1);

            // EXECUTE — respond with result set
            readExecute(in);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeLE16(bos, 1);
            bos.write(1); bos.write("v".getBytes(), 0, 1); bos.write(PWireCodec.TYPE_TEXT);
            writeLE32(bos, 1);
            bos.write(0);
            writeLE16(bos, 5); bos.write("world".getBytes(), 0, 5);
            sendFrame(out, PWireCodec.RESP_RESULT_SET, bos.toByteArray());

            // CLOSE
            handleClose(in, out);

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        PreparedStatement ps = conn.prepareStatement("SELECT v FROM t WHERE id = ?");
        ps.setInt(1, 7);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("world", rs.getString(1));
        assertFalse(rs.next());

        ps.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testTransactions() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // setAutoCommit(false) -> BEGIN
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("BEGIN", new String(frame.payload, StandardCharsets.UTF_8));
            sendOk(out, 0, "OK");

            // query
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            sendOk(out, 1, "INSERT");

            // commit -> COMMIT
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("COMMIT", new String(frame.payload, StandardCharsets.UTF_8));
            sendOk(out, 0, "OK");

            // new BEGIN after commit
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("BEGIN", new String(frame.payload, StandardCharsets.UTF_8));
            sendOk(out, 0, "OK");

            // another statement
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            sendOk(out, 1, "INSERT");

            // rollback -> ROLLBACK
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("ROLLBACK", new String(frame.payload, StandardCharsets.UTF_8));
            sendOk(out, 0, "OK");

            // new BEGIN after rollback
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("BEGIN", new String(frame.payload, StandardCharsets.UTF_8));
            sendOk(out, 0, "OK");

            // setAutoCommit(true) -> COMMIT
            frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            assertEquals("COMMIT", new String(frame.payload, StandardCharsets.UTF_8));
            sendOk(out, 0, "OK");

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        assertTrue(conn.getAutoCommit());

        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());

        Statement stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO t(x) VALUES(1)");
        conn.commit();

        stmt.executeUpdate("INSERT INTO t(x) VALUES(2)");
        conn.rollback();

        conn.setAutoCommit(true);

        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testErrorHandling() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUERY, frame.type);
            // Send error
            sendError(out, "42S02", "Table 'users' not found");

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        Statement stmt = conn.createStatement();

        try {
            stmt.executeQuery("SELECT * FROM users");
            fail("Expected SQLException");
        } catch (SQLException e) {
            assertEquals("42S02", e.getSQLState());
            assertTrue(e.getMessage().contains("Table 'users' not found"));
        }

        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testAuthFailure() throws Exception {
        startMockServer((in, out) -> {
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_AUTH, frame.type);
            sendError(out, "28000", "Invalid credentials");
        });

        try {
            DriverManager.getConnection(
                    "jdbc:pyrosql://localhost:" + port + "/testdb", "bad", "cred");
            fail("Expected SQLException");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Invalid credentials"));
        }
        serverThread.join(5000);
    }

    @Test
    public void testBatchOperations() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // Batch: 3 statements
            for (int i = 0; i < 3; i++) {
                PWireCodec.Frame frame = readFrameFromStream(in);
                assertEquals(PWireCodec.MSG_QUERY, frame.type);
                sendOk(out, 1, "INSERT");
            }

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        Statement stmt = conn.createStatement();
        stmt.addBatch("INSERT INTO t VALUES(1)");
        stmt.addBatch("INSERT INTO t VALUES(2)");
        stmt.addBatch("INSERT INTO t VALUES(3)");
        int[] results = stmt.executeBatch();
        assertEquals(3, results.length);
        assertEquals(1, results[0]);
        assertEquals(1, results[1]);
        assertEquals(1, results[2]);

        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testPreparedStatementBatch() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // PREPARE (one prepare, reused for batch)
            handlePrepare(in, out, 1);

            // 2 EXECUTE calls (one per batch entry)
            for (int i = 0; i < 2; i++) {
                readExecute(in);
                sendOk(out, 1, "INSERT");
            }

            // CLOSE
            handleClose(in, out);

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t(name) VALUES(?)");
        ps.setString(1, "Alice");
        ps.addBatch();
        ps.setString(1, "Bob");
        ps.addBatch();
        int[] results = ps.executeBatch();
        assertEquals(2, results.length);
        assertEquals(1, results[0]);
        assertEquals(1, results[1]);

        ps.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testConnectionIsValid() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // PING
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_PING, frame.type);
            sendFrame(out, PWireCodec.RESP_PONG, new byte[0]);

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        assertTrue(conn.isValid(5));
        conn.close();
        assertFalse(conn.isValid(5));
        serverThread.join(5000);
    }

    @Test
    public void testStatementExecuteReturnsCorrectFlag() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // First execute() -> result set
            PWireCodec.Frame frame = readFrameFromStream(in);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeLE16(bos, 1);
            bos.write(1); bos.write("x".getBytes(), 0, 1); bos.write(PWireCodec.TYPE_I64);
            writeLE32(bos, 0);
            sendFrame(out, PWireCodec.RESP_RESULT_SET, bos.toByteArray());

            // Second execute() -> OK
            frame = readFrameFromStream(in);
            sendOk(out, 0, "OK");

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        Statement stmt = conn.createStatement();

        assertTrue(stmt.execute("SELECT 1"));
        assertNotNull(stmt.getResultSet());
        assertEquals(-1, stmt.getUpdateCount());

        assertFalse(stmt.execute("CREATE TABLE t(id INT)"));
        assertNull(stmt.getResultSet());
        assertEquals(0, stmt.getUpdateCount());

        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testNullBitmapMultipleBytes() {
        // Test with 9+ columns to exercise multi-byte null bitmaps
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        writeLE16(bos, 10); // 10 columns

        for (int i = 0; i < 10; i++) {
            String name = "c" + i;
            bos.write(name.length());
            bos.write(name.getBytes(StandardCharsets.UTF_8), 0, name.length());
            bos.write(PWireCodec.TYPE_I64);
        }

        writeLE32(bos, 1); // 1 row

        // Null bitmap: 2 bytes, cols 0,2,4,6,8 are null (odd pattern)
        // bit 0=1, bit 2=1, bit 4=1, bit 6=1 -> 0x55
        // bit 0(col8)=1 -> 0x01
        bos.write(0x55);
        bos.write(0x01);

        // Cols 1,3,5,7,9 have values
        writeLE64(bos, 1L); // col 1
        writeLE64(bos, 3L); // col 3
        writeLE64(bos, 5L); // col 5
        writeLE64(bos, 7L); // col 7
        writeLE64(bos, 9L); // col 9

        PWireCodec.ResultSet rs = PWireCodec.decodeResultSet(bos.toByteArray());
        assertEquals(10, rs.columns.length);
        assertEquals(1, rs.rows.size());
        Object[] row = rs.rows.get(0);
        assertNull(row[0]);
        assertEquals(1L, row[1]);
        assertNull(row[2]);
        assertEquals(3L, row[3]);
        assertNull(row[4]);
        assertEquals(5L, row[5]);
        assertNull(row[6]);
        assertEquals(7L, row[7]);
        assertNull(row[8]);
        assertEquals(9L, row[9]);
    }

    @Test
    public void testDatabaseMetaData() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);
            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        DatabaseMetaData meta = conn.getMetaData();
        assertEquals("PyroSQL", meta.getDatabaseProductName());
        assertEquals("PyroSQL JDBC Driver", meta.getDriverName());
        assertEquals("1.0.0", meta.getDriverVersion());
        assertEquals(1, meta.getDriverMajorVersion());
        assertEquals(0, meta.getDriverMinorVersion());
        assertTrue(meta.supportsTransactions());
        assertTrue(meta.supportsBatchUpdates());
        assertTrue(meta.supportsSavepoints());
        assertEquals(4, meta.getJDBCMajorVersion());

        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testPreparedStatementParameterTypes() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // PREPARE
            handlePrepare(in, out, 1);

            // EXECUTE
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_EXECUTE, frame.type);
            // Verify the params are encoded properly
            ByteBuffer buf = ByteBuffer.wrap(frame.payload).order(ByteOrder.LITTLE_ENDIAN);
            int handle = buf.getInt();
            assertEquals(1, handle);
            int paramCount = buf.getShort() & 0xFFFF;
            assertEquals(5, paramCount);

            sendOk(out, 1, "INSERT");

            // CLOSE
            handleClose(in, out);

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t VALUES(?,?,?,?,?)");
        ps.setLong(1, 9999999999L);
        ps.setDouble(2, 3.14159);
        ps.setString(3, "utf8text");
        ps.setBoolean(4, true);
        ps.setBigDecimal(5, new BigDecimal("123.456"));
        assertEquals(1, ps.executeUpdate());

        ps.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testPreparedStatementClearParameters() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            // PREPARE
            handlePrepare(in, out, 1);

            // First EXECUTE
            readExecute(in);
            sendOk(out, 1, "INSERT");

            // Second EXECUTE
            readExecute(in);
            sendOk(out, 1, "INSERT");

            // CLOSE
            handleClose(in, out);

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t(a) VALUES(?)");
        ps.setString(1, "first");
        ps.executeUpdate();

        ps.clearParameters();
        ps.setString(1, "second");
        ps.executeUpdate();

        ps.close();
        conn.close();
        serverThread.join(5000);
    }

    @Test
    public void testResultSetMetaDataFromQuery() throws Exception {
        startMockServer((in, out) -> {
            handleAuth(in, out);
            handleUseDb(in, out);

            readFrameFromStream(in);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeLE16(bos, 3);
            bos.write(2); bos.write("id".getBytes(), 0, 2); bos.write(PWireCodec.TYPE_I64);
            bos.write(4); bos.write("name".getBytes(), 0, 4); bos.write(PWireCodec.TYPE_TEXT);
            bos.write(5); bos.write("score".getBytes(), 0, 5); bos.write(PWireCodec.TYPE_F64);
            writeLE32(bos, 0);
            sendFrame(out, PWireCodec.RESP_RESULT_SET, bos.toByteArray());

            handleQuit(in);
        });

        Connection conn = DriverManager.getConnection(
                "jdbc:pyrosql://localhost:" + port + "/testdb", "user", "pass");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, name, score FROM t");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
        assertEquals("id", meta.getColumnName(1));
        assertEquals(Types.BIGINT, meta.getColumnType(1));
        assertEquals("name", meta.getColumnName(2));
        assertEquals(Types.VARCHAR, meta.getColumnType(2));
        assertEquals("score", meta.getColumnName(3));
        assertEquals(Types.DOUBLE, meta.getColumnType(3));

        rs.close();
        stmt.close();
        conn.close();
        serverThread.join(5000);
    }

    // ---- Helper methods ----

    private PWireCodec.ResultSet buildSimpleResultSet(int rowCount) {
        PWireCodec.ColumnDef[] cols = new PWireCodec.ColumnDef[]{
            new PWireCodec.ColumnDef("val", PWireCodec.TYPE_I64),
        };
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(new Object[]{(long) (i + 1)});
        }
        return new PWireCodec.ResultSet(cols, rows);
    }

    @FunctionalInterface
    interface MockServerHandler {
        void handle(InputStream in, OutputStream out) throws Exception;
    }

    private void startMockServer(MockServerHandler handler) throws Exception {
        serverSocket = new ServerSocket(0);
        port = serverSocket.getLocalPort();
        serverThread = new Thread(() -> {
            try (Socket client = serverSocket.accept()) {
                client.setSoTimeout(10000);
                handler.handle(client.getInputStream(), client.getOutputStream());
            } catch (Exception e) {
                // test will fail if assertions trip here
                e.printStackTrace();
            } finally {
                try { serverSocket.close(); } catch (IOException ignored) {}
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

    private PWireCodec.Frame readFrameFromStream(InputStream in) throws IOException {
        // Read header
        byte[] header = new byte[PWireCodec.HEADER_SIZE];
        readFully(in, header);
        ByteBuffer hdr = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        int type = hdr.get() & 0xFF;
        int length = hdr.getInt();
        byte[] payload = new byte[length];
        if (length > 0) readFully(in, payload);
        return new PWireCodec.Frame(type, payload, PWireCodec.HEADER_SIZE + length);
    }

    private void readFully(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n < 0) throw new IOException("Unexpected EOF");
            off += n;
        }
    }

    private void sendFrame(OutputStream out, int type, byte[] payload) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(PWireCodec.HEADER_SIZE + payload.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) (type & 0xFF));
        buf.putInt(payload.length);
        buf.put(payload);
        out.write(buf.array());
        out.flush();
    }

    private void sendOk(OutputStream out, long rowsAffected, String tag) throws IOException {
        byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload = ByteBuffer.allocate(9 + tagBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        payload.putLong(rowsAffected);
        payload.put((byte) tagBytes.length);
        payload.put(tagBytes);
        sendFrame(out, PWireCodec.RESP_OK, payload.array());
    }

    private void sendError(OutputStream out, String sqlState, String message) throws IOException {
        byte[] stateBytes = sqlState.getBytes(StandardCharsets.UTF_8);
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(stateBytes, 0, 5);
        writeLE16(bos, msgBytes.length);
        bos.write(msgBytes);
        sendFrame(out, PWireCodec.RESP_ERROR, bos.toByteArray());
    }

    private void handleAuth(InputStream in, OutputStream out) throws IOException {
        PWireCodec.Frame frame = readFrameFromStream(in);
        assertEquals(PWireCodec.MSG_AUTH, frame.type);
        // Send RESP_READY with 4-byte server caps (no LZ4 for mock tests)
        byte[] caps = new byte[4];
        sendFrame(out, PWireCodec.RESP_READY, caps);
    }

    /**
     * Handle a PREPARE request: read MSG_PREPARE, respond with RESP_READY containing a handle.
     */
    private int handlePrepare(InputStream in, OutputStream out, int handle) throws IOException {
        PWireCodec.Frame frame = readFrameFromStream(in);
        assertEquals(PWireCodec.MSG_PREPARE, frame.type);
        byte[] readyPayload = new byte[4];
        ByteBuffer.wrap(readyPayload).order(ByteOrder.LITTLE_ENDIAN).putInt(handle);
        sendFrame(out, PWireCodec.RESP_READY, readyPayload);
        return handle;
    }

    /**
     * Read a MSG_EXECUTE frame and return the params.
     * Does NOT send a response.
     */
    private PWireCodec.Frame readExecute(InputStream in) throws IOException {
        PWireCodec.Frame frame = readFrameFromStream(in);
        assertEquals(PWireCodec.MSG_EXECUTE, frame.type);
        return frame;
    }

    /**
     * Handle a CLOSE request: read MSG_CLOSE, respond with RESP_OK.
     */
    private void handleClose(InputStream in, OutputStream out) throws IOException {
        PWireCodec.Frame frame = readFrameFromStream(in);
        assertEquals(PWireCodec.MSG_CLOSE, frame.type);
        sendOk(out, 0, "OK");
    }

    private void handleUseDb(InputStream in, OutputStream out) throws IOException {
        PWireCodec.Frame frame = readFrameFromStream(in);
        assertEquals(PWireCodec.MSG_QUERY, frame.type);
        sendOk(out, 0, "OK");
    }

    private void handleQuit(InputStream in) throws IOException {
        try {
            PWireCodec.Frame frame = readFrameFromStream(in);
            assertEquals(PWireCodec.MSG_QUIT, frame.type);
        } catch (IOException ignored) {
            // Connection may already be closed
        }
    }

    // ---- Binary helpers ----

    private static void writeLE16(ByteArrayOutputStream bos, int value) {
        bos.write(value & 0xFF);
        bos.write((value >> 8) & 0xFF);
    }

    private static void writeLE32(ByteArrayOutputStream bos, int value) {
        bos.write(value & 0xFF);
        bos.write((value >> 8) & 0xFF);
        bos.write((value >> 16) & 0xFF);
        bos.write((value >> 24) & 0xFF);
    }

    private static void writeLE64(ByteArrayOutputStream bos, long value) {
        for (int i = 0; i < 8; i++) {
            bos.write((int) ((value >> (i * 8)) & 0xFF));
        }
    }

    private static void writeLE64Double(ByteArrayOutputStream bos, double value) {
        writeLE64(bos, Double.doubleToLongBits(value));
    }

    // ---- LZ4 compression unit tests ----

    @Test
    public void testCompressFrameSmallPayloadNotCompressed() {
        // Payloads <= 8KB should not be compressed (returned as-is)
        byte[] smallPayload = new byte[100];
        Arrays.fill(smallPayload, (byte) 'A');
        byte[] frame = PWireCodec.compressFrame(PWireCodec.MSG_QUERY, smallPayload);
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        int type = buf.get() & 0xFF;
        assertEquals("Small payload should not be compressed", PWireCodec.MSG_QUERY, type);
    }

    @Test
    public void testCompressFrameLargePayloadCompressed() {
        // Payloads > 8KB with good ratio should be compressed
        byte[] largePayload = new byte[16 * 1024];
        Arrays.fill(largePayload, (byte) 'X'); // Highly compressible
        byte[] frame = PWireCodec.compressFrame(PWireCodec.MSG_QUERY, largePayload);
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        int type = buf.get() & 0xFF;
        assertEquals("Large compressible payload should use MSG_COMPRESSED",
                PWireCodec.MSG_COMPRESSED, type);
        // Compressed frame should be smaller than the original
        assertTrue("Compressed frame should be smaller", frame.length < largePayload.length);
    }

    @Test
    public void testCompressDecompressRoundTrip() {
        // Build a large compressible payload
        byte[] original = new byte[16 * 1024];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) (i % 26 + 'a');
        }
        byte[] frame = PWireCodec.compressFrame(PWireCodec.MSG_QUERY, original);
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        int frameType = buf.get() & 0xFF;
        int payloadLen = buf.getInt();
        byte[] payload = new byte[payloadLen];
        buf.get(payload);

        assertEquals("Should be MSG_COMPRESSED", PWireCodec.MSG_COMPRESSED, frameType);

        PWireCodec.Frame decompressed = PWireCodec.decompressFrame(payload);
        assertEquals("Original type should be MSG_QUERY", PWireCodec.MSG_QUERY, decompressed.type);
        assertArrayEquals("Decompressed data should match original", original, decompressed.payload);
    }

    @Test
    public void testEncodeAuthWithCaps() {
        byte[] frame = PWireCodec.encodeAuthWithCaps("admin", "secret", PWireCodec.CAP_LZ4);
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(PWireCodec.MSG_AUTH, buf.get() & 0xFF);
        int len = buf.getInt();
        // user(1+5) + pass(1+6) + caps(1) = 14
        assertEquals(14, len);
        assertEquals(5, buf.get() & 0xFF); // user len
        byte[] user = new byte[5];
        buf.get(user);
        assertEquals("admin", new String(user, StandardCharsets.UTF_8));
        assertEquals(6, buf.get() & 0xFF); // pass len
        byte[] pass = new byte[6];
        buf.get(pass);
        assertEquals("secret", new String(pass, StandardCharsets.UTF_8));
        assertEquals(PWireCodec.CAP_LZ4, buf.get()); // caps byte
    }
}
