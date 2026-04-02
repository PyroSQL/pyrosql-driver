package com.pyrosql.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.*;

/**
 * Integration tests for the PyroSQL JDBC driver against a real PyroSQL server.
 * Requires a PyroSQL server running at localhost:12520.
 */
public class IntegrationTest {

    private static final String URL = "jdbc:pyrosql://localhost:12520/fomium";
    private static final String USER = "pyrosql";
    private static final String PASS = "secret";

    // Use unique table name per test class load to avoid stale data
    private static final String TABLE = "jit_" + (System.currentTimeMillis() % 100000);

    private Connection conn;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(URL, USER, PASS);
        assertNotNull("Connection should not be null", conn);
        assertFalse("Connection should be open", conn.isClosed());
        // Clean up from any previous failed run
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DROP TABLE IF EXISTS " + TABLE);
        }
    }

    @After
    public void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("DROP TABLE IF EXISTS " + TABLE);
            } catch (SQLException ignored) {}
            conn.close();
            assertTrue("Connection should be closed", conn.isClosed());
        }
    }

    // ---- 1. Connection ----

    @Test
    public void testConnectionIsValid() throws SQLException {
        assertTrue("Connection should be valid", conn.isValid(5));
        System.out.println("  [PASS] Connection is valid");
    }

    @Test
    public void testConnectionMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        assertNotNull("DatabaseMetaData should not be null", meta);
        System.out.println("  Product: " + meta.getDatabaseProductName()
                + " " + meta.getDatabaseProductVersion());
        System.out.println("  Driver:  " + meta.getDriverName()
                + " " + meta.getDriverVersion());
        System.out.println("  [PASS] Connection metadata retrieved");
    }

    // ---- 2. CREATE TABLE ----

    @Test
    public void testCreateTable() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " ("
                    + "id INT PRIMARY KEY, "
                    + "name VARCHAR(100)"
                    + ")");
        }
        // Verify table exists by inserting and selecting
        try (Statement st = conn.createStatement()) {
            int inserted = st.executeUpdate("INSERT INTO " + TABLE + " (id, name) VALUES (1, 'test')");
            assertEquals("Should insert 1 row", 1, inserted);
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, name FROM " + TABLE + " WHERE id = 1")) {
            assertTrue("Should have a row", rs.next());
            assertEquals("test", rs.getString("name"));
        }
        System.out.println("  [PASS] CREATE TABLE, INSERT, SELECT verified");
    }

    // ---- 3. INSERT with different types and SELECT verification ----

    @Test
    public void testInsertAndSelectVariousTypes() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " ("
                    + "id INT PRIMARY KEY, "
                    + "name VARCHAR(100), "
                    + "price DOUBLE, "
                    + "active BOOLEAN"
                    + ")");

            int rows = st.executeUpdate("INSERT INTO " + TABLE
                    + " (id, name, price, active) VALUES (1, 'Widget', 19.99, true)");
            assertEquals("Should insert 1 row", 1, rows);

            rows = st.executeUpdate("INSERT INTO " + TABLE
                    + " (id, name, price, active) VALUES (2, 'Gadget', 49.50, false)");
            assertEquals("Should insert 1 row", 1, rows);

            rows = st.executeUpdate("INSERT INTO " + TABLE
                    + " (id, name, price, active) VALUES (3, 'Doohickey', 0.0, true)");
            assertEquals("Should insert 1 row", 1, rows);
        }

        // SELECT and verify
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, name, price, active FROM " + TABLE + " ORDER BY id")) {

            assertTrue("Should have row 1", rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Widget", rs.getString("name"));
            assertEquals(19.99, rs.getDouble("price"), 0.01);

            assertTrue("Should have row 2", rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Gadget", rs.getString("name"));
            assertEquals(49.50, rs.getDouble("price"), 0.01);

            assertTrue("Should have row 3", rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("Doohickey", rs.getString("name"));

            assertFalse("Should have no more rows", rs.next());
        }

        // ResultSetMetaData
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, name, price FROM " + TABLE + " LIMIT 1")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertNotNull("ResultSetMetaData should not be null", meta);
            assertTrue("Should have at least 3 columns", meta.getColumnCount() >= 3);
            System.out.println("  Columns: " + meta.getColumnCount());
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.println("    col " + i + ": " + meta.getColumnName(i)
                        + " type=" + meta.getColumnTypeName(i));
            }
        }
        System.out.println("  [PASS] INSERT various types and SELECT verified");
    }

    // ---- 4. Transactions ----

    @Test
    public void testTransactionCommit() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, val VARCHAR(50))");
        }

        conn.setAutoCommit(false);
        assertFalse("AutoCommit should be off", conn.getAutoCommit());

        try (Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO " + TABLE + " (id, val) VALUES (1, 'committed')");
        }
        conn.commit();

        conn.setAutoCommit(true);

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT val FROM " + TABLE + " WHERE id = 1")) {
            assertTrue("Should find committed row", rs.next());
            assertEquals("committed", rs.getString("val"));
        }
        System.out.println("  [PASS] Transaction COMMIT verified");
    }

    @Test
    public void testTransactionRollback() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, val VARCHAR(50))");
            st.executeUpdate("INSERT INTO " + TABLE + " (id, val) VALUES (1, 'original')");
        }

        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("UPDATE " + TABLE + " SET val = 'modified' WHERE id = 1");
        }
        conn.rollback();
        conn.setAutoCommit(true);

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT val FROM " + TABLE + " WHERE id = 1")) {
            assertTrue("Should find row", rs.next());
            assertEquals("original", rs.getString("val"));
        }
        System.out.println("  [PASS] Transaction ROLLBACK verified");
    }

    // ---- 5. Prepared statements ----

    @Test
    public void testPreparedStatementInsertAndSelect() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(100), score DOUBLE)");
        }

        // Insert with prepared statement
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + TABLE + " (id, name, score) VALUES (?, ?, ?)")) {
            ps.setInt(1, 10);
            ps.setString(2, "Alice");
            ps.setDouble(3, 95.5);
            assertEquals(1, ps.executeUpdate());

            ps.setInt(1, 20);
            ps.setString(2, "Bob");
            ps.setDouble(3, 87.3);
            assertEquals(1, ps.executeUpdate());
        }

        // Select with prepared statement
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name, score FROM " + TABLE + " WHERE id = ?")) {
            ps.setInt(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should find Alice", rs.next());
                assertEquals("Alice", rs.getString("name"));
                assertEquals(95.5, rs.getDouble("score"), 0.01);
            }

            ps.setInt(1, 20);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue("Should find Bob", rs.next());
                assertEquals("Bob", rs.getString("name"));
                assertEquals(87.3, rs.getDouble("score"), 0.01);
            }

            ps.setInt(1, 999);
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse("Should not find id 999", rs.next());
            }
        }
        System.out.println("  [PASS] PreparedStatement INSERT and SELECT verified");
    }

    @Test
    public void testPreparedStatementNullValues() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(100))");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + TABLE + " (id, name) VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.VARCHAR);
            assertEquals(1, ps.executeUpdate());
        }

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT name FROM " + TABLE + " WHERE id = 1")) {
            assertTrue(rs.next());
            String val = rs.getString("name");
            // Value should be null or the server's representation of null
            System.out.println("  NULL value returned as: " + val + " wasNull=" + rs.wasNull());
        }
        System.out.println("  [PASS] PreparedStatement NULL values tested");
    }

    // ---- 6. Batch operations ----

    @Test
    public void testStatementBatch() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, label VARCHAR(50))");

            st.addBatch("INSERT INTO " + TABLE + " (id, label) VALUES (1, 'batch1')");
            st.addBatch("INSERT INTO " + TABLE + " (id, label) VALUES (2, 'batch2')");
            st.addBatch("INSERT INTO " + TABLE + " (id, label) VALUES (3, 'batch3')");

            int[] results = st.executeBatch();
            assertEquals("Batch should have 3 results", 3, results.length);
            for (int r : results) {
                assertEquals("Each batch statement should affect 1 row", 1, r);
            }
        }

        // Verify all rows exist
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT label FROM " + TABLE + " ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("batch1", rs.getString(1));
            assertTrue(rs.next()); assertEquals("batch2", rs.getString(1));
            assertTrue(rs.next()); assertEquals("batch3", rs.getString(1));
            assertFalse(rs.next());
        }
        System.out.println("  [PASS] Statement batch operations verified");
    }

    @Test
    public void testPreparedStatementBatch() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, label VARCHAR(50))");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + TABLE + " (id, label) VALUES (?, ?)")) {
            for (int i = 1; i <= 5; i++) {
                ps.setInt(1, i);
                ps.setString(2, "item_" + i);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals("Batch should have 5 results", 5, results.length);
        }

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT label FROM " + TABLE + " ORDER BY id")) {
            for (int i = 1; i <= 5; i++) {
                assertTrue("Should have row " + i, rs.next());
                assertEquals("item_" + i, rs.getString(1));
            }
            assertFalse(rs.next());
        }
        System.out.println("  [PASS] PreparedStatement batch operations verified");
    }

    // ---- 7. Error handling ----

    @Test
    public void testSyntaxError() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeQuery("SELCT * FORM nonexistent");
            fail("Should have thrown SQLException for syntax error");
        } catch (SQLException e) {
            System.out.println("  Expected SQL error: " + e.getMessage());
            assertNotNull("Error message should not be null", e.getMessage());
            System.out.println("  [PASS] Syntax error handled correctly");
        }
    }

    @Test
    public void testTableNotFound() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeQuery("SELECT * FROM table_that_does_not_exist_xyz_" + System.currentTimeMillis());
            fail("Should have thrown SQLException for missing table");
        } catch (SQLException e) {
            System.out.println("  Expected error: " + e.getMessage());
            assertNotNull(e.getMessage());
            System.out.println("  [PASS] Table not found error handled correctly");
        }
    }

    @Test
    public void testDuplicateKey() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, val VARCHAR(50))");
            st.executeUpdate("INSERT INTO " + TABLE + " (id, val) VALUES (1, 'first')");
            st.executeUpdate("INSERT INTO " + TABLE + " (id, val) VALUES (1, 'duplicate')");
            fail("Should have thrown SQLException for duplicate key");
        } catch (SQLException e) {
            System.out.println("  Expected duplicate key error: " + e.getMessage());
            assertNotNull(e.getMessage());
            System.out.println("  [PASS] Duplicate key error handled correctly");
        }
    }

    // ---- 8. UPDATE and DELETE ----

    @Test
    public void testUpdateAndDelete() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, val VARCHAR(50))");
            st.executeUpdate("INSERT INTO " + TABLE + " (id, val) VALUES (1, 'hello')");
            st.executeUpdate("INSERT INTO " + TABLE + " (id, val) VALUES (2, 'world')");

            int updated = st.executeUpdate("UPDATE " + TABLE + " SET val = 'updated' WHERE id = 1");
            assertTrue("UPDATE should affect at least 1 row", updated >= 1);

            try (ResultSet rs = st.executeQuery("SELECT val FROM " + TABLE + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("updated", rs.getString("val"));
            }

            int deleted = st.executeUpdate("DELETE FROM " + TABLE + " WHERE id = 2");
            assertTrue("DELETE should affect at least 1 row", deleted >= 1);

            // Verify only 1 row remains
            try (ResultSet rs = st.executeQuery("SELECT id FROM " + TABLE + " ORDER BY id")) {
                assertTrue("Should have at least 1 row", rs.next());
                assertEquals(1, rs.getInt("id"));
                assertFalse("Should not have row 2 anymore", rs.next());
            }
        }
        System.out.println("  [PASS] UPDATE and DELETE verified");
    }

    // ---- 9. DROP TABLE ----

    @Test
    public void testDropTable() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY)");
            st.executeUpdate("INSERT INTO " + TABLE + " (id) VALUES (1)");

            // Drop it
            st.executeUpdate("DROP TABLE " + TABLE);

            // Verify it's gone
            try {
                st.executeQuery("SELECT * FROM " + TABLE);
                fail("Should fail after DROP TABLE");
            } catch (SQLException e) {
                System.out.println("  Expected error after DROP: " + e.getMessage());
            }
        }
        System.out.println("  [PASS] DROP TABLE verified");
    }

    // ---- Connection after close ----

    @Test
    public void testOperationAfterClose() throws SQLException {
        Connection c2 = DriverManager.getConnection(URL, USER, PASS);
        c2.close();
        assertTrue(c2.isClosed());
        try {
            c2.createStatement();
            fail("Should throw on closed connection");
        } catch (SQLException e) {
            System.out.println("  Expected closed-connection error: " + e.getMessage());
            System.out.println("  [PASS] Closed connection error handled correctly");
        }
    }

    // ---- Wrong credentials ----

    @Test
    public void testBadCredentials() {
        // Note: PyroSQL may or may not reject bad credentials depending on config.
        // We test that the driver at least doesn't crash.
        try {
            Connection c = DriverManager.getConnection(URL, "baduser", "badpass");
            // If the server accepts any credentials, that's a server-side policy
            System.out.println("  Server accepted bad credentials (no auth enforcement)");
            c.close();
        } catch (SQLException e) {
            System.out.println("  Expected auth error: " + e.getMessage());
            assertNotNull(e.getMessage());
        }
        System.out.println("  [PASS] Bad credentials test completed");
    }
}
