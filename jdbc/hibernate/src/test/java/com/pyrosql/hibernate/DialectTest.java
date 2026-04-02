package com.pyrosql.hibernate;

import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.junit.Before;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;

/**
 * Tests for PyroSQLDialect covering column type mapping, identity support,
 * pagination, boolean literals, sequences, and SQL functions.
 */
public class DialectTest {

    private PyroSQLDialect dialect;

    @Before
    public void setUp() {
        dialect = new PyroSQLDialect();
    }

    // -----------------------------------------------------------------------
    // Column type mapping
    // -----------------------------------------------------------------------

    @Test
    public void testBooleanColumnType() {
        String type = dialect.getTypeName(Types.BOOLEAN);
        assertEquals("BOOLEAN", type);
    }

    @Test
    public void testBitColumnType() {
        String type = dialect.getTypeName(Types.BIT);
        assertEquals("BOOLEAN", type);
    }

    @Test
    public void testSmallintColumnType() {
        String type = dialect.getTypeName(Types.SMALLINT);
        assertEquals("SMALLINT", type);
    }

    @Test
    public void testIntegerColumnType() {
        String type = dialect.getTypeName(Types.INTEGER);
        assertEquals("INTEGER", type);
    }

    @Test
    public void testBigintColumnType() {
        String type = dialect.getTypeName(Types.BIGINT);
        assertEquals("BIGINT", type);
    }

    @Test
    public void testRealColumnType() {
        String type = dialect.getTypeName(Types.REAL);
        assertEquals("REAL", type);
    }

    @Test
    public void testFloatColumnType() {
        String type = dialect.getTypeName(Types.FLOAT);
        assertEquals("REAL", type);
    }

    @Test
    public void testDoubleColumnType() {
        String type = dialect.getTypeName(Types.DOUBLE);
        assertEquals("DOUBLE PRECISION", type);
    }

    @Test
    public void testVarcharColumnType() {
        String type = dialect.getTypeName(Types.VARCHAR, 255, 0, 0);
        assertEquals("VARCHAR(255)", type);
    }

    @Test
    public void testCharColumnType() {
        String type = dialect.getTypeName(Types.CHAR, 10, 0, 0);
        assertEquals("CHAR(10)", type);
    }

    @Test
    public void testTextColumnType() {
        String type = dialect.getTypeName(Types.LONGVARCHAR);
        assertEquals("TEXT", type);
    }

    @Test
    public void testClobColumnType() {
        String type = dialect.getTypeName(Types.CLOB);
        assertEquals("TEXT", type);
    }

    @Test
    public void testBlobColumnType() {
        String type = dialect.getTypeName(Types.BLOB);
        assertEquals("BYTEA", type);
    }

    @Test
    public void testBinaryColumnType() {
        String type = dialect.getTypeName(Types.BINARY);
        assertEquals("BYTEA", type);
    }

    @Test
    public void testVarbinaryColumnType() {
        String type = dialect.getTypeName(Types.VARBINARY);
        assertEquals("BYTEA", type);
    }

    @Test
    public void testTimestampColumnType() {
        String type = dialect.getTypeName(Types.TIMESTAMP);
        assertEquals("TIMESTAMPTZ", type);
    }

    @Test
    public void testDateColumnType() {
        String type = dialect.getTypeName(Types.DATE);
        assertEquals("DATE", type);
    }

    @Test
    public void testTimeColumnType() {
        String type = dialect.getTypeName(Types.TIME);
        assertEquals("TIME", type);
    }

    @Test
    public void testNumericColumnType() {
        String type = dialect.getTypeName(Types.NUMERIC, 0, 10, 2);
        assertEquals("NUMERIC(10,2)", type);
    }

    @Test
    public void testJsonbColumnType() {
        String type = dialect.getTypeName(Types.OTHER);
        assertEquals("JSONB", type);
    }

    @Test
    public void testJavaObjectColumnType() {
        String type = dialect.getTypeName(Types.JAVA_OBJECT);
        assertEquals("JSONB", type);
    }

    // -----------------------------------------------------------------------
    // Identity / SERIAL support
    // -----------------------------------------------------------------------

    @Test
    public void testSupportsIdentityColumns() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertTrue("Should support identity columns", support.supportsIdentityColumns());
    }

    @Test
    public void testIdentityColumnStringSerial() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertEquals("SERIAL", support.getIdentityColumnString(Types.INTEGER));
    }

    @Test
    public void testIdentityColumnStringBigserial() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertEquals("BIGSERIAL", support.getIdentityColumnString(Types.BIGINT));
    }

    @Test
    public void testIdentityColumnStringSmallserial() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertEquals("SMALLSERIAL", support.getIdentityColumnString(Types.SMALLINT));
    }

    @Test
    public void testIdentitySelectString() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        String select = support.getIdentitySelectString("users", "id", Types.BIGINT);
        assertEquals("SELECT CURRVAL('users_id_seq')", select);
    }

    @Test
    public void testIdentityInsertString() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertEquals("DEFAULT", support.getIdentityInsertString());
    }

    @Test
    public void testAppendIdentitySelectToInsert() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        String result = support.appendIdentitySelectToInsert("INSERT INTO users (name) VALUES ('test')");
        assertTrue("Should append RETURNING", result.endsWith("RETURNING *"));
    }

    @Test
    public void testHasDataTypeInIdentityColumn() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertFalse("SERIAL types include data type", support.hasDataTypeInIdentityColumn());
    }

    @Test
    public void testSupportsInsertSelectIdentity() {
        IdentityColumnSupport support = dialect.getIdentityColumnSupport();
        assertTrue("Should support insert select identity", support.supportsInsertSelectIdentity());
    }

    // -----------------------------------------------------------------------
    // Pagination (LIMIT/OFFSET)
    // -----------------------------------------------------------------------

    @Test
    public void testSupportsLimit() {
        assertTrue("Should support LIMIT", dialect.supportsLimit());
    }

    @Test
    public void testSupportsLimitOffset() {
        assertTrue("Should support LIMIT/OFFSET", dialect.supportsLimitOffset());
    }

    @Test
    public void testLimitStringWithoutOffset() {
        String result = dialect.getLimitString("SELECT * FROM users", false);
        assertEquals("SELECT * FROM users LIMIT ?", result);
    }

    @Test
    public void testLimitStringWithOffset() {
        String result = dialect.getLimitString("SELECT * FROM users", true);
        assertEquals("SELECT * FROM users LIMIT ? OFFSET ?", result);
    }

    @Test
    public void testBindLimitParametersOrder() {
        assertFalse("Should not reverse LIMIT parameters", dialect.bindLimitParametersInReverseOrder());
    }

    @Test
    public void testLimitHandler() {
        assertNotNull("LimitHandler should not be null", dialect.getLimitHandler());
        assertTrue("LimitHandler should support limit", dialect.getLimitHandler().supportsLimit());
        assertTrue("LimitHandler should support offset", dialect.getLimitHandler().supportsLimitOffset());
    }

    // -----------------------------------------------------------------------
    // Boolean literals
    // -----------------------------------------------------------------------

    @Test
    public void testBooleanTrue() {
        assertEquals("TRUE", dialect.toBooleanValueString(true));
    }

    @Test
    public void testBooleanFalse() {
        assertEquals("FALSE", dialect.toBooleanValueString(false));
    }

    // -----------------------------------------------------------------------
    // Sequences
    // -----------------------------------------------------------------------

    @Test
    public void testSupportsSequences() {
        assertTrue("Should support sequences", dialect.supportsSequences());
    }

    @Test
    public void testSupportsPooledSequences() {
        assertTrue("Should support pooled sequences", dialect.supportsPooledSequences());
    }

    @Test
    public void testGetSequenceNextValString() {
        String sql = dialect.getSequenceNextValString("my_seq");
        assertEquals("SELECT NEXTVAL('my_seq')", sql);
    }

    @Test
    public void testCreateSequence() {
        String sql = dialect.getCreateSequenceString("my_seq");
        assertEquals("CREATE SEQUENCE my_seq", sql);
    }

    @Test
    public void testDropSequence() {
        String sql = dialect.getDropSequenceString("my_seq");
        assertEquals("DROP SEQUENCE IF EXISTS my_seq", sql);
    }

    @Test
    public void testQuerySequences() {
        String sql = dialect.getQuerySequencesString();
        assertNotNull("Should return query for sequences", sql);
        assertTrue("Should query information_schema", sql.contains("information_schema"));
    }

    // -----------------------------------------------------------------------
    // DDL support
    // -----------------------------------------------------------------------

    @Test
    public void testSupportsIfExistsBeforeTableName() {
        assertTrue(dialect.supportsIfExistsBeforeTableName());
    }

    @Test
    public void testGetAddColumnString() {
        assertEquals("ADD COLUMN", dialect.getAddColumnString());
    }

    @Test
    public void testGetCascadeConstraintsString() {
        assertEquals(" CASCADE", dialect.getCascadeConstraintsString());
    }

    @Test
    public void testSupportsColumnCheck() {
        assertTrue(dialect.supportsColumnCheck());
    }

    @Test
    public void testSupportsTableCheck() {
        assertTrue(dialect.supportsTableCheck());
    }

    @Test
    public void testHasAlterTable() {
        assertTrue(dialect.hasAlterTable());
    }

    @Test
    public void testDropConstraints() {
        assertTrue(dialect.dropConstraints());
    }

    @Test
    public void testQualifyIndexName() {
        assertFalse(dialect.qualifyIndexName());
    }

    // -----------------------------------------------------------------------
    // FOR UPDATE
    // -----------------------------------------------------------------------

    @Test
    public void testForUpdateString() {
        assertEquals(" FOR UPDATE", dialect.getForUpdateString());
    }

    @Test
    public void testForUpdateNowait() {
        assertEquals(" FOR UPDATE NOWAIT", dialect.getForUpdateNowaitString());
    }

    @Test
    public void testForUpdateSkipLocked() {
        assertEquals(" FOR UPDATE SKIP LOCKED", dialect.getForUpdateSkipLockedString());
    }

    // -----------------------------------------------------------------------
    // Miscellaneous
    // -----------------------------------------------------------------------

    @Test
    public void testCurrentTimestampSelection() {
        assertTrue(dialect.supportsCurrentTimestampSelection());
        assertFalse(dialect.isCurrentTimestampSelectStringCallable());
        assertEquals("SELECT CURRENT_TIMESTAMP", dialect.getCurrentTimestampSelectString());
    }

    @Test
    public void testSupportsUnionAll() {
        assertTrue(dialect.supportsUnionAll());
    }

    @Test
    public void testNoColumnsInsertString() {
        assertEquals("DEFAULT VALUES", dialect.getNoColumnsInsertString());
    }

    @Test
    public void testSupportsCommentOn() {
        assertTrue(dialect.supportsCommentOn());
    }

    @Test
    public void testCaseInsensitiveLike() {
        assertTrue(dialect.supportsCaseInsensitiveLike());
        assertEquals("ILIKE", dialect.getCaseInsensitiveLike());
    }

    @Test
    public void testSupportsRowValueConstructor() {
        assertTrue(dialect.supportsRowValueConstructorSyntax());
        assertTrue(dialect.supportsRowValueConstructorSyntaxInInList());
    }

    @Test
    public void testDoesNotSupportEmptyInList() {
        assertFalse(dialect.supportsEmptyInList());
    }

    @Test
    public void testDoesNotSupportOuterJoinForUpdate() {
        assertFalse(dialect.supportsOuterJoinForUpdate());
    }

    @Test
    public void testDoesNotSupportTupleDistinctCounts() {
        assertFalse(dialect.supportsTupleDistinctCounts());
    }

    @Test
    public void testRequiresParensForTupleDistinctCounts() {
        assertTrue(dialect.requiresParensForTupleDistinctCounts());
    }
}
