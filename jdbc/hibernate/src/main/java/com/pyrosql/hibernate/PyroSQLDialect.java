package com.pyrosql.hibernate;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.SQLFunctionTemplate;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.identity.IdentityColumnSupportImpl;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.type.StandardBasicTypes;

import java.sql.Types;

/**
 * Hibernate dialect for the PyroSQL database.
 *
 * Supports PostgreSQL-compatible syntax including:
 * - SERIAL/BIGSERIAL auto-increment columns
 * - LIMIT/OFFSET pagination
 * - RETURNING clause for identity generation
 * - Boolean literals (TRUE/FALSE)
 * - Sequence-based identity generation
 * - JSONB and UUID column types
 * - Standard SQL functions
 *
 * Configuration:
 * <pre>
 * &lt;property name="hibernate.dialect"&gt;com.pyrosql.hibernate.PyroSQLDialect&lt;/property&gt;
 * &lt;property name="hibernate.connection.driver_class"&gt;com.pyrosql.jdbc.PyroDriver&lt;/property&gt;
 * </pre>
 */
public class PyroSQLDialect extends Dialect {

    /** The LIMIT handler for PyroSQL (PostgreSQL-style LIMIT/OFFSET). */
    private static final LimitHandler LIMIT_HANDLER = new AbstractLimitHandler() {
        @Override
        public String processSql(String sql, RowSelection selection) {
            boolean hasOffset = LimitHelper.hasFirstRow(selection);
            StringBuilder sb = new StringBuilder(sql.length() + 20);
            sb.append(sql);
            sb.append(" LIMIT ?");
            if (hasOffset) {
                sb.append(" OFFSET ?");
            }
            return sb.toString();
        }

        @Override
        public boolean supportsLimit() {
            return true;
        }

        @Override
        public boolean supportsLimitOffset() {
            return true;
        }

        @Override
        public boolean bindLimitParametersInReverseOrder() {
            return false;
        }
    };

    public PyroSQLDialect() {
        super();
        registerColumnTypes();
        registerFunctions();
    }

    // -----------------------------------------------------------------------
    // Column type registration
    // -----------------------------------------------------------------------

    private void registerColumnTypes() {
        // Standard SQL types
        registerColumnType(Types.BIT, "BOOLEAN");
        registerColumnType(Types.BOOLEAN, "BOOLEAN");
        registerColumnType(Types.TINYINT, "SMALLINT");
        registerColumnType(Types.SMALLINT, "SMALLINT");
        registerColumnType(Types.INTEGER, "INTEGER");
        registerColumnType(Types.BIGINT, "BIGINT");
        registerColumnType(Types.FLOAT, "REAL");
        registerColumnType(Types.REAL, "REAL");
        registerColumnType(Types.DOUBLE, "DOUBLE PRECISION");
        registerColumnType(Types.NUMERIC, "NUMERIC($p,$s)");
        registerColumnType(Types.DECIMAL, "NUMERIC($p,$s)");

        // Character types
        registerColumnType(Types.CHAR, "CHAR($l)");
        registerColumnType(Types.VARCHAR, "VARCHAR($l)");
        registerColumnType(Types.LONGVARCHAR, "TEXT");
        registerColumnType(Types.NCHAR, "CHAR($l)");
        registerColumnType(Types.NVARCHAR, "VARCHAR($l)");
        registerColumnType(Types.LONGNVARCHAR, "TEXT");
        registerColumnType(Types.CLOB, "TEXT");
        registerColumnType(Types.NCLOB, "TEXT");

        // Binary types
        registerColumnType(Types.BINARY, "BYTEA");
        registerColumnType(Types.VARBINARY, "BYTEA");
        registerColumnType(Types.LONGVARBINARY, "BYTEA");
        registerColumnType(Types.BLOB, "BYTEA");

        // Date/time types
        registerColumnType(Types.DATE, "DATE");
        registerColumnType(Types.TIME, "TIME");
        registerColumnType(Types.TIMESTAMP, "TIMESTAMPTZ");
        registerColumnType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMPTZ");

        // Other types
        registerColumnType(Types.OTHER, "JSONB");
        registerColumnType(Types.JAVA_OBJECT, "JSONB");
    }

    // -----------------------------------------------------------------------
    // SQL function registration
    // -----------------------------------------------------------------------

    private void registerFunctions() {
        // String functions
        registerFunction("lower", new StandardSQLFunction("LOWER", StandardBasicTypes.STRING));
        registerFunction("upper", new StandardSQLFunction("UPPER", StandardBasicTypes.STRING));
        registerFunction("length", new StandardSQLFunction("LENGTH", StandardBasicTypes.INTEGER));
        registerFunction("char_length", new StandardSQLFunction("CHAR_LENGTH", StandardBasicTypes.INTEGER));
        registerFunction("trim", new StandardSQLFunction("TRIM", StandardBasicTypes.STRING));
        registerFunction("ltrim", new StandardSQLFunction("LTRIM", StandardBasicTypes.STRING));
        registerFunction("rtrim", new StandardSQLFunction("RTRIM", StandardBasicTypes.STRING));
        registerFunction("replace", new StandardSQLFunction("REPLACE", StandardBasicTypes.STRING));
        registerFunction("substring", new StandardSQLFunction("SUBSTRING", StandardBasicTypes.STRING));
        registerFunction("position", new StandardSQLFunction("POSITION", StandardBasicTypes.INTEGER));
        registerFunction("concat", new VarArgsSQLFunction(StandardBasicTypes.STRING, "", " || ", ""));
        registerFunction("coalesce", new VarArgsSQLFunction(StandardBasicTypes.STRING, "COALESCE(", ",", ")"));

        // Numeric functions
        registerFunction("abs", new StandardSQLFunction("ABS"));
        registerFunction("ceil", new StandardSQLFunction("CEIL", StandardBasicTypes.INTEGER));
        registerFunction("ceiling", new StandardSQLFunction("CEIL", StandardBasicTypes.INTEGER));
        registerFunction("floor", new StandardSQLFunction("FLOOR", StandardBasicTypes.INTEGER));
        registerFunction("round", new StandardSQLFunction("ROUND"));
        registerFunction("mod", new StandardSQLFunction("MOD", StandardBasicTypes.INTEGER));
        registerFunction("power", new StandardSQLFunction("POWER", StandardBasicTypes.DOUBLE));
        registerFunction("sqrt", new StandardSQLFunction("SQRT", StandardBasicTypes.DOUBLE));
        registerFunction("sign", new StandardSQLFunction("SIGN", StandardBasicTypes.INTEGER));
        registerFunction("random", new NoArgSQLFunction("RANDOM", StandardBasicTypes.DOUBLE));

        // Date/time functions
        registerFunction("current_date", new NoArgSQLFunction("CURRENT_DATE", StandardBasicTypes.DATE, false));
        registerFunction("current_time", new NoArgSQLFunction("CURRENT_TIME", StandardBasicTypes.TIME, false));
        registerFunction("current_timestamp", new NoArgSQLFunction("CURRENT_TIMESTAMP", StandardBasicTypes.TIMESTAMP, false));
        registerFunction("now", new NoArgSQLFunction("NOW", StandardBasicTypes.TIMESTAMP));
        registerFunction("extract", new SQLFunctionTemplate(StandardBasicTypes.INTEGER, "EXTRACT(?1 FROM ?2)"));
        registerFunction("date_trunc", new StandardSQLFunction("DATE_TRUNC", StandardBasicTypes.TIMESTAMP));

        // Aggregate functions
        registerFunction("count", new StandardSQLFunction("COUNT", StandardBasicTypes.LONG));
        registerFunction("sum", new StandardSQLFunction("SUM"));
        registerFunction("avg", new StandardSQLFunction("AVG", StandardBasicTypes.DOUBLE));
        registerFunction("min", new StandardSQLFunction("MIN"));
        registerFunction("max", new StandardSQLFunction("MAX"));

        // Type casting
        registerFunction("cast", new StandardSQLFunction("CAST"));

        // JSON functions
        registerFunction("jsonb_extract_path", new StandardSQLFunction("JSONB_EXTRACT_PATH", StandardBasicTypes.STRING));
        registerFunction("jsonb_extract_path_text", new StandardSQLFunction("JSONB_EXTRACT_PATH_TEXT", StandardBasicTypes.STRING));
        registerFunction("jsonb_typeof", new StandardSQLFunction("JSONB_TYPEOF", StandardBasicTypes.STRING));
        registerFunction("jsonb_array_length", new StandardSQLFunction("JSONB_ARRAY_LENGTH", StandardBasicTypes.INTEGER));

        // UUID function
        registerFunction("gen_random_uuid", new NoArgSQLFunction("GEN_RANDOM_UUID", StandardBasicTypes.STRING));
    }

    // -----------------------------------------------------------------------
    // Identity column support (SERIAL / BIGSERIAL / RETURNING)
    // -----------------------------------------------------------------------

    @Override
    public IdentityColumnSupport getIdentityColumnSupport() {
        return new PyroSQLIdentityColumnSupport();
    }

    /**
     * Identity column support using SERIAL types and RETURNING clause, similar to PostgreSQL.
     */
    public static class PyroSQLIdentityColumnSupport extends IdentityColumnSupportImpl {

        @Override
        public boolean supportsIdentityColumns() {
            return true;
        }

        @Override
        public boolean supportsInsertSelectIdentity() {
            return true;
        }

        @Override
        public boolean hasDataTypeInIdentityColumn() {
            return false;
        }

        @Override
        public String getIdentityColumnString(int type) {
            switch (type) {
                case Types.SMALLINT:
                    return "SMALLSERIAL";
                case Types.INTEGER:
                    return "SERIAL";
                case Types.BIGINT:
                default:
                    return "BIGSERIAL";
            }
        }

        @Override
        public String getIdentitySelectString(String table, String column, int type) {
            return "SELECT CURRVAL('" + table + "_" + column + "_seq')";
        }

        @Override
        public String getIdentityInsertString() {
            return "DEFAULT";
        }

        @Override
        public String appendIdentitySelectToInsert(String insertString) {
            return insertString + " RETURNING *";
        }
    }

    // -----------------------------------------------------------------------
    // Pagination (LIMIT/OFFSET)
    // -----------------------------------------------------------------------

    @Override
    public LimitHandler getLimitHandler() {
        return LIMIT_HANDLER;
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsLimitOffset() {
        return true;
    }

    @Override
    public String getLimitString(String sql, boolean hasOffset) {
        StringBuilder sb = new StringBuilder(sql.length() + 20);
        sb.append(sql);
        sb.append(" LIMIT ?");
        if (hasOffset) {
            sb.append(" OFFSET ?");
        }
        return sb.toString();
    }

    @Override
    public boolean bindLimitParametersInReverseOrder() {
        return false;
    }

    // -----------------------------------------------------------------------
    // Boolean literals
    // -----------------------------------------------------------------------

    @Override
    public String toBooleanValueString(boolean bool) {
        return bool ? "TRUE" : "FALSE";
    }

    // -----------------------------------------------------------------------
    // Sequences
    // -----------------------------------------------------------------------

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public boolean supportsPooledSequences() {
        return true;
    }

    @Override
    public String getSequenceNextValString(String sequenceName) {
        return "SELECT NEXTVAL('" + sequenceName + "')";
    }

    @Override
    public String getCreateSequenceString(String sequenceName) {
        return "CREATE SEQUENCE " + sequenceName;
    }

    @Override
    public String getDropSequenceString(String sequenceName) {
        return "DROP SEQUENCE IF EXISTS " + sequenceName;
    }

    @Override
    public String getQuerySequencesString() {
        return "SELECT sequence_name FROM information_schema.sequences";
    }

    // -----------------------------------------------------------------------
    // Schema DDL support
    // -----------------------------------------------------------------------

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    @Override
    public boolean supportsIfExistsAfterAlterTable() {
        return true;
    }

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public String getAddColumnString() {
        return "ADD COLUMN";
    }

    @Override
    public String getCascadeConstraintsString() {
        return " CASCADE";
    }

    @Override
    public boolean supportsColumnCheck() {
        return true;
    }

    @Override
    public boolean supportsTableCheck() {
        return true;
    }

    @Override
    public boolean hasAlterTable() {
        return true;
    }

    @Override
    public boolean dropConstraints() {
        return true;
    }

    // -----------------------------------------------------------------------
    // FOR UPDATE support
    // -----------------------------------------------------------------------

    @Override
    public boolean supportsOuterJoinForUpdate() {
        return false;
    }

    @Override
    public String getForUpdateString() {
        return " FOR UPDATE";
    }

    @Override
    public String getForUpdateNowaitString() {
        return " FOR UPDATE NOWAIT";
    }

    @Override
    public String getForUpdateSkipLockedString() {
        return " FOR UPDATE SKIP LOCKED";
    }

    // -----------------------------------------------------------------------
    // Transaction & misc
    // -----------------------------------------------------------------------

    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public String getCurrentTimestampSelectString() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    @Override
    public boolean supportsTupleDistinctCounts() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsEmptyInList() {
        return false;
    }

    @Override
    public boolean supportsRowValueConstructorSyntax() {
        return true;
    }

    @Override
    public boolean supportsRowValueConstructorSyntaxInInList() {
        return true;
    }

    @Override
    public String getNoColumnsInsertString() {
        return "DEFAULT VALUES";
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public boolean supportsCaseInsensitiveLike() {
        return true;
    }

    @Override
    public String getCaseInsensitiveLike() {
        return "ILIKE";
    }

    @Override
    public boolean requiresParensForTupleDistinctCounts() {
        return true;
    }
}
