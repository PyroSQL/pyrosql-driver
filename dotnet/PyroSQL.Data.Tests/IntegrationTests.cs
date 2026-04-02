using System.Data;
using Xunit;

namespace PyroSQL.Data.Tests;

/// <summary>
/// Integration tests that require a running PyroSQL server at localhost:12520.
/// </summary>
[Collection("Integration")]
public class IntegrationTests : IDisposable
{
    private const string ConnString = "Host=localhost;Port=12520;Username=admin;Password=admin;Database=test_integration";
    private readonly PyroSqlConnection _conn;

    public IntegrationTests()
    {
        _conn = new PyroSqlConnection(ConnString);
        _conn.Open();

        // Clean up any leftover test tables
        TryExecute("DROP TABLE IF EXISTS test_crud");
        TryExecute("DROP TABLE IF EXISTS test_types");
        TryExecute("DROP TABLE IF EXISTS test_txn");
        TryExecute("DROP TABLE IF EXISTS test_params");
    }

    public void Dispose()
    {
        TryExecute("DROP TABLE IF EXISTS test_crud");
        TryExecute("DROP TABLE IF EXISTS test_types");
        TryExecute("DROP TABLE IF EXISTS test_txn");
        TryExecute("DROP TABLE IF EXISTS test_params");
        _conn.Close();
        _conn.Dispose();
    }

    private void TryExecute(string sql)
    {
        try
        {
            using var cmd = new PyroSqlCommand(sql, _conn);
            cmd.ExecuteNonQuery();
        }
        catch { }
    }

    // ---- Connection Tests ----

    [Fact]
    public void Integration_Connect_Succeeds()
    {
        Assert.Equal(ConnectionState.Open, _conn.State);
    }

    [Fact]
    public void Integration_Ping_Succeeds()
    {
        // Ping is done at protocol level; we test by running a simple query
        using var cmd = new PyroSqlCommand("SELECT 1", _conn);
        var result = cmd.ExecuteScalar();
        Assert.NotNull(result);
    }

    [Fact]
    public void Integration_OpenClose_Multiple()
    {
        using var conn2 = new PyroSqlConnection(ConnString);
        conn2.Open();
        Assert.Equal(ConnectionState.Open, conn2.State);
        conn2.Close();
        Assert.Equal(ConnectionState.Closed, conn2.State);
        conn2.Open();
        Assert.Equal(ConnectionState.Open, conn2.State);
    }

    // ---- DDL Tests ----

    [Fact]
    public void Integration_CreateTable_Succeeds()
    {
        using var cmd = new PyroSqlCommand(
            "CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT, value DOUBLE)", _conn);
        cmd.ExecuteNonQuery(); // Should not throw
    }

    // ---- CRUD Tests ----

    [Fact]
    public void Integration_InsertAndSelect()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT, value DOUBLE)", _conn);
        create.ExecuteNonQuery();

        using var insert1 = new PyroSqlCommand(
            "INSERT INTO test_crud (id, name, value) VALUES (1, 'Alice', 3.14)", _conn);
        var rows1 = insert1.ExecuteNonQuery();

        using var insert2 = new PyroSqlCommand(
            "INSERT INTO test_crud (id, name, value) VALUES (2, 'Bob', 2.718)", _conn);
        var rows2 = insert2.ExecuteNonQuery();

        // SELECT all
        using var selectAll = new PyroSqlCommand("SELECT id, name, value FROM test_crud ORDER BY id", _conn);
        using var reader = selectAll.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("1", reader.GetValue(0)?.ToString());
        Assert.Equal("Alice", reader.GetString(1));
        Assert.Equal(3.14, Convert.ToDouble(reader.GetValue(2)), 2);

        Assert.True(reader.Read());
        Assert.Equal("2", reader.GetValue(0)?.ToString());
        Assert.Equal("Bob", reader.GetString(1));
        Assert.Equal(2.718, Convert.ToDouble(reader.GetValue(2)), 2);

        Assert.False(reader.Read());
    }

    [Fact]
    public void Integration_Update()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_crud (id, name) VALUES (1, 'Alice')", _conn);
        insert.ExecuteNonQuery();

        using var update = new PyroSqlCommand(
            "UPDATE test_crud SET name = 'Alicia' WHERE id = 1", _conn);
        var affected = update.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT name FROM test_crud WHERE id = 1", _conn);
        var result = select.ExecuteScalar();
        Assert.Equal("Alicia", result);
    }

    [Fact]
    public void Integration_Delete()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_crud (id, name) VALUES (1, 'Alice')", _conn);
        insert.ExecuteNonQuery();

        using var insert2 = new PyroSqlCommand(
            "INSERT INTO test_crud (id, name) VALUES (2, 'Bob')", _conn);
        insert2.ExecuteNonQuery();

        using var delete = new PyroSqlCommand(
            "DELETE FROM test_crud WHERE id = 1", _conn);
        delete.ExecuteNonQuery();

        using var count = new PyroSqlCommand("SELECT id FROM test_crud", _conn);
        using var reader = count.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("2", reader.GetValue(0)?.ToString());
        Assert.False(reader.Read());
    }

    // ---- Data Types ----

    [Fact]
    public void Integration_DataTypes_IntAndDouble()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, int_val BIGINT, float_val DOUBLE)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_types (id, int_val, float_val) VALUES (1, 9999999999, 1.23456789)", _conn);
        insert.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT int_val, float_val FROM test_types WHERE id = 1", _conn);
        using var reader = select.ExecuteReader();
        Assert.True(reader.Read());
        // Server returns all columns as TEXT type
        Assert.Equal(9999999999L, Convert.ToInt64(reader.GetValue(0)));
        Assert.Equal(1.23456789, Convert.ToDouble(reader.GetValue(1)), 5);
    }

    [Fact]
    public void Integration_DataTypes_Boolean()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, active BOOLEAN)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_types (id, active) VALUES (1, TRUE)", _conn);
        insert.ExecuteNonQuery();

        using var insert2 = new PyroSqlCommand(
            "INSERT INTO test_types (id, active) VALUES (2, FALSE)", _conn);
        insert2.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT active FROM test_types ORDER BY id", _conn);
        using var reader = select.ExecuteReader();
        Assert.True(reader.Read());
        // Server returns boolean as TEXT: "t"/"f"
        var val1 = reader.GetValue(0)?.ToString();
        Assert.True(val1 == "t" || val1?.ToLower() == "true", $"Expected true-ish, got '{val1}'");
        Assert.True(reader.Read());
        var val2 = reader.GetValue(0)?.ToString();
        Assert.True(val2 == "f" || val2?.ToLower() == "false", $"Expected false-ish, got '{val2}'");
    }

    [Fact]
    public void Integration_DataTypes_Text()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, text_val TEXT)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_types (id, text_val) VALUES (1, 'Hello, World!')", _conn);
        insert.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT text_val FROM test_types WHERE id = 1", _conn);
        var result = select.ExecuteScalar();
        Assert.Equal("Hello, World!", result);
    }

    [Fact]
    public void Integration_DataTypes_Null()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, nullable_col TEXT)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_types (id, nullable_col) VALUES (1, NULL)", _conn);
        insert.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT nullable_col FROM test_types WHERE id = 1", _conn);
        using var reader = select.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
    }

    // ---- ExecuteScalar ----

    [Fact]
    public void Integration_ExecuteScalar_ReturnsFirstColumn()
    {
        using var cmd = new PyroSqlCommand("SELECT 42", _conn);
        var result = cmd.ExecuteScalar();
        Assert.NotNull(result);
        // Server may return as long
        Assert.Equal(42L, Convert.ToInt64(result));
    }

    // ---- DataReader metadata ----

    [Fact]
    public void Integration_DataReader_FieldCount()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, name TEXT, value DOUBLE)", _conn);
        create.ExecuteNonQuery();

        using var select = new PyroSqlCommand("SELECT id, name, value FROM test_types", _conn);
        using var reader = select.ExecuteReader();
        Assert.Equal(3, reader.FieldCount);
    }

    [Fact]
    public void Integration_DataReader_GetName()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        using var select = new PyroSqlCommand("SELECT id, name FROM test_types", _conn);
        using var reader = select.ExecuteReader();
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
    }

    // ---- Transactions ----

    [Fact]
    public void Integration_Transaction_Commit()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_txn (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        using var txn = _conn.BeginTransaction();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_txn (id, name) VALUES (1, 'TxnTest')", _conn, txn);
        insert.ExecuteNonQuery();

        txn.Commit();

        using var select = new PyroSqlCommand(
            "SELECT name FROM test_txn WHERE id = 1", _conn);
        var result = select.ExecuteScalar();
        Assert.Equal("TxnTest", result);
    }

    [Fact]
    public void Integration_Transaction_Rollback()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_txn (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        using var txn = _conn.BeginTransaction();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_txn (id, name) VALUES (1, 'ShouldNotExist')", _conn, txn);
        insert.ExecuteNonQuery();

        txn.Rollback();

        using var select = new PyroSqlCommand(
            "SELECT name FROM test_txn WHERE id = 1", _conn);
        using var reader = select.ExecuteReader();
        Assert.False(reader.Read()); // Should be empty after rollback
    }

    // ---- Prepared Statements / Parameters ----

    [Fact]
    public void Integration_PreparedStatement_Insert()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_params (id BIGINT NOT NULL PRIMARY KEY, name TEXT, score DOUBLE)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_params (id, name, score) VALUES ($1, $2, $3)", _conn);
        insert.Parameters.Add("@id", 1);
        insert.Parameters.Add("@name", "Alice");
        insert.Parameters.Add("@score", 95.5);
        insert.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT id, name, score FROM test_params WHERE id = 1", _conn);
        using var reader = select.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("1", reader.GetValue(0)?.ToString());
        Assert.Equal("Alice", reader.GetString(1));
        Assert.Equal(95.5, Convert.ToDouble(reader.GetValue(2)), 1);
    }

    [Fact]
    public void Integration_PreparedStatement_Select()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_params (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        using var insert = new PyroSqlCommand(
            "INSERT INTO test_params (id, name) VALUES (1, 'Alice')", _conn);
        insert.ExecuteNonQuery();
        using var insert2 = new PyroSqlCommand(
            "INSERT INTO test_params (id, name) VALUES (2, 'Bob')", _conn);
        insert2.ExecuteNonQuery();

        using var select = new PyroSqlCommand(
            "SELECT name FROM test_params WHERE id = $1", _conn);
        select.Parameters.Add("@id", 2);
        using var reader = select.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Bob", reader.GetValue(0)?.ToString());
        Assert.False(reader.Read());
    }

    // ---- Error Handling ----

    [Fact]
    public void Integration_InvalidSQL_ThrowsPyroSqlException()
    {
        using var cmd = new PyroSqlCommand("THIS IS NOT VALID SQL AT ALL", _conn);
        Assert.Throws<PyroSqlException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void Integration_SelectFromNonexistentTable_Throws()
    {
        using var cmd = new PyroSqlCommand("SELECT * FROM nonexistent_table_xyz", _conn);
        Assert.Throws<PyroSqlException>(() => cmd.ExecuteReader());
    }

    // ---- Multiple rows ----

    [Fact]
    public void Integration_MultipleRows()
    {
        using var create = new PyroSqlCommand(
            "CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT)", _conn);
        create.ExecuteNonQuery();

        for (int i = 1; i <= 10; i++)
        {
            using var insert = new PyroSqlCommand(
                $"INSERT INTO test_crud (id, name) VALUES ({i}, 'User{i}')", _conn);
            insert.ExecuteNonQuery();
        }

        using var select = new PyroSqlCommand("SELECT id, name FROM test_crud ORDER BY id", _conn);
        using var reader = select.ExecuteReader();

        int count = 0;
        while (reader.Read())
        {
            count++;
            Assert.Equal(count.ToString(), reader.GetValue(0)?.ToString());
            Assert.Equal($"User{count}", reader.GetString(1));
        }
        Assert.Equal(10, count);
    }
}
