using System.Data;
using Xunit;

namespace PyroSQL.Data.Tests;

public class ConnectionTests
{
    [Fact]
    public void ConnectionStringBuilder_ParsesAllProperties()
    {
        var builder = new PyroSqlConnectionStringBuilder("Host=myhost;Port=5555;Database=testdb;Username=admin;Password=secret;Timeout=60");

        Assert.Equal("myhost", builder.Host);
        Assert.Equal(5555, builder.Port);
        Assert.Equal("testdb", builder.Database);
        Assert.Equal("admin", builder.Username);
        Assert.Equal("secret", builder.Password);
        Assert.Equal(60, builder.Timeout);
    }

    [Fact]
    public void ConnectionStringBuilder_DefaultValues()
    {
        var builder = new PyroSqlConnectionStringBuilder();

        Assert.Equal("localhost", builder.Host);
        Assert.Equal(12520, builder.Port);
        Assert.Equal("", builder.Database);
        Assert.Equal("", builder.Username);
        Assert.Equal("", builder.Password);
        Assert.Equal(30, builder.Timeout);
    }

    [Fact]
    public void ConnectionStringBuilder_SetsProperties()
    {
        var builder = new PyroSqlConnectionStringBuilder
        {
            Host = "db.example.com",
            Port = 9999,
            Database = "mydb",
            Username = "user",
            Password = "pass",
            Timeout = 15
        };

        Assert.Contains("db.example.com", builder.ConnectionString);
        Assert.Contains("9999", builder.ConnectionString);
        Assert.Contains("mydb", builder.ConnectionString);
    }

    [Fact]
    public void Connection_InitialState_IsClosed()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Port=12520;Database=mydb;Username=user;Password=pass");

        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void Connection_ExposesConnectionString()
    {
        const string cs = "Host=localhost;Port=12520;Database=mydb;Username=user;Password=pass";
        using var conn = new PyroSqlConnection(cs);

        Assert.Equal(cs, conn.ConnectionString);
    }

    [Fact]
    public void Connection_Database_ReturnsDatabaseFromConnectionString()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Port=12520;Database=testdb;Username=user;Password=pass");

        Assert.Equal("testdb", conn.Database);
    }

    [Fact]
    public void Connection_DataSource_ReturnsHostAndPort()
    {
        using var conn = new PyroSqlConnection("Host=myhost;Port=5555;Database=testdb;Username=user;Password=pass");

        Assert.Equal("myhost:5555", conn.DataSource);
    }

    [Fact]
    public void Connection_CreateCommand_ReturnsCommandWithConnection()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Port=12520;Database=mydb;Username=user;Password=pass");
        using var cmd = conn.CreateCommand();

        Assert.IsType<PyroSqlCommand>(cmd);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void Connection_Open_ThrowsWithoutConnectionString()
    {
        using var conn = new PyroSqlConnection();

        Assert.Throws<InvalidOperationException>(() => conn.Open());
    }

    [Fact]
    public void Connection_Close_OnClosedConnection_DoesNotThrow()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Port=12520;Database=mydb;Username=user;Password=pass");

        conn.Close(); // Should not throw
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void Connection_ChangeDatabase_ThrowsNotSupported()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Port=12520;Database=mydb;Username=user;Password=pass");

        Assert.Throws<NotSupportedException>(() => conn.ChangeDatabase("other"));
    }

    [Fact]
    public void Connection_Open_ThrowsOnUnreachableHost()
    {
        using var conn = new PyroSqlConnection("Host=192.0.2.1;Port=12520;Database=mydb;Username=user;Password=pass;Timeout=1");

        Assert.Throws<PyroSqlException>(() => conn.Open());
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void Connection_ServerVersion_ReturnsValue()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Port=12520;Database=mydb;Username=user;Password=pass");

        Assert.False(string.IsNullOrEmpty(conn.ServerVersion));
    }

    [Fact]
    public void Connection_NullConnectionString_SetsEmpty()
    {
        using var conn = new PyroSqlConnection();
        conn.ConnectionString = null;

        Assert.Equal("", conn.ConnectionString);
    }
}
