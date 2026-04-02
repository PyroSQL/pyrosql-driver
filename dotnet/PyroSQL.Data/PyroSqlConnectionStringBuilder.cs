using System.Data.Common;

namespace PyroSQL.Data;

/// <summary>
/// Builds and parses PyroSQL connection strings.
/// </summary>
public sealed class PyroSqlConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string HostKey = "Host";
    private const string PortKey = "Port";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string TimeoutKey = "Timeout";

    public PyroSqlConnectionStringBuilder() { }

    public PyroSqlConnectionStringBuilder(string connectionString)
    {
        ConnectionString = connectionString;
    }

    public string Host
    {
        get => TryGetValue(HostKey, out var v) ? (string)v : "localhost";
        set => this[HostKey] = value;
    }

    public int Port
    {
        get => TryGetValue(PortKey, out var v) ? Convert.ToInt32(v) : 12520;
        set => this[PortKey] = value;
    }

    public string Database
    {
        get => TryGetValue(DatabaseKey, out var v) ? (string)v : "";
        set => this[DatabaseKey] = value;
    }

    public string Username
    {
        get => TryGetValue(UsernameKey, out var v) ? (string)v : "";
        set => this[UsernameKey] = value;
    }

    public string Password
    {
        get => TryGetValue(PasswordKey, out var v) ? (string)v : "";
        set => this[PasswordKey] = value;
    }

    /// <summary>
    /// Connection timeout in seconds. Default is 30.
    /// </summary>
    public int Timeout
    {
        get => TryGetValue(TimeoutKey, out var v) ? Convert.ToInt32(v) : 30;
        set => this[TimeoutKey] = value;
    }
}
