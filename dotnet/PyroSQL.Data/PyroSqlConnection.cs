using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace PyroSQL.Data;

/// <summary>
/// Represents a connection to a PyroSQL database.
/// </summary>
public sealed class PyroSqlConnection : DbConnection
{
    private string _connectionString = "";
    private ConnectionState _state = ConnectionState.Closed;
    internal PWireConnection? Wire { get; private set; }
    internal PyroSqlTransaction? ActiveTransaction { get; set; }

    public PyroSqlConnection() { }

    public PyroSqlConnection(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value ?? "";
    }

    public override string Database
    {
        get
        {
            var builder = new PyroSqlConnectionStringBuilder(_connectionString);
            return builder.Database;
        }
    }

    public override string DataSource
    {
        get
        {
            var builder = new PyroSqlConnectionStringBuilder(_connectionString);
            return $"{builder.Host}:{builder.Port}";
        }
    }

    public override string ServerVersion => "1.0";

    public override ConnectionState State => _state;

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("PyroSQL does not support changing databases on an open connection.");
    }

    public override void Open()
    {
        if (_state == ConnectionState.Open)
            return;

        if (string.IsNullOrEmpty(_connectionString))
            throw new InvalidOperationException("ConnectionString has not been set.");

        var builder = new PyroSqlConnectionStringBuilder(_connectionString);

        _state = ConnectionState.Connecting;
        try
        {
            var wire = new PWireConnection();
            wire.Connect(builder.Host, builder.Port, builder.Timeout * 1000);
            wire.Authenticate(builder.Username, builder.Password);
            Wire = wire;
            _state = ConnectionState.Open;
        }
        catch
        {
            _state = ConnectionState.Closed;
            Wire?.Dispose();
            Wire = null;
            throw;
        }
    }

    public override void Close()
    {
        if (_state == ConnectionState.Closed)
            return;

        ActiveTransaction = null;

        Wire?.SendQuit();
        Wire?.Dispose();
        Wire = null;
        _state = ConnectionState.Closed;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");

        if (ActiveTransaction != null)
            throw new InvalidOperationException("A transaction is already in progress.");

        var tx = new PyroSqlTransaction(this, isolationLevel);
        tx.Begin();
        return tx;
    }

    protected override DbCommand CreateDbCommand()
    {
        return new PyroSqlCommand { Connection = this };
    }

    public new PyroSqlCommand CreateCommand()
    {
        return new PyroSqlCommand { Connection = this };
    }

    public new PyroSqlTransaction BeginTransaction()
    {
        return (PyroSqlTransaction)BeginDbTransaction(IsolationLevel.Unspecified);
    }

    public new PyroSqlTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        return (PyroSqlTransaction)BeginDbTransaction(isolationLevel);
    }

    internal void EnsureOpen()
    {
        if (_state != ConnectionState.Open || Wire == null)
            throw new InvalidOperationException("Connection is not open.");
    }

    internal (byte Type, byte[] Payload) SendAndReceive(byte[] frame)
    {
        EnsureOpen();
        return Wire!.SendAndReceive(frame);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();
        base.Dispose(disposing);
    }
}
