using System.Data;
using System.Data.Common;

namespace PyroSQL.Data;

/// <summary>
/// Represents a transaction to be performed at a PyroSQL database.
/// </summary>
public sealed class PyroSqlTransaction : DbTransaction
{
    private readonly PyroSqlConnection _connection;
    private readonly IsolationLevel _isolationLevel;
    private bool _completed;

    internal PyroSqlTransaction(PyroSqlConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;
    }

    protected override DbConnection DbConnection => _connection;

    public override IsolationLevel IsolationLevel => _isolationLevel;

    internal void Begin()
    {
        _connection.EnsureOpen();
        _connection.ActiveTransaction = this;

        string sql = _isolationLevel switch
        {
            IsolationLevel.ReadUncommitted => "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel.ReadCommitted => "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
            IsolationLevel.RepeatableRead => "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel.Serializable => "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            _ => "BEGIN"
        };

        ExecuteInternal(sql);
    }

    public override void Commit()
    {
        ThrowIfCompleted();
        ExecuteInternal("COMMIT");
        Complete();
    }

    public override void Rollback()
    {
        ThrowIfCompleted();
        ExecuteInternal("ROLLBACK");
        Complete();
    }

    private void ExecuteInternal(string sql)
    {
        var (type, payload) = _connection.SendAndReceive(PWireCodec.EncodeQuery(sql));

        if (type == PWireCodec.RESP_ERROR)
        {
            var err = PWireCodec.DecodeError(payload);
            throw new PyroSqlException(err.SqlState, err.Message);
        }

        // Accept OK or RESULT_SET as success
        if (type != PWireCodec.RESP_OK && type != PWireCodec.RESP_RESULT_SET)
            throw new PyroSqlException($"Unexpected response type 0x{type:X2} for transaction command");
    }

    private void Complete()
    {
        _completed = true;
        _connection.ActiveTransaction = null;
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
            throw new InvalidOperationException("This transaction has already been committed or rolled back.");
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed)
        {
            try
            {
                Rollback();
            }
            catch
            {
                // Best-effort rollback on dispose
                _completed = true;
                _connection.ActiveTransaction = null;
            }
        }
        base.Dispose(disposing);
    }
}
