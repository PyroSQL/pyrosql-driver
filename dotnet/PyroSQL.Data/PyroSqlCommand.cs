using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace PyroSQL.Data;

/// <summary>
/// Represents a SQL statement to execute against a PyroSQL database.
/// </summary>
public sealed class PyroSqlCommand : DbCommand
{
    private string _commandText = "";
    private PyroSqlConnection? _connection;
    private PyroSqlTransaction? _transaction;
    private readonly PyroSqlParameterCollection _parameters = new();
    private int _commandTimeout = 30;

    public PyroSqlCommand() { }

    public PyroSqlCommand(string commandText)
    {
        _commandText = commandText ?? "";
    }

    public PyroSqlCommand(string commandText, PyroSqlConnection connection)
    {
        _commandText = commandText ?? "";
        _connection = connection;
    }

    public PyroSqlCommand(string commandText, PyroSqlConnection connection, PyroSqlTransaction? transaction)
    {
        _commandText = commandText ?? "";
        _connection = connection;
        _transaction = transaction;
    }

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set => _commandText = value ?? "";
    }

    public override int CommandTimeout
    {
        get => _commandTimeout;
        set => _commandTimeout = value;
    }

    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override bool DesignTimeVisible { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;

    public new PyroSqlConnection? Connection
    {
        get => _connection;
        set => _connection = value;
    }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (PyroSqlConnection?)value;
    }

    public new PyroSqlTransaction? Transaction
    {
        get => _transaction;
        set => _transaction = value;
    }

    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = (PyroSqlTransaction?)value;
    }

    public new PyroSqlParameterCollection Parameters => _parameters;

    protected override DbParameterCollection DbParameterCollection => _parameters;

    public override void Cancel()
    {
        // Not supported by PWire protocol
    }

    public override void Prepare()
    {
        // Prepare is handled implicitly when parameters are present.
        // ExecuteWithParametersRaw sends MSG_PREPARE + MSG_EXECUTE + MSG_CLOSE
        // for each execution.
    }

    protected override DbParameter CreateDbParameter()
    {
        return new PyroSqlParameter();
    }

    public new PyroSqlParameter CreateParameter()
    {
        return new PyroSqlParameter();
    }

    public override int ExecuteNonQuery()
    {
        var conn = GetOpenConnection();

        if (_parameters.Count > 0)
        {
            return ExecuteWithParameters(conn).RowsAffected;
        }

        var (type, payload) = conn.SendAndReceive(PWireCodec.EncodeQuery(_commandText));
        return HandleNonQueryResponse(type, payload);
    }

    public override object? ExecuteScalar()
    {
        using var reader = ExecuteDbDataReader(CommandBehavior.Default);
        if (reader.Read() && reader.FieldCount > 0)
            return reader.IsDBNull(0) ? null : reader.GetValue(0);
        return null;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var conn = GetOpenConnection();

        byte type;
        byte[] payload;

        if (_parameters.Count > 0)
        {
            var okOrResultSet = ExecuteWithParametersRaw(conn);
            type = okOrResultSet.Type;
            payload = okOrResultSet.Payload;
        }
        else
        {
            (type, payload) = conn.SendAndReceive(PWireCodec.EncodeQuery(_commandText));
        }

        if (type == PWireCodec.RESP_ERROR)
        {
            var err = PWireCodec.DecodeError(payload);
            throw new PyroSqlException(err.SqlState, err.Message);
        }

        if (type == PWireCodec.RESP_RESULT_SET)
        {
            var rs = PWireCodec.DecodeResultSet(payload);
            return new PyroSqlDataReader(rs, behavior, _connection);
        }

        if (type == PWireCodec.RESP_OK)
        {
            // Return an empty reader for non-query statements
            var emptyRs = new ResultSet(Array.Empty<ColumnInfo>(), Array.Empty<object?[]>());
            return new PyroSqlDataReader(emptyRs, behavior, _connection);
        }

        throw new PyroSqlException($"Unexpected response type 0x{type:X2}");
    }

    private (byte Type, byte[] Payload) ExecuteWithParametersRaw(PyroSqlConnection conn)
    {
        // Rewrite ? placeholders to $1, $2, ... for server-side binding
        string preparedSql = RewritePlaceholders(_commandText);

        // PREPARE via binary message — server returns RESP_READY with u32 handle
        var (prepType, prepPayload) = conn.SendAndReceive(PWireCodec.EncodePrepare(preparedSql));

        if (prepType == PWireCodec.RESP_ERROR)
        {
            var err = PWireCodec.DecodeError(prepPayload);
            throw new PyroSqlException(err.SqlState, err.Message);
        }

        if (prepType != PWireCodec.RESP_READY)
            throw new PyroSqlException($"Expected READY from PREPARE, got 0x{prepType:X2}");

        if (prepPayload.Length < 4)
            throw new PyroSqlException("PREPARE response payload too short");

        uint handle = BitConverter.ToUInt32(prepPayload, 0);

        try
        {
            // EXECUTE via binary MSG_EXECUTE with handle + params
            var paramStrings = new List<string>(_parameters.Count);
            foreach (var p in _parameters.InternalList)
                paramStrings.Add(ParamToWireString(p));

            var (execType, execPayload) = conn.SendAndReceive(PWireCodec.EncodeExecute(handle, paramStrings));
            return (execType, execPayload);
        }
        finally
        {
            // CLOSE the prepared statement via binary MSG_CLOSE
            try
            {
                conn.SendAndReceive(PWireCodec.EncodeClose(handle));
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }

    private static string RewritePlaceholders(string sql)
    {
        var result = new System.Text.StringBuilder(sql.Length);
        int paramIndex = 0;
        bool inSingleQuote = false;
        bool inDoubleQuote = false;

        foreach (char c in sql)
        {
            if (c == '\'' && !inDoubleQuote)
            {
                inSingleQuote = !inSingleQuote;
                result.Append(c);
            }
            else if (c == '"' && !inSingleQuote)
            {
                inDoubleQuote = !inDoubleQuote;
                result.Append(c);
            }
            else if (c == '?' && !inSingleQuote && !inDoubleQuote)
            {
                paramIndex++;
                result.Append('$').Append(paramIndex);
            }
            else
            {
                // If it already uses $N syntax, pass through
                result.Append(c);
            }
        }
        return result.ToString();
    }

    private static string ParamToWireString(PyroSqlParameter param)
    {
        if (param.Value == null || param.Value == DBNull.Value)
            return "NULL";

        if (param.Value is bool boolVal)
            return boolVal ? "true" : "false";

        if (param.Value is int or long or short or byte or uint or ulong or ushort or sbyte)
            return param.Value.ToString()!;

        if (param.Value is float or double or decimal)
            return param.ToWireString();

        if (param.Value is byte[] bytes)
            return $"\\x{Convert.ToHexString(bytes)}";

        // Send raw string value — the server handles quoting
        return param.Value.ToString() ?? "";
    }

    private (int RowsAffected, string Tag) ExecuteWithParameters(PyroSqlConnection conn)
    {
        var (type, payload) = ExecuteWithParametersRaw(conn);
        int rows = HandleNonQueryResponse(type, payload);
        return (rows, "");
    }

    private static int HandleNonQueryResponse(byte type, byte[] payload)
    {
        if (type == PWireCodec.RESP_ERROR)
        {
            var err = PWireCodec.DecodeError(payload);
            throw new PyroSqlException(err.SqlState, err.Message);
        }

        if (type == PWireCodec.RESP_OK)
        {
            var ok = PWireCodec.DecodeOk(payload);
            return (int)ok.RowsAffected;
        }

        if (type == PWireCodec.RESP_RESULT_SET)
        {
            // DDL or SELECT used with ExecuteNonQuery; just return -1
            return -1;
        }

        throw new PyroSqlException($"Unexpected response type 0x{type:X2}");
    }

    private PyroSqlConnection GetOpenConnection()
    {
        if (_connection == null)
            throw new InvalidOperationException("Connection property has not been set.");
        _connection.EnsureOpen();
        return _connection;
    }
}
