using System.Collections;
using System.Data;
using System.Data.Common;

namespace PyroSQL.Data;

/// <summary>
/// Reads a forward-only stream of rows from a PyroSQL result set.
/// </summary>
public sealed class PyroSqlDataReader : DbDataReader
{
    private readonly ResultSet _resultSet;
    private readonly CommandBehavior _behavior;
    private readonly PyroSqlConnection? _connection;
    private int _currentRow = -1;
    private bool _closed;

    internal PyroSqlDataReader(ResultSet resultSet, CommandBehavior behavior, PyroSqlConnection? connection)
    {
        _resultSet = resultSet;
        _behavior = behavior;
        _connection = connection;
    }

    public override int Depth => 0;
    public override int FieldCount => _resultSet.Columns.Length;
    public override bool HasRows => _resultSet.Rows.Length > 0;
    public override bool IsClosed => _closed;
    public override int RecordsAffected => -1;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override bool Read()
    {
        ThrowIfClosed();
        _currentRow++;
        return _currentRow < _resultSet.Rows.Length;
    }

    public override bool NextResult()
    {
        // Single result set only
        return false;
    }

    public override void Close()
    {
        if (_closed) return;
        _closed = true;

        if (_behavior.HasFlag(CommandBehavior.CloseConnection))
            _connection?.Close();
    }

    public override bool GetBoolean(int ordinal) => (bool)GetValueNotNull(ordinal);

    public override byte GetByte(int ordinal) => Convert.ToByte(GetValueNotNull(ordinal));

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var val = GetValueNotNull(ordinal);
        byte[] data;
        if (val is byte[] b)
            data = b;
        else
            throw new InvalidCastException($"Column {ordinal} is not a byte array.");

        if (buffer == null)
            return data.Length;

        int available = data.Length - (int)dataOffset;
        int toCopy = Math.Min(available, length);
        if (toCopy > 0)
            Array.Copy(data, (int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }

    public override char GetChar(int ordinal) => Convert.ToChar(GetValueNotNull(ordinal));

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var str = GetString(ordinal);
        if (buffer == null)
            return str.Length;
        int available = str.Length - (int)dataOffset;
        int toCopy = Math.Min(available, length);
        if (toCopy > 0)
            str.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }

    public override DateTime GetDateTime(int ordinal) => DateTime.Parse(GetString(ordinal));

    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValueNotNull(ordinal));

    public override double GetDouble(int ordinal)
    {
        var val = GetValueNotNull(ordinal);
        return val is double d ? d : Convert.ToDouble(val);
    }

    public override float GetFloat(int ordinal) => Convert.ToSingle(GetValueNotNull(ordinal));

    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));

    public override short GetInt16(int ordinal) => Convert.ToInt16(GetValueNotNull(ordinal));

    public override int GetInt32(int ordinal)
    {
        var val = GetValueNotNull(ordinal);
        return val is long l ? (int)l : Convert.ToInt32(val);
    }

    public override long GetInt64(int ordinal)
    {
        var val = GetValueNotNull(ordinal);
        return val is long l ? l : Convert.ToInt64(val);
    }

    public override string GetName(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _resultSet.Columns.Length)
            throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");
        return _resultSet.Columns[ordinal].Name;
    }

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < _resultSet.Columns.Length; i++)
        {
            if (string.Equals(_resultSet.Columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public override string GetString(int ordinal)
    {
        var val = GetValueNotNull(ordinal);
        return val is string s ? s : val.ToString()!;
    }

    public override object GetValue(int ordinal)
    {
        ThrowIfClosed();
        ThrowIfNoRow();
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");
        return _resultSet.Rows[_currentRow][ordinal] ?? DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        ThrowIfClosed();
        ThrowIfNoRow();
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
            values[i] = _resultSet.Rows[_currentRow][i] ?? DBNull.Value;
        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        ThrowIfClosed();
        ThrowIfNoRow();
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");
        return _resultSet.Rows[_currentRow][ordinal] == null;
    }

    public override string GetDataTypeName(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _resultSet.Columns.Length)
            throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");

        return _resultSet.Columns[ordinal].TypeTag switch
        {
            PWireCodec.TYPE_I64 => "bigint",
            PWireCodec.TYPE_F64 => "double",
            PWireCodec.TYPE_TEXT => "text",
            PWireCodec.TYPE_BOOL => "boolean",
            PWireCodec.TYPE_BYTES => "blob",
            PWireCodec.TYPE_NULL => "null",
            _ => "unknown"
        };
    }

    public override Type GetFieldType(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _resultSet.Columns.Length)
            throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");

        return _resultSet.Columns[ordinal].TypeTag switch
        {
            PWireCodec.TYPE_I64 => typeof(long),
            PWireCodec.TYPE_F64 => typeof(double),
            PWireCodec.TYPE_TEXT => typeof(string),
            PWireCodec.TYPE_BOOL => typeof(bool),
            PWireCodec.TYPE_BYTES => typeof(byte[]),
            _ => typeof(object)
        };
    }

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable("SchemaTable");
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("DataTypeName", typeof(string));

        for (int i = 0; i < _resultSet.Columns.Length; i++)
        {
            var row = table.NewRow();
            row["ColumnName"] = _resultSet.Columns[i].Name;
            row["ColumnOrdinal"] = i;
            row["DataType"] = GetFieldType(i);
            row["DataTypeName"] = GetDataTypeName(i);
            table.Rows.Add(row);
        }

        return table;
    }

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, closeReader: false);
    }

    private object GetValueNotNull(int ordinal)
    {
        var val = GetValue(ordinal);
        if (val == DBNull.Value)
            throw new InvalidCastException($"Column {ordinal} is null.");
        return val;
    }

    private void ThrowIfClosed()
    {
        if (_closed)
            throw new InvalidOperationException("DataReader is closed.");
    }

    private void ThrowIfNoRow()
    {
        if (_currentRow < 0 || _currentRow >= _resultSet.Rows.Length)
            throw new InvalidOperationException("No current row. Call Read() first.");
    }
}
