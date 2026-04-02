using System.Buffers.Binary;
using System.Data;
using System.Text;
using Xunit;

namespace PyroSQL.Data.Tests;

public class QueryTests
{
    [Fact]
    public void Command_DefaultState()
    {
        using var cmd = new PyroSqlCommand();

        Assert.Equal("", cmd.CommandText);
        Assert.Equal(30, cmd.CommandTimeout);
        Assert.Equal(CommandType.Text, cmd.CommandType);
        Assert.Null(cmd.Connection);
        Assert.Null(cmd.Transaction);
        Assert.NotNull(cmd.Parameters);
        Assert.Equal(0, cmd.Parameters.Count);
    }

    [Fact]
    public void Command_Constructor_SetsCommandText()
    {
        using var cmd = new PyroSqlCommand("SELECT 1");

        Assert.Equal("SELECT 1", cmd.CommandText);
    }

    [Fact]
    public void Command_Constructor_SetsCommandTextAndConnection()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Database=test;Username=u;Password=p");
        using var cmd = new PyroSqlCommand("SELECT 1", conn);

        Assert.Equal("SELECT 1", cmd.CommandText);
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void Command_CreateParameter_ReturnsPyroSqlParameter()
    {
        using var cmd = new PyroSqlCommand();
        var param = cmd.CreateParameter();

        Assert.IsType<PyroSqlParameter>(param);
    }

    [Fact]
    public void Command_ExecuteNonQuery_ThrowsWithoutConnection()
    {
        using var cmd = new PyroSqlCommand("INSERT INTO t VALUES (1)");

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void Command_ExecuteScalar_ThrowsWithoutConnection()
    {
        using var cmd = new PyroSqlCommand("SELECT 1");

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
    }

    [Fact]
    public void Command_ExecuteReader_ThrowsWhenNotOpen()
    {
        using var conn = new PyroSqlConnection("Host=localhost;Database=test;Username=u;Password=p");
        using var cmd = new PyroSqlCommand("SELECT 1", conn);

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
    }

    [Fact]
    public void Parameter_DefaultValues()
    {
        var param = new PyroSqlParameter();

        Assert.Equal("", param.ParameterName);
        Assert.Equal(DbType.String, param.DbType);
        Assert.Equal(ParameterDirection.Input, param.Direction);
        Assert.Null(param.Value);
        Assert.Equal("", param.SourceColumn);
        Assert.Equal(0, param.Size);
    }

    [Fact]
    public void Parameter_ConstructorWithNameAndValue()
    {
        var param = new PyroSqlParameter("@id", 42);

        Assert.Equal("@id", param.ParameterName);
        Assert.Equal(42, param.Value);
    }

    [Fact]
    public void Parameter_ConstructorWithNameAndDbType()
    {
        var param = new PyroSqlParameter("@name", DbType.Int64);

        Assert.Equal("@name", param.ParameterName);
        Assert.Equal(DbType.Int64, param.DbType);
    }

    [Fact]
    public void Parameter_ToWireString_Conversions()
    {
        Assert.Equal("42", new PyroSqlParameter("@p", 42).ToWireString());
        Assert.Equal("3.14", new PyroSqlParameter("@p", 3.14).ToWireString());
        Assert.Equal("hello", new PyroSqlParameter("@p", "hello").ToWireString());
        Assert.Equal("1", new PyroSqlParameter("@p", true).ToWireString());
        Assert.Equal("0", new PyroSqlParameter("@p", false).ToWireString());
        Assert.Equal("", new PyroSqlParameter("@p", null).ToWireString());
        Assert.Equal("", new PyroSqlParameter("@p", DBNull.Value).ToWireString());
    }

    [Fact]
    public void Parameter_ToWireString_Long()
    {
        Assert.Equal("9999999999", new PyroSqlParameter("@p", 9999999999L).ToWireString());
    }

    [Fact]
    public void Parameter_ToWireString_ByteArray()
    {
        var bytes = new byte[] { 0x01, 0x02, 0x03 };
        var result = new PyroSqlParameter("@p", bytes).ToWireString();
        Assert.Equal(Convert.ToBase64String(bytes), result);
    }

    [Fact]
    public void Parameter_ResetDbType_SetsToString()
    {
        var param = new PyroSqlParameter("@p", DbType.Int64);
        param.ResetDbType();
        Assert.Equal(DbType.String, param.DbType);
    }

    [Fact]
    public void ParameterCollection_AddAndAccess()
    {
        var coll = new PyroSqlParameterCollection();

        coll.Add("@id", 1);
        coll.Add("@name", "test");

        Assert.Equal(2, coll.Count);
        Assert.Equal("@id", coll[0].ParameterName);
        Assert.Equal("@name", coll[1].ParameterName);
        Assert.Equal(1, coll["@id"].Value);
        Assert.Equal("test", coll["@name"].Value);
    }

    [Fact]
    public void ParameterCollection_IndexOfByName()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@first", 1);
        coll.Add("@second", 2);

        Assert.Equal(0, coll.IndexOf("@first"));
        Assert.Equal(1, coll.IndexOf("@second"));
        Assert.Equal(-1, coll.IndexOf("@nonexistent"));
    }

    [Fact]
    public void ParameterCollection_Contains()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@id", 1);

        Assert.True(coll.Contains("@id"));
        Assert.False(coll.Contains("@missing"));
    }

    [Fact]
    public void ParameterCollection_Remove()
    {
        var coll = new PyroSqlParameterCollection();
        var p = coll.Add("@id", 1);
        coll.Add("@name", "test");

        coll.Remove(p);
        Assert.Equal(1, coll.Count);
        Assert.Equal("@name", coll[0].ParameterName);
    }

    [Fact]
    public void ParameterCollection_RemoveAt()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@a", 1);
        coll.Add("@b", 2);
        coll.Add("@c", 3);

        coll.RemoveAt(1);
        Assert.Equal(2, coll.Count);
        Assert.Equal("@a", coll[0].ParameterName);
        Assert.Equal("@c", coll[1].ParameterName);
    }

    [Fact]
    public void ParameterCollection_RemoveAtByName()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@a", 1);
        coll.Add("@b", 2);

        coll.RemoveAt("@a");
        Assert.Equal(1, coll.Count);
        Assert.Equal("@b", coll[0].ParameterName);
    }

    [Fact]
    public void ParameterCollection_Clear()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@a", 1);
        coll.Add("@b", 2);

        coll.Clear();
        Assert.Equal(0, coll.Count);
    }

    [Fact]
    public void ParameterCollection_Insert()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@a", 1);
        coll.Add("@c", 3);

        coll.Insert(1, new PyroSqlParameter("@b", 2));
        Assert.Equal(3, coll.Count);
        Assert.Equal("@b", coll[1].ParameterName);
    }

    [Fact]
    public void ParameterCollection_CaseInsensitiveLookup()
    {
        var coll = new PyroSqlParameterCollection();
        coll.Add("@MyParam", 42);

        Assert.Equal(0, coll.IndexOf("@myparam"));
        Assert.Equal(0, coll.IndexOf("@MYPARAM"));
        Assert.True(coll.Contains("@myparam"));
    }

    // --- PWireCodec Tests ---

    [Fact]
    public void Codec_EncodeAuth_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodeAuth("user", "pass");

        Assert.Equal(PWireCodec.MSG_AUTH, frame[0]);
        uint payloadLen = BitConverter.ToUInt32(frame, 1);
        Assert.Equal((uint)(1 + 4 + 1 + 4), payloadLen); // 1+user+1+pass
        Assert.Equal(5 + payloadLen, (uint)frame.Length);

        // Verify username
        int pos = 5;
        Assert.Equal(4, frame[pos]); // username length
        Assert.Equal("user", Encoding.UTF8.GetString(frame, pos + 1, 4));
        pos += 5;
        Assert.Equal(4, frame[pos]); // password length
        Assert.Equal("pass", Encoding.UTF8.GetString(frame, pos + 1, 4));
    }

    [Fact]
    public void Codec_EncodeQuery_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodeQuery("SELECT 1");

        Assert.Equal(PWireCodec.MSG_QUERY, frame[0]);
        uint payloadLen = BitConverter.ToUInt32(frame, 1);
        Assert.Equal("SELECT 1", Encoding.UTF8.GetString(frame, 5, (int)payloadLen));
    }

    [Fact]
    public void Codec_EncodePrepare_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodePrepare("SELECT $1");

        Assert.Equal(PWireCodec.MSG_PREPARE, frame[0]);
    }

    [Fact]
    public void Codec_EncodeExecute_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodeExecute(42, new[] { "hello", "world" });

        Assert.Equal(PWireCodec.MSG_EXECUTE, frame[0]);
        uint payloadLen = BitConverter.ToUInt32(frame, 1);
        int pos = 5;

        uint handle = BitConverter.ToUInt32(frame, pos);
        Assert.Equal(42u, handle);
        pos += 4;

        ushort paramCount = BitConverter.ToUInt16(frame, pos);
        Assert.Equal(2, paramCount);
        pos += 2;

        ushort p1Len = BitConverter.ToUInt16(frame, pos);
        pos += 2;
        Assert.Equal("hello", Encoding.UTF8.GetString(frame, pos, p1Len));
        pos += p1Len;

        ushort p2Len = BitConverter.ToUInt16(frame, pos);
        pos += 2;
        Assert.Equal("world", Encoding.UTF8.GetString(frame, pos, p2Len));
    }

    [Fact]
    public void Codec_EncodeClose_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodeClose(123);

        Assert.Equal(PWireCodec.MSG_CLOSE, frame[0]);
        Assert.Equal(123u, BitConverter.ToUInt32(frame, 5));
    }

    [Fact]
    public void Codec_EncodePing_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodePing();

        Assert.Equal(PWireCodec.MSG_PING, frame[0]);
        Assert.Equal(0u, BitConverter.ToUInt32(frame, 1));
    }

    [Fact]
    public void Codec_EncodeQuit_ProducesValidFrame()
    {
        var frame = PWireCodec.EncodeQuit();

        Assert.Equal(PWireCodec.MSG_QUIT, frame[0]);
        Assert.Equal(0u, BitConverter.ToUInt32(frame, 1));
    }

    [Fact]
    public void Codec_DecodeFrame_ValidFrame()
    {
        var original = PWireCodec.EncodeQuery("test");
        var (type, payload) = PWireCodec.DecodeFrame(original, 0, original.Length);

        Assert.Equal(PWireCodec.MSG_QUERY, type);
        Assert.Equal("test", Encoding.UTF8.GetString(payload));
    }

    [Fact]
    public void Codec_DecodeFrame_IncompleteHeader_Throws()
    {
        Assert.Throws<PyroSqlException>(() => PWireCodec.DecodeFrame(new byte[3], 0, 3));
    }

    [Fact]
    public void Codec_DecodeFrame_IncompletePayload_Throws()
    {
        var frame = new byte[5];
        frame[0] = PWireCodec.MSG_QUERY;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(1), 100); // claims 100 bytes payload

        Assert.Throws<PyroSqlException>(() => PWireCodec.DecodeFrame(frame, 0, frame.Length));
    }

    [Fact]
    public void Codec_DecodeOk_ValidPayload()
    {
        var payload = new byte[9 + 6];
        BinaryPrimitives.WriteInt64LittleEndian(payload, 42);
        payload[8] = 6; // tag length
        Encoding.UTF8.GetBytes("INSERT").CopyTo(payload.AsSpan(9));

        var ok = PWireCodec.DecodeOk(payload);
        Assert.Equal(42, ok.RowsAffected);
        Assert.Equal("INSERT", ok.Tag);
    }

    [Fact]
    public void Codec_DecodeOk_MalformedThrows()
    {
        Assert.Throws<PyroSqlException>(() => PWireCodec.DecodeOk(new byte[5]));
    }

    [Fact]
    public void Codec_DecodeError_ValidPayload()
    {
        var sqlstate = Encoding.ASCII.GetBytes("42P01");
        var msg = Encoding.UTF8.GetBytes("table not found");
        var payload = new byte[5 + 2 + msg.Length];
        sqlstate.CopyTo(payload, 0);
        BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(5), (ushort)msg.Length);
        msg.CopyTo(payload, 7);

        var err = PWireCodec.DecodeError(payload);
        Assert.Equal("42P01", err.SqlState);
        Assert.Equal("table not found", err.Message);
    }

    [Fact]
    public void Codec_DecodeError_MalformedThrows()
    {
        Assert.Throws<PyroSqlException>(() => PWireCodec.DecodeError(new byte[3]));
    }

    [Fact]
    public void Codec_DecodeResultSet_AllTypes()
    {
        // Build a result set with columns: id(I64), value(F64), name(TEXT), active(BOOL), data(BYTES)
        using var ms = new MemoryStream();
        var buf2 = new byte[2];
        var buf4 = new byte[4];
        var buf8 = new byte[8];

        // col_count = 5
        BinaryPrimitives.WriteUInt16LittleEndian(buf2, 5);
        ms.Write(buf2);

        // Column definitions
        void WriteCol(string name, byte typeTag)
        {
            var nameBytes = Encoding.UTF8.GetBytes(name);
            ms.WriteByte((byte)nameBytes.Length);
            ms.Write(nameBytes);
            ms.WriteByte(typeTag);
        }

        WriteCol("id", PWireCodec.TYPE_I64);
        WriteCol("value", PWireCodec.TYPE_F64);
        WriteCol("name", PWireCodec.TYPE_TEXT);
        WriteCol("active", PWireCodec.TYPE_BOOL);
        WriteCol("data", PWireCodec.TYPE_BYTES);

        // row_count = 1
        BinaryPrimitives.WriteUInt32LittleEndian(buf4, 1);
        ms.Write(buf4);

        // Null bitmap: 1 byte, no nulls (all zeros)
        ms.WriteByte(0);

        // Row values
        // I64: 12345
        BinaryPrimitives.WriteInt64LittleEndian(buf8, 12345);
        ms.Write(buf8);

        // F64: 3.14
        BitConverter.TryWriteBytes(buf8, 3.14);
        ms.Write(buf8);

        // TEXT: "hello"
        var textBytes = Encoding.UTF8.GetBytes("hello");
        BinaryPrimitives.WriteUInt16LittleEndian(buf2, (ushort)textBytes.Length);
        ms.Write(buf2);
        ms.Write(textBytes);

        // BOOL: true
        ms.WriteByte(1);

        // BYTES: [0xDE, 0xAD]
        BinaryPrimitives.WriteUInt16LittleEndian(buf2, 2);
        ms.Write(buf2);
        ms.Write(new byte[] { 0xDE, 0xAD });

        var payload = ms.ToArray();
        var rs = PWireCodec.DecodeResultSet(payload);

        Assert.Equal(5, rs.Columns.Length);
        Assert.Equal(1, rs.Rows.Length);

        Assert.Equal("id", rs.Columns[0].Name);
        Assert.Equal(PWireCodec.TYPE_I64, rs.Columns[0].TypeTag);
        Assert.Equal("value", rs.Columns[1].Name);
        Assert.Equal("name", rs.Columns[2].Name);
        Assert.Equal("active", rs.Columns[3].Name);
        Assert.Equal("data", rs.Columns[4].Name);

        Assert.Equal(12345L, rs.Rows[0][0]);
        Assert.Equal(3.14, (double)rs.Rows[0][1]!, 10);
        Assert.Equal("hello", rs.Rows[0][2]);
        Assert.Equal(true, rs.Rows[0][3]);
        Assert.Equal(new byte[] { 0xDE, 0xAD }, (byte[])rs.Rows[0][4]!);
    }

    [Fact]
    public void Codec_DecodeResultSet_WithNulls()
    {
        using var ms = new MemoryStream();
        var buf2 = new byte[2];
        var buf4 = new byte[4];

        // 2 columns
        BinaryPrimitives.WriteUInt16LittleEndian(buf2, 2);
        ms.Write(buf2);

        // Column: "a" I64
        var aBytes = Encoding.UTF8.GetBytes("a");
        ms.WriteByte((byte)aBytes.Length);
        ms.Write(aBytes);
        ms.WriteByte(PWireCodec.TYPE_I64);

        // Column: "b" TEXT
        var bBytes = Encoding.UTF8.GetBytes("b");
        ms.WriteByte((byte)bBytes.Length);
        ms.Write(bBytes);
        ms.WriteByte(PWireCodec.TYPE_TEXT);

        // 1 row
        BinaryPrimitives.WriteUInt32LittleEndian(buf4, 1);
        ms.Write(buf4);

        // Null bitmap: bit 1 set (column "b" is null)
        ms.WriteByte(0b00000010);

        // Column "a": I64 = 99
        var buf8 = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buf8, 99);
        ms.Write(buf8);

        // Column "b" is null, no data written

        var payload = ms.ToArray();
        var rs = PWireCodec.DecodeResultSet(payload);

        Assert.Equal(2, rs.Columns.Length);
        Assert.Equal(1, rs.Rows.Length);
        Assert.Equal(99L, rs.Rows[0][0]);
        Assert.Null(rs.Rows[0][1]);
    }

    [Fact]
    public void Codec_DecodeResultSet_EmptyResultSet()
    {
        using var ms = new MemoryStream();
        var buf2 = new byte[2];
        var buf4 = new byte[4];

        // 1 column
        BinaryPrimitives.WriteUInt16LittleEndian(buf2, 1);
        ms.Write(buf2);

        var nameBytes = Encoding.UTF8.GetBytes("x");
        ms.WriteByte((byte)nameBytes.Length);
        ms.Write(nameBytes);
        ms.WriteByte(PWireCodec.TYPE_TEXT);

        // 0 rows
        BinaryPrimitives.WriteUInt32LittleEndian(buf4, 0);
        ms.Write(buf4);

        var payload = ms.ToArray();
        var rs = PWireCodec.DecodeResultSet(payload);

        Assert.Single(rs.Columns);
        Assert.Empty(rs.Rows);
    }

    [Fact]
    public void Codec_DecodeResultSet_MalformedThrows()
    {
        Assert.Throws<PyroSqlException>(() => PWireCodec.DecodeResultSet(new byte[1]));
    }

    [Fact]
    public void DataReader_GetFieldType_ReturnsCorrectTypes()
    {
        var columns = new[]
        {
            new ColumnInfo("a", PWireCodec.TYPE_I64),
            new ColumnInfo("b", PWireCodec.TYPE_F64),
            new ColumnInfo("c", PWireCodec.TYPE_TEXT),
            new ColumnInfo("d", PWireCodec.TYPE_BOOL),
            new ColumnInfo("e", PWireCodec.TYPE_BYTES),
        };
        var rs = new ResultSet(columns, Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.Equal(typeof(double), reader.GetFieldType(1));
        Assert.Equal(typeof(string), reader.GetFieldType(2));
        Assert.Equal(typeof(bool), reader.GetFieldType(3));
        Assert.Equal(typeof(byte[]), reader.GetFieldType(4));
    }

    [Fact]
    public void DataReader_GetDataTypeName_ReturnsCorrectNames()
    {
        var columns = new[]
        {
            new ColumnInfo("a", PWireCodec.TYPE_I64),
            new ColumnInfo("b", PWireCodec.TYPE_F64),
            new ColumnInfo("c", PWireCodec.TYPE_TEXT),
            new ColumnInfo("d", PWireCodec.TYPE_BOOL),
            new ColumnInfo("e", PWireCodec.TYPE_BYTES),
        };
        var rs = new ResultSet(columns, Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.Equal("bigint", reader.GetDataTypeName(0));
        Assert.Equal("double", reader.GetDataTypeName(1));
        Assert.Equal("text", reader.GetDataTypeName(2));
        Assert.Equal("boolean", reader.GetDataTypeName(3));
        Assert.Equal("blob", reader.GetDataTypeName(4));
    }

    [Fact]
    public void DataReader_ReadAndGetValues()
    {
        var columns = new[]
        {
            new ColumnInfo("id", PWireCodec.TYPE_I64),
            new ColumnInfo("name", PWireCodec.TYPE_TEXT),
        };
        var rows = new[]
        {
            new object?[] { 1L, "Alice" },
            new object?[] { 2L, "Bob" },
        };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.HasRows);
        Assert.Equal(2, reader.FieldCount);

        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal("Alice", reader.GetString(1));

        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.Equal("Bob", reader.GetString(1));

        Assert.False(reader.Read());
    }

    [Fact]
    public void DataReader_GetOrdinal_CaseInsensitive()
    {
        var columns = new[] { new ColumnInfo("MyColumn", PWireCodec.TYPE_TEXT) };
        var rs = new ResultSet(columns, Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.Equal(0, reader.GetOrdinal("mycolumn"));
        Assert.Equal(0, reader.GetOrdinal("MYCOLUMN"));
        Assert.Equal(0, reader.GetOrdinal("MyColumn"));
    }

    [Fact]
    public void DataReader_GetOrdinal_ThrowsForUnknown()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_TEXT) };
        var rs = new ResultSet(columns, Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("nonexistent"));
    }

    [Fact]
    public void DataReader_IsDBNull()
    {
        var columns = new[]
        {
            new ColumnInfo("a", PWireCodec.TYPE_TEXT),
            new ColumnInfo("b", PWireCodec.TYPE_I64),
        };
        var rows = new[] { new object?[] { null, 42L } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
        Assert.False(reader.IsDBNull(1));
    }

    [Fact]
    public void DataReader_GetValue_ReturnsDBNullForNull()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_TEXT) };
        var rows = new[] { new object?[] { null } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        Assert.Equal(DBNull.Value, reader.GetValue(0));
    }

    [Fact]
    public void DataReader_IndexerByName()
    {
        var columns = new[]
        {
            new ColumnInfo("id", PWireCodec.TYPE_I64),
            new ColumnInfo("name", PWireCodec.TYPE_TEXT),
        };
        var rows = new[] { new object?[] { 1L, "Alice" } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        Assert.Equal(1L, reader["id"]);
        Assert.Equal("Alice", reader["name"]);
    }

    [Fact]
    public void DataReader_GetValues_FillsArray()
    {
        var columns = new[]
        {
            new ColumnInfo("a", PWireCodec.TYPE_I64),
            new ColumnInfo("b", PWireCodec.TYPE_TEXT),
        };
        var rows = new[] { new object?[] { 10L, "x" } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        var values = new object[2];
        int count = reader.GetValues(values);
        Assert.Equal(2, count);
        Assert.Equal(10L, values[0]);
        Assert.Equal("x", values[1]);
    }

    [Fact]
    public void DataReader_GetInt32_ConvertsFromInt64()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_I64) };
        var rows = new[] { new object?[] { 42L } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        Assert.Equal(42, reader.GetInt32(0));
    }

    [Fact]
    public void DataReader_GetDouble()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_F64) };
        var rows = new[] { new object?[] { 2.718 } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        Assert.Equal(2.718, reader.GetDouble(0), 10);
    }

    [Fact]
    public void DataReader_GetBoolean()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_BOOL) };
        var rows = new[] { new object?[] { true } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));
    }

    [Fact]
    public void DataReader_GetBytes()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_BYTES) };
        var data = new byte[] { 1, 2, 3, 4, 5 };
        var rows = new[] { new object?[] { data } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.True(reader.Read());

        // Get total length
        Assert.Equal(5, reader.GetBytes(0, 0, null, 0, 0));

        // Read partial
        var buffer = new byte[3];
        long read = reader.GetBytes(0, 1, buffer, 0, 3);
        Assert.Equal(3, read);
        Assert.Equal(new byte[] { 2, 3, 4 }, buffer);
    }

    [Fact]
    public void DataReader_GetSchemaTable()
    {
        var columns = new[]
        {
            new ColumnInfo("id", PWireCodec.TYPE_I64),
            new ColumnInfo("name", PWireCodec.TYPE_TEXT),
        };
        var rs = new ResultSet(columns, Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        var schema = reader.GetSchemaTable();
        Assert.NotNull(schema);
        Assert.Equal(2, schema!.Rows.Count);
        Assert.Equal("id", schema.Rows[0]["ColumnName"]);
        Assert.Equal(typeof(long), schema.Rows[0]["DataType"]);
        Assert.Equal("name", schema.Rows[1]["ColumnName"]);
        Assert.Equal(typeof(string), schema.Rows[1]["DataType"]);
    }

    [Fact]
    public void DataReader_ReadBeforeFirst_ThrowsOnGetValue()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_I64) };
        var rows = new[] { new object?[] { 1L } };
        var rs = new ResultSet(columns, rows);
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        // Before first Read()
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
    }

    [Fact]
    public void DataReader_ClosedReader_ThrowsOnRead()
    {
        var columns = new[] { new ColumnInfo("a", PWireCodec.TYPE_I64) };
        var rs = new ResultSet(columns, Array.Empty<object?[]>());
        var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);
        reader.Close();

        Assert.True(reader.IsClosed);
        Assert.Throws<InvalidOperationException>(() => reader.Read());
    }

    [Fact]
    public void DataReader_NextResult_ReturnsFalse()
    {
        var rs = new ResultSet(Array.Empty<ColumnInfo>(), Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.False(reader.NextResult());
    }

    [Fact]
    public void DataReader_Depth_IsZero()
    {
        var rs = new ResultSet(Array.Empty<ColumnInfo>(), Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.Equal(0, reader.Depth);
    }

    [Fact]
    public void DataReader_RecordsAffected_IsNegativeOne()
    {
        var rs = new ResultSet(Array.Empty<ColumnInfo>(), Array.Empty<object?[]>());
        using var reader = new PyroSqlDataReader(rs, CommandBehavior.Default, null);

        Assert.Equal(-1, reader.RecordsAffected);
    }

    // ---- LZ4 compression unit tests ----

    [Fact]
    public void CompressFrame_SmallPayload_NotCompressed()
    {
        var smallPayload = new byte[100];
        Array.Fill(smallPayload, (byte)'A');
        var frame = PWireCodec.CompressFrame(PWireCodec.MSG_QUERY, smallPayload);
        Assert.Equal(PWireCodec.MSG_QUERY, frame[0]);
    }

    [Fact]
    public void CompressFrame_LargePayload_Compressed()
    {
        var largePayload = new byte[16 * 1024];
        Array.Fill(largePayload, (byte)'X');
        var frame = PWireCodec.CompressFrame(PWireCodec.MSG_QUERY, largePayload);
        Assert.Equal(PWireCodec.MSG_COMPRESSED, frame[0]);
        Assert.True(frame.Length < largePayload.Length, "Compressed frame should be smaller");
    }

    [Fact]
    public void CompressDecompress_RoundTrip()
    {
        var original = new byte[16 * 1024];
        for (int i = 0; i < original.Length; i++)
            original[i] = (byte)(i % 26 + 'a');

        var frame = PWireCodec.CompressFrame(PWireCodec.MSG_QUERY, original);
        Assert.Equal(PWireCodec.MSG_COMPRESSED, frame[0]);

        // Extract the inner payload (skip the 5-byte header)
        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(1, 4));
        var payload = new byte[payloadLen];
        Array.Copy(frame, PWireCodec.HEADER_SIZE, payload, 0, (int)payloadLen);

        var (origType, decompressed) = PWireCodec.DecompressFrame(payload);
        Assert.Equal(PWireCodec.MSG_QUERY, origType);
        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void EncodeAuthWithCaps_IncludesCapsByte()
    {
        var frame = PWireCodec.EncodeAuthWithCaps("admin", "secret", PWireCodec.CAP_LZ4);
        Assert.Equal(PWireCodec.MSG_AUTH, frame[0]);
        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(frame.AsSpan(1, 4));
        // user(1+5) + pass(1+6) + caps(1) = 14
        Assert.Equal(14u, payloadLen);
        // Last byte of payload is the caps
        Assert.Equal(PWireCodec.CAP_LZ4, frame[PWireCodec.HEADER_SIZE + 13]);
    }
}
