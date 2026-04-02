using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;

namespace PyroSQL.Data;

/// <summary>
/// PWire binary protocol codec for PyroSQL.
/// Header: 1 byte type + 4 bytes length (little-endian).
/// </summary>
internal static class PWireCodec
{
    // Message types (client -> server)
    public const byte MSG_QUERY   = 0x01;
    public const byte MSG_PREPARE = 0x02;
    public const byte MSG_EXECUTE = 0x03;
    public const byte MSG_CLOSE   = 0x04;
    public const byte MSG_PING    = 0x05;
    public const byte MSG_AUTH       = 0x06;
    public const byte MSG_COMPRESSED = 0x10;
    public const byte MSG_QUIT       = 0xFF;

    // Capability flags for LZ4 negotiation
    public const byte CAP_LZ4 = 0x01;
    public const int COMPRESSION_THRESHOLD = 8 * 1024;

    // Response types (server -> client)
    public const byte RESP_RESULT_SET = 0x01;
    public const byte RESP_OK        = 0x02;
    public const byte RESP_ERROR     = 0x03;
    public const byte RESP_PONG      = 0x04;
    public const byte RESP_READY     = 0x05;

    // Value types
    public const byte TYPE_NULL  = 0;
    public const byte TYPE_I64   = 1;
    public const byte TYPE_F64   = 2;
    public const byte TYPE_TEXT  = 3;
    public const byte TYPE_BOOL  = 4;
    public const byte TYPE_BYTES = 5;

    public const int HEADER_SIZE = 5;

    private static byte[] Frame(byte type, ReadOnlySpan<byte> payload)
    {
        var frame = new byte[HEADER_SIZE + payload.Length];
        frame[0] = type;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(1, 4), (uint)payload.Length);
        payload.CopyTo(frame.AsSpan(HEADER_SIZE));
        return frame;
    }

    /// <summary>
    /// Compress a frame payload with LZ4 if it exceeds the threshold and the
    /// compression ratio is beneficial (&lt; 0.9). Returns MSG_COMPRESSED frame or the original.
    /// Uses lz4_flex-compatible format: lz4_data = [u32 LE original_size][raw LZ4 block].
    /// </summary>
    public static byte[] CompressFrame(byte type, ReadOnlySpan<byte> payload)
    {
        if (payload.Length <= COMPRESSION_THRESHOLD)
            return Frame(type, payload);

        int maxLen = LZ4Codec.MaximumOutputSize(payload.Length);
        var rawCompressed = new byte[maxLen];
        int rawCompressedLen = LZ4Codec.Encode(payload, rawCompressed);
        if (rawCompressedLen <= 0)
            return Frame(type, payload);

        // lz4_flex format: prepend u32 LE original size
        int lz4DataLen = 4 + rawCompressedLen;

        double ratio = (double)lz4DataLen / payload.Length;
        if (ratio > 0.9)
            return Frame(type, payload);

        // Inner: [original_type: u8][uncompressed_length: u32 LE][lz4_data]
        // lz4_data = [u32 LE original_size][raw LZ4 block]
        var inner = new byte[1 + 4 + lz4DataLen];
        inner[0] = type;
        BinaryPrimitives.WriteUInt32LittleEndian(inner.AsSpan(1, 4), (uint)payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(inner.AsSpan(5, 4), (uint)payload.Length); // lz4_flex size
        rawCompressed.AsSpan(0, rawCompressedLen).CopyTo(inner.AsSpan(9));
        return Frame(MSG_COMPRESSED, inner);
    }

    /// <summary>
    /// Decompress a MSG_COMPRESSED frame payload.
    /// Returns (original_type, decompressed_payload).
    /// lz4_data is in lz4_flex format: [u32 LE original_size][raw LZ4 block].
    /// </summary>
    public static (byte Type, byte[] Payload) DecompressFrame(byte[] payload)
    {
        if (payload.Length < 5)
            throw new PyroSqlException("Compressed payload too short");

        byte originalType = payload[0];
        int uncompressedLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(1, 4));
        var lz4Data = payload.AsSpan(5);

        if (lz4Data.Length < 4)
            throw new PyroSqlException("LZ4 data too short");

        // Skip the 4-byte lz4_flex size prefix
        var rawBlock = lz4Data.Slice(4);

        var decompressed = new byte[uncompressedLen];
        int decoded = LZ4Codec.Decode(rawBlock, decompressed);
        if (decoded < 0)
            throw new PyroSqlException("LZ4 decompression failed");

        return (originalType, decompressed);
    }

    /// <summary>
    /// Build an AUTH frame that includes a capability byte for LZ4 negotiation.
    /// </summary>
    public static byte[] EncodeAuthWithCaps(string user, string password, byte caps)
    {
        var userBytes = Encoding.UTF8.GetBytes(user);
        var passBytes = Encoding.UTF8.GetBytes(password);
        var payload = new byte[1 + userBytes.Length + 1 + passBytes.Length + 1];
        payload[0] = (byte)userBytes.Length;
        userBytes.CopyTo(payload.AsSpan(1));
        payload[1 + userBytes.Length] = (byte)passBytes.Length;
        passBytes.CopyTo(payload.AsSpan(2 + userBytes.Length));
        payload[2 + userBytes.Length + passBytes.Length] = caps;
        return Frame(MSG_AUTH, payload);
    }

    public static byte[] EncodeAuth(string user, string password)
    {
        var userBytes = Encoding.UTF8.GetBytes(user);
        var passBytes = Encoding.UTF8.GetBytes(password);
        var payload = new byte[1 + userBytes.Length + 1 + passBytes.Length];
        payload[0] = (byte)userBytes.Length;
        userBytes.CopyTo(payload.AsSpan(1));
        payload[1 + userBytes.Length] = (byte)passBytes.Length;
        passBytes.CopyTo(payload.AsSpan(2 + userBytes.Length));
        return Frame(MSG_AUTH, payload);
    }

    public static byte[] EncodeQuery(string sql)
    {
        return Frame(MSG_QUERY, Encoding.UTF8.GetBytes(sql));
    }

    public static byte[] EncodePrepare(string sql)
    {
        return Frame(MSG_PREPARE, Encoding.UTF8.GetBytes(sql));
    }

    public static byte[] EncodeExecute(uint handle, IReadOnlyList<string> parameters)
    {
        // 4 bytes handle + 2 bytes param count + params
        using var ms = new MemoryStream();
        var buf = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, handle);
        ms.Write(buf, 0, 4);

        var countBuf = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(countBuf, (ushort)parameters.Count);
        ms.Write(countBuf, 0, 2);

        foreach (var param in parameters)
        {
            var paramBytes = Encoding.UTF8.GetBytes(param);
            var lenBuf = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(lenBuf, (ushort)paramBytes.Length);
            ms.Write(lenBuf, 0, 2);
            ms.Write(paramBytes, 0, paramBytes.Length);
        }

        return Frame(MSG_EXECUTE, ms.ToArray());
    }

    public static byte[] EncodeClose(uint handle)
    {
        var payload = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, handle);
        return Frame(MSG_CLOSE, payload);
    }

    public static byte[] EncodePing()
    {
        return Frame(MSG_PING, ReadOnlySpan<byte>.Empty);
    }

    public static byte[] EncodeQuit()
    {
        return Frame(MSG_QUIT, ReadOnlySpan<byte>.Empty);
    }

    public static (byte Type, byte[] Payload) DecodeFrame(byte[] data, int offset, int length)
    {
        if (length < HEADER_SIZE)
            throw new PyroSqlException("Incomplete frame: need at least 5 bytes");

        byte type = data[offset];
        uint payloadLen = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset + 1, 4));

        if (length < HEADER_SIZE + (int)payloadLen)
            throw new PyroSqlException($"Incomplete frame: need {HEADER_SIZE + payloadLen} bytes, have {length}");

        var payload = new byte[payloadLen];
        Array.Copy(data, offset + HEADER_SIZE, payload, 0, (int)payloadLen);
        return (type, payload);
    }

    public static ResultSet DecodeResultSet(byte[] payload)
    {
        int pos = 0;
        if (payload.Length < 2)
            throw new PyroSqlException("Malformed result set");

        ushort colCount = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(pos, 2));
        pos += 2;

        var columns = new ColumnInfo[colCount];
        for (int i = 0; i < colCount; i++)
        {
            if (pos >= payload.Length)
                throw new PyroSqlException("Malformed result set: unexpected end in column definitions");

            byte nameLen = payload[pos++];
            if (pos + nameLen + 1 > payload.Length)
                throw new PyroSqlException("Malformed result set: column name overflow");

            string name = Encoding.UTF8.GetString(payload, pos, nameLen);
            pos += nameLen;

            byte typeTag = payload[pos++];
            columns[i] = new ColumnInfo(name, typeTag);
        }

        if (pos + 4 > payload.Length)
            throw new PyroSqlException("Malformed result set: missing row count");

        uint rowCount = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(pos, 4));
        pos += 4;

        int nullBitmapLen = (colCount + 7) / 8;
        var rows = new object?[rowCount][];

        for (uint r = 0; r < rowCount; r++)
        {
            if (pos + nullBitmapLen > payload.Length)
                throw new PyroSqlException("Malformed result set: missing null bitmap");

            var bitmap = payload.AsSpan(pos, nullBitmapLen);
            pos += nullBitmapLen;

            var row = new object?[colCount];
            for (int c = 0; c < colCount; c++)
            {
                int byteIdx = c / 8;
                int bitIdx = c % 8;
                bool isNull = byteIdx < nullBitmapLen && ((bitmap[byteIdx] >> bitIdx) & 1) == 1;

                if (isNull)
                {
                    row[c] = null;
                    continue;
                }

                switch (columns[c].TypeTag)
                {
                    case TYPE_I64:
                        if (pos + 8 > payload.Length)
                            throw new PyroSqlException("Malformed result set: i64 overflow");
                        row[c] = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(pos, 8));
                        pos += 8;
                        break;

                    case TYPE_F64:
                        if (pos + 8 > payload.Length)
                            throw new PyroSqlException("Malformed result set: f64 overflow");
                        row[c] = BitConverter.ToDouble(payload, pos);
                        pos += 8;
                        break;

                    case TYPE_BOOL:
                        if (pos >= payload.Length)
                            throw new PyroSqlException("Malformed result set: bool overflow");
                        row[c] = payload[pos] != 0;
                        pos++;
                        break;

                    case TYPE_TEXT:
                    case TYPE_BYTES:
                    default:
                        if (pos + 2 > payload.Length)
                            throw new PyroSqlException("Malformed result set: text/bytes length overflow");
                        ushort len = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(pos, 2));
                        pos += 2;
                        if (pos + len > payload.Length)
                            throw new PyroSqlException("Malformed result set: text/bytes data overflow");
                        if (columns[c].TypeTag == TYPE_BYTES)
                        {
                            var bytes = new byte[len];
                            Array.Copy(payload, pos, bytes, 0, len);
                            row[c] = bytes;
                        }
                        else
                        {
                            row[c] = Encoding.UTF8.GetString(payload, pos, len);
                        }
                        pos += len;
                        break;
                }
            }
            rows[r] = row;
        }

        return new ResultSet(columns, rows);
    }

    public static OkResponse DecodeOk(byte[] payload)
    {
        if (payload.Length < 9)
            throw new PyroSqlException("Malformed OK response");

        long rowsAffected = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(0, 8));
        byte tagLen = payload[8];

        if (9 + tagLen > payload.Length)
            throw new PyroSqlException("Malformed OK response: tag overflow");

        string tag = Encoding.UTF8.GetString(payload, 9, tagLen);
        return new OkResponse(rowsAffected, tag);
    }

    public static ErrorResponse DecodeError(byte[] payload)
    {
        if (payload.Length < 7)
            throw new PyroSqlException("Malformed ERROR response");

        string sqlState = Encoding.ASCII.GetString(payload, 0, 5);
        ushort msgLen = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(5, 2));

        if (7 + msgLen > payload.Length)
            throw new PyroSqlException("Malformed ERROR response: message overflow");

        string message = Encoding.UTF8.GetString(payload, 7, msgLen);
        return new ErrorResponse(sqlState, message);
    }
}

internal readonly record struct ColumnInfo(string Name, byte TypeTag);

internal sealed class ResultSet
{
    public ColumnInfo[] Columns { get; }
    public object?[][] Rows { get; }

    public ResultSet(ColumnInfo[] columns, object?[][] rows)
    {
        Columns = columns;
        Rows = rows;
    }
}

internal readonly record struct OkResponse(long RowsAffected, string Tag);

internal readonly record struct ErrorResponse(string SqlState, string Message);
