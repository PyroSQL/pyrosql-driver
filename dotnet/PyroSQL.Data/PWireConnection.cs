using System.Net.Sockets;

namespace PyroSQL.Data;

/// <summary>
/// Low-level TCP connection that speaks the PWire binary protocol.
/// </summary>
internal sealed class PWireConnection : IDisposable
{
    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private readonly byte[] _headerBuf = new byte[PWireCodec.HEADER_SIZE];
    private bool _supportsLZ4;

    public bool IsConnected => _tcp?.Connected == true;
    public bool SupportsLZ4 => _supportsLZ4;

    public void Connect(string host, int port, int timeoutMs)
    {
        _tcp = new TcpClient();
        var connectTask = _tcp.ConnectAsync(host, port);
        if (!connectTask.Wait(timeoutMs))
        {
            _tcp.Dispose();
            _tcp = null;
            throw new PyroSqlException($"Connection to {host}:{port} timed out after {timeoutMs}ms");
        }
        if (connectTask.IsFaulted)
        {
            _tcp.Dispose();
            _tcp = null;
            throw new PyroSqlException($"Failed to connect to {host}:{port}", connectTask.Exception!.InnerException!);
        }
        _stream = _tcp.GetStream();
    }

    public void Authenticate(string user, string password)
    {
        SendRaw(PWireCodec.EncodeAuthWithCaps(user, password, PWireCodec.CAP_LZ4));
        var (type, payload) = ReceiveFrame();

        if (type == PWireCodec.RESP_ERROR)
        {
            var err = PWireCodec.DecodeError(payload);
            throw new PyroSqlException(err.SqlState, err.Message);
        }

        if (type != PWireCodec.RESP_READY && type != PWireCodec.RESP_OK)
            throw new PyroSqlException($"Expected READY after auth, got response type 0x{type:X2}");

        // Check server caps in READY payload (first 4 bytes = u32 LE caps).
        if (type == PWireCodec.RESP_READY && payload.Length >= 4)
        {
            uint serverCaps = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(0, 4));
            _supportsLZ4 = (serverCaps & PWireCodec.CAP_LZ4) != 0;
        }
    }

    public (byte Type, byte[] Payload) SendAndReceive(byte[] frame)
    {
        if (_supportsLZ4 && frame.Length > PWireCodec.HEADER_SIZE)
        {
            byte msgType = frame[0];
            var payload = frame.AsSpan(PWireCodec.HEADER_SIZE);
            frame = PWireCodec.CompressFrame(msgType, payload);
        }
        SendRaw(frame);
        return ReceiveFrame();
    }

    public void SendRaw(byte[] data)
    {
        var stream = _stream ?? throw new PyroSqlException("Not connected");
        stream.Write(data, 0, data.Length);
        stream.Flush();
    }

    public (byte Type, byte[] Payload) ReceiveFrame()
    {
        var stream = _stream ?? throw new PyroSqlException("Not connected");
        ReadExact(stream, _headerBuf, 0, PWireCodec.HEADER_SIZE);

        byte type = _headerBuf[0];
        uint length = BitConverter.ToUInt32(_headerBuf, 1);

        var payload = new byte[length];
        if (length > 0)
            ReadExact(stream, payload, 0, (int)length);

        // Transparently decompress MSG_COMPRESSED frames.
        if (type == PWireCodec.MSG_COMPRESSED)
        {
            var (origType, decompressed) = PWireCodec.DecompressFrame(payload);
            return (origType, decompressed);
        }

        return (type, payload);
    }

    private static void ReadExact(NetworkStream stream, byte[] buffer, int offset, int count)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = stream.Read(buffer, offset + totalRead, count - totalRead);
            if (read == 0)
                throw new PyroSqlException("Connection closed by server");
            totalRead += read;
        }
    }

    public void SendQuit()
    {
        try
        {
            if (_stream != null && _tcp?.Connected == true)
                SendRaw(PWireCodec.EncodeQuit());
        }
        catch
        {
            // Best effort
        }
    }

    public void Dispose()
    {
        _stream?.Dispose();
        _stream = null;
        _tcp?.Dispose();
        _tcp = null;
    }
}
