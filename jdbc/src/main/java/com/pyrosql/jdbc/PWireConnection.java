package com.pyrosql.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.List;

/**
 * Low-level TCP connection that speaks the PWire binary protocol.
 * Manages socket I/O, frame reading, and message send/receive.
 */
public class PWireConnection implements AutoCloseable {

    private Socket socket;
    private OutputStream out;
    private InputStream in;
    private final byte[] readBuffer = new byte[65536];
    private int bufPos = 0;
    private int bufLen = 0;
    private boolean supportsLZ4 = false;

    public PWireConnection(String host, int port, int timeoutMillis) throws SQLException {
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(timeoutMillis > 0 ? timeoutMillis : 30000);
            socket.connect(new InetSocketAddress(host, port), timeoutMillis > 0 ? timeoutMillis : 30000);
            out = socket.getOutputStream();
            in = socket.getInputStream();
        } catch (IOException e) {
            throw new SQLException("Failed to connect to PyroSQL at " + host + ":" + port, "08001", e);
        }
    }

    public void authenticate(String user, String password) throws SQLException {
        sendRaw(PWireCodec.encodeAuthWithCaps(user, password, PWireCodec.CAP_LZ4));
        PWireCodec.Frame frame = readFrame();
        if (frame.type == PWireCodec.RESP_ERROR) {
            PWireCodec.ErrorResult err = PWireCodec.decodeError(frame.payload);
            throw new SQLException("Authentication failed: " + err.message, err.sqlState);
        }
        if (frame.type != PWireCodec.RESP_READY && frame.type != PWireCodec.RESP_OK) {
            throw new SQLException("Unexpected response during auth: type=" + frame.type, "08001");
        }
        // Check server caps in the READY payload (first 4 bytes = u32 LE caps).
        if (frame.type == PWireCodec.RESP_READY && frame.payload != null && frame.payload.length >= 4) {
            int serverCaps = (frame.payload[0] & 0xFF)
                    | ((frame.payload[1] & 0xFF) << 8)
                    | ((frame.payload[2] & 0xFF) << 16)
                    | ((frame.payload[3] & 0xFF) << 24);
            supportsLZ4 = (serverCaps & PWireCodec.CAP_LZ4) != 0;
        }
    }

    public PWireCodec.Frame query(String sql) throws SQLException {
        sendCompressed(PWireCodec.MSG_QUERY, sql.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return readFrame();
    }

    /**
     * Send MSG_PREPARE with SQL template, receive RESP_READY with u32 handle.
     */
    public int prepare(String sql) throws SQLException {
        sendCompressed(PWireCodec.MSG_PREPARE, sql.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        PWireCodec.Frame frame = readFrame();
        if (frame.type == PWireCodec.RESP_ERROR) {
            PWireCodec.ErrorResult err = PWireCodec.decodeError(frame.payload);
            throw new SQLException(err.message, err.sqlState);
        }
        if (frame.type != PWireCodec.RESP_READY) {
            throw new SQLException("Unexpected response for PREPARE: type=" + frame.type);
        }
        return PWireCodec.decodeReady(frame.payload);
    }

    /**
     * Send MSG_EXECUTE with u32 handle + params, return the response frame.
     */
    public PWireCodec.Frame execute(int handle, List<String> params) throws SQLException {
        byte[] fullFrame = PWireCodec.encodeExecute(handle, params);
        byte[] payload = java.util.Arrays.copyOfRange(fullFrame, PWireCodec.HEADER_SIZE, fullFrame.length);
        sendCompressed(PWireCodec.MSG_EXECUTE, payload);
        return readFrame();
    }

    /**
     * Send MSG_CLOSE with u32 handle.
     */
    public void closeHandle(int handle) throws SQLException {
        byte[] fullFrame = PWireCodec.encodeClose(handle);
        byte[] payload = java.util.Arrays.copyOfRange(fullFrame, PWireCodec.HEADER_SIZE, fullFrame.length);
        sendCompressed(PWireCodec.MSG_CLOSE, payload);
        PWireCodec.Frame frame = readFrame();
        if (frame.type == PWireCodec.RESP_ERROR) {
            PWireCodec.ErrorResult err = PWireCodec.decodeError(frame.payload);
            throw new SQLException(err.message, err.sqlState);
        }
    }

    public void ping() throws SQLException {
        sendCompressed(PWireCodec.MSG_PING, new byte[0]);
        PWireCodec.Frame frame = readFrame();
        if (frame.type != PWireCodec.RESP_PONG) {
            throw new SQLException("Unexpected response for PING: type=" + frame.type);
        }
    }

    public void quit() throws SQLException {
        try {
            sendRaw(PWireCodec.encodeQuit());
        } catch (SQLException ignored) {
            // best effort
        }
    }

    /**
     * Send a pre-built frame, optionally compressing it if LZ4 is negotiated.
     * Used for frames that are already fully encoded (e.g., auth, ping, quit).
     */
    private void sendRaw(byte[] data) throws SQLException {
        try {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            throw new SQLException("Failed to send data to PyroSQL", "08S01", e);
        }
    }

    /**
     * Build a frame from type + payload, compressing if LZ4 is negotiated.
     */
    private void sendCompressed(int msgType, byte[] payload) throws SQLException {
        byte[] data;
        if (supportsLZ4) {
            data = PWireCodec.compressFrame(msgType, payload);
        } else {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(PWireCodec.HEADER_SIZE + payload.length)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN);
            buf.put((byte) (msgType & 0xFF));
            buf.putInt(payload.length);
            buf.put(payload);
            data = buf.array();
        }
        sendRaw(data);
    }

    private PWireCodec.Frame readFrame() throws SQLException {
        try {
            // Ensure we have at least the header
            while (available() < PWireCodec.HEADER_SIZE) {
                fillBuffer();
            }
            // Peek at payload length
            int payloadLen = ((readBuffer[bufPos + 1] & 0xFF))
                    | ((readBuffer[bufPos + 2] & 0xFF) << 8)
                    | ((readBuffer[bufPos + 3] & 0xFF) << 16)
                    | ((readBuffer[bufPos + 4] & 0xFF) << 24);
            int totalNeeded = PWireCodec.HEADER_SIZE + payloadLen;

            while (available() < totalNeeded) {
                fillBuffer();
            }

            PWireCodec.Frame frame = PWireCodec.decodeFrame(readBuffer, bufPos, available());
            bufPos += frame.totalBytes;

            // Transparently decompress MSG_COMPRESSED frames.
            if (frame.type == PWireCodec.MSG_COMPRESSED) {
                return PWireCodec.decompressFrame(frame.payload);
            }

            return frame;
        } catch (IOException e) {
            throw new SQLException("Failed to read response from PyroSQL", "08S01", e);
        }
    }

    private int available() {
        return bufLen - bufPos;
    }

    private void fillBuffer() throws IOException {
        if (bufPos > 0 && available() > 0) {
            System.arraycopy(readBuffer, bufPos, readBuffer, 0, available());
            bufLen = available();
            bufPos = 0;
        } else if (bufPos > 0) {
            bufPos = 0;
            bufLen = 0;
        }

        if (bufLen >= readBuffer.length) {
            throw new IOException("Read buffer overflow");
        }

        int n = in.read(readBuffer, bufLen, readBuffer.length - bufLen);
        if (n < 0) {
            throw new IOException("Connection closed by server");
        }
        bufLen += n;
    }

    @Override
    public void close() throws SQLException {
        try {
            quit();
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
                throw new SQLException("Error closing connection", "08S01", e);
            }
        }
    }

    public boolean isClosed() {
        return socket == null || socket.isClosed();
    }
}
