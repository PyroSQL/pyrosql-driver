package com.pyrosql.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Encodes and decodes messages for the PWire binary protocol used by PyroSQL.
 *
 * Frame format: 1 byte type + 4 bytes payload length (little-endian) + payload
 */
public final class PWireCodec {

    // Client message types
    public static final int MSG_QUERY   = 0x01;
    public static final int MSG_PREPARE = 0x02;
    public static final int MSG_EXECUTE = 0x03;
    public static final int MSG_CLOSE   = 0x04;
    public static final int MSG_PING    = 0x05;
    public static final int MSG_AUTH    = 0x06;
    public static final int MSG_COMPRESSED = 0x10;
    public static final int MSG_QUIT    = 0xFF;

    // Server response types
    public static final int RESP_RESULT_SET = 0x01;
    public static final int RESP_OK         = 0x02;
    public static final int RESP_ERROR      = 0x03;
    public static final int RESP_PONG       = 0x04;
    public static final int RESP_READY      = 0x05;

    // Value type tags
    public static final int TYPE_NULL  = 0;
    public static final int TYPE_I64   = 1;
    public static final int TYPE_F64   = 2;
    public static final int TYPE_TEXT  = 3;
    public static final int TYPE_BOOL  = 4;
    public static final int TYPE_BYTES = 5;

    public static final int HEADER_SIZE = 5;

    // LZ4 compression constants
    public static final byte CAP_LZ4 = 0x01;
    public static final int COMPRESSION_THRESHOLD = 8 * 1024;

    private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();

    private PWireCodec() {}

    // ---- LZ4 compression ----

    /**
     * Compress a frame payload with LZ4 if it exceeds the threshold and compression
     * is beneficial (ratio < 0.9). Returns a MSG_COMPRESSED frame or the original frame.
     *
     * Uses lz4_flex-compatible format: the lz4_data within the inner frame is
     * [u32 LE original_size][raw LZ4 block].
     */
    public static byte[] compressFrame(int msgType, byte[] payload) {
        if (payload.length <= COMPRESSION_THRESHOLD) {
            return frame(msgType, payload);
        }

        LZ4Compressor compressor = lz4Factory.fastCompressor();
        int maxLen = compressor.maxCompressedLength(payload.length);
        byte[] rawCompressed = new byte[maxLen];
        int rawCompressedLen = compressor.compress(payload, 0, payload.length, rawCompressed, 0, maxLen);

        // lz4_flex format: prepend u32 LE original size
        int lz4DataLen = 4 + rawCompressedLen;

        double ratio = (double) lz4DataLen / payload.length;
        if (ratio > 0.9) {
            return frame(msgType, payload);
        }

        // Inner payload: [original_type: u8][uncompressed_length: u32 LE][lz4_data]
        // lz4_data = [u32 LE original_size][raw LZ4 block]
        ByteBuffer inner = ByteBuffer.allocate(1 + 4 + lz4DataLen).order(ByteOrder.LITTLE_ENDIAN);
        inner.put((byte) (msgType & 0xFF));
        inner.putInt(payload.length);
        inner.putInt(payload.length); // lz4_flex prepended size
        inner.put(rawCompressed, 0, rawCompressedLen);
        return frame(MSG_COMPRESSED, inner.array());
    }

    /**
     * Decompress a MSG_COMPRESSED frame payload.
     * Returns a Frame with the original type and decompressed payload.
     *
     * lz4_data is in lz4_flex format: [u32 LE original_size][raw LZ4 block].
     */
    public static Frame decompressFrame(byte[] payload) {
        if (payload.length < 5) {
            throw new PWireException("Compressed payload too short");
        }
        int originalType = payload[0] & 0xFF;
        int uncompressedLen = ByteBuffer.wrap(payload, 1, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();

        // lz4_data starts at offset 5, first 4 bytes are the lz4_flex prepended size
        if (payload.length < 9) {
            throw new PWireException("LZ4 data too short");
        }
        // Skip the 4-byte lz4_flex size prefix
        int lz4BlockOffset = 9;
        int lz4BlockLen = payload.length - lz4BlockOffset;

        LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();
        byte[] decompressed = new byte[uncompressedLen];
        decompressor.decompress(payload, lz4BlockOffset, decompressed, 0, uncompressedLen);
        return new Frame(originalType, decompressed, 0);
    }

    /**
     * Build an AUTH frame that includes the capability byte for LZ4 negotiation.
     */
    public static byte[] encodeAuthWithCaps(String user, String password, byte caps) {
        byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);
        byte[] passBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + userBytes.length + 1 + passBytes.length + 1];
        payload[0] = (byte) userBytes.length;
        System.arraycopy(userBytes, 0, payload, 1, userBytes.length);
        payload[1 + userBytes.length] = (byte) passBytes.length;
        System.arraycopy(passBytes, 0, payload, 2 + userBytes.length, passBytes.length);
        payload[2 + userBytes.length + passBytes.length] = caps;
        return frame(MSG_AUTH, payload);
    }

    // ---- Encoding helpers ----

    private static byte[] frame(int type, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) (type & 0xFF));
        buf.putInt(payload.length);
        buf.put(payload);
        return buf.array();
    }

    public static byte[] encodeAuth(String user, String password) {
        byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);
        byte[] passBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + userBytes.length + 1 + passBytes.length];
        payload[0] = (byte) userBytes.length;
        System.arraycopy(userBytes, 0, payload, 1, userBytes.length);
        payload[1 + userBytes.length] = (byte) passBytes.length;
        System.arraycopy(passBytes, 0, payload, 2 + userBytes.length, passBytes.length);
        return frame(MSG_AUTH, payload);
    }

    public static byte[] encodeQuery(String sql) {
        return frame(MSG_QUERY, sql.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] encodePrepare(String sql) {
        return frame(MSG_PREPARE, sql.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] encodeExecute(int handle, List<String> params) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            // handle: u32 LE
            ByteBuffer handleBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            handleBuf.putInt(handle);
            bos.write(handleBuf.array());
            // param_count: u16 LE
            ByteBuffer paramCount = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
            paramCount.putShort((short) params.size());
            bos.write(paramCount.array());
            // for each param: len u16 LE + UTF-8 bytes
            for (String p : params) {
                byte[] pBytes = p.getBytes(StandardCharsets.UTF_8);
                ByteBuffer lenBuf = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
                lenBuf.putShort((short) pBytes.length);
                bos.write(lenBuf.array());
                bos.write(pBytes);
            }
            return frame(MSG_EXECUTE, bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] encodeClose(int handle) {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(handle);
        return frame(MSG_CLOSE, buf.array());
    }

    public static int decodeReady(byte[] payload) {
        if (payload.length < 4) {
            throw new PWireException("READY payload too short");
        }
        ByteBuffer buf = ByteBuffer.wrap(payload, 0, 4).order(ByteOrder.LITTLE_ENDIAN);
        return buf.getInt();
    }

    public static byte[] encodePing() {
        return frame(MSG_PING, new byte[0]);
    }

    public static byte[] encodeQuit() {
        return frame(MSG_QUIT, new byte[0]);
    }

    // ---- Decoding helpers ----

    /**
     * Decodes a frame header+payload from a raw buffer.
     * Returns [type, payloadBytes].
     */
    public static Frame decodeFrame(byte[] data, int offset, int length) {
        if (length < HEADER_SIZE) {
            throw new PWireException("Incomplete frame: need at least " + HEADER_SIZE + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(data, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        int type = buf.get() & 0xFF;
        int payloadLen = buf.getInt();
        if (length < HEADER_SIZE + payloadLen) {
            throw new PWireException("Incomplete frame: need " + (HEADER_SIZE + payloadLen) + " bytes, have " + length);
        }
        byte[] payload = new byte[payloadLen];
        buf.get(payload);
        return new Frame(type, payload, HEADER_SIZE + payloadLen);
    }

    public static ResultSet decodeResultSet(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);

        int colCount = buf.getShort() & 0xFFFF;
        ColumnDef[] columns = new ColumnDef[colCount];
        for (int i = 0; i < colCount; i++) {
            int nameLen = buf.get() & 0xFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            int typeTag = buf.get() & 0xFF;
            columns[i] = new ColumnDef(new String(nameBytes, StandardCharsets.UTF_8), typeTag);
        }

        int rowCount = buf.getInt();
        int nullBitmapLen = (colCount + 7) / 8;
        List<Object[]> rows = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            byte[] bitmap = new byte[nullBitmapLen];
            buf.get(bitmap);
            Object[] row = new Object[colCount];
            for (int c = 0; c < colCount; c++) {
                int byteIdx = c / 8;
                int bitIdx = c % 8;
                boolean isNull = (bitmap[byteIdx] >> bitIdx & 1) == 1;
                if (isNull) {
                    row[c] = null;
                    continue;
                }
                int typeTag = columns[c].typeTag;
                switch (typeTag) {
                    case TYPE_I64:
                        row[c] = buf.getLong();
                        break;
                    case TYPE_F64:
                        row[c] = buf.getDouble();
                        break;
                    case TYPE_BOOL:
                        row[c] = buf.get() != 0;
                        break;
                    case TYPE_TEXT:
                    case TYPE_BYTES:
                    default: {
                        int len = buf.getShort() & 0xFFFF;
                        byte[] bytes = new byte[len];
                        buf.get(bytes);
                        if (typeTag == TYPE_BYTES) {
                            row[c] = bytes;
                        } else {
                            row[c] = new String(bytes, StandardCharsets.UTF_8);
                        }
                        break;
                    }
                }
            }
            rows.add(row);
        }
        return new ResultSet(columns, rows);
    }

    public static OkResult decodeOk(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        long rowsAffected = buf.getLong();
        int tagLen = buf.get() & 0xFF;
        byte[] tagBytes = new byte[tagLen];
        buf.get(tagBytes);
        return new OkResult(rowsAffected, new String(tagBytes, StandardCharsets.UTF_8));
    }

    public static ErrorResult decodeError(byte[] payload) {
        byte[] sqlstate = new byte[5];
        System.arraycopy(payload, 0, sqlstate, 0, 5);
        ByteBuffer buf = ByteBuffer.wrap(payload, 5, 2).order(ByteOrder.LITTLE_ENDIAN);
        int msgLen = buf.getShort() & 0xFFFF;
        byte[] msgBytes = new byte[msgLen];
        System.arraycopy(payload, 7, msgBytes, 0, msgLen);
        return new ErrorResult(new String(sqlstate, StandardCharsets.UTF_8),
                new String(msgBytes, StandardCharsets.UTF_8));
    }

    // ---- Data classes ----

    public static final class Frame {
        public final int type;
        public final byte[] payload;
        public final int totalBytes;
        Frame(int type, byte[] payload, int totalBytes) {
            this.type = type;
            this.payload = payload;
            this.totalBytes = totalBytes;
        }
    }

    public static final class ColumnDef {
        public final String name;
        public final int typeTag;
        ColumnDef(String name, int typeTag) {
            this.name = name;
            this.typeTag = typeTag;
        }
    }

    public static final class ResultSet {
        public final ColumnDef[] columns;
        public final List<Object[]> rows;
        ResultSet(ColumnDef[] columns, List<Object[]> rows) {
            this.columns = columns;
            this.rows = rows;
        }
    }

    public static final class OkResult {
        public final long rowsAffected;
        public final String tag;
        OkResult(long rowsAffected, String tag) {
            this.rowsAffected = rowsAffected;
            this.tag = tag;
        }
    }

    public static final class ErrorResult {
        public final String sqlState;
        public final String message;
        ErrorResult(String sqlState, String message) {
            this.sqlState = sqlState;
            this.message = message;
        }
    }
}
