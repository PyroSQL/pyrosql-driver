/**
 * Pure-JavaScript PWire binary protocol codec with LZ4 compression
 * and server-side PREPARE/EXECUTE/CLOSE binding.
 *
 * Provides a direct TCP connection to PyroSQL without requiring
 * the FFI shared library (ffi-napi / koffi).
 *
 * @example
 * const { TCPClient } = require('./pwire');
 * const client = new TCPClient('127.0.0.1', 12520);
 * client.connect();
 * const result = client.query('SELECT * FROM users WHERE id = $1', [42]);
 * console.log(result.rows);
 * client.close();
 */

'use strict';

const net = require('net');

// ── LZ4 optional dependency ──────────────────────────────────────────────
let lz4;
try {
    lz4 = require('lz4');
} catch (_) {
    lz4 = null;
}

// ── Protocol constants ───────────────────────────────────────────────────

const MSG_QUERY      = 0x01;
const MSG_PREPARE    = 0x02;
const MSG_EXECUTE    = 0x03;
const MSG_CLOSE      = 0x04;
const MSG_PING       = 0x05;
const MSG_AUTH       = 0x06;
const MSG_COMPRESSED = 0x10;
const MSG_QUIT       = 0xFF;

const RESP_RESULT_SET = 0x01;
const RESP_OK         = 0x02;
const RESP_ERROR      = 0x03;
const RESP_PONG       = 0x04;
const RESP_READY      = 0x05;

const TYPE_NULL  = 0;
const TYPE_I64   = 1;
const TYPE_F64   = 2;
const TYPE_TEXT  = 3;
const TYPE_BOOL  = 4;
const TYPE_BYTES = 5;

const HEADER_SIZE = 5;
const CAP_LZ4 = 0x01;
const COMPRESSION_THRESHOLD = 8 * 1024;

// ── Frame encoding ──────────────────────────────────────────────────────

function frame(type, payload) {
    const buf = Buffer.alloc(HEADER_SIZE + payload.length);
    buf.writeUInt8(type, 0);
    buf.writeUInt32LE(payload.length, 1);
    payload.copy(buf, HEADER_SIZE);
    return buf;
}

function encodeAuth(user, password) {
    const userBuf = Buffer.from(user, 'utf8');
    const passBuf = Buffer.from(password, 'utf8');
    const payload = Buffer.alloc(1 + userBuf.length + 1 + passBuf.length);
    payload.writeUInt8(userBuf.length, 0);
    userBuf.copy(payload, 1);
    payload.writeUInt8(passBuf.length, 1 + userBuf.length);
    passBuf.copy(payload, 2 + userBuf.length);
    return frame(MSG_AUTH, payload);
}

function encodeAuthWithCaps(user, password, caps) {
    const userBuf = Buffer.from(user, 'utf8');
    const passBuf = Buffer.from(password, 'utf8');
    const payload = Buffer.alloc(1 + userBuf.length + 1 + passBuf.length + 1);
    payload.writeUInt8(userBuf.length, 0);
    userBuf.copy(payload, 1);
    payload.writeUInt8(passBuf.length, 1 + userBuf.length);
    passBuf.copy(payload, 2 + userBuf.length);
    payload.writeUInt8(caps, 2 + userBuf.length + passBuf.length);
    return frame(MSG_AUTH, payload);
}

function encodeQuery(sql) {
    return frame(MSG_QUERY, Buffer.from(sql, 'utf8'));
}

function encodePrepare(sql) {
    return frame(MSG_PREPARE, Buffer.from(sql, 'utf8'));
}

function encodeExecute(handle, params) {
    const parts = [];
    const headerBuf = Buffer.alloc(6);
    headerBuf.writeUInt32LE(handle, 0);
    headerBuf.writeUInt16LE(params.length, 4);
    parts.push(headerBuf);
    for (const p of params) {
        const pBuf = Buffer.from(String(p), 'utf8');
        const lenBuf = Buffer.alloc(2);
        lenBuf.writeUInt16LE(pBuf.length, 0);
        parts.push(lenBuf);
        parts.push(pBuf);
    }
    return frame(MSG_EXECUTE, Buffer.concat(parts));
}

function encodeClose(handle) {
    const payload = Buffer.alloc(4);
    payload.writeUInt32LE(handle, 0);
    return frame(MSG_CLOSE, payload);
}

function encodePing() {
    return frame(MSG_PING, Buffer.alloc(0));
}

function encodeQuit() {
    return frame(MSG_QUIT, Buffer.alloc(0));
}

// ── LZ4 compression ─────────────────────────────────────────────────────

/**
 * Compress a frame with LZ4 if beneficial.
 * Uses lz4_flex-compatible format: lz4_data = [u32 LE original_size][raw LZ4 block].
 */
function compressFrame(msgType, payload) {
    if (!lz4 || payload.length <= COMPRESSION_THRESHOLD) {
        return frame(msgType, payload);
    }

    const rawCompressed = Buffer.alloc(lz4.encodeBound(payload.length));
    const rawCompressedSize = lz4.encodeBlock(payload, rawCompressed);
    const rawCompressedBuf = rawCompressed.slice(0, rawCompressedSize);

    // lz4_flex format: prepend u32 LE original size
    const lz4DataLen = 4 + rawCompressedSize;

    const ratio = lz4DataLen / payload.length;
    if (ratio > 0.9) {
        return frame(msgType, payload);
    }

    // Inner: [original_type: u8][uncompressed_length: u32 LE][lz4_data]
    // lz4_data = [u32 LE original_size][raw LZ4 block]
    const inner = Buffer.alloc(1 + 4 + lz4DataLen);
    inner.writeUInt8(msgType, 0);
    inner.writeUInt32LE(payload.length, 1);
    inner.writeUInt32LE(payload.length, 5); // lz4_flex prepended size
    rawCompressedBuf.copy(inner, 9);
    return frame(MSG_COMPRESSED, inner);
}

/**
 * Decompress a MSG_COMPRESSED frame payload.
 * lz4_data is in lz4_flex format: [u32 LE original_size][raw LZ4 block].
 */
function decompressFrame(payload) {
    if (payload.length < 5) {
        throw new Error('Compressed payload too short');
    }
    const originalType = payload.readUInt8(0);
    const uncompressedLen = payload.readUInt32LE(1);
    const lz4Data = payload.slice(5);

    if (lz4Data.length < 4) {
        throw new Error('LZ4 data too short');
    }
    // Skip the 4-byte lz4_flex size prefix
    const rawBlock = lz4Data.slice(4);

    const decompressed = Buffer.alloc(uncompressedLen);
    const decoded = lz4.decodeBlock(rawBlock, decompressed);
    if (decoded < 0) {
        throw new Error('LZ4 decompression failed');
    }
    return { type: originalType, payload: decompressed.slice(0, decoded) };
}

// ── Response decoding ───────────────────────────────────────────────────

function decodeResultSet(payload) {
    let pos = 0;
    const colCount = payload.readUInt16LE(pos); pos += 2;

    const columns = [];
    for (let i = 0; i < colCount; i++) {
        const nameLen = payload.readUInt8(pos); pos++;
        const name = payload.slice(pos, pos + nameLen).toString('utf8'); pos += nameLen;
        const typeTag = payload.readUInt8(pos); pos++;
        columns.push({ name, typeTag });
    }

    const rowCount = payload.readUInt32LE(pos); pos += 4;
    const nullBitmapLen = Math.ceil(colCount / 8);
    const rows = [];

    for (let r = 0; r < rowCount; r++) {
        const bitmap = payload.slice(pos, pos + nullBitmapLen); pos += nullBitmapLen;
        const row = [];
        for (let c = 0; c < colCount; c++) {
            const byteIdx = Math.floor(c / 8);
            const bitIdx = c % 8;
            const isNull = byteIdx < bitmap.length && ((bitmap[byteIdx] >> bitIdx) & 1) === 1;
            if (isNull) {
                row.push(null);
                continue;
            }
            const tt = columns[c].typeTag;
            if (tt === TYPE_I64) {
                // Read as BigInt then convert to Number (may lose precision for very large values)
                const lo = payload.readUInt32LE(pos);
                const hi = payload.readInt32LE(pos + 4);
                row.push(hi * 0x100000000 + lo);
                pos += 8;
            } else if (tt === TYPE_F64) {
                row.push(payload.readDoubleLE(pos));
                pos += 8;
            } else if (tt === TYPE_BOOL) {
                row.push(payload.readUInt8(pos) !== 0);
                pos++;
            } else if (tt === TYPE_BYTES) {
                const len = payload.readUInt16LE(pos); pos += 2;
                row.push(payload.slice(pos, pos + len));
                pos += len;
            } else {
                // TYPE_TEXT and unknown
                const len = payload.readUInt16LE(pos); pos += 2;
                row.push(payload.slice(pos, pos + len).toString('utf8'));
                pos += len;
            }
        }
        rows.push(row);
    }

    return {
        columns: columns.map(c => c.name),
        rows,
        rows_affected: 0
    };
}

function decodeOk(payload) {
    const lo = payload.readUInt32LE(0);
    const hi = payload.readInt32LE(4);
    const rowsAffected = hi * 0x100000000 + lo;
    const tagLen = payload.readUInt8(8);
    const tag = payload.slice(9, 9 + tagLen).toString('utf8');
    return { columns: [], rows: [], rows_affected: rowsAffected, tag };
}

function decodeError(payload) {
    const sqlState = payload.slice(0, 5).toString('ascii');
    const msgLen = payload.readUInt16LE(5);
    const message = payload.slice(7, 7 + msgLen).toString('utf8');
    return { sqlState, message };
}

// ── Synchronous TCP connection ──────────────────────────────────────────

/**
 * Pure-JS synchronous TCP client for PyroSQL.
 * Uses blocking I/O (via synchronous socket reads).
 *
 * Note: Node.js does not natively support synchronous TCP reads.
 * This implementation buffers incoming data and provides a blocking-like API
 * that works with the synchronous FFI test pattern.
 * For production use, prefer the async version or the FFI-based Client.
 */
class TCPClient {
    /**
     * @param {string} host
     * @param {number} port
     * @param {object} [options]
     * @param {string} [options.user='']
     * @param {string} [options.password='']
     * @param {number} [options.timeout=30000]
     */
    constructor(host, port, options = {}) {
        this._host = host;
        this._port = port;
        this._user = options.user || '';
        this._password = options.password || '';
        this._timeout = options.timeout || 30000;
        this._socket = null;
        this._supportsLZ4 = false;
        this._recvBuf = Buffer.alloc(0);
        this._connected = false;
    }

    /**
     * Connect to the server and authenticate.
     */
    connect() {
        return new Promise((resolve, reject) => {
            this._socket = new net.Socket();
            this._socket.setTimeout(this._timeout);

            this._socket.on('data', (data) => {
                this._recvBuf = Buffer.concat([this._recvBuf, data]);
                if (this._pendingResolve && this._tryReadFrame()) {
                    // Frame was read, resolve is handled in _tryReadFrame
                }
            });

            this._socket.on('error', (err) => {
                if (this._pendingReject) {
                    this._pendingReject(err);
                    this._pendingResolve = null;
                    this._pendingReject = null;
                }
            });

            this._socket.connect(this._port, this._host, () => {
                this._connected = true;
                // Authenticate
                const authFrame = lz4
                    ? encodeAuthWithCaps(this._user, this._password, CAP_LZ4)
                    : encodeAuth(this._user, this._password);
                this._socket.write(authFrame);
                this._readFrame().then(({ type, payload }) => {
                    if (type === RESP_ERROR) {
                        const err = decodeError(payload);
                        reject(new Error(`Auth failed: [${err.sqlState}] ${err.message}`));
                        return;
                    }
                    if (type === RESP_READY && payload.length >= 4 && lz4) {
                        const serverCaps = payload.readUInt32LE(0);
                        this._supportsLZ4 = (serverCaps & CAP_LZ4) !== 0;
                    }
                    resolve();
                }).catch(reject);
            });
        });
    }

    /**
     * Execute a query. Uses server-side binding when params are provided.
     * @param {string} sql
     * @param {Array} [params=[]]
     * @returns {Promise<{columns: string[], rows: any[][], rows_affected: number}>}
     */
    async query(sql, params = []) {
        if (params.length > 0) {
            return this._queryPrepared(sql, params);
        }
        this._sendCompressed(MSG_QUERY, Buffer.from(sql, 'utf8'));
        return this._handleResponse();
    }

    /**
     * Execute a DML statement.
     * @param {string} sql
     * @param {Array} [params=[]]
     * @returns {Promise<number>}
     */
    async execute(sql, params = []) {
        if (params.length > 0) {
            const result = await this._queryPrepared(sql, params);
            return result.rows_affected || 0;
        }
        this._sendCompressed(MSG_QUERY, Buffer.from(sql, 'utf8'));
        const result = await this._handleResponse();
        return result.rows_affected || 0;
    }

    /**
     * Server-side prepared statement execution.
     * @private
     */
    async _queryPrepared(sql, params) {
        // PREPARE
        this._sendCompressed(MSG_PREPARE, Buffer.from(sql, 'utf8'));
        const prepFrame = await this._readFrame();
        if (prepFrame.type === RESP_ERROR) {
            const err = decodeError(prepFrame.payload);
            throw new Error(`Prepare error: [${err.sqlState}] ${err.message}`);
        }
        if (prepFrame.type !== RESP_READY || prepFrame.payload.length < 4) {
            throw new Error('Unexpected response for PREPARE');
        }
        const handle = prepFrame.payload.readUInt32LE(0);

        try {
            // EXECUTE
            const wireParams = params.map(v => {
                if (v === null || v === undefined) return 'NULL';
                if (typeof v === 'boolean') return v ? 'true' : 'false';
                return String(v);
            });
            const execFrame = encodeExecute(handle, wireParams);
            const execPayload = execFrame.slice(HEADER_SIZE);
            this._sendCompressed(MSG_EXECUTE, execPayload);
            return await this._handleResponse();
        } finally {
            // CLOSE
            try {
                const closeFrame = encodeClose(handle);
                const closePayload = closeFrame.slice(HEADER_SIZE);
                this._sendCompressed(MSG_CLOSE, closePayload);
                await this._readFrame();
            } catch (_) {
                // best effort
            }
        }
    }

    /**
     * Close the connection.
     */
    close() {
        if (this._socket) {
            try {
                this._socket.write(encodeQuit());
            } catch (_) {}
            this._socket.destroy();
            this._socket = null;
            this._connected = false;
        }
    }

    // ── Internal ─────────────────────────────────────────────────────────

    _sendCompressed(msgType, payload) {
        let data;
        if (this._supportsLZ4) {
            data = compressFrame(msgType, payload);
        } else {
            data = frame(msgType, payload);
        }
        this._socket.write(data);
    }

    _readFrame() {
        return new Promise((resolve, reject) => {
            this._pendingResolve = resolve;
            this._pendingReject = reject;
            // Check if we already have enough data buffered
            this._tryReadFrame();
        });
    }

    _tryReadFrame() {
        if (!this._pendingResolve) return false;
        if (this._recvBuf.length < HEADER_SIZE) return false;

        const msgType = this._recvBuf.readUInt8(0);
        const payloadLen = this._recvBuf.readUInt32LE(1);
        const totalNeeded = HEADER_SIZE + payloadLen;

        if (this._recvBuf.length < totalNeeded) return false;

        const payload = this._recvBuf.slice(HEADER_SIZE, totalNeeded);
        this._recvBuf = this._recvBuf.slice(totalNeeded);

        let result;
        if (msgType === MSG_COMPRESSED && lz4) {
            result = decompressFrame(payload);
        } else {
            result = { type: msgType, payload };
        }

        const resolve = this._pendingResolve;
        this._pendingResolve = null;
        this._pendingReject = null;
        resolve(result);
        return true;
    }

    async _handleResponse() {
        const { type, payload } = await this._readFrame();
        if (type === RESP_RESULT_SET) {
            return decodeResultSet(payload);
        } else if (type === RESP_OK) {
            return decodeOk(payload);
        } else if (type === RESP_ERROR) {
            const err = decodeError(payload);
            throw new Error(`[${err.sqlState}] ${err.message}`);
        } else {
            throw new Error(`Unexpected response type 0x${type.toString(16)}`);
        }
    }
}

// ── Exports ─────────────────────────────────────────────────────────────

module.exports = {
    // Protocol constants
    MSG_QUERY, MSG_PREPARE, MSG_EXECUTE, MSG_CLOSE, MSG_PING, MSG_AUTH,
    MSG_COMPRESSED, MSG_QUIT,
    RESP_RESULT_SET, RESP_OK, RESP_ERROR, RESP_PONG, RESP_READY,
    TYPE_NULL, TYPE_I64, TYPE_F64, TYPE_TEXT, TYPE_BOOL, TYPE_BYTES,
    HEADER_SIZE, CAP_LZ4, COMPRESSION_THRESHOLD,

    // Encoding
    frame, encodeAuth, encodeAuthWithCaps, encodeQuery, encodePrepare,
    encodeExecute, encodeClose, encodePing, encodeQuit,

    // Compression
    compressFrame, decompressFrame,

    // Decoding
    decodeResultSet, decodeOk, decodeError,

    // Client
    TCPClient,
};
