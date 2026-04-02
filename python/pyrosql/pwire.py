"""Pure-Python PWire binary protocol codec with LZ4 compression support.

This module provides a direct TCP connection to PyroSQL without requiring
the FFI shared library, including LZ4 compression and server-side binding
via PREPARE/EXECUTE/CLOSE messages.
"""

from __future__ import annotations

import socket
import struct
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import lz4.block as lz4_block
    _HAS_LZ4 = True
except ImportError:
    _HAS_LZ4 = False


# Message types (client -> server)
MSG_QUERY = 0x01
MSG_PREPARE = 0x02
MSG_EXECUTE = 0x03
MSG_CLOSE = 0x04
MSG_PING = 0x05
MSG_AUTH = 0x06
MSG_COMPRESSED = 0x10
MSG_QUIT = 0xFF

# Response types (server -> client)
RESP_RESULT_SET = 0x01
RESP_OK = 0x02
RESP_ERROR = 0x03
RESP_PONG = 0x04
RESP_READY = 0x05

# Value type tags
TYPE_NULL = 0
TYPE_I64 = 1
TYPE_F64 = 2
TYPE_TEXT = 3
TYPE_BOOL = 4
TYPE_BYTES = 5

HEADER_SIZE = 5
CAP_LZ4 = 0x01
COMPRESSION_THRESHOLD = 8 * 1024


def _frame(msg_type: int, payload: bytes) -> bytes:
    """Build a PWire frame: 1 byte type + 4 byte LE length + payload."""
    return struct.pack("<BI", msg_type, len(payload)) + payload


def encode_auth(user: str, password: str) -> bytes:
    user_b = user.encode("utf-8")
    pass_b = password.encode("utf-8")
    payload = struct.pack("B", len(user_b)) + user_b + struct.pack("B", len(pass_b)) + pass_b
    return _frame(MSG_AUTH, payload)


def encode_auth_with_caps(user: str, password: str, caps: int) -> bytes:
    user_b = user.encode("utf-8")
    pass_b = password.encode("utf-8")
    payload = (
        struct.pack("B", len(user_b)) + user_b
        + struct.pack("B", len(pass_b)) + pass_b
        + struct.pack("B", caps)
    )
    return _frame(MSG_AUTH, payload)


def encode_query(sql: str) -> bytes:
    return _frame(MSG_QUERY, sql.encode("utf-8"))


def encode_prepare(sql: str) -> bytes:
    return _frame(MSG_PREPARE, sql.encode("utf-8"))


def encode_execute(handle: int, params: List[str]) -> bytes:
    payload = struct.pack("<IH", handle, len(params))
    for p in params:
        p_bytes = p.encode("utf-8")
        payload += struct.pack("<H", len(p_bytes)) + p_bytes
    return _frame(MSG_EXECUTE, payload)


def encode_close(handle: int) -> bytes:
    return _frame(MSG_CLOSE, struct.pack("<I", handle))


def encode_ping() -> bytes:
    return _frame(MSG_PING, b"")


def encode_quit() -> bytes:
    return _frame(MSG_QUIT, b"")


def compress_frame(msg_type: int, payload: bytes) -> bytes:
    """Compress a frame with LZ4 if beneficial. Returns framed bytes.

    Uses lz4_flex-compatible format: the lz4_data within the inner frame is
    [u32 LE original_size][raw LZ4 block].
    """
    if not _HAS_LZ4 or len(payload) <= COMPRESSION_THRESHOLD:
        return _frame(msg_type, payload)

    raw_compressed = lz4_block.compress(payload, store_size=False)
    # lz4_flex format: prepend u32 LE original size
    lz4_data = struct.pack("<I", len(payload)) + raw_compressed

    ratio = len(lz4_data) / len(payload)
    if ratio > 0.9:
        return _frame(msg_type, payload)

    # Inner: [original_type: u8][uncompressed_length: u32 LE][lz4_data]
    inner = struct.pack("<BI", msg_type, len(payload)) + lz4_data
    return _frame(MSG_COMPRESSED, inner)


def decompress_frame(payload: bytes) -> Tuple[int, bytes]:
    """Decompress a MSG_COMPRESSED payload. Returns (original_type, data).

    lz4_data is in lz4_flex format: [u32 LE original_size][raw LZ4 block].
    """
    if len(payload) < 5:
        raise ValueError("Compressed payload too short")

    original_type = payload[0]
    uncompressed_len = struct.unpack_from("<I", payload, 1)[0]
    lz4_data = payload[5:]

    # lz4_data starts with [u32 LE original size] from lz4_flex, skip it
    if len(lz4_data) < 4:
        raise ValueError("LZ4 data too short")
    raw_block = lz4_data[4:]
    decompressed = lz4_block.decompress(raw_block, uncompressed_size=uncompressed_len)
    return original_type, decompressed


def _recv_exact(sock: socket.socket, count: int) -> bytes:
    """Read exactly count bytes from a socket."""
    buf = bytearray()
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        buf.extend(chunk)
    return bytes(buf)


def read_frame(sock: socket.socket) -> Tuple[int, bytes]:
    """Read a complete frame from a socket. Transparently decompresses MSG_COMPRESSED."""
    header = _recv_exact(sock, HEADER_SIZE)
    msg_type, length = struct.unpack("<BI", header)

    payload = _recv_exact(sock, length) if length > 0 else b""

    if msg_type == MSG_COMPRESSED:
        orig_type, decompressed = decompress_frame(payload)
        return orig_type, decompressed

    return msg_type, payload


def decode_result_set(payload: bytes) -> Dict[str, Any]:
    """Decode a RESP_RESULT_SET payload into columns and rows."""
    pos = 0
    col_count = struct.unpack_from("<H", payload, pos)[0]
    pos += 2

    columns = []
    for _ in range(col_count):
        name_len = payload[pos]
        pos += 1
        name = payload[pos:pos + name_len].decode("utf-8")
        pos += name_len
        type_tag = payload[pos]
        pos += 1
        columns.append({"name": name, "type_tag": type_tag})

    row_count = struct.unpack_from("<I", payload, pos)[0]
    pos += 4
    null_bitmap_len = (col_count + 7) // 8

    rows: List[List[Any]] = []
    for _ in range(row_count):
        bitmap = payload[pos:pos + null_bitmap_len]
        pos += null_bitmap_len
        row: List[Any] = []
        for c in range(col_count):
            byte_idx = c // 8
            bit_idx = c % 8
            is_null = byte_idx < len(bitmap) and ((bitmap[byte_idx] >> bit_idx) & 1) == 1
            if is_null:
                row.append(None)
                continue
            tt = columns[c]["type_tag"]
            if tt == TYPE_I64:
                val = struct.unpack_from("<q", payload, pos)[0]
                pos += 8
                row.append(val)
            elif tt == TYPE_F64:
                val = struct.unpack_from("<d", payload, pos)[0]
                pos += 8
                row.append(val)
            elif tt == TYPE_BOOL:
                val = payload[pos] != 0
                pos += 1
                row.append(val)
            elif tt == TYPE_BYTES:
                l = struct.unpack_from("<H", payload, pos)[0]
                pos += 2
                row.append(payload[pos:pos + l])
                pos += l
            else:
                # TYPE_TEXT and unknown
                l = struct.unpack_from("<H", payload, pos)[0]
                pos += 2
                row.append(payload[pos:pos + l].decode("utf-8"))
                pos += l
        rows.append(row)

    return {
        "columns": [c["name"] for c in columns],
        "rows": rows,
        "rows_affected": 0,
    }


def decode_ok(payload: bytes) -> Dict[str, Any]:
    """Decode a RESP_OK payload."""
    rows_affected = struct.unpack_from("<q", payload, 0)[0]
    tag_len = payload[8]
    tag = payload[9:9 + tag_len].decode("utf-8")
    return {"columns": [], "rows": [], "rows_affected": rows_affected, "tag": tag}


def decode_error(payload: bytes) -> Tuple[str, str]:
    """Decode a RESP_ERROR payload. Returns (sql_state, message)."""
    sql_state = payload[0:5].decode("ascii")
    msg_len = struct.unpack_from("<H", payload, 5)[0]
    message = payload[7:7 + msg_len].decode("utf-8")
    return sql_state, message


class PWireConnection:
    """Pure-Python TCP connection to PyroSQL with LZ4 and server-side binding."""

    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(timeout)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.connect((host, port))
        self._supports_lz4 = False

    def authenticate(self, user: str = "", password: str = "") -> None:
        if _HAS_LZ4:
            self._send_raw(encode_auth_with_caps(user, password, CAP_LZ4))
        else:
            self._send_raw(encode_auth(user, password))

        msg_type, payload = read_frame(self._sock)

        if msg_type == RESP_ERROR:
            sql_state, message = decode_error(payload)
            raise ConnectionError(f"[{sql_state}] {message}")

        if msg_type not in (RESP_READY, RESP_OK):
            raise ConnectionError(f"Unexpected auth response type 0x{msg_type:02X}")

        if msg_type == RESP_READY and len(payload) >= 4 and _HAS_LZ4:
            server_caps = struct.unpack_from("<I", payload, 0)[0]
            self._supports_lz4 = (server_caps & CAP_LZ4) != 0

    def query(self, sql: str) -> Dict[str, Any]:
        self._send_compressed(MSG_QUERY, sql.encode("utf-8"))
        return self._handle_response()

    def prepare(self, sql: str) -> int:
        """Send MSG_PREPARE. Returns the u32 handle from RESP_READY."""
        self._send_compressed(MSG_PREPARE, sql.encode("utf-8"))
        msg_type, payload = read_frame(self._sock)
        if msg_type == RESP_ERROR:
            sql_state, message = decode_error(payload)
            raise QueryError(f"[{sql_state}] {message}")
        if msg_type != RESP_READY:
            raise QueryError(f"Expected READY from PREPARE, got 0x{msg_type:02X}")
        if len(payload) < 4:
            raise QueryError("PREPARE response too short")
        return struct.unpack_from("<I", payload, 0)[0]

    def execute_prepared(self, handle: int, params: List[str]) -> Dict[str, Any]:
        """Send MSG_EXECUTE with handle + params and return the response."""
        full_frame = encode_execute(handle, params)
        exec_payload = full_frame[HEADER_SIZE:]
        self._send_compressed(MSG_EXECUTE, exec_payload)
        return self._handle_response()

    def close_prepared(self, handle: int) -> None:
        """Send MSG_CLOSE for the given handle."""
        full_frame = encode_close(handle)
        close_payload = full_frame[HEADER_SIZE:]
        self._send_compressed(MSG_CLOSE, close_payload)
        msg_type, payload = read_frame(self._sock)
        if msg_type == RESP_ERROR:
            sql_state, message = decode_error(payload)
            raise QueryError(f"[{sql_state}] {message}")

    def execute(self, sql: str) -> Dict[str, Any]:
        self._send_compressed(MSG_QUERY, sql.encode("utf-8"))
        return self._handle_response()

    def ping(self) -> bool:
        self._send_compressed(MSG_PING, b"")
        msg_type, _ = read_frame(self._sock)
        return msg_type == RESP_PONG

    def close(self) -> None:
        try:
            self._send_raw(encode_quit())
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass

    def _send_raw(self, data: bytes) -> None:
        self._sock.sendall(data)

    def _send_compressed(self, msg_type: int, payload: bytes) -> None:
        if self._supports_lz4:
            data = compress_frame(msg_type, payload)
        else:
            data = _frame(msg_type, payload)
        self._send_raw(data)

    def _handle_response(self) -> Dict[str, Any]:
        msg_type, payload = read_frame(self._sock)
        if msg_type == RESP_RESULT_SET:
            return decode_result_set(payload)
        elif msg_type == RESP_OK:
            return decode_ok(payload)
        elif msg_type == RESP_ERROR:
            sql_state, message = decode_error(payload)
            raise QueryError(f"[{sql_state}] {message}")
        else:
            raise QueryError(f"Unexpected response type 0x{msg_type:02X}")


class QueryError(Exception):
    """Raised when a query fails."""
