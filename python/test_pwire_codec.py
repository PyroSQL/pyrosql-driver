"""Unit tests for the pure-Python PWire codec with LZ4 compression."""

import struct
import unittest


class TestPWireCodec(unittest.TestCase):
    """Tests for pyrosql.pwire codec functions."""

    def test_frame_encoding(self):
        from pyrosql.pwire import _frame, MSG_QUERY
        payload = b"SELECT 1"
        frame = _frame(MSG_QUERY, payload)
        self.assertEqual(frame[0], MSG_QUERY)
        length = struct.unpack_from("<I", frame, 1)[0]
        self.assertEqual(length, len(payload))
        self.assertEqual(frame[5:], payload)

    def test_encode_auth(self):
        from pyrosql.pwire import encode_auth, MSG_AUTH
        frame = encode_auth("admin", "secret")
        self.assertEqual(frame[0], MSG_AUTH)

    def test_encode_auth_with_caps(self):
        from pyrosql.pwire import encode_auth_with_caps, MSG_AUTH, CAP_LZ4, HEADER_SIZE
        frame = encode_auth_with_caps("admin", "secret", CAP_LZ4)
        self.assertEqual(frame[0], MSG_AUTH)
        payload_len = struct.unpack_from("<I", frame, 1)[0]
        # user(1+5) + pass(1+6) + caps(1) = 14
        self.assertEqual(payload_len, 14)
        # Last byte of payload is the caps
        self.assertEqual(frame[HEADER_SIZE + 13], CAP_LZ4)

    def test_encode_prepare(self):
        from pyrosql.pwire import encode_prepare, MSG_PREPARE, HEADER_SIZE
        frame = encode_prepare("SELECT $1")
        self.assertEqual(frame[0], MSG_PREPARE)
        payload = frame[HEADER_SIZE:]
        self.assertEqual(payload.decode("utf-8"), "SELECT $1")

    def test_encode_execute(self):
        from pyrosql.pwire import encode_execute, MSG_EXECUTE, HEADER_SIZE
        frame = encode_execute(42, ["hello", "world"])
        self.assertEqual(frame[0], MSG_EXECUTE)
        payload = frame[HEADER_SIZE:]
        handle = struct.unpack_from("<I", payload, 0)[0]
        param_count = struct.unpack_from("<H", payload, 4)[0]
        self.assertEqual(handle, 42)
        self.assertEqual(param_count, 2)

    def test_encode_close(self):
        from pyrosql.pwire import encode_close, MSG_CLOSE, HEADER_SIZE
        frame = encode_close(99)
        self.assertEqual(frame[0], MSG_CLOSE)
        payload = frame[HEADER_SIZE:]
        handle = struct.unpack_from("<I", payload, 0)[0]
        self.assertEqual(handle, 99)

    def test_compress_frame_small_payload_not_compressed(self):
        from pyrosql.pwire import compress_frame, MSG_QUERY
        small = b"A" * 100
        frame = compress_frame(MSG_QUERY, small)
        self.assertEqual(frame[0], MSG_QUERY)

    def test_compress_frame_large_payload_compressed(self):
        from pyrosql.pwire import compress_frame, MSG_QUERY, MSG_COMPRESSED, _HAS_LZ4
        if not _HAS_LZ4:
            self.skipTest("lz4 not installed")
        large = b"X" * (16 * 1024)
        frame = compress_frame(MSG_QUERY, large)
        self.assertEqual(frame[0], MSG_COMPRESSED)
        self.assertLess(len(frame), len(large))

    def test_compress_decompress_roundtrip(self):
        from pyrosql.pwire import compress_frame, decompress_frame, MSG_QUERY, MSG_COMPRESSED, HEADER_SIZE, _HAS_LZ4
        if not _HAS_LZ4:
            self.skipTest("lz4 not installed")
        original = bytes(i % 256 for i in range(16 * 1024))
        frame = compress_frame(MSG_QUERY, original)
        self.assertEqual(frame[0], MSG_COMPRESSED)

        payload_len = struct.unpack_from("<I", frame, 1)[0]
        inner_payload = frame[HEADER_SIZE:HEADER_SIZE + payload_len]

        orig_type, decompressed = decompress_frame(inner_payload)
        self.assertEqual(orig_type, MSG_QUERY)
        self.assertEqual(decompressed, original)

    def test_decode_result_set(self):
        from pyrosql.pwire import decode_result_set, TYPE_TEXT
        # Build a simple result set payload: 1 column ("name", TEXT), 1 row ("alice")
        payload = b""
        payload += struct.pack("<H", 1)  # col count
        payload += struct.pack("B", 4) + b"name"
        payload += struct.pack("B", TYPE_TEXT)
        payload += struct.pack("<I", 1)  # row count
        payload += struct.pack("B", 0)  # null bitmap (no nulls)
        payload += struct.pack("<H", 5) + b"alice"

        result = decode_result_set(payload)
        self.assertEqual(result["columns"], ["name"])
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0][0], "alice")

    def test_decode_ok(self):
        from pyrosql.pwire import decode_ok
        payload = struct.pack("<q", 42) + struct.pack("B", 5) + b"DONE!"
        result = decode_ok(payload)
        self.assertEqual(result["rows_affected"], 42)
        self.assertEqual(result["tag"], "DONE!")

    def test_decode_error(self):
        from pyrosql.pwire import decode_error
        payload = b"42P01" + struct.pack("<H", 13) + b"table missing"
        sql_state, message = decode_error(payload)
        self.assertEqual(sql_state, "42P01")
        self.assertEqual(message, "table missing")


if __name__ == "__main__":
    unittest.main()
