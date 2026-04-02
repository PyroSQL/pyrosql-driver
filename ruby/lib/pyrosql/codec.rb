# frozen_string_literal: true

require "extlz4"

module PyroSQL
  # PWire binary protocol codec for PyroSQL.
  # Header format: 1 byte type + 4 bytes little-endian length + payload.
  module Codec
    # Client -> server message types
    MSG_QUERY      = 0x01
    MSG_PREPARE    = 0x02
    MSG_EXECUTE    = 0x03
    MSG_CLOSE      = 0x04
    MSG_PING       = 0x05
    MSG_AUTH       = 0x06
    MSG_COMPRESSED = 0x10
    MSG_QUIT       = 0xFF

    # Capability flags
    CAP_LZ4 = 0x01
    COMPRESSION_THRESHOLD = 8 * 1024

    # Server -> client response types
    RESP_RESULT_SET = 0x01
    RESP_OK         = 0x02
    RESP_ERROR      = 0x03
    RESP_PONG       = 0x04
    RESP_READY      = 0x05

    # Value type tags
    TYPE_NULL  = 0
    TYPE_I64   = 1
    TYPE_F64   = 2
    TYPE_TEXT  = 3
    TYPE_BOOL  = 4
    TYPE_BYTES = 5

    HEADER_SIZE = 5

    class << self
      # Build a framed PWire message.
      def frame(type, payload = "".b)
        payload = payload.b if payload.is_a?(String) && payload.encoding != Encoding::BINARY
        header = [type, payload.bytesize].pack("CV")
        header + payload
      end

      def encode_auth(user, password)
        user_bytes = user.encode("UTF-8").b
        pass_bytes = password.encode("UTF-8").b
        payload = [user_bytes.bytesize].pack("C") + user_bytes +
                  [pass_bytes.bytesize].pack("C") + pass_bytes
        frame(MSG_AUTH, payload)
      end

      # Build an AUTH frame with capability byte for LZ4 negotiation.
      def encode_auth_with_caps(user, password, caps)
        user_bytes = user.encode("UTF-8").b
        pass_bytes = password.encode("UTF-8").b
        payload = [user_bytes.bytesize].pack("C") + user_bytes +
                  [pass_bytes.bytesize].pack("C") + pass_bytes +
                  [caps].pack("C")
        frame(MSG_AUTH, payload)
      end

      # Compress a frame payload with LZ4 if it exceeds the threshold.
      # Returns a MSG_COMPRESSED frame or the original frame.
      # Uses lz4_flex-compatible format: [u32 LE original size][raw lz4 block].
      def compress_frame(type, payload)
        payload = payload.b if payload.is_a?(String) && payload.encoding != Encoding::BINARY
        return frame(type, payload) if payload.bytesize <= COMPRESSION_THRESHOLD

        raw_compressed = LZ4.block_encode(payload)
        # lz4_flex format: prepend u32 LE original size
        lz4_data = [payload.bytesize].pack("V") + raw_compressed

        ratio = lz4_data.bytesize.to_f / payload.bytesize
        return frame(type, payload) if ratio > 0.9

        # Inner: [original_type: u8][uncompressed_length: u32 LE][lz4_data]
        inner = [type].pack("C") + [payload.bytesize].pack("V") + lz4_data
        frame(MSG_COMPRESSED, inner)
      end

      # Decompress a MSG_COMPRESSED frame payload.
      # Returns [original_type, decompressed_payload].
      # lz4_data is in lz4_flex format: [u32 LE original size][raw lz4 block].
      def decompress_frame(payload)
        raise PyroSQL::ConnectionError, "Compressed payload too short" if payload.bytesize < 5

        original_type = payload.getbyte(0)
        uncompressed_len = payload[1, 4].unpack1("V")
        lz4_data = payload[5..]

        # lz4_data starts with [u32 LE original size] from lz4_flex, skip it
        if lz4_data.bytesize >= 4
          raw_block = lz4_data[4..]
          decompressed = LZ4.block_decode(raw_block, uncompressed_len)
        else
          raise PyroSQL::ConnectionError, "LZ4 data too short"
        end

        [original_type, decompressed]
      end

      def encode_query(sql)
        frame(MSG_QUERY, sql.encode("UTF-8").b)
      end

      def encode_prepare(sql)
        frame(MSG_PREPARE, sql.encode("UTF-8").b)
      end

      def encode_execute(handle, params)
        payload = [handle].pack("V") + [params.size].pack("v")
        params.each do |p|
          p_bytes = p.to_s.encode("UTF-8").b
          payload += [p_bytes.bytesize].pack("v") + p_bytes
        end
        frame(MSG_EXECUTE, payload)
      end

      def encode_close(handle)
        frame(MSG_CLOSE, [handle].pack("V"))
      end

      def encode_ping
        frame(MSG_PING)
      end

      def encode_quit
        frame(MSG_QUIT)
      end

      # Read a complete frame from an IO stream.
      # Returns [type, payload].
      # Transparently decompresses MSG_COMPRESSED frames.
      def read_frame(io)
        header = read_exact(io, HEADER_SIZE)
        type, length = header.unpack("CV")
        payload = length > 0 ? read_exact(io, length) : "".b

        if type == MSG_COMPRESSED
          orig_type, decompressed = decompress_frame(payload)
          return [orig_type, decompressed]
        end

        [type, payload]
      end

      # Decode a RESULT_SET response payload.
      # Returns { columns: [{ name:, type_tag: }], rows: [[values]] }.
      def decode_result_set(payload)
        pos = 0
        col_count = payload[pos, 2].unpack1("v")
        pos += 2

        columns = col_count.times.map do
          name_len = payload.getbyte(pos)
          pos += 1
          name = payload[pos, name_len].force_encoding("UTF-8")
          pos += name_len
          type_tag = payload.getbyte(pos)
          pos += 1
          { name: name, type_tag: type_tag }
        end

        row_count = payload[pos, 4].unpack1("V")
        pos += 4
        null_bitmap_len = (col_count + 7) / 8

        rows = row_count.times.map do
          bitmap = payload[pos, null_bitmap_len].bytes
          pos += null_bitmap_len

          col_count.times.map do |c|
            byte_idx = c / 8
            bit_idx = c % 8
            is_null = byte_idx < bitmap.size && (bitmap[byte_idx] >> bit_idx) & 1 == 1

            if is_null
              nil
            else
              case columns[c][:type_tag]
              when TYPE_I64
                val = payload[pos, 8].unpack1("q<")
                pos += 8
                val
              when TYPE_F64
                val = payload[pos, 8].unpack1("E")
                pos += 8
                val
              when TYPE_BOOL
                val = payload.getbyte(pos) != 0
                pos += 1
                val
              when TYPE_TEXT
                len = payload[pos, 2].unpack1("v")
                pos += 2
                val = payload[pos, len].force_encoding("UTF-8")
                pos += len
                val
              when TYPE_BYTES
                len = payload[pos, 2].unpack1("v")
                pos += 2
                val = payload[pos, len].b
                pos += len
                val
              else
                len = payload[pos, 2].unpack1("v")
                pos += 2
                val = payload[pos, len].force_encoding("UTF-8")
                pos += len
                val
              end
            end
          end
        end

        { columns: columns, rows: rows }
      end

      # Decode an OK response payload.
      # Returns { rows_affected:, tag: }.
      def decode_ok(payload)
        rows_affected = payload[0, 8].unpack1("q<")
        tag_len = payload.getbyte(8)
        tag = payload[9, tag_len].force_encoding("UTF-8")
        { rows_affected: rows_affected, tag: tag }
      end

      # Decode an ERROR response payload.
      # Returns { sql_state:, message: }.
      def decode_error(payload)
        sql_state = payload[0, 5].force_encoding("ASCII")
        msg_len = payload[5, 2].unpack1("v")
        message = payload[7, msg_len].force_encoding("UTF-8")
        { sql_state: sql_state, message: message }
      end

      private

      def read_exact(io, count)
        buf = "".b
        while buf.bytesize < count
          chunk = io.read(count - buf.bytesize)
          raise PyroSQL::ConnectionError, "Connection closed by server" if chunk.nil? || chunk.empty?
          buf << chunk
        end
        buf
      end
    end
  end
end
