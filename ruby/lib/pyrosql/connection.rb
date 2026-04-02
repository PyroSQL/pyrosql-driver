# frozen_string_literal: true

require "socket"
require_relative "codec"

module PyroSQL
  # Base error class for PyroSQL.
  class Error < StandardError; end

  # Raised when a connection-level failure occurs.
  class ConnectionError < Error; end

  # Raised when the server returns an error response.
  class ServerError < Error
    attr_reader :sql_state

    def initialize(sql_state, message)
      @sql_state = sql_state
      super("[#{sql_state}] #{message}")
    end
  end

  # Column metadata returned from a query.
  Column = Struct.new(:name, :type_tag, keyword_init: true)

  # Result of a query that returns rows.
  ResultSet = Struct.new(:columns, :rows, keyword_init: true) do
    def each(&block)
      rows.each(&block)
    end

    include Enumerable
  end

  # Result of a non-query command (INSERT, UPDATE, DELETE, DDL).
  OkResult = Struct.new(:rows_affected, :tag, keyword_init: true)

  # Low-level TCP connection speaking the PWire binary protocol.
  class Connection
    attr_reader :host, :port

    def initialize(host: "localhost", port: 12520, connect_timeout: 30)
      @host = host
      @port = port
      @connect_timeout = connect_timeout
      @socket = nil
      @supports_lz4 = false
    end

    # Establish TCP connection and authenticate with LZ4 capability negotiation.
    def connect(username:, password:)
      @socket = Socket.tcp(@host, @port, connect_timeout: @connect_timeout)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      send_raw(Codec.encode_auth_with_caps(username, password, Codec::CAP_LZ4))
      type, payload = read_frame

      if type == Codec::RESP_ERROR
        err = Codec.decode_error(payload)
        raise ServerError.new(err[:sql_state], err[:message])
      end

      unless type == Codec::RESP_READY || type == Codec::RESP_OK
        raise ConnectionError, "Expected READY after auth, got 0x#{type.to_s(16).upcase}"
      end

      # Check server caps in READY payload (first 4 bytes = u32 LE caps).
      if type == Codec::RESP_READY && payload.bytesize >= 4
        server_caps = payload[0, 4].unpack1("V")
        @supports_lz4 = (server_caps & Codec::CAP_LZ4) != 0
      end

      self
    end

    # Send a simple query and return the parsed response.
    def query(sql)
      send_compressed(Codec::MSG_QUERY, sql.encode("UTF-8").b)
    end

    # Send a prepared statement query with parameters using binary protocol.
    # Rewrites ? placeholders to $1, $2, ... before sending to server.
    def query_prepared(sql, params)
      # Rewrite ? placeholders to $1, $2, ...
      prepared_sql = rewrite_placeholders(sql)

      # PREPARE via binary MSG_PREPARE — server returns RESP_READY with u32 handle
      send_frame_maybe_compressed(Codec::MSG_PREPARE, prepared_sql.encode("UTF-8").b)
      type, payload = read_frame

      if type == Codec::RESP_ERROR
        err = Codec.decode_error(payload)
        raise ServerError.new(err[:sql_state], err[:message])
      end

      unless type == Codec::RESP_READY
        raise ConnectionError, "Expected READY from PREPARE, got 0x#{type.to_s(16).upcase}"
      end

      handle = payload[0, 4].unpack1("V")

      begin
        # EXECUTE via binary MSG_EXECUTE with handle + params
        wire_params = params.map { |p| param_to_wire_string(p) }
        exec_frame = Codec.encode_execute(handle, wire_params)
        exec_payload = exec_frame[Codec::HEADER_SIZE..]
        send_frame_maybe_compressed(Codec::MSG_EXECUTE, exec_payload)
        type, payload = read_frame
        parse_response(type, payload)
      ensure
        # CLOSE the prepared statement via binary MSG_CLOSE
        begin
          send_raw(Codec.encode_close(handle))
          read_frame # consume response
        rescue StandardError
          # best effort
        end
      end
    end

    # Send a ping to verify the connection.
    def ping
      send_raw(Codec.encode_ping)
      type, _payload = read_frame
      type == Codec::RESP_PONG
    end

    # Gracefully close the connection.
    def close
      return unless @socket

      begin
        send_raw(Codec.encode_quit)
      rescue StandardError
        # best effort
      end
      @socket.close rescue nil
      @socket = nil
    end

    # Whether the socket is currently connected.
    def connected?
      !@socket.nil? && !@socket.closed?
    end

    private

    def send_and_handle(frame)
      send_raw(frame)
      type, payload = read_frame
      parse_response(type, payload)
    end

    # Build a frame from type + payload, optionally compressing if LZ4 is negotiated.
    def send_frame_maybe_compressed(msg_type, payload)
      if @supports_lz4
        data = Codec.compress_frame(msg_type, payload)
      else
        data = Codec.frame(msg_type, payload)
      end
      send_raw(data)
    end

    # Send a frame with optional compression and return the parsed response.
    def send_compressed(msg_type, payload)
      send_frame_maybe_compressed(msg_type, payload)
      type, resp_payload = read_frame
      parse_response(type, resp_payload)
    end

    def parse_response(type, payload)
      case type
      when Codec::RESP_RESULT_SET
        raw = Codec.decode_result_set(payload)
        columns = raw[:columns].map { |c| Column.new(name: c[:name], type_tag: c[:type_tag]) }
        ResultSet.new(columns: columns, rows: raw[:rows])
      when Codec::RESP_OK
        ok = Codec.decode_ok(payload)
        OkResult.new(rows_affected: ok[:rows_affected], tag: ok[:tag])
      when Codec::RESP_ERROR
        err = Codec.decode_error(payload)
        raise ServerError.new(err[:sql_state], err[:message])
      else
        raise ConnectionError, "Unexpected response type 0x#{type.to_s(16).upcase}"
      end
    end

    def send_raw(data)
      raise ConnectionError, "Not connected" unless @socket
      @socket.write(data)
      @socket.flush
    end

    def read_frame
      raise ConnectionError, "Not connected" unless @socket
      Codec.read_frame(@socket)
    end

    def rewrite_placeholders(sql)
      idx = 0
      in_quote = false
      result = +""
      sql.each_char do |c|
        if c == "'"
          in_quote = !in_quote
          result << c
        elsif c == "?" && !in_quote
          idx += 1
          result << "$#{idx}"
        else
          result << c
        end
      end
      result
    end

    def param_to_wire_string(value)
      return "NULL" if value.nil?

      case value
      when true then "true"
      when false then "false"
      when Integer, Float then value.to_s
      else
        # Send raw string — server handles quoting
        value.to_s
      end
    end
  end
end
