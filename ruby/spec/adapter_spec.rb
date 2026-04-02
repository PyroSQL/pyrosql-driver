# frozen_string_literal: true

require "rspec"
require_relative "../lib/pyrosql/codec"
require_relative "../lib/pyrosql/connection"
require_relative "../lib/active_record/connection_adapters/pyrosql_adapter"

RSpec.describe PyroSQL::Codec do
  describe ".frame" do
    it "builds a frame with correct header" do
      frame = PyroSQL::Codec.frame(0x01, "hello".b)
      expect(frame.bytesize).to eq(5 + 5)
      expect(frame.getbyte(0)).to eq(0x01)
      expect(frame[1, 4].unpack1("V")).to eq(5)
      expect(frame[5, 5]).to eq("hello".b)
    end

    it "builds an empty frame" do
      frame = PyroSQL::Codec.frame(0xFF)
      expect(frame.bytesize).to eq(5)
      expect(frame.getbyte(0)).to eq(0xFF)
      expect(frame[1, 4].unpack1("V")).to eq(0)
    end
  end

  describe ".encode_auth" do
    it "encodes username and password" do
      frame = PyroSQL::Codec.encode_auth("admin", "secret")
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_AUTH)
      payload_len = frame[1, 4].unpack1("V")
      payload = frame[5, payload_len]
      # username length (1) + "admin" (5) + password length (1) + "secret" (6)
      expect(payload.bytesize).to eq(1 + 5 + 1 + 6)
      expect(payload.getbyte(0)).to eq(5) # username length
      expect(payload[1, 5]).to eq("admin".b)
      expect(payload.getbyte(6)).to eq(6) # password length
      expect(payload[7, 6]).to eq("secret".b)
    end
  end

  describe ".encode_query" do
    it "encodes a SQL query" do
      frame = PyroSQL::Codec.encode_query("SELECT 1")
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_QUERY)
      payload_len = frame[1, 4].unpack1("V")
      expect(frame[5, payload_len]).to eq("SELECT 1".b)
    end
  end

  describe ".encode_prepare" do
    it "encodes a prepare statement" do
      frame = PyroSQL::Codec.encode_prepare("SELECT $1")
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_PREPARE)
      payload_len = frame[1, 4].unpack1("V")
      expect(frame[5, payload_len]).to eq("SELECT $1".b)
    end
  end

  describe ".encode_execute" do
    it "encodes execution with parameters" do
      frame = PyroSQL::Codec.encode_execute(42, ["hello", "world"])
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_EXECUTE)
      payload_len = frame[1, 4].unpack1("V")
      payload = frame[5, payload_len]
      handle = payload[0, 4].unpack1("V")
      param_count = payload[4, 2].unpack1("v")
      expect(handle).to eq(42)
      expect(param_count).to eq(2)
    end
  end

  describe ".encode_close" do
    it "encodes a close message with the handle" do
      frame = PyroSQL::Codec.encode_close(99)
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_CLOSE)
      payload_len = frame[1, 4].unpack1("V")
      payload = frame[5, payload_len]
      expect(payload.unpack1("V")).to eq(99)
    end
  end

  describe ".encode_ping" do
    it "encodes a ping message" do
      frame = PyroSQL::Codec.encode_ping
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_PING)
      expect(frame[1, 4].unpack1("V")).to eq(0)
    end
  end

  describe ".encode_quit" do
    it "encodes a quit message" do
      frame = PyroSQL::Codec.encode_quit
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_QUIT)
      expect(frame[1, 4].unpack1("V")).to eq(0)
    end
  end

  describe ".decode_result_set" do
    it "decodes a result set with one text column and one row" do
      # Build a payload: 1 column ("name", type TEXT), 1 row ("alice")
      payload = "".b
      payload << [1].pack("v") # col count
      payload << [4].pack("C") # name length
      payload << "name".b
      payload << [PyroSQL::Codec::TYPE_TEXT].pack("C")
      payload << [1].pack("V") # row count
      payload << [0].pack("C") # null bitmap (1 col = 1 byte, no nulls)
      payload << [5].pack("v") # text length
      payload << "alice".b

      result = PyroSQL::Codec.decode_result_set(payload)
      expect(result[:columns].size).to eq(1)
      expect(result[:columns][0][:name]).to eq("name")
      expect(result[:columns][0][:type_tag]).to eq(PyroSQL::Codec::TYPE_TEXT)
      expect(result[:rows].size).to eq(1)
      expect(result[:rows][0][0]).to eq("alice")
    end

    it "decodes a result set with i64 and null values" do
      payload = "".b
      payload << [2].pack("v") # 2 columns
      payload << [2].pack("C") + "id".b + [PyroSQL::Codec::TYPE_I64].pack("C")
      payload << [4].pack("C") + "name".b + [PyroSQL::Codec::TYPE_TEXT].pack("C")
      payload << [1].pack("V") # 1 row
      # null bitmap: bit 1 set (second column is null)
      payload << [0b00000010].pack("C")
      payload << [42].pack("q<") # id = 42

      result = PyroSQL::Codec.decode_result_set(payload)
      expect(result[:rows][0][0]).to eq(42)
      expect(result[:rows][0][1]).to be_nil
    end

    it "decodes a result set with f64 and bool" do
      payload = "".b
      payload << [2].pack("v") # 2 columns
      payload << [5].pack("C") + "score".b + [PyroSQL::Codec::TYPE_F64].pack("C")
      payload << [6].pack("C") + "active".b + [PyroSQL::Codec::TYPE_BOOL].pack("C")
      payload << [1].pack("V") # 1 row
      payload << [0].pack("C") # no nulls
      payload << [3.14].pack("E")
      payload << [1].pack("C") # true

      result = PyroSQL::Codec.decode_result_set(payload)
      expect(result[:rows][0][0]).to be_within(0.001).of(3.14)
      expect(result[:rows][0][1]).to eq(true)
    end

    it "decodes a result set with bytes column" do
      payload = "".b
      payload << [1].pack("v")
      payload << [4].pack("C") + "data".b + [PyroSQL::Codec::TYPE_BYTES].pack("C")
      payload << [1].pack("V") # 1 row
      payload << [0].pack("C") # no nulls
      payload << [3].pack("v") + "\x01\x02\x03".b

      result = PyroSQL::Codec.decode_result_set(payload)
      expect(result[:rows][0][0]).to eq("\x01\x02\x03".b)
      expect(result[:rows][0][0].encoding).to eq(Encoding::BINARY)
    end
  end

  describe ".decode_ok" do
    it "decodes an OK response" do
      payload = [42].pack("q<") + [5].pack("C") + "DONE!".b
      result = PyroSQL::Codec.decode_ok(payload)
      expect(result[:rows_affected]).to eq(42)
      expect(result[:tag]).to eq("DONE!")
    end
  end

  describe ".decode_error" do
    it "decodes an ERROR response" do
      payload = "42P01".b + [13].pack("v") + "table missing".b
      result = PyroSQL::Codec.decode_error(payload)
      expect(result[:sql_state]).to eq("42P01")
      expect(result[:message]).to eq("table missing")
    end
  end

  describe ".read_frame" do
    it "reads a frame from IO" do
      frame_data = PyroSQL::Codec.frame(0x05, "test".b)
      io = StringIO.new(frame_data)
      type, payload = PyroSQL::Codec.read_frame(io)
      expect(type).to eq(0x05)
      expect(payload).to eq("test".b)
    end

    it "reads an empty frame from IO" do
      frame_data = PyroSQL::Codec.frame(0xFF)
      io = StringIO.new(frame_data)
      type, payload = PyroSQL::Codec.read_frame(io)
      expect(type).to eq(0xFF)
      expect(payload).to eq("".b)
    end
  end
end

RSpec.describe PyroSQL::Codec, "LZ4 compression" do
  describe ".compress_frame" do
    it "does not compress small payloads" do
      payload = "A".b * 100
      frame = PyroSQL::Codec.compress_frame(PyroSQL::Codec::MSG_QUERY, payload)
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_QUERY)
    end

    it "compresses large compressible payloads" do
      payload = "X".b * (16 * 1024)
      frame = PyroSQL::Codec.compress_frame(PyroSQL::Codec::MSG_QUERY, payload)
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_COMPRESSED)
      expect(frame.bytesize).to be < payload.bytesize
    end
  end

  describe ".decompress_frame" do
    it "round-trips compress and decompress" do
      original = ((0...16384).map { |i| (i % 26 + 97).chr }.join).b
      frame = PyroSQL::Codec.compress_frame(PyroSQL::Codec::MSG_QUERY, original)
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_COMPRESSED)

      payload_len = frame[1, 4].unpack1("V")
      inner_payload = frame[5, payload_len]

      orig_type, decompressed = PyroSQL::Codec.decompress_frame(inner_payload)
      expect(orig_type).to eq(PyroSQL::Codec::MSG_QUERY)
      expect(decompressed).to eq(original)
    end
  end

  describe ".encode_auth_with_caps" do
    it "includes the capability byte" do
      frame = PyroSQL::Codec.encode_auth_with_caps("admin", "secret", PyroSQL::Codec::CAP_LZ4)
      expect(frame.getbyte(0)).to eq(PyroSQL::Codec::MSG_AUTH)
      payload_len = frame[1, 4].unpack1("V")
      # user(1+5) + pass(1+6) + caps(1) = 14
      expect(payload_len).to eq(14)
      # Last byte of payload is the caps
      expect(frame.getbyte(5 + 13)).to eq(PyroSQL::Codec::CAP_LZ4)
    end
  end

  describe ".read_frame with MSG_COMPRESSED" do
    it "transparently decompresses" do
      original = "Z".b * (16 * 1024)
      compressed_frame = PyroSQL::Codec.compress_frame(PyroSQL::Codec::MSG_QUERY, original)
      io = StringIO.new(compressed_frame)
      type, payload = PyroSQL::Codec.read_frame(io)
      expect(type).to eq(PyroSQL::Codec::MSG_QUERY)
      expect(payload).to eq(original)
    end
  end
end

RSpec.describe PyroSQL::Connection do
  describe "#initialize" do
    it "sets default host and port" do
      conn = PyroSQL::Connection.new
      expect(conn.host).to eq("localhost")
      expect(conn.port).to eq(12520)
    end

    it "accepts custom host and port" do
      conn = PyroSQL::Connection.new(host: "db.example.com", port: 9999)
      expect(conn.host).to eq("db.example.com")
      expect(conn.port).to eq(9999)
    end

    it "is not connected initially" do
      conn = PyroSQL::Connection.new
      expect(conn.connected?).to eq(false)
    end
  end
end

RSpec.describe ActiveRecord::ConnectionAdapters::PyroSqlAdapter do
  describe "NATIVE_DATABASE_TYPES" do
    it "defines all standard ActiveRecord types" do
      types = ActiveRecord::ConnectionAdapters::PyroSqlAdapter::NATIVE_DATABASE_TYPES
      expect(types).to include(:primary_key, :string, :text, :integer, :bigint,
                               :float, :decimal, :datetime, :binary, :boolean)
    end
  end

  describe ".adapter_name" do
    it "returns PyroSQL" do
      # We need a mock connection to instantiate the adapter
      mock_conn = instance_double(PyroSQL::Connection, connected?: true, ping: true)
      adapter = ActiveRecord::ConnectionAdapters::PyroSqlAdapter.new(
        mock_conn, nil, { adapter: "pyrosql" }
      )
      expect(adapter.adapter_name).to eq("PyroSQL")
    end
  end

  describe "quoting" do
    let(:mock_conn) { instance_double(PyroSQL::Connection, connected?: true, ping: true) }
    let(:adapter) do
      ActiveRecord::ConnectionAdapters::PyroSqlAdapter.new(
        mock_conn, nil, { adapter: "pyrosql" }
      )
    end

    it "quotes column names with double quotes" do
      expect(adapter.quote_column_name("id")).to eq('"id"')
      expect(adapter.quote_column_name("user name")).to eq('"user name"')
    end

    it "escapes double quotes in column names" do
      expect(adapter.quote_column_name('col"name')).to eq('"col""name"')
    end

    it "quotes table names with double quotes" do
      expect(adapter.quote_table_name("users")).to eq('"users"')
    end

    it "escapes single quotes in strings" do
      expect(adapter.quote_string("it's")).to eq("it''s")
    end

    it "returns correct boolean literals" do
      expect(adapter.quoted_true).to eq("TRUE")
      expect(adapter.quoted_false).to eq("FALSE")
    end
  end

  describe "feature detection" do
    let(:mock_conn) { instance_double(PyroSQL::Connection, connected?: true, ping: true) }
    let(:adapter) do
      ActiveRecord::ConnectionAdapters::PyroSqlAdapter.new(
        mock_conn, nil, { adapter: "pyrosql" }
      )
    end

    it "supports DDL transactions" do
      expect(adapter.supports_ddl_transactions?).to be true
    end

    it "supports foreign keys" do
      expect(adapter.supports_foreign_keys?).to be true
    end

    it "supports views" do
      expect(adapter.supports_views?).to be true
    end
  end
end
