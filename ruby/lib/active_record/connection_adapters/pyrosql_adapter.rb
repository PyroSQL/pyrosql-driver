# frozen_string_literal: true

require "active_record"
require "active_record/connection_adapters/abstract_adapter"
require_relative "../../pyrosql/connection"
require_relative "../../pyrosql/codec"

module ActiveRecord
  module ConnectionHandling
    # Establishes a connection to a PyroSQL database.
    #
    #   ActiveRecord::Base.establish_connection(
    #     adapter:  "pyrosql",
    #     host:     "localhost",
    #     port:     12520,
    #     database: "mydb",
    #     username: "admin",
    #     password: "secret"
    #   )
    def pyrosql_connection(config)
      config = config.symbolize_keys
      host     = config[:host] || "localhost"
      port     = (config[:port] || 12520).to_i
      username = config[:username] || ""
      password = config[:password] || ""

      conn = PyroSQL::Connection.new(host: host, port: port)
      conn.connect(username: username, password: password)

      ConnectionAdapters::PyroSqlAdapter.new(conn, logger, config)
    end
  end

  module ConnectionAdapters
    # ActiveRecord adapter for PyroSQL using the PWire binary protocol.
    class PyroSqlAdapter < AbstractAdapter
      ADAPTER_NAME = "PyroSQL"

      NATIVE_DATABASE_TYPES = {
        primary_key: "BIGINT NOT NULL PRIMARY KEY",
        string:      { name: "TEXT" },
        text:        { name: "TEXT" },
        integer:     { name: "BIGINT" },
        bigint:      { name: "BIGINT" },
        float:       { name: "DOUBLE" },
        decimal:     { name: "DECIMAL" },
        datetime:    { name: "TIMESTAMP" },
        date:        { name: "TIMESTAMP" },
        time:        { name: "TEXT" },
        binary:      { name: "BLOB" },
        blob:        { name: "BLOB" },
        boolean:     { name: "BOOLEAN" },
        json:        { name: "TEXT" },
      }.freeze

      def initialize(connection, logger, config)
        super(connection, logger, config)
        @pyro_connection = connection
        @config = config
      end

      def adapter_name
        ADAPTER_NAME
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      # -- Connection management --

      def active?
        @pyro_connection.connected? && @pyro_connection.ping
      rescue StandardError
        false
      end

      def reconnect!
        super
        disconnect!
        config = @config.symbolize_keys
        @pyro_connection = PyroSQL::Connection.new(
          host: config[:host] || "localhost",
          port: (config[:port] || 12520).to_i
        )
        @pyro_connection.connect(
          username: config[:username] || "",
          password: config[:password] || ""
        )
        @connection = @pyro_connection
      end

      def disconnect!
        super
        @pyro_connection.close
      end

      # -- Quoting --

      def quote_column_name(name)
        %("#{name.to_s.gsub('"', '""')}")
      end

      def quote_table_name(name)
        %("#{name.to_s.gsub('"', '""')}")
      end

      def quote_string(s)
        s.gsub("'", "''")
      end

      def quoted_true
        "TRUE"
      end

      def quoted_false
        "FALSE"
      end

      # -- Database statements --

      def execute(sql, name = nil)
        log(sql, name) do
          @pyro_connection.query(sql)
        end
      end

      def exec_query(sql, name = "SQL", binds = [], prepare: false)
        log(sql, name, binds) do
          result = if binds.empty?
            @pyro_connection.query(sql)
          else
            param_values = binds.map { |b| b.respond_to?(:value_for_database) ? b.value_for_database : b }
            @pyro_connection.query_prepared(sql, param_values.map(&:to_s))
          end

          if result.is_a?(PyroSQL::ResultSet)
            columns = result.columns.map(&:name)
            ActiveRecord::Result.new(columns, result.rows)
          else
            ActiveRecord::Result.new([], [])
          end
        end
      end

      def exec_insert(sql, name = nil, binds = [], pk = nil, sequence_name = nil, returning: nil)
        result = exec_query(sql, name, binds)
        result
      end

      def exec_delete(sql, name = nil, binds = [])
        result = execute(sql, name)
        result.is_a?(PyroSQL::OkResult) ? result.rows_affected : 0
      end

      def exec_update(sql, name = nil, binds = [])
        exec_delete(sql, name, binds)
      end

      def last_inserted_id(result)
        # PyroSQL returns last insert id via query
        row = exec_query("SELECT last_insert_id()").rows.first
        row&.first
      end

      # -- Schema statements --

      def tables(name = nil)
        result = exec_query(
          "SELECT table_name FROM pyro_catalog.tables WHERE schema_name = 'public'",
          "SCHEMA"
        )
        result.rows.map(&:first)
      end

      def table_exists?(table_name)
        tables.include?(table_name.to_s)
      end

      def columns(table_name, name = nil)
        result = exec_query(
          "SELECT column_name, data_type, is_nullable, column_default " \
          "FROM pyro_catalog.columns " \
          "WHERE table_name = '#{quote_string(table_name.to_s)}'",
          "SCHEMA"
        )

        result.rows.map do |row|
          col_name, data_type, nullable, default_val = row
          SqlTypeMetadata.new(
            sql_type: data_type,
            type: simplified_type(data_type)
          ).tap do |meta|
            # Build a Column object
          end
          Column.new(
            col_name,
            default_val,
            fetch_type_metadata(data_type),
            nullable == "YES"
          )
        end
      end

      def primary_keys(table_name)
        result = exec_query(
          "SELECT column_name FROM pyro_catalog.columns " \
          "WHERE table_name = '#{quote_string(table_name.to_s)}' AND is_primary_key = 'YES' " \
          "ORDER BY ordinal_position",
          "SCHEMA"
        )
        result.rows.map(&:first)
      end

      def create_table(table_name, **options, &block)
        td = create_table_definition(table_name, **options)
        block.call(td) if block

        sql = schema_creation.accept(td)
        execute(sql)

        td.indexes.each do |column_name, index_options|
          add_index(table_name, column_name, **index_options)
        end if td.respond_to?(:indexes)
      end

      def drop_table(table_name, **options)
        execute("DROP TABLE IF EXISTS #{quote_table_name(table_name)}")
      end

      def add_column(table_name, column_name, type, **options)
        at = create_alter_table(table_name)
        at.add_column(column_name, type, **options)
        execute(schema_creation.accept(at))
      end

      def remove_column(table_name, column_name, type = nil, **options)
        execute("ALTER TABLE #{quote_table_name(table_name)} DROP COLUMN #{quote_column_name(column_name)}")
      end

      def rename_table(table_name, new_name)
        execute("ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_table_name(new_name)}")
      end

      def rename_column(table_name, column_name, new_column_name)
        execute("ALTER TABLE #{quote_table_name(table_name)} RENAME COLUMN #{quote_column_name(column_name)} TO #{quote_column_name(new_column_name)}")
      end

      def add_index(table_name, column_name, **options)
        index_name = options[:name] || index_name(table_name, column_name)
        unique = options[:unique] ? "UNIQUE " : ""
        columns = Array(column_name).map { |c| quote_column_name(c) }.join(", ")
        execute("CREATE #{unique}INDEX #{quote_column_name(index_name)} ON #{quote_table_name(table_name)} (#{columns})")
      end

      def remove_index(table_name, column_name = nil, **options)
        index_name = options[:name] || index_name(table_name, column_name)
        execute("DROP INDEX IF EXISTS #{quote_column_name(index_name)}")
      end

      # -- Transaction support --

      def begin_db_transaction
        execute("BEGIN")
      end

      def commit_db_transaction
        execute("COMMIT")
      end

      def exec_rollback_db_transaction
        execute("ROLLBACK")
      end

      # -- Feature detection --

      def supports_ddl_transactions?
        true
      end

      def supports_index_sort_order?
        true
      end

      def supports_foreign_keys?
        true
      end

      def supports_views?
        true
      end

      def supports_datetime_with_precision?
        true
      end

      def supports_json?
        false
      end

      private

      def simplified_type(data_type)
        case data_type.to_s.upcase
        when /BIGINT/, /INT/
          :integer
        when /DOUBLE/, /FLOAT/, /REAL/
          :float
        when /DECIMAL/, /NUMERIC/
          :decimal
        when /BOOL/
          :boolean
        when /BLOB/, /BYTEA/
          :binary
        when /TIMESTAMP/, /DATETIME/, /DATE/
          :datetime
        else
          :string
        end
      end

      def index_name(table_name, column_name)
        "idx_#{table_name}_#{Array(column_name).join('_')}"
      end
    end
  end
end
