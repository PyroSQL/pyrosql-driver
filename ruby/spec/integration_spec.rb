# frozen_string_literal: true

# Integration tests for the PyroSQL Ruby driver.
# Requires a running PyroSQL server at localhost:12520.

require_relative "../lib/pyrosql/connection"
require_relative "../lib/pyrosql/codec"

HOST     = "localhost"
PORT     = 12520
USERNAME = "admin"
PASSWORD = "admin"

def new_connection
  conn = PyroSQL::Connection.new(host: HOST, port: PORT, connect_timeout: 10)
  conn.connect(username: USERNAME, password: PASSWORD)
  conn
end

def safe_query(conn, sql)
  conn.query(sql)
rescue PyroSQL::ServerError
  # ignore
end

# ===========================================================
# Low-level Connection Tests
# ===========================================================

puts "=" * 60
puts "PyroSQL Ruby Driver - Integration Tests"
puts "=" * 60
puts

pass_count = 0
fail_count = 0
errors = []

def run_test(name)
  print "  #{name}... "
  begin
    yield
    puts "PASS"
    return true
  rescue => e
    puts "FAIL"
    puts "    Error: #{e.class}: #{e.message}"
    puts "    #{e.backtrace&.first}"
    return false
  end
end

# ---- Connection ----

puts "[Connection Tests]"

if run_test("Connect to PyroSQL server") {
  conn = new_connection
  raise "not connected" unless conn.connected?
  conn.close
}
  pass_count += 1
else
  fail_count += 1
  errors << "Connect to PyroSQL server"
end

if run_test("Ping server") {
  conn = new_connection
  result = conn.ping
  raise "ping returned false" unless result
  conn.close
}
  pass_count += 1
else
  fail_count += 1
  errors << "Ping server"
end

if run_test("Close and reconnect") {
  conn = new_connection
  conn.close
  raise "should be disconnected" if conn.connected?
  conn2 = new_connection
  raise "should be connected" unless conn2.connected?
  conn2.close
}
  pass_count += 1
else
  fail_count += 1
  errors << "Close and reconnect"
end

# ---- DDL ----

puts
puts "[DDL Tests]"

conn = new_connection

safe_query(conn, "DROP TABLE IF EXISTS test_crud")
safe_query(conn, "DROP TABLE IF EXISTS test_types")
safe_query(conn, "DROP TABLE IF EXISTS test_txn")
safe_query(conn, "DROP TABLE IF EXISTS test_params")

if run_test("CREATE TABLE") {
  result = conn.query("CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT, value DOUBLE)")
  raise "unexpected result type: #{result.class}" unless result.is_a?(PyroSQL::OkResult)
}
  pass_count += 1
else
  fail_count += 1
  errors << "CREATE TABLE"
end

# ---- CRUD ----

puts
puts "[CRUD Tests]"

if run_test("INSERT rows") {
  r1 = conn.query("INSERT INTO test_crud (id, name, value) VALUES (1, 'Alice', 3.14)")
  r2 = conn.query("INSERT INTO test_crud (id, name, value) VALUES (2, 'Bob', 2.718)")
  raise "expected OkResult" unless r1.is_a?(PyroSQL::OkResult) && r2.is_a?(PyroSQL::OkResult)
}
  pass_count += 1
else
  fail_count += 1
  errors << "INSERT rows"
end

if run_test("SELECT all rows") {
  result = conn.query("SELECT id, name, value FROM test_crud ORDER BY id")
  raise "expected ResultSet, got #{result.class}" unless result.is_a?(PyroSQL::ResultSet)
  raise "expected 2 columns, got #{result.columns.size}" unless result.columns.size >= 2
  raise "expected 2 rows, got #{result.rows.size}" unless result.rows.size == 2

  raise "row 0 id mismatch: #{result.rows[0][0]}" unless result.rows[0][0].to_s == "1"
  raise "row 0 name mismatch: #{result.rows[0][1]}" unless result.rows[0][1] == "Alice"
  raise "row 1 id mismatch: #{result.rows[1][0]}" unless result.rows[1][0].to_s == "2"
  raise "row 1 name mismatch: #{result.rows[1][1]}" unless result.rows[1][1] == "Bob"
}
  pass_count += 1
else
  fail_count += 1
  errors << "SELECT all rows"
end

if run_test("UPDATE a row") {
  conn.query("UPDATE test_crud SET name = 'Alicia' WHERE id = 1")
  result = conn.query("SELECT name FROM test_crud WHERE id = 1")
  raise "expected Alicia, got #{result.rows[0][0]}" unless result.rows[0][0] == "Alicia"
}
  pass_count += 1
else
  fail_count += 1
  errors << "UPDATE a row"
end

if run_test("DELETE a row") {
  conn.query("DELETE FROM test_crud WHERE id = 1")
  result = conn.query("SELECT id FROM test_crud")
  raise "expected 1 row after delete, got #{result.rows.size}" unless result.rows.size == 1
  raise "expected id=2, got #{result.rows[0][0]}" unless result.rows[0][0].to_s == "2"
}
  pass_count += 1
else
  fail_count += 1
  errors << "DELETE a row"
end

# ---- Data Types ----

puts
puts "[Data Type Tests]"

safe_query(conn, "DROP TABLE IF EXISTS test_types")
conn.query("CREATE TABLE test_types (id BIGINT NOT NULL PRIMARY KEY, int_val BIGINT, float_val DOUBLE, text_val TEXT, bool_val BOOLEAN)")

if run_test("Integer values") {
  conn.query("INSERT INTO test_types (id, int_val) VALUES (1, 9999999999)")
  result = conn.query("SELECT int_val FROM test_types WHERE id = 1")
  raise "expected 9999999999, got #{result.rows[0][0]}" unless result.rows[0][0].to_s == "9999999999"
}
  pass_count += 1
else
  fail_count += 1
  errors << "Integer values"
end

if run_test("Float/Double values") {
  conn.query("INSERT INTO test_types (id, float_val) VALUES (2, 1.23456789)")
  result = conn.query("SELECT float_val FROM test_types WHERE id = 2")
  val = result.rows[0][0]
  raise "expected ~1.23456789, got #{val}" unless (val.to_f - 1.23456789).abs < 0.0001
}
  pass_count += 1
else
  fail_count += 1
  errors << "Float/Double values"
end

if run_test("Text values") {
  conn.query("INSERT INTO test_types (id, text_val) VALUES (3, 'Hello, World!')")
  result = conn.query("SELECT text_val FROM test_types WHERE id = 3")
  raise "expected 'Hello, World!', got #{result.rows[0][0]}" unless result.rows[0][0] == "Hello, World!"
}
  pass_count += 1
else
  fail_count += 1
  errors << "Text values"
end

if run_test("Boolean values") {
  conn.query("INSERT INTO test_types (id, bool_val) VALUES (4, TRUE)")
  conn.query("INSERT INTO test_types (id, bool_val) VALUES (5, FALSE)")
  r1 = conn.query("SELECT bool_val FROM test_types WHERE id = 4")
  r2 = conn.query("SELECT bool_val FROM test_types WHERE id = 5")
  raise "expected true, got #{r1.rows[0][0]}" unless r1.rows[0][0].to_s == "true" || r1.rows[0][0] == true
  raise "expected false, got #{r2.rows[0][0]}" unless r2.rows[0][0].to_s == "false" || r2.rows[0][0] == false
}
  pass_count += 1
else
  fail_count += 1
  errors << "Boolean values"
end

if run_test("NULL values") {
  conn.query("INSERT INTO test_types (id, text_val) VALUES (6, NULL)")
  result = conn.query("SELECT text_val FROM test_types WHERE id = 6")
  raise "expected nil, got #{result.rows[0][0].inspect}" unless result.rows[0][0].nil?
}
  pass_count += 1
else
  fail_count += 1
  errors << "NULL values"
end

# ---- Multiple Rows ----

puts
puts "[Multiple Rows Test]"

safe_query(conn, "DROP TABLE IF EXISTS test_crud")
conn.query("CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT)")

if run_test("Insert and select 10 rows") {
  10.times do |i|
    conn.query("INSERT INTO test_crud (id, name) VALUES (#{i + 1}, 'User#{i + 1}')")
  end

  result = conn.query("SELECT id, name FROM test_crud ORDER BY id")
  raise "expected 10 rows, got #{result.rows.size}" unless result.rows.size == 10
  result.rows.each_with_index do |row, idx|
    raise "id mismatch at row #{idx}: #{row[0].inspect}" unless row[0].to_s == (idx + 1).to_s
    raise "name mismatch at row #{idx}: #{row[1].inspect}" unless row[1] == "User#{idx + 1}"
  end
}
  pass_count += 1
else
  fail_count += 1
  errors << "Insert and select 10 rows"
end

# ---- Transactions ----

puts
puts "[Transaction Tests]"

safe_query(conn, "DROP TABLE IF EXISTS test_txn")
conn.query("CREATE TABLE test_txn (id BIGINT NOT NULL PRIMARY KEY, name TEXT)")

if run_test("BEGIN / INSERT / COMMIT") {
  conn.query("BEGIN")
  conn.query("INSERT INTO test_txn (id, name) VALUES (1, 'TxnTest')")
  conn.query("COMMIT")
  result = conn.query("SELECT name FROM test_txn WHERE id = 1")
  raise "expected TxnTest, got #{result.rows[0][0]}" unless result.rows[0][0] == "TxnTest"
}
  pass_count += 1
else
  fail_count += 1
  errors << "BEGIN / INSERT / COMMIT"
end

safe_query(conn, "DELETE FROM test_txn")

if run_test("BEGIN / INSERT / ROLLBACK") {
  conn.query("BEGIN")
  conn.query("INSERT INTO test_txn (id, name) VALUES (2, 'ShouldNotExist')")
  conn.query("ROLLBACK")
  result = conn.query("SELECT id FROM test_txn WHERE id = 2")
  raise "expected 0 rows after rollback, got #{result.rows.size}" unless result.rows.empty?
}
  pass_count += 1
else
  fail_count += 1
  errors << "BEGIN / INSERT / ROLLBACK"
end

# ---- Prepared Statements ----

puts
puts "[Prepared Statement Tests]"

safe_query(conn, "DROP TABLE IF EXISTS test_params")
conn.query("CREATE TABLE test_params (id BIGINT NOT NULL PRIMARY KEY, name TEXT, score DOUBLE)")

if run_test("Prepared INSERT with parameters") {
  conn.query_prepared(
    "INSERT INTO test_params (id, name, score) VALUES ($1, $2, $3)",
    [1, "Alice", 95.5]
  )
  result = conn.query("SELECT id, name, score FROM test_params WHERE id = 1")
  raise "expected 1 row" unless result.rows.size == 1
  raise "name mismatch" unless result.rows[0][1] == "Alice"
}
  pass_count += 1
else
  fail_count += 1
  errors << "Prepared INSERT with parameters"
end

if run_test("Prepared SELECT with parameters") {
  conn.query("INSERT INTO test_params (id, name, score) VALUES (2, 'Bob', 88.0)")
  result = conn.query_prepared(
    "SELECT name, score FROM test_params WHERE id = $1",
    [2]
  )
  raise "expected ResultSet" unless result.is_a?(PyroSQL::ResultSet)
  raise "expected Bob, got #{result.rows[0][0]}" unless result.rows[0][0] == "Bob"
}
  pass_count += 1
else
  fail_count += 1
  errors << "Prepared SELECT with parameters"
end

# ---- Error Handling ----

puts
puts "[Error Handling Tests]"

if run_test("Invalid SQL raises ServerError") {
  begin
    conn.query("THIS IS NOT VALID SQL AT ALL")
    raise "should have raised"
  rescue PyroSQL::ServerError => e
    # Expected
    raise "no sql_state" if e.sql_state.nil? || e.sql_state.empty?
  end
}
  pass_count += 1
else
  fail_count += 1
  errors << "Invalid SQL raises ServerError"
end

if run_test("SELECT from nonexistent table raises ServerError") {
  begin
    conn.query("SELECT * FROM nonexistent_table_xyz")
    raise "should have raised"
  rescue PyroSQL::ServerError
    # Expected
  end
}
  pass_count += 1
else
  fail_count += 1
  errors << "SELECT from nonexistent table raises ServerError"
end

# ---- ResultSet metadata ----

puts
puts "[ResultSet Metadata Tests]"

if run_test("Column names in ResultSet") {
  result = conn.query("SELECT id, name, score FROM test_params ORDER BY id LIMIT 1")
  raise "expected ResultSet" unless result.is_a?(PyroSQL::ResultSet)
  col_names = result.columns.map(&:name)
  raise "expected column 'id', got #{col_names}" unless col_names.include?("id")
  raise "expected column 'name', got #{col_names}" unless col_names.include?("name")
  raise "expected column 'score', got #{col_names}" unless col_names.include?("score")
}
  pass_count += 1
else
  fail_count += 1
  errors << "Column names in ResultSet"
end

if run_test("Enumerable support on ResultSet") {
  safe_query(conn, "DROP TABLE IF EXISTS test_crud")
  conn.query("CREATE TABLE test_crud (id BIGINT NOT NULL PRIMARY KEY, name TEXT)")
  conn.query("INSERT INTO test_crud (id, name) VALUES (1, 'A')")
  conn.query("INSERT INTO test_crud (id, name) VALUES (2, 'B')")
  result = conn.query("SELECT name FROM test_crud ORDER BY id")
  names = result.map { |row| row[0] }
  raise "expected [A, B], got #{names}" unless names == ["A", "B"]
}
  pass_count += 1
else
  fail_count += 1
  errors << "Enumerable support on ResultSet"
end

# ---- Cleanup ----

safe_query(conn, "DROP TABLE IF EXISTS test_crud")
safe_query(conn, "DROP TABLE IF EXISTS test_types")
safe_query(conn, "DROP TABLE IF EXISTS test_txn")
safe_query(conn, "DROP TABLE IF EXISTS test_params")
conn.close

# ---- Summary ----

puts
puts "=" * 60
total = pass_count + fail_count
puts "Results: #{pass_count}/#{total} passed, #{fail_count} failed"
if fail_count > 0
  puts "Failed tests:"
  errors.each { |e| puts "  - #{e}" }
end
puts "=" * 60

exit(fail_count > 0 ? 1 : 0)
