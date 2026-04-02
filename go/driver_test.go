package pyrosql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Mock PWire server for testing
// ---------------------------------------------------------------------------

type mockServer struct {
	listener net.Listener
	handler  func(net.Conn)
	wg       sync.WaitGroup
	mu       sync.Mutex
	closed   bool
}

func newMockServer(t *testing.T, handler func(net.Conn)) *mockServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &mockServer{listener: ln, handler: handler}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				s.mu.Lock()
				c := s.closed
				s.mu.Unlock()
				if c {
					return
				}
				continue
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer conn.Close()
				handler(conn)
			}()
		}
	}()
	return s
}

func (s *mockServer) addr() string {
	return s.listener.Addr().String()
}

func (s *mockServer) dsn() string {
	return fmt.Sprintf("pyrosql://testuser:testpass@%s/testdb", s.addr())
}

func (s *mockServer) dsnNoAuth() string {
	return fmt.Sprintf("pyrosql://%s/testdb", s.addr())
}

func (s *mockServer) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.listener.Close()
	s.wg.Wait()
}

// writeOK writes an OK response frame.
func writeOK(w io.Writer, rowsAffected int64, tag string) {
	payload := make([]byte, 9+len(tag))
	binary.LittleEndian.PutUint64(payload[0:8], uint64(rowsAffected))
	payload[8] = byte(len(tag))
	copy(payload[9:], tag)
	writeFrame(w, RespOK, payload)
}

// writeReady writes a READY response frame.
func writeReady(w io.Writer) {
	writeFrame(w, RespReady, nil)
}

// writeError writes an ERROR response frame.
func writeErrorResp(w io.Writer, sqlstate, message string) {
	payload := make([]byte, 7+len(message))
	copy(payload[0:5], sqlstate)
	binary.LittleEndian.PutUint16(payload[5:7], uint16(len(message)))
	copy(payload[7:], message)
	writeFrame(w, RespError, payload)
}

// writePong writes a PONG response frame.
func writePong(w io.Writer) {
	writeFrame(w, RespPong, nil)
}

// writeFrame writes a raw PWire frame.
func writeFrame(w io.Writer, msgType byte, payload []byte) {
	buf := make([]byte, headerSize+len(payload))
	buf[0] = msgType
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	w.Write(buf)
}

// writeResultSet writes a full RESULT_SET frame with the given columns and rows.
func writeResultSet(w io.Writer, columns []Column, rows [][]interface{}) {
	var buf bytes.Buffer

	// Column count
	binary.Write(&buf, binary.LittleEndian, uint16(len(columns)))

	// Column definitions
	for _, col := range columns {
		buf.WriteByte(byte(len(col.Name)))
		buf.WriteString(col.Name)
		buf.WriteByte(col.TypeTag)
	}

	// Row count
	binary.Write(&buf, binary.LittleEndian, uint32(len(rows)))

	colCount := len(columns)
	nullBitmapLen := (colCount + 7) / 8

	for _, row := range rows {
		// Null bitmap
		bitmap := make([]byte, nullBitmapLen)
		for c := 0; c < colCount; c++ {
			if c < len(row) && row[c] == nil {
				byteIdx := c / 8
				bitIdx := uint(c % 8)
				bitmap[byteIdx] |= 1 << bitIdx
			}
		}
		buf.Write(bitmap)

		// Values
		for c := 0; c < colCount; c++ {
			if c < len(row) && row[c] == nil {
				continue
			}

			switch columns[c].TypeTag {
			case TypeI64:
				v := row[c].(int64)
				binary.Write(&buf, binary.LittleEndian, v)
			case TypeF64:
				v := row[c].(float64)
				binary.Write(&buf, binary.LittleEndian, math.Float64bits(v))
			case TypeBool:
				v := row[c].(bool)
				if v {
					buf.WriteByte(1)
				} else {
					buf.WriteByte(0)
				}
			case TypeText:
				v := row[c].(string)
				binary.Write(&buf, binary.LittleEndian, uint16(len(v)))
				buf.WriteString(v)
			case TypeBytes:
				v := row[c].([]byte)
				binary.Write(&buf, binary.LittleEndian, uint16(len(v)))
				buf.Write(v)
			}
		}
	}

	writeFrame(w, RespResultSet, buf.Bytes())
}

// readClientFrame reads a PWire frame from the client.
func readClientFrame(r io.Reader) (byte, []byte, error) {
	return readFrame(r)
}

// ---------------------------------------------------------------------------
// A general-purpose handler that supports auth, ping, query, exec, prepare,
// execute, close, transactions, and quit.
// ---------------------------------------------------------------------------

func fullHandler(conn net.Conn) {
	stmtCounter := uint32(0)
	for {
		msgType, payload, err := readClientFrame(conn)
		if err != nil {
			return
		}

		switch msgType {
		case MsgAuth:
			writeReady(conn)

		case MsgPing:
			writePong(conn)

		case MsgQuit:
			return

		case MsgQuery:
			sql := string(payload)
			switch {
			case sql == "BEGIN" || startsWith(sql, "BEGIN "):
				writeOK(conn, 0, "BEGIN")
			case sql == "COMMIT":
				writeOK(conn, 0, "COMMIT")
			case sql == "ROLLBACK":
				writeOK(conn, 0, "ROLLBACK")
			case startsWith(sql, "SELECT"):
				// Return a test result set based on the query
				handleSelectQuery(conn, sql)
			case startsWith(sql, "INSERT") || startsWith(sql, "UPDATE") || startsWith(sql, "DELETE"):
				writeOK(conn, 1, "OK")
			case startsWith(sql, "CREATE") || startsWith(sql, "DROP"):
				writeOK(conn, 0, "OK")
			default:
				writeOK(conn, 0, "OK")
			}

		case MsgPrepare:
			stmtCounter++
			writeOK(conn, int64(stmtCounter), "PREPARE")

		case MsgExecute:
			// handle is in first 4 bytes, params follow
			if len(payload) >= 4 {
				// Return a result set so both Query and Exec work with prepared stmts
				writeResultSet(conn,
					[]Column{{Name: "result", TypeTag: TypeText}},
					[][]interface{}{{"ok"}},
				)
			} else {
				writeErrorResp(conn, "42000", "malformed execute")
			}

		case MsgClose:
			writeOK(conn, 0, "CLOSE")

		default:
			writeErrorResp(conn, "42000", fmt.Sprintf("unknown message type 0x%02x", msgType))
		}
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func handleSelectQuery(conn net.Conn, sql string) {
	switch {
	case sql == "SELECT 1":
		writeResultSet(conn,
			[]Column{{Name: "?column?", TypeTag: TypeI64}},
			[][]interface{}{{int64(1)}},
		)

	case sql == "SELECT NULL":
		writeResultSet(conn,
			[]Column{{Name: "?column?", TypeTag: TypeText}},
			[][]interface{}{{nil}},
		)

	case sql == "SELECT_ALL_TYPES":
		columns := []Column{
			{Name: "i64_col", TypeTag: TypeI64},
			{Name: "f64_col", TypeTag: TypeF64},
			{Name: "text_col", TypeTag: TypeText},
			{Name: "bool_col", TypeTag: TypeBool},
			{Name: "bytes_col", TypeTag: TypeBytes},
			{Name: "null_col", TypeTag: TypeText},
		}
		rows := [][]interface{}{
			{int64(42), float64(3.14), "hello", true, []byte{0xDE, 0xAD}, nil},
			{int64(-1), float64(0.0), "", false, []byte{}, nil},
		}
		writeResultSet(conn, columns, rows)

	case sql == "SELECT_EMPTY":
		writeResultSet(conn,
			[]Column{{Name: "id", TypeTag: TypeI64}},
			[][]interface{}{},
		)

	case sql == "SELECT_MULTI_ROW":
		columns := []Column{
			{Name: "id", TypeTag: TypeI64},
			{Name: "name", TypeTag: TypeText},
		}
		rows := [][]interface{}{
			{int64(1), "alice"},
			{int64(2), "bob"},
			{int64(3), "charlie"},
		}
		writeResultSet(conn, columns, rows)

	case sql == "SELECT_LARGE_INT":
		writeResultSet(conn,
			[]Column{{Name: "big", TypeTag: TypeI64}},
			[][]interface{}{{int64(9223372036854775807)}}, // max int64
		)

	case sql == "SELECT_ERROR":
		writeErrorResp(conn, "42P01", "relation does not exist")

	default:
		writeResultSet(conn,
			[]Column{{Name: "result", TypeTag: TypeText}},
			[][]interface{}{{"ok"}},
		)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDriverRegistered(t *testing.T) {
	drivers := sql.Drivers()
	found := false
	for _, d := range drivers {
		if d == "pyrosql" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("pyrosql driver not registered")
	}
}

func TestConnect(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsn())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestConnectNoAuth(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestConnectFailure(t *testing.T) {
	// Try connecting to a port that nothing listens on
	db, err := sql.Open("pyrosql", "pyrosql://127.0.0.1:59999/testdb")
	if err != nil {
		t.Fatalf("Open should not fail: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err == nil {
		t.Fatal("expected ping to fail with no server")
	}
}

func TestPing(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("PingContext: %v", err)
	}
}

func TestQueryRow(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var val int64
	err = db.QueryRow("SELECT 1").Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestQueryMultipleRows(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT_MULTI_ROW")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type row struct {
		ID   int64
		Name string
	}
	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}
	if results[0].ID != 1 || results[0].Name != "alice" {
		t.Fatalf("row 0: got %+v", results[0])
	}
	if results[2].ID != 3 || results[2].Name != "charlie" {
		t.Fatalf("row 2: got %+v", results[2])
	}
}

func TestQueryEmpty(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT_EMPTY")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("expected no rows")
	}
}

func TestQueryAllTypes(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT_ALL_TYPES")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	// First row: 42, 3.14, "hello", true, 0xDEAD, NULL
	if !rows.Next() {
		t.Fatal("expected row 1")
	}
	var (
		i64Val  int64
		f64Val  float64
		textVal string
		boolVal bool
		bytesVal []byte
		nullVal sql.NullString
	)
	if err := rows.Scan(&i64Val, &f64Val, &textVal, &boolVal, &bytesVal, &nullVal); err != nil {
		t.Fatalf("Scan row 1: %v", err)
	}
	if i64Val != 42 {
		t.Errorf("i64: expected 42, got %d", i64Val)
	}
	if math.Abs(f64Val-3.14) > 0.001 {
		t.Errorf("f64: expected 3.14, got %f", f64Val)
	}
	if textVal != "hello" {
		t.Errorf("text: expected 'hello', got '%s'", textVal)
	}
	if !boolVal {
		t.Error("bool: expected true")
	}
	if !bytes.Equal(bytesVal, []byte{0xDE, 0xAD}) {
		t.Errorf("bytes: expected 0xDEAD, got %x", bytesVal)
	}
	if nullVal.Valid {
		t.Error("null: expected NULL")
	}

	// Second row: -1, 0.0, "", false, [], NULL
	if !rows.Next() {
		t.Fatal("expected row 2")
	}
	if err := rows.Scan(&i64Val, &f64Val, &textVal, &boolVal, &bytesVal, &nullVal); err != nil {
		t.Fatalf("Scan row 2: %v", err)
	}
	if i64Val != -1 {
		t.Errorf("i64: expected -1, got %d", i64Val)
	}
	if f64Val != 0.0 {
		t.Errorf("f64: expected 0.0, got %f", f64Val)
	}
	if textVal != "" {
		t.Errorf("text: expected empty string, got '%s'", textVal)
	}
	if boolVal {
		t.Error("bool: expected false")
	}

	if rows.Next() {
		t.Fatal("expected no more rows")
	}
}

func TestNullHandling(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var val sql.NullString
	err = db.QueryRow("SELECT NULL").Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val.Valid {
		t.Fatalf("expected NULL, got %q", val.String)
	}
}

func TestLargeInt(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var val int64
	err = db.QueryRow("SELECT_LARGE_INT").Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != 9223372036854775807 {
		t.Fatalf("expected max int64, got %d", val)
	}
}

func TestExec(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Exec("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 row affected, got %d", affected)
	}
}

func TestExecDDL(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Exec DDL: %v", err)
	}

	_, err = db.Exec("DROP TABLE test")
	if err != nil {
		t.Fatalf("Exec DDL: %v", err)
	}
}

func TestQueryError(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Query("SELECT_ERROR")
	if err == nil {
		t.Fatal("expected error")
	}

	pyroErr, ok := err.(*PyroError)
	if !ok {
		t.Fatalf("expected *PyroError, got %T: %v", err, err)
	}
	if pyroErr.SQLState != "42P01" {
		t.Errorf("expected SQLState 42P01, got %s", pyroErr.SQLState)
	}
	if pyroErr.Message != "relation does not exist" {
		t.Errorf("unexpected message: %s", pyroErr.Message)
	}
}

func TestTransactionCommit(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestTransactionRollback(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestTransactionWithIsolation(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()

	var val int64
	err = tx.QueryRow("SELECT 1").Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow in tx: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestPreparedStatement(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare("INSERT INTO test (id) VALUES ($1)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.Exec(42)
	if err != nil {
		t.Fatalf("stmt.Exec: %v", err)
	}
	_, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
}

func TestPreparedStatementQuery(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT result FROM test WHERE id = $1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatalf("stmt.Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
}

func TestConnectionPooling(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var val int64
			err := db.QueryRow("SELECT 1").Scan(&val)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %w", i, err)
				return
			}
			if val != 1 {
				errs <- fmt.Errorf("goroutine %d: expected 1, got %d", i, val)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestAuthError(t *testing.T) {
	srv := newMockServer(t, func(conn net.Conn) {
		msgType, _, err := readClientFrame(conn)
		if err != nil {
			return
		}
		if msgType == MsgAuth {
			writeErrorResp(conn, "28P01", "authentication failed")
		}
	})
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err == nil {
		t.Fatal("expected auth error")
	}
}

func TestColumnTypes(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT_ALL_TYPES")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}

	expected := []struct {
		name   string
		dbType string
	}{
		{"i64_col", "BIGINT"},
		{"f64_col", "DOUBLE"},
		{"text_col", "TEXT"},
		{"bool_col", "BOOLEAN"},
		{"bytes_col", "BYTEA"},
		{"null_col", "TEXT"},
	}

	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(cols))
	}

	for i, col := range cols {
		if col.Name() != expected[i].name {
			t.Errorf("col %d: expected name %q, got %q", i, expected[i].name, col.Name())
		}
		if col.DatabaseTypeName() != expected[i].dbType {
			t.Errorf("col %d: expected dbtype %q, got %q", i, expected[i].dbType, col.DatabaseTypeName())
		}
		nullable, ok := col.Nullable()
		if !ok || !nullable {
			t.Errorf("col %d: expected nullable=true, ok=true", i)
		}
	}
}

func TestCloseIdempotent(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic or error
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConvenienceConnect(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := Connect(srv.dsnNoAuth())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer db.Close()

	var val int64
	if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

// ---------------------------------------------------------------------------
// Codec unit tests
// ---------------------------------------------------------------------------

func TestCodecEncodeAuth(t *testing.T) {
	data := encodeAuth("user", "pass")
	// Header: type=0x06, length=10 (1+4+1+4)
	if data[0] != MsgAuth {
		t.Fatalf("expected MsgAuth, got 0x%02x", data[0])
	}
	length := binary.LittleEndian.Uint32(data[1:5])
	if length != 10 {
		t.Fatalf("expected payload length 10, got %d", length)
	}
	payload := data[5:]
	if payload[0] != 4 {
		t.Fatalf("expected user length 4, got %d", payload[0])
	}
	if string(payload[1:5]) != "user" {
		t.Fatalf("expected 'user', got %q", string(payload[1:5]))
	}
	if payload[5] != 4 {
		t.Fatalf("expected pass length 4, got %d", payload[5])
	}
	if string(payload[6:10]) != "pass" {
		t.Fatalf("expected 'pass', got %q", string(payload[6:10]))
	}
}

func TestCodecEncodeQuery(t *testing.T) {
	data := encodeQuery("SELECT 1")
	if data[0] != MsgQuery {
		t.Fatalf("expected MsgQuery, got 0x%02x", data[0])
	}
	length := binary.LittleEndian.Uint32(data[1:5])
	if int(length) != len("SELECT 1") {
		t.Fatalf("expected length %d, got %d", len("SELECT 1"), length)
	}
	if string(data[5:]) != "SELECT 1" {
		t.Fatalf("expected 'SELECT 1', got %q", string(data[5:]))
	}
}

func TestCodecEncodePrepare(t *testing.T) {
	data := encodePrepare("INSERT INTO t VALUES ($1)")
	if data[0] != MsgPrepare {
		t.Fatalf("expected MsgPrepare, got 0x%02x", data[0])
	}
}

func TestCodecEncodeExecute(t *testing.T) {
	data := encodeExecute(7, []string{"hello", "42"})
	if data[0] != MsgExecute {
		t.Fatalf("expected MsgExecute, got 0x%02x", data[0])
	}
	payload := data[5:]
	handle := binary.LittleEndian.Uint32(payload[0:4])
	if handle != 7 {
		t.Fatalf("expected handle 7, got %d", handle)
	}
	paramCount := binary.LittleEndian.Uint16(payload[4:6])
	if paramCount != 2 {
		t.Fatalf("expected 2 params, got %d", paramCount)
	}
}

func TestCodecEncodeClose(t *testing.T) {
	data := encodeClose(42)
	if data[0] != MsgClose {
		t.Fatalf("expected MsgClose, got 0x%02x", data[0])
	}
	handle := binary.LittleEndian.Uint32(data[5:9])
	if handle != 42 {
		t.Fatalf("expected handle 42, got %d", handle)
	}
}

func TestCodecEncodePing(t *testing.T) {
	data := encodePing()
	if data[0] != MsgPing {
		t.Fatalf("expected MsgPing, got 0x%02x", data[0])
	}
	length := binary.LittleEndian.Uint32(data[1:5])
	if length != 0 {
		t.Fatalf("expected 0 length, got %d", length)
	}
}

func TestCodecEncodeQuit(t *testing.T) {
	data := encodeQuit()
	if data[0] != MsgQuit {
		t.Fatalf("expected MsgQuit, got 0x%02x", data[0])
	}
}

func TestCodecDecodeOK(t *testing.T) {
	payload := make([]byte, 9+3)
	binary.LittleEndian.PutUint64(payload[0:8], 5)
	payload[8] = 3
	copy(payload[9:], "INS")

	ok, err := decodeOK(payload)
	if err != nil {
		t.Fatal(err)
	}
	if ok.RowsAffected != 5 {
		t.Fatalf("expected 5 rows, got %d", ok.RowsAffected)
	}
	if ok.Tag != "INS" {
		t.Fatalf("expected tag 'INS', got %q", ok.Tag)
	}
}

func TestCodecDecodeError(t *testing.T) {
	msg := "table not found"
	payload := make([]byte, 7+len(msg))
	copy(payload[0:5], "42P01")
	binary.LittleEndian.PutUint16(payload[5:7], uint16(len(msg)))
	copy(payload[7:], msg)

	pyroErr, err := decodeError(payload)
	if err != nil {
		t.Fatal(err)
	}
	if pyroErr.SQLState != "42P01" {
		t.Fatalf("expected SQLState 42P01, got %s", pyroErr.SQLState)
	}
	if pyroErr.Message != msg {
		t.Fatalf("expected message %q, got %q", msg, pyroErr.Message)
	}
}

func TestCodecDecodeResultSet(t *testing.T) {
	var buf bytes.Buffer

	// 2 columns
	binary.Write(&buf, binary.LittleEndian, uint16(2))

	// Col 1: "id" I64
	buf.WriteByte(2)
	buf.WriteString("id")
	buf.WriteByte(TypeI64)

	// Col 2: "name" TEXT
	buf.WriteByte(4)
	buf.WriteString("name")
	buf.WriteByte(TypeText)

	// 1 row
	binary.Write(&buf, binary.LittleEndian, uint32(1))

	// Null bitmap (no nulls)
	buf.WriteByte(0)

	// id = 99
	binary.Write(&buf, binary.LittleEndian, int64(99))

	// name = "test"
	binary.Write(&buf, binary.LittleEndian, uint16(4))
	buf.WriteString("test")

	rs, err := decodeResultSet(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if len(rs.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(rs.Columns))
	}
	if rs.Columns[0].Name != "id" || rs.Columns[0].TypeTag != TypeI64 {
		t.Fatalf("col 0: %+v", rs.Columns[0])
	}
	if rs.Columns[1].Name != "name" || rs.Columns[1].TypeTag != TypeText {
		t.Fatalf("col 1: %+v", rs.Columns[1])
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}

	if rs.Rows[0][0].(int64) != 99 {
		t.Fatalf("expected id=99, got %v", rs.Rows[0][0])
	}
	if rs.Rows[0][1].(string) != "test" {
		t.Fatalf("expected name='test', got %v", rs.Rows[0][1])
	}
}

func TestCodecDecodeResultSetWithNulls(t *testing.T) {
	var buf bytes.Buffer

	// 2 columns
	binary.Write(&buf, binary.LittleEndian, uint16(2))

	// Col 1: "a" I64
	buf.WriteByte(1)
	buf.WriteString("a")
	buf.WriteByte(TypeI64)

	// Col 2: "b" TEXT
	buf.WriteByte(1)
	buf.WriteString("b")
	buf.WriteByte(TypeText)

	// 1 row
	binary.Write(&buf, binary.LittleEndian, uint32(1))

	// Null bitmap: col 1 (bit 1) is null
	buf.WriteByte(0x02)

	// a = 10
	binary.Write(&buf, binary.LittleEndian, int64(10))

	// b is null, no data

	rs, err := decodeResultSet(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if rs.Rows[0][0].(int64) != 10 {
		t.Fatalf("expected a=10, got %v", rs.Rows[0][0])
	}
	if rs.Rows[0][1] != nil {
		t.Fatalf("expected b=nil, got %v", rs.Rows[0][1])
	}
}

func TestCodecReadFrame(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello")
	writeFrame(&buf, RespOK, payload)

	msgType, data, err := readFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != RespOK {
		t.Fatalf("expected RespOK, got 0x%02x", msgType)
	}
	if string(data) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(data))
	}
}

func TestCodecReadFrameEmpty(t *testing.T) {
	var buf bytes.Buffer
	writeFrame(&buf, RespPong, nil)

	msgType, data, err := readFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != RespPong {
		t.Fatalf("expected RespPong, got 0x%02x", msgType)
	}
	if len(data) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(data))
	}
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestCountPlaceholders(t *testing.T) {
	tests := []struct {
		query    string
		expected int
	}{
		{"SELECT 1", 0},
		{"SELECT $1", 1},
		{"SELECT $1, $2", 2},
		{"SELECT $1, $3", 3}, // max placeholder number
		{"SELECT ?", 1},
		{"SELECT ?, ?", 2},
		{"INSERT INTO t VALUES ($1, $2, $3)", 3},
	}

	for _, tt := range tests {
		got := countPlaceholders(tt.query)
		if got != tt.expected {
			t.Errorf("countPlaceholders(%q) = %d, want %d", tt.query, got, tt.expected)
		}
	}
}

func TestInterpolateArgs(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: int64(42)},
		{Ordinal: 2, Value: "hello"},
	}

	result := interpolateArgs("SELECT $1, $2", args)
	if result != "SELECT 42, 'hello'" {
		t.Fatalf("expected \"SELECT 42, 'hello'\", got %q", result)
	}
}

func TestInterpolateArgsNil(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: nil},
	}
	result := interpolateArgs("SELECT $1", args)
	if result != "SELECT NULL" {
		t.Fatalf("expected 'SELECT NULL', got %q", result)
	}
}

func TestInterpolateArgsBool(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: true},
		{Ordinal: 2, Value: false},
	}
	result := interpolateArgs("SELECT $1, $2", args)
	if result != "SELECT TRUE, FALSE" {
		t.Fatalf("expected 'SELECT TRUE, FALSE', got %q", result)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{int64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "TRUE"},
		{false, "FALSE"},
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{[]byte{0x01, 0x02}, "'\x01\x02'"},
	}

	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.expected {
			t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestPyroErrorFormat(t *testing.T) {
	err := &PyroError{SQLState: "42P01", Message: "table not found"}
	s := err.Error()
	if s != "pyrosql [42P01]: table not found" {
		t.Fatalf("unexpected error string: %s", s)
	}
}

func TestRowsColumnTypeScanType(t *testing.T) {
	rs := &ResultSet{
		Columns: []Column{
			{Name: "a", TypeTag: TypeI64},
			{Name: "b", TypeTag: TypeF64},
			{Name: "c", TypeTag: TypeText},
			{Name: "d", TypeTag: TypeBool},
			{Name: "e", TypeTag: TypeBytes},
		},
		Rows: [][]interface{}{},
	}
	rows := newPyroRows(rs)

	expected := []reflect.Type{
		reflect.TypeOf(int64(0)),
		reflect.TypeOf(float64(0)),
		reflect.TypeOf(""),
		reflect.TypeOf(false),
		reflect.TypeOf([]byte{}),
	}

	for i, exp := range expected {
		got := rows.ColumnTypeScanType(i)
		if got != exp {
			t.Errorf("col %d: expected %v, got %v", i, exp, got)
		}
	}
}

func TestDriverOpenConnector(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	d := &PyroDriver{}
	connector, err := d.OpenConnector(srv.dsnNoAuth())
	if err != nil {
		t.Fatalf("OpenConnector: %v", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestSessionResetter(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Force multiple connection reuse cycles
	db.SetMaxOpenConns(1)

	for i := 0; i < 5; i++ {
		var val int64
		if err := db.QueryRow("SELECT 1").Scan(&val); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

func TestExecWithArgs(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Exec("INSERT INTO test (id, name) VALUES ($1, $2)", 42, "hello")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("expected 1 row affected, got %d", affected)
	}
}

func TestLastInsertId(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Exec("INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	// LastInsertId returns 0 (PyroSQL does not provide this)
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	if id != 0 {
		t.Fatalf("expected 0, got %d", id)
	}
}

func TestMultiplePreparedStatements(t *testing.T) {
	srv := newMockServer(t, fullHandler)
	defer srv.close()

	db, err := sql.Open("pyrosql", srv.dsnNoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmt1, err := db.Prepare("INSERT INTO t1 (id) VALUES ($1)")
	if err != nil {
		t.Fatalf("Prepare 1: %v", err)
	}

	stmt2, err := db.Prepare("INSERT INTO t2 (id) VALUES ($1)")
	if err != nil {
		t.Fatalf("Prepare 2: %v", err)
	}

	_, err = stmt1.Exec(1)
	if err != nil {
		t.Fatalf("stmt1.Exec: %v", err)
	}

	_, err = stmt2.Exec(2)
	if err != nil {
		t.Fatalf("stmt2.Exec: %v", err)
	}

	stmt1.Close()
	stmt2.Close()
}

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"it's", "it''s"},
		{"back\\slash", "back\\\\slash"},
		{"", ""},
		{"normal text 123", "normal text 123"},
	}

	for _, tt := range tests {
		got := escapeSQLString(tt.input)
		if got != tt.expected {
			t.Errorf("escapeSQLString(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
