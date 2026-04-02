package pyrosql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// ---------------------------------------------------------------------------
// Mock PWire server for GORM tests
// ---------------------------------------------------------------------------

const (
	msgQuery   byte = 0x01
	msgPrepare byte = 0x02
	msgExecute byte = 0x03
	msgClose   byte = 0x04
	msgPing    byte = 0x05
	msgAuth    byte = 0x06
	msgQuit    byte = 0xFF

	respResultSet byte = 0x01
	respOK        byte = 0x02
	respError     byte = 0x03
	respPong      byte = 0x04
	respReady     byte = 0x05

	typeNull  byte = 0
	typeI64   byte = 1
	typeF64   byte = 2
	typeText  byte = 3
	typeBool  byte = 4
	typeBytes byte = 5
)

const headerSize = 5

func wireFrame(msgType byte, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	buf[0] = msgType
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	return buf
}

func wireReadFrame(r io.Reader) (byte, []byte, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	length := binary.LittleEndian.Uint32(header[1:5])
	if length == 0 {
		return msgType, nil, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

func wireOK(rowsAffected int64, tag string) []byte {
	buf := make([]byte, 9+len(tag))
	binary.LittleEndian.PutUint64(buf[0:8], uint64(rowsAffected))
	buf[8] = byte(len(tag))
	copy(buf[9:], tag)
	return wireFrame(respOK, buf)
}

func wireError(sqlstate, msg string) []byte {
	buf := make([]byte, 7+len(msg))
	copy(buf[0:5], sqlstate)
	binary.LittleEndian.PutUint16(buf[5:7], uint16(len(msg)))
	copy(buf[7:], msg)
	return wireFrame(respError, buf)
}

func wireResultSet(columns []struct {
	Name    string
	TypeTag byte
}, rows [][]interface{}) []byte {
	var buf bytes.Buffer

	// Column count
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(len(columns)))
	buf.Write(b)

	// Column definitions
	for _, col := range columns {
		buf.WriteByte(byte(len(col.Name)))
		buf.WriteString(col.Name)
		buf.WriteByte(col.TypeTag)
	}

	// Row count
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(len(rows)))
	buf.Write(b)

	// Rows
	nullBitmapLen := (len(columns) + 7) / 8
	for _, row := range rows {
		bitmap := make([]byte, nullBitmapLen)
		for c, val := range row {
			if val == nil {
				byteIdx := c / 8
				bitIdx := uint(c % 8)
				bitmap[byteIdx] |= 1 << bitIdx
			}
		}
		buf.Write(bitmap)

		for c, val := range row {
			if val == nil {
				continue
			}
			switch columns[c].TypeTag {
			case typeI64:
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, uint64(val.(int64)))
				buf.Write(b)
			case typeF64:
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, math.Float64bits(val.(float64)))
				buf.Write(b)
			case typeText:
				s := val.(string)
				b := make([]byte, 2)
				binary.LittleEndian.PutUint16(b, uint16(len(s)))
				buf.Write(b)
				buf.WriteString(s)
			case typeBool:
				if val.(bool) {
					buf.WriteByte(1)
				} else {
					buf.WriteByte(0)
				}
			case typeBytes:
				data := val.([]byte)
				b := make([]byte, 2)
				binary.LittleEndian.PutUint16(b, uint16(len(data)))
				buf.Write(b)
				buf.Write(data)
			}
		}
	}

	return wireFrame(respResultSet, buf.Bytes())
}

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
				s.handler(conn)
			}()
		}
	}()
	return s
}

func (s *mockServer) addr() string {
	return s.listener.Addr().String()
}

func (s *mockServer) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.listener.Close()
	s.wg.Wait()
}

// ---------------------------------------------------------------------------
// Test: Dialector interface
// ---------------------------------------------------------------------------

func TestDialectorName(t *testing.T) {
	d := &Dialector{DSN: "pyrosql://localhost/test"}
	if d.Name() != "pyrosql" {
		t.Errorf("expected name 'pyrosql', got %q", d.Name())
	}
}

func TestOpen(t *testing.T) {
	d := Open("pyrosql://localhost:12520/test")
	if d == nil {
		t.Fatal("Open returned nil")
	}
	if d.Name() != "pyrosql" {
		t.Errorf("expected name 'pyrosql', got %q", d.Name())
	}
}

// ---------------------------------------------------------------------------
// Test: DataTypeOf
// ---------------------------------------------------------------------------

func TestDataTypeOf(t *testing.T) {
	d := &Dialector{}

	tests := []struct {
		name     string
		field    schema.Field
		expected string
	}{
		{
			name:     "bool",
			field:    schema.Field{DataType: schema.Bool},
			expected: "BOOLEAN",
		},
		{
			name:     "int auto increment",
			field:    schema.Field{DataType: schema.Int, AutoIncrement: true, Size: 64},
			expected: "BIGSERIAL",
		},
		{
			name:     "int32 auto increment",
			field:    schema.Field{DataType: schema.Int, AutoIncrement: true, Size: 32},
			expected: "SERIAL",
		},
		{
			name:     "int16 auto increment",
			field:    schema.Field{DataType: schema.Int, AutoIncrement: true, Size: 16},
			expected: "SMALLSERIAL",
		},
		{
			name:     "bigint",
			field:    schema.Field{DataType: schema.Int, Size: 64},
			expected: "BIGINT",
		},
		{
			name:     "integer",
			field:    schema.Field{DataType: schema.Int, Size: 32},
			expected: "INTEGER",
		},
		{
			name:     "smallint",
			field:    schema.Field{DataType: schema.Int, Size: 16},
			expected: "SMALLINT",
		},
		{
			name:     "uint auto increment",
			field:    schema.Field{DataType: schema.Uint, AutoIncrement: true, Size: 32},
			expected: "SERIAL",
		},
		{
			name:     "float64",
			field:    schema.Field{DataType: schema.Float, Size: 64},
			expected: "DOUBLE PRECISION",
		},
		{
			name:     "float32",
			field:    schema.Field{DataType: schema.Float, Size: 32},
			expected: "REAL",
		},
		{
			name:     "numeric with precision and scale",
			field:    schema.Field{DataType: schema.Float, Precision: 10, Scale: 2},
			expected: "NUMERIC(10,2)",
		},
		{
			name:     "varchar",
			field:    schema.Field{DataType: schema.String, Size: 255},
			expected: "VARCHAR(255)",
		},
		{
			name:     "text",
			field:    schema.Field{DataType: schema.String},
			expected: "TEXT",
		},
		{
			name:     "bytea",
			field:    schema.Field{DataType: schema.Bytes},
			expected: "BYTEA",
		},
		{
			name:     "timestamptz",
			field:    schema.Field{DataType: schema.Time},
			expected: "TIMESTAMPTZ",
		},
		{
			name:     "timestamptz with precision",
			field:    schema.Field{DataType: schema.Time, Precision: 3},
			expected: "TIMESTAMPTZ(3)",
		},
		{
			name:     "jsonb override",
			field:    schema.Field{DataType: "JSONB"},
			expected: "JSONB",
		},
		{
			name:     "uuid override",
			field:    schema.Field{DataType: "UUID"},
			expected: "UUID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := d.DataTypeOf(&tt.field)
			if result != tt.expected {
				t.Errorf("DataTypeOf(%s) = %q, want %q", tt.name, result, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test: QuoteTo
// ---------------------------------------------------------------------------

// testWriter implements clause.Writer for testing.
type testWriter struct {
	buf strings.Builder
}

func (w *testWriter) WriteByte(b byte) error {
	return w.buf.WriteByte(b)
}

func (w *testWriter) WriteString(s string) (int, error) {
	return w.buf.WriteString(s)
}

func TestQuoteTo(t *testing.T) {
	d := &Dialector{}

	tests := []struct {
		input    string
		expected string
	}{
		{"users", `"users"`},
		{"public.users", `"public"."users"`},
	}

	for _, tt := range tests {
		w := &testWriter{}
		d.QuoteTo(w, tt.input)
		if w.buf.String() != tt.expected {
			t.Errorf("QuoteTo(%q) = %q, want %q", tt.input, w.buf.String(), tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: BindVarTo
// ---------------------------------------------------------------------------

func TestBindVarTo(t *testing.T) {
	d := &Dialector{}
	stmt := &gorm.Statement{Vars: []interface{}{}}

	w := &testWriter{}

	d.BindVarTo(w, stmt, "value1")
	d.BindVarTo(w, stmt, "value2")

	if w.buf.String() != "$1$2" {
		t.Errorf("BindVarTo produced %q, want $1$2", w.buf.String())
	}
	if len(stmt.Vars) != 2 {
		t.Errorf("expected 2 vars, got %d", len(stmt.Vars))
	}
}

// ---------------------------------------------------------------------------
// Test: SavePoint and RollbackTo
// ---------------------------------------------------------------------------

func TestSavePointAndRollbackTo(t *testing.T) {
	var queries []string
	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				queries = append(queries, q)
				conn.Write(wireOK(0, "OK"))
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	err = db.SavePoint("sp1").Error
	if err != nil {
		t.Fatalf("SavePoint: %v", err)
	}

	err = db.RollbackTo("sp1").Error
	if err != nil {
		t.Fatalf("RollbackTo: %v", err)
	}

	foundSavepoint := false
	foundRollback := false
	for _, q := range queries {
		if strings.Contains(q, "SAVEPOINT sp1") {
			foundSavepoint = true
		}
		if strings.Contains(q, "ROLLBACK TO SAVEPOINT sp1") {
			foundRollback = true
		}
	}
	if !foundSavepoint {
		t.Error("expected SAVEPOINT sp1 query")
	}
	if !foundRollback {
		t.Error("expected ROLLBACK TO SAVEPOINT sp1 query")
	}
}

// ---------------------------------------------------------------------------
// Test: Initialize and basic query
// ---------------------------------------------------------------------------

func TestInitializeAndRawQuery(t *testing.T) {
	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				if strings.Contains(q, "SELECT 1") {
					resp := wireResultSet(
						[]struct {
							Name    string
							TypeTag byte
						}{{"result", typeI64}},
						[][]interface{}{{int64(1)}},
					)
					conn.Write(resp)
				} else {
					conn.Write(wireOK(0, "OK"))
				}
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	var result int64
	err = db.Raw("SELECT 1").Scan(&result).Error
	if err != nil {
		t.Fatalf("raw query: %v", err)
	}
	if result != 1 {
		t.Errorf("expected 1, got %d", result)
	}
}

// ---------------------------------------------------------------------------
// Test: Transaction commit and rollback
// ---------------------------------------------------------------------------

func TestTransaction(t *testing.T) {
	var queries []string
	var mu sync.Mutex

	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				mu.Lock()
				queries = append(queries, q)
				mu.Unlock()
				conn.Write(wireOK(0, "OK"))
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	// Test successful transaction
	err = db.Transaction(func(tx *gorm.DB) error {
		return tx.Exec("INSERT INTO test (name) VALUES ('hello')").Error
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	mu.Lock()
	foundBegin := false
	foundCommit := false
	for _, q := range queries {
		if q == "BEGIN" {
			foundBegin = true
		}
		if q == "COMMIT" {
			foundCommit = true
		}
	}
	mu.Unlock()

	if !foundBegin {
		t.Error("expected BEGIN query")
	}
	if !foundCommit {
		t.Error("expected COMMIT query")
	}
}

func TestTransactionRollback(t *testing.T) {
	var queries []string
	var mu sync.Mutex

	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				mu.Lock()
				queries = append(queries, q)
				mu.Unlock()
				conn.Write(wireOK(0, "OK"))
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	// Test rollback on error
	err = db.Transaction(func(tx *gorm.DB) error {
		tx.Exec("INSERT INTO test (name) VALUES ('hello')")
		return fmt.Errorf("intentional error")
	})
	if err == nil {
		t.Fatal("expected error from transaction")
	}

	mu.Lock()
	foundRollback := false
	for _, q := range queries {
		if q == "ROLLBACK" {
			foundRollback = true
		}
	}
	mu.Unlock()

	if !foundRollback {
		t.Error("expected ROLLBACK query")
	}
}

// ---------------------------------------------------------------------------
// Test: CRUD operations via GORM models
// ---------------------------------------------------------------------------

type TestUser struct {
	ID     int64  `gorm:"primaryKey;autoIncrement"`
	Name   string `gorm:"size:100;not null"`
	Email  string `gorm:"size:255;uniqueIndex"`
	Age    int    `gorm:"type:INTEGER"`
	Active bool   `gorm:"default:true"`
}

func TestCreateWithReturning(t *testing.T) {
	var queries []string
	var mu sync.Mutex

	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				mu.Lock()
				queries = append(queries, q)
				mu.Unlock()

				if strings.Contains(strings.ToUpper(q), "RETURNING") {
					resp := wireResultSet(
						[]struct {
							Name    string
							TypeTag byte
						}{{"id", typeI64}},
						[][]interface{}{{int64(42)}},
					)
					conn.Write(resp)
				} else {
					conn.Write(wireOK(1, "INSERT"))
				}
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	user := TestUser{Name: "Alice", Email: "alice@example.com", Age: 30, Active: true}
	result := db.Create(&user)
	if result.Error != nil {
		t.Fatalf("Create: %v", result.Error)
	}

	mu.Lock()
	foundInsert := false
	for _, q := range queries {
		if strings.Contains(strings.ToUpper(q), "INSERT") {
			foundInsert = true
		}
	}
	mu.Unlock()

	if !foundInsert {
		t.Errorf("expected INSERT query in captured queries: %v", queries)
	}
}

func TestFind(t *testing.T) {
	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				if strings.Contains(strings.ToUpper(q), "SELECT") {
					resp := wireResultSet(
						[]struct {
							Name    string
							TypeTag byte
						}{
							{"id", typeI64},
							{"name", typeText},
							{"email", typeText},
							{"age", typeI64},
							{"active", typeBool},
						},
						[][]interface{}{
							{int64(1), "Alice", "alice@example.com", int64(30), true},
							{int64(2), "Bob", "bob@example.com", int64(25), false},
						},
					)
					conn.Write(resp)
				} else {
					conn.Write(wireOK(0, "OK"))
				}
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	var users []TestUser
	result := db.Find(&users)
	if result.Error != nil {
		t.Fatalf("Find: %v", result.Error)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	if users[0].Name != "Alice" {
		t.Errorf("expected Alice, got %s", users[0].Name)
	}
	if users[1].Name != "Bob" {
		t.Errorf("expected Bob, got %s", users[1].Name)
	}
}

func TestUpdate(t *testing.T) {
	var queries []string
	var mu sync.Mutex

	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				mu.Lock()
				queries = append(queries, q)
				mu.Unlock()
				conn.Write(wireOK(1, "UPDATE"))
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	result := db.Model(&TestUser{}).Where("id = $1", 1).Update("name", "Alice Updated")
	if result.Error != nil {
		t.Fatalf("Update: %v", result.Error)
	}

	mu.Lock()
	foundUpdate := false
	for _, q := range queries {
		if strings.Contains(strings.ToUpper(q), "UPDATE") {
			foundUpdate = true
		}
	}
	mu.Unlock()

	if !foundUpdate {
		t.Errorf("expected UPDATE query in captured queries: %v", queries)
	}
}

func TestDelete(t *testing.T) {
	var queries []string
	var mu sync.Mutex

	server := newMockServer(t, func(conn net.Conn) {
		for {
			msgType, payload, err := wireReadFrame(conn)
			if err != nil {
				return
			}
			switch msgType {
			case msgQuery:
				q := string(payload)
				mu.Lock()
				queries = append(queries, q)
				mu.Unlock()
				conn.Write(wireOK(1, "DELETE"))
			case msgAuth:
				conn.Write(wireFrame(respReady, nil))
			case msgPing:
				conn.Write(wireFrame(respPong, nil))
			case msgQuit:
				return
			default:
				conn.Write(wireOK(0, "OK"))
			}
		}
	})
	defer server.close()

	dsn := fmt.Sprintf("pyrosql://user:pass@%s/testdb", server.addr())
	db, err := gorm.Open(Open(dsn), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	result := db.Delete(&TestUser{}, 1)
	if result.Error != nil {
		t.Fatalf("Delete: %v", result.Error)
	}

	mu.Lock()
	foundDelete := false
	for _, q := range queries {
		if strings.Contains(strings.ToUpper(q), "DELETE") {
			foundDelete = true
		}
	}
	mu.Unlock()

	if !foundDelete {
		t.Errorf("expected DELETE query in captured queries: %v", queries)
	}
}

// ---------------------------------------------------------------------------
// Test: Migrator helpers
// ---------------------------------------------------------------------------

func TestIsSerialType(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"SERIAL", true},
		{"BIGSERIAL", true},
		{"SMALLSERIAL", true},
		{"serial", true},
		{"INTEGER", false},
		{"TEXT", false},
	}
	for _, tt := range tests {
		if got := isSerialType(tt.input); got != tt.expected {
			t.Errorf("isSerialType(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: DefaultValueOf
// ---------------------------------------------------------------------------

func TestDefaultValueOf(t *testing.T) {
	d := &Dialector{}
	expr := d.DefaultValueOf(&schema.Field{})
	ce, ok := expr.(clause.Expr)
	if !ok {
		t.Fatal("expected clause.Expr")
	}
	if ce.SQL != "DEFAULT" {
		t.Errorf("expected DEFAULT, got %q", ce.SQL)
	}
}

// ---------------------------------------------------------------------------
// Test: Explain
// ---------------------------------------------------------------------------

func TestExplain(t *testing.T) {
	d := &Dialector{}
	result := d.Explain("SELECT * FROM users WHERE id = $1", 42)
	if result == "" {
		t.Error("Explain returned empty string")
	}
}

// ---------------------------------------------------------------------------
// Test: pyroColumnType
// ---------------------------------------------------------------------------

func TestColumnType(t *testing.T) {
	ct := &pyroColumnType{
		name:         "id",
		dataType:     "BIGINT",
		nullable:     false,
		hasDefault:   true,
		defaultValue: "nextval('id_seq')",
		hasLength:    false,
	}

	if ct.Name() != "id" {
		t.Errorf("expected name 'id', got %q", ct.Name())
	}
	if ct.DatabaseTypeName() != "BIGINT" {
		t.Errorf("expected BIGINT, got %q", ct.DatabaseTypeName())
	}
	nullable, ok := ct.Nullable()
	if !ok || nullable {
		t.Errorf("expected nullable=false, ok=true")
	}
	dflt, hasDflt := ct.DefaultValueValue()
	if !hasDflt || dflt != "nextval('id_seq')" {
		t.Errorf("unexpected default: %q, %v", dflt, hasDflt)
	}
	_, hasLen := ct.Length()
	if hasLen {
		t.Error("expected no length")
	}
	colType, ok := ct.ColumnType()
	if !ok || colType != "BIGINT" {
		t.Errorf("ColumnType = %q, %v", colType, ok)
	}
}
