package pyrosql

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// pyroConn implements driver.Conn, driver.ConnBeginTx, driver.ConnPrepareContext,
// driver.ExecerContext, driver.QueryerContext, and driver.Pinger.
type pyroConn struct {
	netConn      net.Conn
	mu           sync.Mutex
	closed       bool
	database     string
	supportsLZ4  bool
}

func newConn(dsn string) (*pyroConn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("pyrosql: invalid DSN: %w", err)
	}

	host := u.Hostname()
	if host == "" {
		host = "127.0.0.1"
	}
	port := u.Port()
	if port == "" {
		port = "12520"
	}
	addr := net.JoinHostPort(host, port)

	database := ""
	if u.Path != "" && u.Path != "/" {
		database = u.Path[1:] // strip leading /
	}

	user := ""
	password := ""
	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
	}

	dialer := net.Dialer{Timeout: 10 * time.Second}
	netConn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pyrosql: connect to %s: %w", addr, err)
	}

	c := &pyroConn{
		netConn:  netConn,
		database: database,
	}

	// Authenticate with LZ4 capability negotiation.
	if err := c.authenticateWithCaps(user, password); err != nil {
		netConn.Close()
		return nil, err
	}

	return c, nil
}

func (c *pyroConn) authenticate(user, password string) error {
	return c.authenticateWithCaps(user, password)
}

func (c *pyroConn) authenticateWithCaps(user, password string) error {
	if err := c.writeFrame(encodeAuthWithCaps(user, password, CapLZ4)); err != nil {
		return fmt.Errorf("pyrosql: auth write: %w", err)
	}

	msgType, payload, err := readFrame(c.netConn)
	if err != nil {
		return fmt.Errorf("pyrosql: auth read: %w", err)
	}

	switch msgType {
	case RespReady:
		// Check server caps in the handle field (first 4 bytes of payload).
		if len(payload) >= 4 {
			serverCaps := binary.LittleEndian.Uint32(payload[0:4])
			c.supportsLZ4 = (byte(serverCaps) & CapLZ4) != 0
		}
		return nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return decErr
		}
		return pyroErr
	default:
		return fmt.Errorf("pyrosql: unexpected auth response type 0x%02x", msgType)
	}
}

func (c *pyroConn) writeFrame(data []byte) error {
	_, err := c.netConn.Write(data)
	return err
}

func (c *pyroConn) roundTrip(data []byte) (byte, []byte, error) {
	// Optionally compress the outgoing frame if LZ4 is negotiated.
	if c.supportsLZ4 && len(data) > headerSize {
		msgType := data[0]
		payload := data[headerSize:]
		data = compressFrame(msgType, payload)
	}
	if err := c.writeFrame(data); err != nil {
		return 0, nil, err
	}
	return readFrame(c.netConn)
}

// Prepare implements driver.Conn.
func (c *pyroConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext.
func (c *pyroConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	msgType, payload, err := c.roundTrip(encodePrepare(query))
	if err != nil {
		return nil, err
	}

	switch msgType {
	case RespReady:
		if len(payload) < 4 {
			return nil, fmt.Errorf("pyrosql: PREPARE response too short")
		}
		handle := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
		numInput := countPlaceholders(query)
		return &pyroStmt{
			conn:     c,
			query:    query,
			handle:   handle,
			numInput: numInput,
		}, nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return nil, decErr
		}
		return nil, pyroErr
	default:
		return nil, fmt.Errorf("pyrosql: unexpected prepare response type 0x%02x", msgType)
	}
}

// Close implements driver.Conn.
func (c *pyroConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Send QUIT, best-effort
	_ = c.writeFrame(encodeQuit())
	return c.netConn.Close()
}

// Begin implements driver.Conn.
func (c *pyroConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx.
func (c *pyroConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	isolationSQL := "BEGIN"
	switch opts.Isolation {
	case driver.IsolationLevel(0): // default
		// use plain BEGIN
	case driver.IsolationLevel(1): // ReadUncommitted
		isolationSQL = "BEGIN ISOLATION LEVEL READ UNCOMMITTED"
	case driver.IsolationLevel(2): // ReadCommitted
		isolationSQL = "BEGIN ISOLATION LEVEL READ COMMITTED"
	case driver.IsolationLevel(3): // WriteCommitted
		isolationSQL = "BEGIN ISOLATION LEVEL WRITE COMMITTED"
	case driver.IsolationLevel(4): // RepeatableRead
		isolationSQL = "BEGIN ISOLATION LEVEL REPEATABLE READ"
	case driver.IsolationLevel(6): // Serializable
		isolationSQL = "BEGIN ISOLATION LEVEL SERIALIZABLE"
	}

	if opts.ReadOnly {
		isolationSQL += " READ ONLY"
	}

	msgType, payload, err := c.roundTrip(encodeQuery(isolationSQL))
	if err != nil {
		return nil, err
	}

	switch msgType {
	case RespOK:
		return &pyroTx{conn: c}, nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return nil, decErr
		}
		return nil, pyroErr
	default:
		return nil, fmt.Errorf("pyrosql: unexpected begin response type 0x%02x", msgType)
	}
}

// ExecContext implements driver.ExecerContext.
func (c *pyroConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	finalQuery := interpolateArgs(query, args)

	msgType, payload, err := c.roundTrip(encodeQuery(finalQuery))
	if err != nil {
		return nil, err
	}

	return c.handleExecResponse(msgType, payload)
}

// QueryContext implements driver.QueryerContext.
func (c *pyroConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	finalQuery := interpolateArgs(query, args)

	msgType, payload, err := c.roundTrip(encodeQuery(finalQuery))
	if err != nil {
		return nil, err
	}

	return c.handleQueryResponse(msgType, payload)
}

// Ping implements driver.Pinger.
func (c *pyroConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	msgType, payload, err := c.roundTrip(encodePing())
	if err != nil {
		return err
	}

	switch msgType {
	case RespPong:
		return nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return decErr
		}
		return pyroErr
	default:
		return fmt.Errorf("pyrosql: unexpected ping response type 0x%02x", msgType)
	}
}

// ResetSession implements driver.SessionResetter for connection pooling.
func (c *pyroConn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}
	return nil
}

// IsValid implements driver.Validator for connection pooling.
func (c *pyroConn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

func (c *pyroConn) handleExecResponse(msgType byte, payload []byte) (driver.Result, error) {
	switch msgType {
	case RespOK:
		ok, err := decodeOK(payload)
		if err != nil {
			return nil, err
		}
		return &pyroResult{rowsAffected: ok.RowsAffected}, nil
	case RespResultSet:
		// Some exec statements might return a result set (e.g., INSERT ... RETURNING)
		// We consume it and return rows affected = 0
		return &pyroResult{rowsAffected: 0}, nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return nil, decErr
		}
		return nil, pyroErr
	default:
		return nil, fmt.Errorf("pyrosql: unexpected exec response type 0x%02x", msgType)
	}
}

func (c *pyroConn) handleQueryResponse(msgType byte, payload []byte) (driver.Rows, error) {
	switch msgType {
	case RespResultSet:
		rs, err := decodeResultSet(payload)
		if err != nil {
			return nil, err
		}
		return newPyroRows(rs), nil
	case RespOK:
		// Return empty result set for statements that don't return rows
		return newPyroRows(&ResultSet{Columns: nil, Rows: nil}), nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return nil, decErr
		}
		return nil, pyroErr
	default:
		return nil, fmt.Errorf("pyrosql: unexpected query response type 0x%02x", msgType)
	}
}

// execLocked executes a query while already holding the lock.
func (c *pyroConn) execLocked(query string) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	msgType, payload, err := c.roundTrip(encodeQuery(query))
	if err != nil {
		return nil, err
	}

	return c.handleExecResponse(msgType, payload)
}

// pyroResult implements driver.Result.
type pyroResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *pyroResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *pyroResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// pyroTx implements driver.Tx.
type pyroTx struct {
	conn *pyroConn
}

func (tx *pyroTx) Commit() error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	_, err := tx.conn.execLocked("COMMIT")
	return err
}

func (tx *pyroTx) Rollback() error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	_, err := tx.conn.execLocked("ROLLBACK")
	return err
}

// countPlaceholders counts the number of $N or ? placeholders in a query.
func countPlaceholders(query string) int {
	count := 0
	maxNum := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			count++
		} else if query[i] == '$' && i+1 < len(query) && query[i+1] >= '1' && query[i+1] <= '9' {
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			n, _ := strconv.Atoi(query[i+1 : j])
			if n > maxNum {
				maxNum = n
			}
		}
	}
	if maxNum > count {
		return maxNum
	}
	return count
}

// interpolateArgs replaces $N placeholders with argument values for direct query execution.
func interpolateArgs(query string, args []driver.NamedValue) string {
	if len(args) == 0 {
		return query
	}

	// Build a map of ordinal -> string value
	vals := make(map[int]string, len(args))
	for _, a := range args {
		vals[a.Ordinal] = formatValue(a.Value)
	}

	// Replace $N placeholders (scan right to left to avoid offset issues)
	result := []byte(query)
	for i := len(result) - 1; i >= 0; i-- {
		if result[i] == '$' && i+1 < len(result) && result[i+1] >= '1' && result[i+1] <= '9' {
			j := i + 1
			for j < len(result) && result[j] >= '0' && result[j] <= '9' {
				j++
			}
			n, _ := strconv.Atoi(string(result[i+1 : j]))
			if v, ok := vals[n]; ok {
				newResult := make([]byte, 0, len(result)-j+i+len(v))
				newResult = append(newResult, result[:i]...)
				newResult = append(newResult, []byte(v)...)
				newResult = append(newResult, result[j:]...)
				result = newResult
			}
		} else if result[i] == '?' {
			// Find which ordinal this corresponds to (count ? from start)
			qCount := 0
			for k := 0; k <= i; k++ {
				if result[k] == '?' {
					qCount++
				}
			}
			if v, ok := vals[qCount]; ok {
				newResult := make([]byte, 0, len(result)-1+len(v))
				newResult = append(newResult, result[:i]...)
				newResult = append(newResult, []byte(v)...)
				newResult = append(newResult, result[i+1:]...)
				result = newResult
			}
		}
	}
	return string(result)
}

// formatValue formats a Go value for SQL interpolation.
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case []byte:
		return "'" + escapeSQLString(string(val)) + "'"
	case string:
		return "'" + escapeSQLString(val) + "'"
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05.999999") + "'"
	default:
		return "'" + escapeSQLString(fmt.Sprintf("%v", val)) + "'"
	}
}

func escapeSQLString(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else if s[i] == '\\' {
			result = append(result, '\\', '\\')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}
