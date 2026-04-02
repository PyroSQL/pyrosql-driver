package pyrosql

import (
	"context"
	"database/sql/driver"
	"fmt"
)

// pyroStmt implements driver.Stmt, driver.StmtExecContext, driver.StmtQueryContext.
type pyroStmt struct {
	conn     *pyroConn
	query    string
	handle   uint32
	numInput int
	closed   bool
}

// Close implements driver.Stmt.
func (s *pyroStmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if s.conn.closed {
		return nil
	}

	msgType, payload, err := s.conn.roundTrip(encodeClose(s.handle))
	if err != nil {
		return err
	}

	switch msgType {
	case RespOK:
		return nil
	case RespError:
		pyroErr, decErr := decodeError(payload)
		if decErr != nil {
			return decErr
		}
		return pyroErr
	default:
		return nil
	}
}

// NumInput implements driver.Stmt.
func (s *pyroStmt) NumInput() int {
	return s.numInput
}

// Exec implements driver.Stmt.
func (s *pyroStmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, a := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return s.ExecContext(context.Background(), named)
}

// ExecContext implements driver.StmtExecContext.
func (s *pyroStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("pyrosql: statement already closed")
	}
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	params := namedValuesToStrings(args)
	msgType, payload, err := s.conn.roundTrip(encodeExecute(s.handle, params))
	if err != nil {
		return nil, err
	}

	return s.conn.handleExecResponse(msgType, payload)
}

// Query implements driver.Stmt.
func (s *pyroStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, a := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return s.QueryContext(context.Background(), named)
}

// QueryContext implements driver.StmtQueryContext.
func (s *pyroStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("pyrosql: statement already closed")
	}
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	params := namedValuesToStrings(args)
	msgType, payload, err := s.conn.roundTrip(encodeExecute(s.handle, params))
	if err != nil {
		return nil, err
	}

	return s.conn.handleQueryResponse(msgType, payload)
}

func namedValuesToStrings(args []driver.NamedValue) []string {
	params := make([]string, len(args))
	for i, a := range args {
		if a.Value == nil {
			params[i] = "NULL"
		} else {
			params[i] = fmt.Sprintf("%v", a.Value)
		}
	}
	return params
}

// Compile-time interface checks.
var (
	_ driver.Stmt             = (*pyroStmt)(nil)
	_ driver.StmtExecContext  = (*pyroStmt)(nil)
	_ driver.StmtQueryContext = (*pyroStmt)(nil)
)
