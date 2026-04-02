package pyrosql

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"time"
)

// pyroRows implements driver.Rows and driver.RowsColumnTypeScanType.
type pyroRows struct {
	columns  []Column
	rows     [][]interface{}
	pos      int
	closed   bool
}

func newPyroRows(rs *ResultSet) *pyroRows {
	cols := rs.Columns
	if cols == nil {
		cols = []Column{}
	}
	rows := rs.Rows
	if rows == nil {
		rows = [][]interface{}{}
	}
	return &pyroRows{
		columns: cols,
		rows:    rows,
	}
}

// Columns implements driver.Rows.
func (r *pyroRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close implements driver.Rows.
func (r *pyroRows) Close() error {
	r.closed = true
	return nil
}

// Next implements driver.Rows.
func (r *pyroRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}
	if r.pos >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.pos]
	r.pos++

	for i := 0; i < len(dest) && i < len(row); i++ {
		dest[i] = row[i]
	}
	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *pyroRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return reflect.TypeOf("")
	}
	switch r.columns[index].TypeTag {
	case TypeI64:
		return reflect.TypeOf(int64(0))
	case TypeF64:
		return reflect.TypeOf(float64(0))
	case TypeBool:
		return reflect.TypeOf(false)
	case TypeBytes:
		return reflect.TypeOf([]byte{})
	case TypeText:
		return reflect.TypeOf("")
	default:
		return reflect.TypeOf("")
	}
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.
func (r *pyroRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return "TEXT"
	}
	switch r.columns[index].TypeTag {
	case TypeI64:
		return "BIGINT"
	case TypeF64:
		return "DOUBLE"
	case TypeBool:
		return "BOOLEAN"
	case TypeBytes:
		return "BYTEA"
	case TypeText:
		return "TEXT"
	case TypeNull:
		return "NULL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", r.columns[index].TypeTag)
	}
}

// ColumnTypeNullable implements driver.RowsColumnTypeNullable.
func (r *pyroRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	// All columns in PyroSQL are nullable
	return true, true
}

// Ensure interface satisfaction at compile time.
var _ driver.Rows = (*pyroRows)(nil)

// timeType is used for scan type reflection.
var timeType = reflect.TypeOf(time.Time{})
