// Package pyrosql provides a GORM Dialector for the PyroSQL database, enabling
// GORM ORM usage over the PyroSQL PWire protocol.
//
// Usage:
//
//	import (
//	    pyrosql "github.com/pyrosql/pyrosql-driver/go/gorm"
//	    "gorm.io/gorm"
//	)
//
//	db, err := gorm.Open(pyrosql.Open("pyrosql://user:pass@host:12520/dbname"), &gorm.Config{})
package pyrosql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/pyrosql/pyrosql-driver/go"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const DriverName = "pyrosql"

// Dialector implements gorm.Dialector for PyroSQL.
type Dialector struct {
	DSN string
}

// Open creates a new Dialector with the given DSN.
func Open(dsn string) gorm.Dialector {
	return &Dialector{DSN: dsn}
}

// Name returns the dialect name.
func (d Dialector) Name() string {
	return "pyrosql"
}

// Initialize sets up the GORM database connection and registers callbacks.
func (d Dialector) Initialize(db *gorm.DB) error {
	// Register default callbacks with RETURNING support for insert/update/delete
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses: []string{"UPDATE", "SET", "FROM", "WHERE", "RETURNING"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
		QueryClauses:  []string{"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "FOR"},
	})

	sqlDB, err := sql.Open(DriverName, d.DSN)
	if err != nil {
		return fmt.Errorf("pyrosql gorm: failed to open connection: %w", err)
	}

	db.ConnPool = sqlDB
	return nil
}

// Migrator returns a PyroSQL-specific migrator.
func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return &PyroMigrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:                          db,
				Dialector:                   d,
				CreateIndexAfterCreateTable: true,
			},
		},
	}
}

// DataTypeOf maps GORM schema fields to PyroSQL column types.
func (d Dialector) DataTypeOf(field *schema.Field) string {
	// Check for explicit column type override via `gorm:"type:..."`
	if field.DataType != "" {
		switch strings.ToUpper(string(field.DataType)) {
		case "JSONB":
			return "JSONB"
		case "UUID":
			return "UUID"
		case "JSON":
			return "JSON"
		}
	}

	switch field.DataType {
	case schema.Bool:
		return "BOOLEAN"
	case schema.Int:
		if field.AutoIncrement {
			switch {
			case field.Size <= 16:
				return "SMALLSERIAL"
			case field.Size <= 32:
				return "SERIAL"
			default:
				return "BIGSERIAL"
			}
		}
		switch {
		case field.Size <= 16:
			return "SMALLINT"
		case field.Size <= 32:
			return "INTEGER"
		default:
			return "BIGINT"
		}
	case schema.Uint:
		if field.AutoIncrement {
			switch {
			case field.Size <= 16:
				return "SMALLSERIAL"
			case field.Size <= 32:
				return "SERIAL"
			default:
				return "BIGSERIAL"
			}
		}
		switch {
		case field.Size <= 16:
			return "SMALLINT"
		case field.Size <= 32:
			return "INTEGER"
		default:
			return "BIGINT"
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("NUMERIC(%d,%d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("NUMERIC(%d)", field.Precision)
		}
		if field.Size <= 32 {
			return "REAL"
		}
		return "DOUBLE PRECISION"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("VARCHAR(%d)", field.Size)
		}
		return "TEXT"
	case schema.Bytes:
		return "BYTEA"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("TIMESTAMPTZ(%d)", field.Precision)
		}
		return "TIMESTAMPTZ"
	default:
		return "TEXT"
	}
}

// DefaultValueOf returns the default value clause for a field.
func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

// BindVarTo writes positional bind variables ($1, $2, etc.) for PyroSQL.
func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	stmt.Vars = append(stmt.Vars, v)
	writer.WriteByte('$')
	writer.WriteString(fmt.Sprintf("%d", len(stmt.Vars)))
}

// QuoteTo quotes identifiers with double quotes.
func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	if strings.Contains(str, ".") {
		parts := strings.Split(str, ".")
		for i, part := range parts {
			if i > 0 {
				writer.WriteString(`"."`)
			}
			writer.WriteString(part)
		}
	} else {
		writer.WriteString(str)
	}
	writer.WriteByte('"')
}

// Explain formats a SQL statement with its variables for logging.
func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

// SavePoint creates a savepoint.
func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	return tx.Exec("SAVEPOINT " + name).Error
}

// RollbackTo rolls back to a savepoint.
func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	return tx.Exec("ROLLBACK TO SAVEPOINT " + name).Error
}

// Compile-time check that Dialector implements gorm.Dialector.
var _ gorm.Dialector = (*Dialector)(nil)
