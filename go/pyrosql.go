// Package pyrosql provides a database/sql driver for PyroSQL using the PWire
// binary protocol over TCP.
//
// Import this package with a blank identifier to register the "pyrosql" driver:
//
//	import (
//	    "database/sql"
//	    _ "github.com/pyrosql/pyrosql-driver/go"
//	)
//
//	db, err := sql.Open("pyrosql", "pyrosql://user:pass@host:12520/dbname")
//
// Connection string format:
//
//	pyrosql://[user[:password]@]host[:port]/dbname
//
// Default port is 12520.
package pyrosql

import (
	"database/sql"
	"fmt"
)

// Connect is a convenience function that opens and pings a PyroSQL connection.
func Connect(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pyrosql", dsn)
	if err != nil {
		return nil, fmt.Errorf("pyrosql: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pyrosql: ping: %w", err)
	}
	return db, nil
}
