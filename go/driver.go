package pyrosql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("pyrosql", &PyroDriver{})
}

// PyroDriver implements database/sql/driver.Driver and driver.DriverContext.
type PyroDriver struct{}

// Open implements driver.Driver.
func (d *PyroDriver) Open(dsn string) (driver.Conn, error) {
	return newConn(dsn)
}

// OpenConnector implements driver.DriverContext.
func (d *PyroDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return &pyroConnector{dsn: dsn, driver: d}, nil
}

// pyroConnector implements driver.Connector.
type pyroConnector struct {
	dsn    string
	driver *PyroDriver
}

// Connect implements driver.Connector.
func (c *pyroConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return newConn(c.dsn)
}

// Driver implements driver.Connector.
func (c *pyroConnector) Driver() driver.Driver {
	return c.driver
}

// Compile-time interface checks.
var (
	_ driver.Driver        = (*PyroDriver)(nil)
	_ driver.DriverContext = (*PyroDriver)(nil)
	_ driver.Connector     = (*pyroConnector)(nil)
)
