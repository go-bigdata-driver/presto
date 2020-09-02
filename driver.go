package presto

import (
	"database/sql"
	"database/sql/driver"
)

type pDriver struct{}

// Open open connection
func (d *pDriver) Open(dsn string) (driver.Conn, error) {
	return &conn{}, nil
}

func init() {
	sql.Register("presto", &pDriver{})
}
