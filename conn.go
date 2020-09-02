package presto

import "database/sql/driver"

type conn struct{}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{}, nil
}

func (c *conn) Close() error {
	return nil
}

// Begin begin transaction
func (c *conn) Begin() (driver.Tx, error) {
	return &tx{}, nil
}
