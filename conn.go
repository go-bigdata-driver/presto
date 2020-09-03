package presto

import (
	"database/sql/driver"
	"fmt"
	"net/http"
)

type conn struct {
	cfg *config
	cli *http.Client
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{
		query: query,
		conn:  c,
	}, nil
}

func (c *conn) Close() error {
	c.cfg = nil
	c.cli = nil
	return nil
}

// Begin begin transaction
func (c *conn) Begin() (driver.Tx, error) {
	fmt.Println("begin")
	return &tx{}, nil
}
