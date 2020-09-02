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
	fmt.Println("prepare: ", query)
	return &stmt{}, nil
}

func (c *conn) Close() error {
	return nil
}

// Begin begin transaction
func (c *conn) Begin() (driver.Tx, error) {
	fmt.Println("begin")
	return &tx{}, nil
}
