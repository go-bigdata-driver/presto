package presto

import (
	"database/sql/driver"
	"fmt"
)

type stmt struct{}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return 0
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	fmt.Println("exec: ", args)
	return &result{}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	fmt.Println("query: ", args)
	return &rows{}, nil
}
