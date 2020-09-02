package presto

import "database/sql/driver"

type stmt struct{}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return 0
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return &result{}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return &rows{}, nil
}
