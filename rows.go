package presto

import "database/sql/driver"

type rows struct{}

func (rs *rows) Columns() []string {
	return nil
}

func (rs *rows) Close() error {
	return nil
}

func (rs *rows) Next(dest []driver.Value) error {
	return nil
}
