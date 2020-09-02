package presto

import (
	"database/sql"
	"database/sql/driver"
	"net/http"
)

type pDriver struct{}

// Open open connection
func (d *pDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &conn{
		cfg: cfg,
		cli: &http.Client{Timeout: cfg.timeout},
	}, nil
}

func init() {
	sql.Register("presto", &pDriver{})
}
