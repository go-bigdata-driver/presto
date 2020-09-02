package presto

import (
	"errors"
	"net/url"
	"strconv"
	"time"
)

type config struct {
	addr    string
	schema  string
	timeout time.Duration

	parseTime bool
}

func newConfig() *config {
	return &config{}
}

func parseDSN(dsn string) (*config, error) {
	cfg := newConfig()
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	cfg.addr = u.Scheme + "://"
	if u.User != nil {
		cfg.addr += u.User.Username()
		if pass, ok := u.User.Password(); ok {
			cfg.addr += ":" + pass
		}
		cfg.addr += "@"
	}
	cfg.addr += u.Host
	args := u.Query()

	// timeout
	timeout := args.Get("timeout")
	if len(timeout) > 0 {
		cfg.timeout, err = time.ParseDuration(timeout)
		if err != nil {
			return nil, err
		}
	}

	// parseTime
	parseTime := args.Get("parseTime")
	if len(parseTime) > 0 {
		cfg.parseTime, err = strconv.ParseBool(parseTime)
		if err != nil {
			return nil, errors.New("invalid bool value: " + parseTime)
		}
	}
	return cfg, nil
}
