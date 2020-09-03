package presto

import (
	"errors"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
	"time"
)

type config struct {
	addr    string
	catalog string
	schema  string
	timeout time.Duration

	parseTime bool

	// for request header
	user     string
	timeZone string
	lang     string
}

func getLang() string {
	envlang, ok := os.LookupEnv("LANG")
	if ok {
		return strings.Split(envlang, ".")[0]
	}
	cmd := exec.Command("powershell", "Get-Culture | select -exp Name")
	output, err := cmd.Output()
	if err == nil {
		return strings.Trim(string(output), "\r\n")
	}
	return ""
}

func getUserName() string {
	u, err := user.Current()
	if err != nil {
		return ""
	}
	return u.Username
}

func newConfig() *config {
	return &config{
		user:     getUserName(),
		timeZone: time.Local.String(),
		lang:     getLang(),
	}
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

	// catalog
	cfg.catalog = args.Get("catalog")
	if len(cfg.catalog) == 0 {
		return nil, errors.New("missing catalog")
	}

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
