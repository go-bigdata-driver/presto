package presto

import (
	"crypto/md5"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

type stmt struct {
	query string
	conn  *conn
	next  string
}

func (s *stmt) Close() error {
	s.query = ""
	s.conn = nil
	s.next = ""
	return nil
}

func (s *stmt) NumInput() int {
	return strings.Count(s.query, "?")
}

func (s *stmt) CheckNamedValue(v *driver.NamedValue) error {
	switch v.Value.(type) {
	case int, int8, int16, int32, int64:
	case uint, uint8, uint16, uint32, uint64:
	case float32, float64:
	case bool:
	case []byte:
	case string:
	case time.Time:
	case Date:
	default:
		rv := reflect.ValueOf(v.Value)
		return fmt.Errorf("unsupported type %T, a %s", v.Value, rv.Kind())
	}
	return nil
}

func buildPrepareRequest(addr, query string, args []driver.Value) (*http.Request, error) {
	arr := make([]string, len(args))
	for i, arg := range args {
		switch arg.(type) {
		case string:
			arr[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(arg.(string), "'", "''"))
		case []byte:
			return nil, errors.New("not supported []byte arguments")
		case Date:
			arr[i] = arg.(Date).Quote()
		case time.Time:
			arr[i] = "TIMESTAMP '" + arg.(time.Time).Format("2006-01-02 15:04:05.999") + "'"
		default:
			arr[i] = fmt.Sprintf("%v", arg)
		}
	}
	id := "go_presto_client_" + fmt.Sprintf("%x", md5.Sum([]byte(query)))
	req, err := http.NewRequest("POST", addr+"/v1/statement", strings.NewReader("EXECUTE "+id+" USING "+strings.Join(arr, ",")))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Presto-Prepared-Statement", id+"="+url.QueryEscape(query))
	return req, nil
}

func buildNormalRequest(addr, query string) (*http.Request, error) {
	return http.NewRequest("POST", addr+"/v1/statement", strings.NewReader(query))
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	var req *http.Request
	var err error
	if len(args) > 0 {
		req, err = buildPrepareRequest(s.conn.cfg.addr, s.query, args)
	} else {
		req, err = buildNormalRequest(s.conn.cfg.addr, s.query)
	}
	if err != nil {
		return nil, err
	}
	resp, err := s.conn.call(req)
	if err != nil {
		return nil, err
	}
	s.next = resp.Next
	if len(s.next) == 0 {
		return nil, ErrNoMore
	}
	var lastID int64
	for len(s.next) > 0 {
		req, err := http.NewRequest("GET", s.next, nil)
		if err != nil {
			return nil, err
		}
		resp, err = s.conn.call(req)
		if err != nil {
			return nil, err
		}
		s.next = resp.Next
		if len(s.next) == 0 {
			break
		}
	}
	return &result{
		lastID:   lastID,
		affected: resp.Affected,
	}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	var req *http.Request
	var err error
	if len(args) > 0 {
		req, err = buildPrepareRequest(s.conn.cfg.addr, s.query, args)
	} else {
		req, err = buildNormalRequest(s.conn.cfg.addr, s.query)
	}
	if err != nil {
		return nil, err
	}
	resp, err := s.conn.call(req)
	if err != nil {
		return nil, err
	}
	s.next = resp.Next
	if len(s.next) == 0 {
		return nil, ErrNoMore
	}
	for len(s.next) > 0 {
		req, err := http.NewRequest("GET", s.next, nil)
		if err != nil {
			return nil, err
		}
		resp, err := s.conn.call(req)
		if err != nil {
			return nil, err
		}
		s.next = resp.Next
		if len(s.next) == 0 {
			break
		}
		if resp.Stats.State != "QUEUED" {
			rs := &rows{
				conn:    s.conn,
				columns: parseColumns(resp),
				next:    resp.Next,
			}
			for _, d := range resp.Data {
				rs.data.PushBack(d)
			}
			return rs, nil
		}
	}
	return nil, sql.ErrNoRows
}
