package presto

import (
	"container/list"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
)

type rows struct {
	conn    *conn
	columns []column
	data    list.List
	next    string
}

func (rs *rows) Columns() []string {
	ret := make([]string, len(rs.columns))
	for i := range rs.columns {
		ret[i] = rs.columns[i].name
	}
	return ret
}

func (rs *rows) Close() error {
	rs.columns = rs.columns[:0]
	var next *list.Element
	for this := rs.data.Front(); this != nil; this = next {
		next = this.Next()
		rs.data.Remove(this)
	}
	rs.next = ""
	return nil
}

func (rs *rows) Next(dest []driver.Value) error {
	if err := rs.scan(dest); err == nil {
		return nil
	} else if err != ErrNoData {
		return err
	}
	for rs.data.Len() == 0 {
		if len(rs.next) == 0 {
			return sql.ErrNoRows
		}
		req, err := http.NewRequest("GET", rs.next, nil)
		if err != nil {
			return err
		}
		resp, err := rs.conn.call(req)
		if err != nil {
			return err
		}
		rs.next = resp.Next
		for _, d := range resp.Data {
			rs.data.PushBack(d)
		}
		if err := rs.scan(dest); err == nil {
			return nil
		} else if err != ErrNoData {
			return err
		}
		if len(rs.next) == 0 {
			break
		}
	}
	return sql.ErrNoRows
}

func (rs *rows) scan(dest []driver.Value) error {
	node := rs.data.Front()
	if node == nil {
		return ErrNoData
	}
	data := rs.data.Remove(node).([]dataType)
	for i, col := range rs.columns {
		var err error
		switch col.t {
		case typeBoolean:
			err = data[i].toBoolean(&dest[i])
		case typeInteger:
			err = data[i].toInteger(&dest[i])
		case typeDouble:
			err = data[i].toDouble(&dest[i])
		case typeString:
			err = data[i].toString(&dest[i])
		case typeDate:
			err = data[i].toDate(&dest[i])
		case typeTime:
			err = data[i].toTime(&dest[i])
		default:
			err = errors.New("not supported type")
		}
		if err != nil {
			return err
		}
	}
	return nil
}
