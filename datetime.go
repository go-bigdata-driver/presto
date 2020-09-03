package presto

import (
	"time"
)

// Date date value
type Date time.Time

// DateFromTime create date from time
func DateFromTime(t time.Time) Date {
	return Date(t)
}

// Quote get quote string
func (d Date) Quote() string {
	return "DATE '" + time.Time(d).Format("2006-01-02") + "'"
}
