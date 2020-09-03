package presto

import (
	"errors"
)

// ErrNoMore no more error
var ErrNoMore = errors.New("no more")

// ErrNoData no data when parse rows data
var ErrNoData = errors.New("no data")

// Error presto error
type Error struct {
	line   int
	column int
	msg    string
}

func newError(line, column int, msg string) *Error {
	return &Error{
		line:   line,
		column: column,
		msg:    msg,
	}
}

func (e *Error) Error() string {
	return e.msg
}
