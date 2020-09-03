package presto

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strconv"
)

type columnType int

func (t *columnType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "boolean":
		*t = typeBoolean
	case "tinyint", "smallint", "integer", "bigint":
		*t = typeInteger
	case "real", "double", "decimal":
		*t = typeDouble
	case "varchar", "char", "varbinary", "json":
		*t = typeString
	case "date", "time", "time with time zone",
		"timestamp", "timestamp with timezone",
		"interval year to month", "interval day to second":
		*t = typeTime
	case "array":
		*t = typeArray
	case "map":
		*t = typeMap
	default:
		return errors.New("unsupported type: " + str)
	}
	return nil
}

const (
	typeBoolean columnType = iota
	typeInteger
	typeDouble
	typeString
	typeTime
	typeArray
	typeMap
)

type column struct {
	name string
	t    columnType
}

func parseColumns(resp response) []column {
	ret := make([]column, len(resp.Columns))
	for i, col := range resp.Columns {
		ret[i].name = col.Name
		ret[i].t = col.Type
	}
	return ret
}

type dataType []byte

func (d *dataType) UnmarshalJSON(data []byte) error {
	*d = data
	return nil
}

func (d dataType) toBoolean(v *driver.Value) error {
	b, err := strconv.ParseBool(string(d))
	if err != nil {
		return err
	}
	*v = b
	return nil
}

func (d dataType) toInteger(v *driver.Value) error {
	n, err := strconv.ParseInt(string(d), 10, 64)
	if err != nil {
		return err
	}
	*v = n
	return nil
}
