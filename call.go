package presto

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
)

type responseStats struct {
	State      string `json:"state"`
	Nodes      int    `json:"nodes"`
	ElapsedMS  int    `json:"elapsedTimeMillis"`
	PeekMemory int    `json:"peakMemoryBytes"`
}

type responseError struct {
	Msg      string `json:"message"`
	Location struct {
		Line   int `json:"lineNumber"`
		Column int `json:"columnNumber"`
	} `json:"errorLocation"`
}

type responseColumn struct {
	Name string     `json:"name"`
	Type columnType `json:"type"`
}

type response struct {
	ID       string           `json:"id"`
	Info     string           `json:"infoUri"`
	Next     string           `json:"nextUri"`
	Columns  []responseColumn `json:"columns"`
	Data     [][]dataType     `json:"data"`
	Stats    responseStats    `json:"stats"`
	Error    responseError    `json:"error"`
	Affected int64            `json:"updateCount"`
}

var emptyResponse response

func (c *conn) setCommonHeader(req *http.Request) {
	req.Header.Set("User-Agent", "go-presto-client/"+clientVersion)
	req.Header.Set("X-Presto-User", c.cfg.user)
	req.Header.Set("X-Presto-Source", "go-presto-client")
	req.Header.Set("X-Presto-Catalog", c.cfg.catalog)
	req.Header.Set("X-Presto-Schema", c.cfg.schema)
	if c.cfg.timeZone != "Local" {
		req.Header.Set("X-Presto-Time-Zone", c.cfg.timeZone)
	}
	req.Header.Set("X-Presto-Language", c.cfg.lang)
}

func (c *conn) call(req *http.Request) (response, error) {
	c.setCommonHeader(req)
	resp, err := c.cli.Do(req)
	if err != nil {
		return emptyResponse, err
	}
	defer resp.Body.Close()
	if false {
		data, _ := httputil.DumpResponse(resp, true)
		fmt.Println(string(data))
	}
	var result response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return emptyResponse, err
	}
	if result.Stats.State == "FAILED" {
		return emptyResponse, newError(
			result.Error.Location.Line,
			result.Error.Location.Column,
			result.Error.Msg,
		)
	}
	return result, nil
}
