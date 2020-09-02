package presto

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

var dsn string

func init() {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	// user := env("PRESTO_TEST_USER", "root")
	// pass := env("PRESTO_TEST_PASS", "")
	prot := env("PRESTO_TEST_PROT", "http")
	addr := env("PRESTO_TEST_ADDR", "localhost:8080")
	dbname := env("PRESTO_TEST_DBNAME", "default")
	// dsn = fmt.Sprintf("%s://%s:%s@%s/%s?timeout=30s", prot, user, pass, addr, dbname)
	dsn = fmt.Sprintf("%s://%s/%s?timeout=30s", prot, addr, dbname)
}

type dbt struct {
	*testing.T
	db *sql.DB
}

func (db *dbt) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	db.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (db *dbt) mustExec(query string, args ...interface{}) sql.Result {
	res, err := db.db.Exec(query, args...)
	if err != nil {
		db.fail("exec", query, err)
	}
	return res
}

func (db *dbt) mustBegin() *sql.Tx {
	tx, err := db.db.Begin()
	if err != nil {
		db.fail("begin", "", err)
	}
	return tx
}

func runTest(t *testing.T, dsn string, fn func(*dbt)) {
	db, err := sql.Open("presto", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	dbt := &dbt{t, db}
	fn(dbt)
}

func TestCRUD(t *testing.T) {
	runTest(t, dsn, func(db *dbt) {
		db.mustExec("CREATE TABLE test(value bool)")
	})
}
