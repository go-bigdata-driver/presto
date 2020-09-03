package presto

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
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
	catalog := env("PRESTO_TEST_CATALOG", "hive")
	dbname := env("PRESTO_TEST_DBNAME", "default")
	// dsn = fmt.Sprintf("%s://%s:%s@%s/%s?timeout=30s", prot, user, pass, addr, dbname)
	dsn = fmt.Sprintf("%s://%s/%s?timeout=30s&catalog=%s", prot, addr, dbname, catalog)
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

func (db *dbt) exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *dbt) mustExec(query string, args ...interface{}) sql.Result {
	res, err := db.db.Exec(query, args...)
	if err != nil {
		db.fail("exec", query, err)
	}
	return res
}

func (db *dbt) mustQuery(query string, args ...interface{}) *sql.Rows {
	rows, err := db.db.Query(query, args...)
	if err != nil {
		db.fail("query", query, err)
	}
	return rows
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
		defer db.exec("DROP TABLE IF EXISTS default.test")
		db.mustExec(`CREATE TABLE
			IF NOT EXISTS default.test(
				value int,
				ts timestamp,
				dt date
			) WITH (
				partitioned_by=ARRAY['dt']
			)`)

		rows := db.mustQuery("SELECT * FROM default.test")
		if rows.Next() {
			db.Error("unexpected data in empty table")
		}
		rows.Close()

		dt := DateFromTime(time.Now())
		res := db.mustExec("INSERT INTO default.test (dt, ts, value) VALUES(?, ?, ?)", dt, time.Now(), 1)
		count, err := res.RowsAffected()
		if err != nil {
			db.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			db.Fatalf("expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			db.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != 0 {
			db.Fatalf("expected InsertId 0, got %d", id)
		}

		var t time.Time
		var out int
		rows = db.mustQuery("SELECT dt, ts, value FROM default.test")
		if rows.Next() {
			rows.Scan(&dt, &t, &out)
			if out != 1 {
				db.Errorf("%d != 1", out)
			}

			if rows.Next() {
				db.Error("unexpected data")
			}
		} else {
			db.Error("no data: " + rows.Err().Error())
		}
		rows.Close()

		// presto not supported update
		// res = db.mustExec("UPDATE default.test SET value = ? WHERE value = ?", 0, 1)
		// count, err = res.RowsAffected()
		// if err != nil {
		// 	db.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		// }
		// if count != 1 {
		// 	db.Fatalf("expected 1 affected row, got %d", count)
		// }

		res = db.mustExec("DELETE FROM default.test WHERE dt=?", dt)
		count, err = res.RowsAffected()
		if err != nil {
			db.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}

		db.mustExec("DROP TABLE IF EXISTS default.test")
	})
}
