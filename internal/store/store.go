// Package store opens T3 Code state databases and checks that their schema is
// one t3rry knows how to move threads between.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver
)

//go:generate go run ./schemagen -version 56

// BusyTimeoutMillis bounds how long t3rry waits for a lock held by another
// connection before failing instead of blocking forever.
const BusyTimeoutMillis = 5000

// URI returns a SQLite URI for path. Read-only URIs never create or modify the
// file; read-write URIs refuse to create a missing database.
func URI(path string, readOnly bool) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	mode := "rw"
	if readOnly {
		mode = "ro"
	}
	query := url.Values{}
	query.Set("mode", mode)
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", BusyTimeoutMillis))
	u := url.URL{Scheme: "file", Path: abs, RawQuery: query.Encode()}
	return u.String(), nil
}

// Open returns a dedicated connection to the database at path. Callers own the
// returned *sql.DB and must close it after the connection.
func Open(ctx context.Context, path string, readOnly bool) (*sql.DB, *sql.Conn, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("%s is not a regular file", path)
	}
	uri, err := URI(path, readOnly)
	if err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, nil, err
	}
	return db, conn, nil
}

// Attach attaches the database at path to conn under name, read-only. It must
// run outside a transaction.
func Attach(ctx context.Context, conn *sql.Conn, path, name string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	uri, err := URI(path, true)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "ATTACH DATABASE ? AS "+name, uri)
	return err
}

// Rollback ends an open transaction and ignores the error SQLite returns when
// none is active.
func Rollback(conn *sql.Conn) {
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
}

// ErrNotT3 reports a database without T3 Code's migration ledger.
var ErrNotT3 = errors.New("not a T3 Code state database")
