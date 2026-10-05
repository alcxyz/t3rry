package store

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// LoadFixture builds a Schema from a captured `.schema` dump of a fresh T3 Code
// database and its `migration_id|name` ledger. It backs the code generator and
// the test that keeps generated schemas in sync with their fixtures.
func LoadFixture(ctx context.Context, schemaSQL, ledger string) (Schema, error) {
	migrations, err := ParseLedger(ledger)
	if err != nil {
		return Schema{}, err
	}
	if len(migrations) == 0 {
		return Schema{}, fmt.Errorf("migration ledger is empty")
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return Schema{}, err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return Schema{}, err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, schemaSQL); err != nil {
		return Schema{}, fmt.Errorf("load schema fixture: %w", err)
	}
	tables, err := ReadTables(ctx, conn, "main")
	if err != nil {
		return Schema{}, err
	}
	return Schema{
		MaxMigration: migrations[len(migrations)-1].ID,
		Migrations:   migrations,
		Tables:       tables,
	}, nil
}

// ParseLedger parses `migration_id|name` lines as printed by sqlite3.
func ParseLedger(ledger string) ([]Migration, error) {
	var migrations []Migration
	scanner := bufio.NewScanner(strings.NewReader(ledger))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		idText, name, ok := strings.Cut(line, "|")
		if !ok {
			return nil, fmt.Errorf("malformed ledger line %q", line)
		}
		id, err := strconv.Atoi(idText)
		if err != nil {
			return nil, fmt.Errorf("malformed ledger line %q: %w", line, err)
		}
		if len(migrations) > 0 && id <= migrations[len(migrations)-1].ID {
			return nil, fmt.Errorf("ledger ids are not ascending at %d", id)
		}
		migrations = append(migrations, Migration{ID: id, Name: name})
	}
	return migrations, scanner.Err()
}
