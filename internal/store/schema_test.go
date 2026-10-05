package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func readFixture(t *testing.T) (string, string) {
	t.Helper()
	schemaSQL, err := os.ReadFile("testdata/schema-v56.sql")
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := os.ReadFile("testdata/migrations-v56.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(schemaSQL), string(ledger)
}

// TestSchemaV56MatchesFixture keeps the generated schema in sync with its
// fixture; run go generate ./internal/store after changing either.
func TestSchemaV56MatchesFixture(t *testing.T) {
	schemaSQL, ledger := readFixture(t)
	loaded, err := LoadFixture(context.Background(), schemaSQL, ledger)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, schemaV56) {
		t.Fatal("schema_v56.go is stale; run go generate ./internal/store")
	}
	for _, name := range UsedTables {
		if schemaV56.Table(name) == nil {
			t.Errorf("used table %s is not in the v56 schema", name)
		}
	}
}

func newDatabase(t *testing.T) (*sql.DB, *sql.Conn) {
	t.Helper()
	schemaSQL, ledger := readFixture(t)
	path := filepath.Join(t.TempDir(), "statev2.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatal(err)
	}
	migrations, err := ParseLedger(ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if _, err := db.Exec("INSERT INTO effect_sql_migrations (migration_id, name) VALUES (?, ?)", m.ID, m.Name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO orchestration_v2_projection_metadata VALUES ('thread-projections', 2, 0, 'now')"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	ro, conn, err := Open(context.Background(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = ro.Close()
	})
	return ro, conn
}

func TestCheck(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		mutate  string
		problem string
		warning string
	}{
		{name: "supported"},
		{name: "extra table", mutate: "CREATE TABLE fork_extra (id TEXT)", warning: "unknown table fork_extra"},
		{name: "unused table differs", mutate: "ALTER TABLE projection_threads ADD COLUMN title_source TEXT", warning: "table projection_threads differs"},
		{name: "used table differs", mutate: "ALTER TABLE orchestration_v2_projection_runs ADD COLUMN extra TEXT", problem: "unexpected columns extra"},
		{name: "renamed migration", mutate: "UPDATE effect_sql_migrations SET name = 'Other' WHERE migration_id = 3", problem: `migration 3 is "Other"`},
		{name: "newer schema", mutate: "INSERT INTO effect_sql_migrations (migration_id, name) VALUES (57, 'Next')", problem: "highest migration is 57"},
		{name: "projection version", mutate: "UPDATE orchestration_v2_projection_metadata SET schema_version = 3", problem: "projection schema version is 3"},
		{name: "cursor behind", mutate: `INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
			VALUES ('e1', 'thread', 't1', 0, 'thread.created', 'now', 'server', '{}', '{}', 2)`, problem: "cursor 0 does not match the newest thread event 1"},
		{name: "no metadata", mutate: "DELETE FROM orchestration_v2_projection_metadata", problem: "metadata row is missing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, conn := newDatabase(t)
			if test.mutate != "" {
				if _, err := conn.ExecContext(ctx, test.mutate); err != nil {
					t.Fatal(err)
				}
			}
			report, err := Check(ctx, conn, "main")
			if err != nil {
				t.Fatal(err)
			}
			if test.problem == "" && !report.OK() {
				t.Fatalf("unexpected problems: %v", report.Problems)
			}
			if test.problem != "" && (report.OK() || !strings.Contains(strings.Join(report.Problems, "\n"), test.problem)) {
				t.Fatalf("problems %v lack %q", report.Problems, test.problem)
			}
			if test.warning != "" && !strings.Contains(strings.Join(report.Warnings, "\n"), test.warning) {
				t.Fatalf("warnings %v lack %q", report.Warnings, test.warning)
			}
		})
	}
}

func TestCheckRejectsOtherDatabases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "other.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE notes (id TEXT)"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	ro, conn, err := Open(context.Background(), path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	defer func() { _ = conn.Close() }()
	report, err := Check(context.Background(), conn, "main")
	if err != nil {
		t.Fatal(err)
	}
	if report.OK() || !strings.Contains(report.Problems[0], ErrNotT3.Error()) {
		t.Fatalf("report = %+v", report)
	}
}

func TestOpenReadOnlyDoesNotCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sqlite")
	if _, _, err := Open(context.Background(), path, true); err == nil {
		t.Fatal("opened a missing database")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Open created %s", path)
	}
}

func TestURIEscapesPaths(t *testing.T) {
	uri, err := URI("/tmp/with space/#hash?.sqlite", true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(uri, "file:///tmp/with%20space/%23hash%3F.sqlite?") || !strings.Contains(uri, "mode=ro") {
		t.Fatalf("URI = %s", uri)
	}
}
