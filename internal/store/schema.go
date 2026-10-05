package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// ProjectionSchemaVersion is the orchestration v2 projection version t3rry
// writes (ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION upstream).
const ProjectionSchemaVersion = 2

// ProjectionName is the orchestration_v2_projection_metadata row that tracks
// the thread projection cursor.
const ProjectionName = "thread-projections"

// Migration is one row of T3 Code's effect_sql_migrations ledger.
type Migration struct {
	ID   int
	Name string
}

// Table describes a table's columns in declaration order and its primary key.
type Table struct {
	Name       string
	Columns    []string
	PrimaryKey []string
}

// Schema is a T3 Code storage schema t3rry supports, identified by its
// highest migration.
type Schema struct {
	MaxMigration int
	Migrations   []Migration
	Tables       []Table
}

// Table returns the named table, or nil when the schema has no such table.
func (s *Schema) Table(name string) *Table {
	for i := range s.Tables {
		if s.Tables[i].Name == name {
			return &s.Tables[i]
		}
	}
	return nil
}

// supportedSchemas lists every schema t3rry can move threads between, keyed by
// the highest migration id. Add a schema by capturing a fresh fixture and
// running go generate.
var supportedSchemas = map[int]*Schema{
	56: &schemaV56,
}

// SupportedMigrations returns the supported highest migration ids, ascending.
func SupportedMigrations() []int {
	ids := make([]int, 0, len(supportedSchemas))
	for id := range supportedSchemas {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// UsedTables lists every table t3rry copies, writes or reads more than a few
// stable key columns from. Their columns must match the supported schema
// exactly. The legacy projection_threads table is only probed by id, project
// and deletion for a warning, so differences there are reported but tolerated.
var UsedTables = []string{
	"effect_sql_migrations",
	"orchestration_events",
	"projection_projects",
	"provider_session_runtime",
	"scheduled_tasks",
	"orchestration_v2_effect_outbox",
	"orchestration_v2_legacy_imports",
	"orchestration_v2_projection_metadata",
	"orchestration_v2_projection_threads",
	"orchestration_v2_projection_runs",
	"orchestration_v2_projection_run_attempts",
	"orchestration_v2_projection_nodes",
	"orchestration_v2_projection_provider_sessions",
	"orchestration_v2_projection_provider_session_bindings",
	"orchestration_v2_projection_provider_threads",
	"orchestration_v2_projection_provider_turns",
	"orchestration_v2_projection_runtime_requests",
	"orchestration_v2_projection_messages",
	"orchestration_v2_projection_plans",
	"orchestration_v2_projection_turn_items",
	"orchestration_v2_projection_checkpoint_scopes",
	"orchestration_v2_projection_checkpoints",
	"orchestration_v2_projection_context_handoffs",
	"orchestration_v2_projection_context_transfers",
	"orchestration_v2_projection_subagents",
	"orchestration_v2_turn_item_positions",
}

// ReadMigrations returns the migration ledger of database db ("main" or an
// attached name), ascending by id.
func ReadMigrations(ctx context.Context, conn *sql.Conn, db string) ([]Migration, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT migration_id, name FROM "+db+".effect_sql_migrations ORDER BY migration_id")
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return nil, ErrNotT3
		}
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var migrations []Migration
	for rows.Next() {
		var m Migration
		if err := rows.Scan(&m.ID, &m.Name); err != nil {
			return nil, err
		}
		migrations = append(migrations, m)
	}
	return migrations, rows.Err()
}

// ReadTables returns every ordinary table of database db, sorted by name.
func ReadTables(ctx context.Context, conn *sql.Conn, db string) ([]Table, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT name FROM "+db+".sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name")
	if err != nil {
		return nil, err
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	tables := make([]Table, 0, len(names))
	for _, name := range names {
		table, err := readTable(ctx, conn, db, name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func readTable(ctx context.Context, conn *sql.Conn, db, name string) (Table, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT name, pk FROM pragma_table_info(?, ?) ORDER BY cid", name, db)
	if err != nil {
		return Table{}, err
	}
	defer func() { _ = rows.Close() }()
	table := Table{Name: name}
	pkByIndex := map[int]string{}
	for rows.Next() {
		var column string
		var pk int
		if err := rows.Scan(&column, &pk); err != nil {
			return Table{}, err
		}
		table.Columns = append(table.Columns, column)
		if pk > 0 {
			pkByIndex[pk] = column
		}
	}
	if err := rows.Err(); err != nil {
		return Table{}, err
	}
	for i := 1; i <= len(pkByIndex); i++ {
		table.PrimaryKey = append(table.PrimaryKey, pkByIndex[i])
	}
	return table, nil
}

// Report is the result of checking one database against the supported schemas.
type Report struct {
	// Schema is the matched supported schema, or nil.
	Schema *Schema
	// MaxMigration is the highest migration id found, or 0.
	MaxMigration int
	// ProjectionVersion and ProjectionSequence come from the thread
	// projection metadata row; EventSequence is the newest v2 thread event.
	ProjectionVersion  int
	ProjectionSequence int64
	EventSequence      int64
	// Problems make the database unusable for a move.
	Problems []string
	// Warnings do not block a move.
	Warnings []string
}

// OK reports whether the database matched a supported schema without problems.
func (r Report) OK() bool {
	return r.Schema != nil && len(r.Problems) == 0
}

// Check compares database db on conn with the supported schemas and verifies
// that the thread projection cursor matches the event log.
func Check(ctx context.Context, conn *sql.Conn, db string) (Report, error) {
	var report Report
	migrations, err := ReadMigrations(ctx, conn, db)
	if err == ErrNotT3 {
		report.Problems = append(report.Problems, "no effect_sql_migrations table: "+ErrNotT3.Error())
		return report, nil
	}
	if err != nil {
		return report, err
	}
	if len(migrations) == 0 {
		report.Problems = append(report.Problems, "the migration ledger is empty")
		return report, nil
	}
	report.MaxMigration = migrations[len(migrations)-1].ID
	schema := supportedSchemas[report.MaxMigration]
	if schema == nil {
		report.Problems = append(report.Problems, fmt.Sprintf(
			"unsupported schema: highest migration is %d, t3rry supports %s",
			report.MaxMigration, joinInts(SupportedMigrations())))
		return report, nil
	}
	if problem := compareMigrations(schema.Migrations, migrations); problem != "" {
		report.Problems = append(report.Problems, problem)
		return report, nil
	}

	tables, err := ReadTables(ctx, conn, db)
	if err != nil {
		return report, err
	}
	actual := map[string]Table{}
	for _, table := range tables {
		actual[table.Name] = table
	}
	for _, expected := range schema.Tables {
		got, ok := actual[expected.Name]
		used := slices.Contains(UsedTables, expected.Name)
		var problem string
		if !ok {
			problem = "table " + expected.Name + " is missing"
		} else if diff := compareTable(expected, got); diff != "" {
			problem = "table " + expected.Name + " differs: " + diff
		}
		if problem == "" {
			continue
		}
		if used {
			report.Problems = append(report.Problems, problem)
		} else {
			report.Warnings = append(report.Warnings, problem+" (t3rry does not depend on its columns)")
		}
	}
	for _, table := range tables {
		if schema.Table(table.Name) == nil {
			report.Warnings = append(report.Warnings, "unknown table "+table.Name+" is ignored")
		}
	}
	if len(report.Problems) > 0 {
		return report, nil
	}

	err = conn.QueryRowContext(ctx,
		"SELECT schema_version, last_sequence FROM "+db+".orchestration_v2_projection_metadata WHERE projection_name = ?",
		ProjectionName).Scan(&report.ProjectionVersion, &report.ProjectionSequence)
	if err == sql.ErrNoRows {
		report.Problems = append(report.Problems, "the thread projection metadata row is missing")
		return report, nil
	}
	if err != nil {
		return report, err
	}
	report.EventSequence, err = LatestThreadEventSequence(ctx, conn, db)
	if err != nil {
		return report, err
	}
	if report.ProjectionVersion != ProjectionSchemaVersion {
		report.Problems = append(report.Problems, fmt.Sprintf(
			"thread projection schema version is %d, t3rry supports %d",
			report.ProjectionVersion, ProjectionSchemaVersion))
	}
	if report.ProjectionSequence != report.EventSequence {
		report.Problems = append(report.Problems, fmt.Sprintf(
			"thread projection cursor %d does not match the newest thread event %d",
			report.ProjectionSequence, report.EventSequence))
	}
	report.Schema = schema
	return report, nil
}

// LatestThreadEventSequence returns the newest v2 thread event sequence of
// database db, or 0.
func LatestThreadEventSequence(ctx context.Context, conn *sql.Conn, db string) (int64, error) {
	var sequence sql.NullInt64
	err := conn.QueryRowContext(ctx,
		"SELECT MAX(sequence) FROM "+db+".orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'").
		Scan(&sequence)
	return sequence.Int64, err
}

func compareMigrations(expected, actual []Migration) string {
	if len(expected) != len(actual) {
		return fmt.Sprintf("migration ledger has %d entries, the supported schema has %d", len(actual), len(expected))
	}
	for i := range expected {
		if expected[i] != actual[i] {
			return fmt.Sprintf("migration %d is %q, the supported schema expects %d %q",
				actual[i].ID, actual[i].Name, expected[i].ID, expected[i].Name)
		}
	}
	return ""
}

func compareTable(expected, actual Table) string {
	var missing, extra []string
	for _, column := range expected.Columns {
		if !slices.Contains(actual.Columns, column) {
			missing = append(missing, column)
		}
	}
	for _, column := range actual.Columns {
		if !slices.Contains(expected.Columns, column) {
			extra = append(extra, column)
		}
	}
	var parts []string
	if len(missing) > 0 {
		parts = append(parts, "missing columns "+strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		parts = append(parts, "unexpected columns "+strings.Join(extra, ", "))
	}
	if !slices.Equal(expected.PrimaryKey, actual.PrimaryKey) {
		parts = append(parts, fmt.Sprintf("primary key (%s), expected (%s)",
			strings.Join(actual.PrimaryKey, ", "), strings.Join(expected.PrimaryKey, ", ")))
	}
	return strings.Join(parts, "; ")
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ", ")
}
