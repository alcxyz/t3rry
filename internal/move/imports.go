package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

// ImportOptions select imported threads to soft-delete in one base
// directory, for imports the automatic cleanup of a move leaves alone.
type ImportOptions struct {
	Base instance.Instance
	// Threads are the ids to soft-delete; empty only lists imports.
	Threads []string
	// BackupDir receives the backup; empty uses
	// <base>/userdata/t3rry-backups/<timestamp>.
	BackupDir string
	// Now is the clock; nil uses time.Now.
	Now func() time.Time
}

// ImportThread is a thread that "import recent sessions" created.
type ImportThread struct {
	ID        string
	Title     string
	Project   string
	CreatedAt string
	Runs      int
	Deleted   bool
}

// ImportPlan lists a base directory's imports and the ones to soft-delete.
type ImportPlan struct {
	Base     instance.Instance
	Activity instance.Activity
	Schema   store.Report
	// Imports lists the live imports when no thread was named.
	Imports  []ImportThread
	Delete   []ImportThread
	Skipped  []string
	Blockers []string
	Warnings []string
}

// Blocked reports whether anything prevents the deletion.
func (p *ImportPlan) Blocked() bool {
	return len(p.Blockers) > 0
}

// AnalyzeImports lists imports or plans soft-deleting the named ones without
// writing.
func AnalyzeImports(ctx context.Context, opts ImportOptions) (*ImportPlan, error) {
	db, conn, err := store.Open(ctx, opts.Base.DBPath, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return nil, err
	}
	defer store.Rollback(conn)
	plan := &ImportPlan{Base: opts.Base, Activity: opts.Base.Activity()}
	if err := analyzeImports(ctx, conn, opts, plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func analyzeImports(ctx context.Context, conn *sql.Conn, opts ImportOptions, plan *ImportPlan) error {
	for _, reason := range plan.Activity.Reasons {
		plan.Blockers = append(plan.Blockers, "server appears to be running: "+reason)
	}
	var err error
	plan.Schema, err = store.Check(ctx, conn, "main")
	if err != nil {
		return err
	}
	for _, problem := range plan.Schema.Problems {
		plan.Blockers = append(plan.Blockers, "schema: "+problem)
	}
	for _, warning := range plan.Schema.Warnings {
		plan.Warnings = append(plan.Warnings, "schema: "+warning)
	}
	if !plan.Schema.OK() {
		return nil
	}

	if len(opts.Threads) == 0 {
		rows, err := conn.QueryContext(ctx, importQuery+` WHERE t.thread_id LIKE 'import:%' AND t.deleted_at IS NULL
			ORDER BY p.workspace_root, t.created_at, t.thread_id`)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var it ImportThread
			var origin string
			if err := rows.Scan(&it.ID, &it.Title, &it.Project, &it.CreatedAt, &it.Runs, &it.Deleted, &origin); err != nil {
				return err
			}
			if origin == "v1_import" {
				plan.Imports = append(plan.Imports, it)
			}
		}
		return rows.Err()
	}

	seen := map[string]bool{}
	for _, id := range opts.Threads {
		if seen[id] {
			continue
		}
		seen[id] = true
		var it ImportThread
		var origin string
		err := conn.QueryRowContext(ctx, importQuery+` WHERE t.thread_id = ?`, id).
			Scan(&it.ID, &it.Title, &it.Project, &it.CreatedAt, &it.Runs, &it.Deleted, &origin)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("thread %s does not exist", id))
			continue
		case err != nil:
			return err
		case !strings.HasPrefix(id, "import:") || origin != "v1_import":
			plan.Blockers = append(plan.Blockers, fmt.Sprintf(
				"thread %s was not created by \"import recent sessions\"; delete it in T3 Code instead", id))
			continue
		case it.Deleted:
			plan.Skipped = append(plan.Skipped, id)
			continue
		}
		for _, check := range importActivityChecks {
			var n int
			if err := conn.QueryRowContext(ctx, check.query, id).Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				plan.Blockers = append(plan.Blockers, fmt.Sprintf(check.message, id, n))
			}
		}
		plan.Delete = append(plan.Delete, it)
	}
	return nil
}

const importQuery = `
	SELECT t.thread_id, t.title, COALESCE(p.workspace_root, t.project_id), t.created_at,
		(SELECT count(*) FROM orchestration_v2_projection_runs r WHERE r.thread_id = t.thread_id),
		t.deleted_at IS NOT NULL, COALESCE(json_extract(t.payload_json, '$.historyOrigin'), '')
	FROM orchestration_v2_projection_threads t
	LEFT JOIN projection_projects p ON p.project_id = t.project_id`

// importActivityChecks refuse to delete a thread the server is still working
// on, like the move's active-work blockers.
var importActivityChecks = []struct {
	query   string
	message string
}{
	{`SELECT count(*) FROM orchestration_v2_projection_runs WHERE thread_id = ? AND status IN ` + sqlList(activeRunStatuses),
		"thread %s has %d run(s) that are not finished"},
	{`SELECT count(*) FROM orchestration_v2_projection_provider_threads WHERE thread_id = ?
		AND (status = 'active' OR (json_valid(payload_json) AND json_array_length(payload_json, '$.pendingBackgroundTasks') > 0))`,
		"thread %s has %d provider thread(s) that are active or have background tasks"},
	{`SELECT count(*) FROM orchestration_v2_projection_runtime_requests WHERE thread_id = ? AND status = 'pending'`,
		"thread %s has %d pending approval or input request(s)"},
	{`SELECT count(*) FROM orchestration_v2_effect_outbox WHERE thread_id = ? AND status IN ('pending', 'running')`,
		"thread %s has %d queued server effect(s)"},
}

// ImportResult reports soft-deleted imports.
type ImportResult struct {
	BackupDir    string
	Deleted      int
	Detached     int
	Verification []string
}

// DeleteImports backs up the base directory and soft-deletes the planned
// imports in one transaction, as T3 Code's delete command does: a
// thread.deleted snapshot event, the projection row update, and detaching
// sessions that were not stopped.
func DeleteImports(ctx context.Context, opts ImportOptions) (*ImportPlan, *ImportResult, error) {
	plan, err := AnalyzeImports(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	if plan.Blocked() {
		return plan, nil, ErrBlocked
	}
	if len(plan.Delete) == 0 {
		return plan, &ImportResult{}, nil
	}
	now := time.Now()
	if opts.Now != nil {
		now = opts.Now()
	}
	backupDir := opts.BackupDir
	if backupDir == "" {
		backupDir = filepath.Join(opts.Base.UserData, "t3rry-backups", now.UTC().Format("20060102T150405Z"))
	}
	if err := backupTarget(ctx, opts.Base, backupDir); err != nil {
		return plan, nil, fmt.Errorf("back up: %w", err)
	}
	result := &ImportResult{BackupDir: backupDir}

	db, conn, err := store.Open(ctx, opts.Base.DBPath, false)
	if err != nil {
		return plan, result, err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return plan, result, fmt.Errorf("lock database: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			store.Rollback(conn)
		}
	}()
	// Re-plan under the write lock so the deletion matches the committed state.
	locked := &ImportPlan{Base: opts.Base, Activity: opts.Base.Activity()}
	if err := analyzeImports(ctx, conn, opts, locked); err != nil {
		return plan, result, err
	}
	if locked.Blocked() {
		return locked, nil, ErrBlocked
	}
	commandID, err := newCommandID(deleteCommandPrefix)
	if err != nil {
		return locked, result, err
	}
	stamp := timestamp(now)
	for _, it := range locked.Delete {
		n, err := detachSessions(ctx, conn, it.ID, commandID, stamp, "Thread deleted.")
		if err != nil {
			return locked, result, fmt.Errorf("detach sessions of %s: %w", it.ID, err)
		}
		result.Detached += n
		if err := softDeleteImport(ctx, conn, it.ID, commandID, stamp); err != nil {
			return locked, result, fmt.Errorf("soft-delete %s: %w", it.ID, err)
		}
	}
	if err := advanceCursor(ctx, conn, stamp); err != nil {
		return locked, result, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return locked, result, fmt.Errorf("commit: %w", err)
	}
	committed = true
	result.Deleted = len(locked.Delete)

	report, err := store.Check(ctx, conn, "main")
	switch {
	case err != nil:
		result.Verification = append(result.Verification, "schema check failed: "+err.Error())
	case !report.OK():
		for _, problem := range report.Problems {
			result.Verification = append(result.Verification, "PROBLEM: "+problem)
		}
	default:
		result.Verification = append(result.Verification,
			fmt.Sprintf("projection cursor matches the newest thread event (%d)", report.EventSequence))
	}
	return locked, result, nil
}

// Write prints the import plan for people.
func (p *ImportPlan) Write(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "base dir %s  (%s; %s)\n", p.Base.BaseDir, schemaSummary(p.Schema), activitySummary(p.Activity))
	writeList(&b, "", "blocked", p.Blockers)
	writeList(&b, "", "warning", p.Warnings)
	if len(p.Imports) > 0 {
		fmt.Fprintf(&b, "\n%d live import(s):\n", len(p.Imports))
		project := ""
		for _, it := range p.Imports {
			if it.Project != project {
				project = it.Project
				fmt.Fprintf(&b, "project %s\n", project)
			}
			fmt.Fprintf(&b, "  %s  %s  runs %d  %s\n", it.ID, it.CreatedAt, it.Runs, it.Title)
		}
	}
	for _, it := range p.Delete {
		fmt.Fprintf(&b, "soft-delete %s  (%s; runs %d)  %s\n", it.ID, it.Project, it.Runs, it.Title)
	}
	for _, id := range p.Skipped {
		fmt.Fprintf(&b, "skip        %s  (already deleted)\n", id)
	}
	switch {
	case p.Blocked():
		fmt.Fprintln(&b, "result: blocked")
	case len(p.Delete) > 0:
		fmt.Fprintf(&b, "result: ready to soft-delete %d import(s)\n", len(p.Delete))
	case len(p.Skipped) > 0:
		fmt.Fprintln(&b, "result: nothing to delete")
	case len(p.Imports) == 0:
		fmt.Fprintln(&b, "result: no imports")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// Write prints the result of DeleteImports.
func (r *ImportResult) Write(w io.Writer) error {
	var b strings.Builder
	if r.BackupDir != "" {
		fmt.Fprintf(&b, "backup     %s\n", r.BackupDir)
	}
	fmt.Fprintf(&b, "imports soft-deleted %d\n", r.Deleted)
	if r.Detached > 0 {
		fmt.Fprintf(&b, "sessions detached %d\n", r.Detached)
	}
	for _, line := range r.Verification {
		fmt.Fprintf(&b, "verify     %s\n", line)
	}
	_, err := io.WriteString(w, b.String())
	return err
}
