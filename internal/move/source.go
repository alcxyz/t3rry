package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

// finishSource archives the selected threads in the source, detaches their
// live sessions and disables their scheduled tasks, in one source transaction
// after the target commit. It covers threads an earlier run already copied
// and skips work already done, so a retry completes a failed cleanup.
// Archiving mirrors the server's thread.archive command: a thread.archived
// snapshot event plus the projection row update, and detaching sessions that
// were not stopped.
func finishSource(ctx context.Context, opts Options, plan *Plan, now time.Time, result *Result) error {
	if !opts.ArchiveSource || len(plan.selected) == 0 {
		return nil
	}
	if activity := opts.From.Activity(); activity.Running() {
		return fmt.Errorf("source server appears to be running: %s", activity.Reasons[0])
	}
	db, conn, err := store.Open(ctx, opts.From.DBPath, false)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = conn.Close() }()
	report, err := store.Check(ctx, conn, "main")
	if err != nil {
		return err
	}
	if !report.OK() {
		return fmt.Errorf("source schema check failed: %v", report.Problems)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			store.Rollback(conn)
		}
	}()

	commandID, err := newCommandID(archiveCommandPrefix)
	if err != nil {
		return err
	}
	stamp := timestamp(now)
	archived, marked, detached := 0, 0, 0
	for _, id := range plan.selected {
		var archivedAlready, deleted, hasMarker bool
		err := conn.QueryRowContext(ctx, `
			SELECT archived_at IS NOT NULL, deleted_at IS NOT NULL,
				EXISTS (SELECT 1 FROM main.orchestration_events e
					WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread' AND e.stream_id = ?1
						AND e.event_type = 'thread.archived' AND e.command_id LIKE ?2)
			FROM main.`+tableThreads+` WHERE thread_id = ?1`, id, archiveCommandPrefix+"%").
			Scan(&archivedAlready, &deleted, &hasMarker)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		switch {
		case deleted || hasMarker:
		case archivedAlready:
			// Archived by the user before the move: record the marker
			// without changing when it was archived.
			if err := appendThreadSnapshot(ctx, conn, id, "thread.archived", commandID, stamp,
				"'$.titleRegeneration', NULL"); err != nil {
				return fmt.Errorf("mark %s: %w", id, err)
			}
			marked++
		default:
			if err := appendThreadSnapshot(ctx, conn, id, "thread.archived", commandID, stamp,
				"'$.archivedAt', ?2, '$.titleRegeneration', NULL, '$.updatedAt', ?2"); err != nil {
				return fmt.Errorf("archive %s: %w", id, err)
			}
			archived++
		}
		n, err := detachSessions(ctx, conn, id, commandID, stamp)
		if err != nil {
			return fmt.Errorf("detach sessions of %s: %w", id, err)
		}
		detached += n
	}

	ids, err := json.Marshal(plan.selected)
	if err != nil {
		return err
	}
	tasks, err := exec(ctx, conn, `
		UPDATE main.scheduled_tasks SET enabled = 0, updated_at = ?1
		WHERE enabled <> 0 AND thread_id IN (SELECT value FROM json_each(?2))`, stamp, string(ids))
	if err != nil {
		return fmt.Errorf("disable scheduled tasks: %w", err)
	}
	if archived == 0 && marked == 0 && detached == 0 && tasks == 0 {
		return nil
	}
	if err := advanceCursor(ctx, conn, stamp); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	result.Archived = archived
	result.TasksDisabled = int(tasks)
	return nil
}

// detachSessions mirrors the provider-session.detached events the server
// emits when archiving a thread whose bound sessions are not stopped. The
// server is offline, so there is no live session process to stop.
func detachSessions(ctx context.Context, conn *sql.Conn, threadID, commandID, stamp string) (int, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT s.provider_session_id, s.driver, s.provider_instance_id
		FROM main.orchestration_v2_projection_provider_session_bindings b
		JOIN main.orchestration_v2_projection_provider_sessions s ON s.provider_session_id = b.provider_session_id
		WHERE b.thread_id = ? AND s.status NOT IN ('stopped', 'error')
		ORDER BY s.provider_session_id`, threadID)
	if err != nil {
		return 0, err
	}
	type session struct {
		id               string
		driver, instance sql.NullString
	}
	var sessions []session
	for rows.Next() {
		var s session
		if err := rows.Scan(&s.id, &s.driver, &s.instance); err != nil {
			_ = rows.Close()
			return 0, err
		}
		sessions = append(sessions, s)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, s := range sessions {
		payload, err := json.Marshal(struct {
			ProviderSessionID string `json:"providerSessionId"`
			DetachedAt        string `json:"detachedAt"`
			Reason            string `json:"reason"`
		}{s.id, stamp, "Thread archived."})
		if err != nil {
			return 0, err
		}
		metadata := map[string]string{}
		if s.driver.Valid {
			metadata["driver"] = s.driver.String
		}
		if s.instance.Valid {
			metadata["providerInstanceId"] = s.instance.String
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return 0, err
		}
		eventID, err := newEventID(threadID, commandID)
		if err != nil {
			return 0, err
		}
		if err := insertEvent(ctx, conn, eventID, threadID, "provider-session.detached", stamp, commandID,
			string(payload), string(metadataJSON)); err != nil {
			return 0, err
		}
		if _, err := conn.ExecContext(ctx, `
			DELETE FROM main.orchestration_v2_projection_provider_session_bindings
			WHERE provider_session_id = ? AND thread_id = ?`, s.id, threadID); err != nil {
			return 0, err
		}
	}
	return len(sessions), nil
}

// backupTarget writes a consistent copy of the target database with VACUUM
// INTO and copies its attachments directory.
func backupTarget(ctx context.Context, to instance.Instance, dir string) error {
	if err := checkBackupDir(to.AttachmentsDir, dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	dbCopy := filepath.Join(dir, "statev2.sqlite")
	if _, err := os.Stat(dbCopy); err == nil {
		return fmt.Errorf("%s already exists", dbCopy)
	}
	db, conn, err := store.Open(ctx, to.DBPath, true)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "VACUUM INTO ?", dbCopy)
	_ = conn.Close()
	_ = db.Close()
	if err != nil {
		return err
	}
	return copyTree(to.AttachmentsDir, filepath.Join(dir, "attachments"))
}

// checkBackupDir requires <dir>/attachments not to exist yet and rejects
// destinations whose resolved attachments copy would land inside, or contain,
// the target's attachments tree, following symlinks in both paths.
func checkBackupDir(attachments, dir string) error {
	dest := filepath.Join(dir, "attachments")
	if _, err := os.Lstat(dest); err == nil {
		return fmt.Errorf("backup destination %s already exists", dest)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	attachmentsReal := resolveExisting(attachments)
	destReal := filepath.Join(resolveExisting(dir), "attachments")
	if within(destReal, attachmentsReal) || within(attachmentsReal, destReal) {
		return fmt.Errorf("backup directory %s overlaps the target's attachments directory %s", dir, attachments)
	}
	return nil
}

// resolveExisting makes path absolute and resolves symlinks in its longest
// existing prefix.
func resolveExisting(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = filepath.Clean(path)
	}
	var rest []string
	current := abs
	for {
		real, err := filepath.EvalSymlinks(current)
		if err == nil {
			return filepath.Join(append([]string{real}, rest...)...)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return abs
		}
		rest = append([]string{filepath.Base(current)}, rest...)
		current = parent
	}
}

// within reports whether path is root or below it.
func within(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// copyTree copies the regular files below src, following a symlinked src
// root, which WalkDir would otherwise not descend into.
func copyTree(src, dst string) error {
	root, err := filepath.EvalSymlinks(src)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		switch {
		case entry.IsDir():
			return os.MkdirAll(target, 0o700)
		case entry.Type().IsRegular():
			return copyExclusive(path, target)
		default:
			return nil
		}
	})
}

// verifyTarget re-reads the committed target and reports what it checked.
func verifyTarget(ctx context.Context, opts Options, plan *Plan) []string {
	db, conn, err := store.Open(ctx, opts.To.DBPath, true)
	if err != nil {
		return []string{"could not reopen the target: " + err.Error()}
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = conn.Close() }()
	var lines []string
	report, err := store.Check(ctx, conn, "main")
	switch {
	case err != nil:
		lines = append(lines, "schema check failed: "+err.Error())
	case !report.OK():
		for _, problem := range report.Problems {
			lines = append(lines, "PROBLEM: "+problem)
		}
	default:
		lines = append(lines, fmt.Sprintf("projection cursor matches the newest thread event (%d)", report.EventSequence))
	}
	ids, err := json.Marshal(plan.threads)
	if err != nil {
		return append(lines, err.Error())
	}
	var rows, created int
	err = conn.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM orchestration_v2_projection_threads WHERE thread_id IN (SELECT value FROM json_each(?1))),
			(SELECT count(DISTINCT stream_id) FROM orchestration_events
				WHERE application_event_version = 2 AND aggregate_kind = 'thread' AND event_type = 'thread.created'
					AND stream_id IN (SELECT value FROM json_each(?1)))`, string(ids)).Scan(&rows, &created)
	if err != nil {
		return append(lines, "thread check failed: "+err.Error())
	}
	status := "ok"
	if rows != len(plan.threads) || created != len(plan.threads) {
		status = "PROBLEM"
	}
	lines = append(lines, fmt.Sprintf("%s: %d of %d moved threads have a projection row and %d a thread.created event",
		status, rows, len(plan.threads), created))
	return lines
}
