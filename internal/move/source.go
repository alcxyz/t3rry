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
	"time"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

// finishSource archives the moved threads in the source and disables their
// scheduled tasks, in one source transaction after the target commit.
// Archiving mirrors the server's thread.archive command: a thread.archived
// snapshot event plus the projection row update, and detaching sessions that
// were not stopped.
func finishSource(ctx context.Context, opts Options, plan *Plan, now time.Time, result *Result) error {
	if !opts.ArchiveSource || len(plan.archive) == 0 && plan.counts[tableTasks] == 0 {
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

	commandID, err := newCommandID()
	if err != nil {
		return err
	}
	stamp := timestamp(now)
	for _, id := range plan.archive {
		var open bool
		err := conn.QueryRowContext(ctx,
			"SELECT archived_at IS NULL AND deleted_at IS NULL FROM main."+tableThreads+" WHERE thread_id = ?", id).Scan(&open)
		if errors.Is(err, sql.ErrNoRows) || err == nil && !open {
			continue
		}
		if err != nil {
			return err
		}
		if err := appendThreadSnapshot(ctx, conn, id, "thread.archived", commandID, stamp,
			"'$.archivedAt', ?2, '$.titleRegeneration', NULL, '$.updatedAt', ?2"); err != nil {
			return fmt.Errorf("archive %s: %w", id, err)
		}
		if err := detachSessions(ctx, conn, id, commandID, stamp); err != nil {
			return fmt.Errorf("archive %s: %w", id, err)
		}
		result.Archived++
	}

	if len(plan.threads) > 0 {
		ids, err := json.Marshal(plan.threads)
		if err != nil {
			return err
		}
		n, err := exec(ctx, conn, `
			UPDATE main.scheduled_tasks SET enabled = 0, updated_at = ?1
			WHERE enabled <> 0 AND thread_id IN (SELECT value FROM json_each(?2))`, stamp, string(ids))
		if err != nil {
			return fmt.Errorf("disable scheduled tasks: %w", err)
		}
		result.TasksDisabled = int(n)
	}
	if err := advanceCursor(ctx, conn, stamp); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

// detachSessions mirrors the provider-session.detached events the server
// emits when archiving a thread whose bound sessions are not stopped. The
// server is offline, so there is no live session process to stop.
func detachSessions(ctx context.Context, conn *sql.Conn, threadID, commandID, stamp string) error {
	rows, err := conn.QueryContext(ctx, `
		SELECT s.provider_session_id, s.driver, s.provider_instance_id
		FROM main.orchestration_v2_projection_provider_session_bindings b
		JOIN main.orchestration_v2_projection_provider_sessions s ON s.provider_session_id = b.provider_session_id
		WHERE b.thread_id = ? AND s.status NOT IN ('stopped', 'error')
		ORDER BY s.provider_session_id`, threadID)
	if err != nil {
		return err
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
			return err
		}
		sessions = append(sessions, s)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, s := range sessions {
		payload, err := json.Marshal(struct {
			ProviderSessionID string `json:"providerSessionId"`
			DetachedAt        string `json:"detachedAt"`
			Reason            string `json:"reason"`
		}{s.id, stamp, "Thread archived."})
		if err != nil {
			return err
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
			return err
		}
		eventID, err := newEventID(threadID, commandID)
		if err != nil {
			return err
		}
		if err := insertEvent(ctx, conn, eventID, threadID, "provider-session.detached", stamp, commandID,
			string(payload), string(metadataJSON)); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
			DELETE FROM main.orchestration_v2_projection_provider_session_bindings
			WHERE provider_session_id = ? AND thread_id = ?`, s.id, threadID); err != nil {
			return err
		}
	}
	return nil
}

// backupTarget writes a consistent copy of the target database with VACUUM
// INTO and copies its attachments directory.
func backupTarget(ctx context.Context, to instance.Instance, dir string) error {
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

func copyTree(src, dst string) error {
	if _, err := os.Stat(src); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
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
