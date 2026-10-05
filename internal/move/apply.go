package move

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/alcxyz/t3rry/internal/store"
)

// applyTarget copies the planned rows into the target and soft-deletes
// duplicate imports. It runs inside the caller's BEGIN IMMEDIATE transaction
// on a connection with the source attached as src.
func applyTarget(ctx context.Context, conn *sql.Conn, plan *Plan, now time.Time, commandID string, result *Result) error {
	schema := plan.schema

	// Events first, in source order, so AUTOINCREMENT assigns new sequences
	// in the same order. Thread snapshots carry the target project id.
	// Source-archive markers are copied as ordinary archive events: the
	// marker describes only the database it was written in.
	events := schema.Table(tableEvents)
	cols, exprs := selectList(events, "e", map[string]string{
		"metadata_json": "json_remove(e.metadata_json, '$." + sourceArchiveMarker + "')",
		"payload_json": "CASE WHEN e.event_type IN " + sqlList(threadSnapshotEventTypes) +
			" AND json_type(e.payload_json, '$.projectId') = 'text'" +
			" THEN json_set(e.payload_json, '$.projectId', m.target_project_id) ELSE e.payload_json END",
	}, "sequence")
	n, err := exec(ctx, conn, "INSERT INTO main."+tableEvents+" ("+cols+") SELECT "+exprs+
		" FROM src."+tableEvents+" e JOIN temp.t3rry_moved m ON m.thread_id = e.stream_id"+
		" WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread' ORDER BY e.sequence")
	if err != nil {
		return fmt.Errorf("copy events: %w", err)
	}
	result.Rows[tableEvents] = n

	threads := schema.Table(tableThreads)
	cols, exprs = selectList(threads, "t", map[string]string{
		"project_id":   "m.target_project_id",
		"payload_json": "json_set(t.payload_json, '$.projectId', m.target_project_id)",
	})
	n, err = exec(ctx, conn, "INSERT INTO main."+tableThreads+" ("+cols+") SELECT "+exprs+
		" FROM src."+tableThreads+" t JOIN temp.t3rry_moved m ON m.thread_id = t.thread_id")
	if err != nil {
		return fmt.Errorf("copy threads: %w", err)
	}
	result.Rows[tableThreads] = n

	for _, rc := range rowCopies {
		table := schema.Table(rc.table)
		cols, exprs := selectList(table, "", nil)
		n, err := exec(ctx, conn, "INSERT INTO main."+rc.table+" ("+cols+") SELECT "+exprs+
			" FROM src."+rc.table+" WHERE "+rc.filter)
		if err != nil {
			return fmt.Errorf("copy %s: %w", rc.table, err)
		}
		result.Rows[rc.table] = n
	}

	if err := copySessions(ctx, conn, plan, result); err != nil {
		return err
	}

	tasks := schema.Table(tableTasks)
	// Tasks of deleted threads arrive disabled; the source keeps its copy.
	cols, exprs = selectList(tasks, "s", map[string]string{
		"project_id": "m.target_project_id",
		"enabled": "(SELECT CASE WHEN t.deleted_at IS NULL THEN s.enabled ELSE 0 END" +
			" FROM src." + tableThreads + " t WHERE t.thread_id = s.thread_id)",
	})
	n, err = exec(ctx, conn, "INSERT INTO main."+tableTasks+" ("+cols+") SELECT "+exprs+
		" FROM src."+tableTasks+" s JOIN temp.t3rry_moved m ON m.thread_id = s.thread_id")
	if err != nil {
		return fmt.Errorf("copy scheduled tasks: %w", err)
	}
	result.Rows[tableTasks] = n

	for table, want := range plan.counts {
		if got := result.Rows[table]; got != want {
			return fmt.Errorf("%s received %d row(s), the plan expected %d", table, got, want)
		}
	}

	stamp := timestamp(now)
	for _, id := range plan.duplicates {
		err := appendThreadSnapshot(ctx, conn, id, "thread.deleted", commandID, stamp, false,
			"'$.deletedAt', COALESCE(json_extract(payload_json, '$.deletedAt'), ?2), '$.titleRegeneration', NULL, '$.updatedAt', ?2", stamp)
		if err != nil {
			return fmt.Errorf("soft-delete duplicate %s: %w", id, err)
		}
		result.Duplicates++
	}
	return advanceCursor(ctx, conn, stamp)
}

// copySessions copies sessions owned by moved threads verbatim and adds shared
// sessions the target lacks, stopped and bound to one moved thread.
func copySessions(ctx context.Context, conn *sql.Conn, plan *Plan, result *Result) error {
	table := plan.schema.Table(tableSessions)
	for _, s := range plan.sessions {
		if s.inTarget {
			continue
		}
		overrides := map[string]string{}
		args := []any{s.id}
		if s.shared {
			overrides["thread_id"] = "?2"
			overrides["status"] = "'stopped'"
			overrides["payload_json"] = "json_set(payload_json, '$.status', 'stopped')"
			args = append(args, s.boundThread)
		}
		cols, exprs := selectList(table, "", overrides)
		n, err := exec(ctx, conn, "INSERT INTO main."+tableSessions+" ("+cols+") SELECT "+exprs+
			" FROM src."+tableSessions+" WHERE provider_session_id = ?1", args...)
		if err != nil {
			return fmt.Errorf("copy provider session %s: %w", s.id, err)
		}
		result.Rows[tableSessions] += n
	}
	return nil
}

// appendThreadSnapshot appends a thread snapshot event built from the thread's
// projection row with the given json_set path/value pairs, and writes the same
// snapshot back to the row, as the server's projector does. The pairs refer to
// setArgs as ?2 onwards. marker adds the source-archive marker to the event
// metadata. It works on the main database of conn.
func appendThreadSnapshot(ctx context.Context, conn *sql.Conn, threadID, eventType, commandID, stamp string,
	marker bool, sets string, setArgs ...any) error {
	metadataExpr := "json_object('providerInstanceId', json_extract(payload_json, '$.providerInstanceId'))"
	if marker {
		metadataExpr = "json_object('providerInstanceId', json_extract(payload_json, '$.providerInstanceId'), '" +
			sourceArchiveMarker + "', json('true'))"
	}
	var payload, metadata string
	err := conn.QueryRowContext(ctx,
		"SELECT json_set(payload_json, "+sets+"), "+metadataExpr+" FROM main."+tableThreads+" WHERE thread_id = ?1",
		append([]any{threadID}, setArgs...)...).Scan(&payload, &metadata)
	if err != nil {
		return err
	}
	eventID, err := newEventID(threadID, commandID)
	if err != nil {
		return err
	}
	if err := insertEvent(ctx, conn, eventID, threadID, eventType, stamp, commandID, payload, metadata); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `
		UPDATE main.`+tableThreads+` SET
			payload_json = ?1,
			updated_at = json_extract(?1, '$.updatedAt'),
			archived_at = json_extract(?1, '$.archivedAt'),
			deleted_at = json_extract(?1, '$.deletedAt')
		WHERE thread_id = ?2`, payload, threadID)
	return err
}

// insertEvent appends one server-authored v2 thread event to main, numbering
// it after the thread's newest stream version, like the server's event store.
func insertEvent(ctx context.Context, conn *sql.Conn, eventID, threadID, eventType, stamp, commandID, payload, metadata string) error {
	_, err := conn.ExecContext(ctx, `
		INSERT INTO main.orchestration_events (
			event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
			command_id, causation_event_id, correlation_id, actor_kind,
			payload_json, metadata_json, application_event_version)
		VALUES (?1, 'thread', ?2,
			COALESCE((SELECT MAX(stream_version) + 1 FROM main.orchestration_events
				WHERE aggregate_kind = 'thread' AND stream_id = ?2), 0),
			?3, ?4, ?5, NULL, ?5, 'server', ?6, ?7, 2)`,
		eventID, threadID, eventType, stamp, commandID, payload, metadata)
	return err
}

// advanceCursor sets the thread projection cursor of main to its newest v2
// thread event, as the server's EventSink does after every commit.
func advanceCursor(ctx context.Context, conn *sql.Conn, stamp string) error {
	n, err := exec(ctx, conn, `
		UPDATE main.orchestration_v2_projection_metadata SET
			last_sequence = (SELECT COALESCE(MAX(sequence), 0) FROM main.orchestration_events
				WHERE application_event_version = 2 AND aggregate_kind = 'thread'),
			updated_at = ?2
		WHERE projection_name = ?1 AND schema_version = ?3`,
		store.ProjectionName, stamp, store.ProjectionSchemaVersion)
	if err != nil {
		return fmt.Errorf("advance projection cursor: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("advance projection cursor: updated %d metadata rows", n)
	}
	return nil
}

// selectList returns a table's column list and the matching SELECT
// expressions, qualified by alias, with per-column overrides and skips.
func selectList(table *store.Table, alias string, overrides map[string]string, skip ...string) (string, string) {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	var cols, exprs []string
	for _, column := range table.Columns {
		skipped := false
		for _, s := range skip {
			if s == column {
				skipped = true
			}
		}
		if skipped {
			continue
		}
		cols = append(cols, quoteIdent(column))
		if expr, ok := overrides[column]; ok {
			exprs = append(exprs, expr)
		} else {
			exprs = append(exprs, prefix+quoteIdent(column))
		}
	}
	return strings.Join(cols, ", "), strings.Join(exprs, ", ")
}

func exec(ctx context.Context, conn *sql.Conn, query string, args ...any) (int64, error) {
	res, err := conn.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
