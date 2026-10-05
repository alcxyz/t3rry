package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

// movedThreads is the SQL set of threads being moved, held in a temp table on
// the planning connection.
const movedThreads = "(SELECT thread_id FROM temp.t3rry_moved)"

// rowCopy selects the rows of one projection table that belong to the moved
// threads. Filters name the source table's own columns.
type rowCopy struct {
	table  string
	filter string
}

// rowCopies are the projection tables copied verbatim. Threads, events,
// provider sessions and scheduled tasks need transforms and are handled
// separately.
var rowCopies = []rowCopy{
	{"orchestration_v2_projection_runs", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_run_attempts", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_nodes", "thread_id IN " + movedThreads},
	// Provider threads are read by app thread or by owning node.
	{"orchestration_v2_projection_provider_threads", "thread_id IN " + movedThreads +
		" OR owner_node_id IN (SELECT node_id FROM src.orchestration_v2_projection_nodes WHERE thread_id IN " + movedThreads + ")"},
	{"orchestration_v2_projection_provider_turns", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_runtime_requests", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_messages", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_plans", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_turn_items", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_checkpoint_scopes", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_checkpoints", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_context_handoffs", "thread_id IN " + movedThreads},
	// The lineage closure puts both ends of a transfer in the moved set.
	{"orchestration_v2_projection_context_transfers", "source_thread_id IN " + movedThreads + " OR target_thread_id IN " + movedThreads},
	{"orchestration_v2_projection_subagents", "thread_id IN " + movedThreads},
	{"orchestration_v2_turn_item_positions", "thread_id IN " + movedThreads},
	{"orchestration_v2_projection_provider_session_bindings", "thread_id IN " + movedThreads},
}

const (
	tableEvents   = "orchestration_events"
	tableThreads  = "orchestration_v2_projection_threads"
	tableSessions = "orchestration_v2_projection_provider_sessions"
	tableTasks    = "scheduled_tasks"
)

type project struct {
	id       string
	root     string
	real     string
	resolved bool
}

type sourceThread struct {
	id        string
	projectID string
	deleted   bool
	archived  bool
	worktree  string
}

type edge struct {
	other string
	kind  string
}

// sessionCopy is a provider session bound to a moved thread.
type sessionCopy struct {
	id string
	// shared sessions are also bound to threads that stay in the source.
	// They are inserted stopped, bound to boundThread, unless the target
	// already has them.
	shared      bool
	inTarget    bool
	boundThread string
}

func analyze(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan) error {
	for _, side := range []struct {
		name     string
		activity instance.Activity
	}{{"source", plan.FromActivity}, {"target", plan.ToActivity}} {
		for _, reason := range side.activity.Reasons {
			plan.Blockers = append(plan.Blockers, side.name+" server appears to be running: "+reason)
		}
		for _, note := range side.activity.Notes {
			plan.Warnings = append(plan.Warnings, side.name+": "+note)
		}
	}

	var err error
	plan.FromSchema, err = store.Check(ctx, conn, "src")
	if err != nil {
		return fmt.Errorf("check source schema: %w", err)
	}
	plan.ToSchema, err = store.Check(ctx, conn, "main")
	if err != nil {
		return fmt.Errorf("check target schema: %w", err)
	}
	for _, side := range []struct {
		name   string
		report store.Report
	}{{"source", plan.FromSchema}, {"target", plan.ToSchema}} {
		for _, problem := range side.report.Problems {
			plan.Blockers = append(plan.Blockers, side.name+" schema: "+problem)
		}
		for _, warning := range side.report.Warnings {
			plan.Warnings = append(plan.Warnings, side.name+" schema: "+warning)
		}
	}
	if !plan.FromSchema.OK() || !plan.ToSchema.OK() {
		return nil
	}
	if plan.FromSchema.Schema != plan.ToSchema.Schema {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf(
			"source schema %d and target schema %d differ; upgrade both servers to the same T3 Code version",
			plan.FromSchema.MaxMigration, plan.ToSchema.MaxMigration))
		return nil
	}
	plan.schema = plan.ToSchema.Schema

	if err := selectProjects(ctx, conn, opts, plan); err != nil {
		return err
	}
	if len(plan.Projects) == 0 {
		return nil
	}
	threads, err := loadThreads(ctx, conn)
	if err != nil {
		return err
	}
	moved, err := closeLineage(ctx, conn, opts, plan, threads)
	if err != nil {
		return err
	}
	if err := fillMovedTable(ctx, conn, plan, threads, moved); err != nil {
		return err
	}
	if err := excludeAlreadyMoved(ctx, conn, opts, plan, threads); err != nil {
		return err
	}
	steps := []func(context.Context, *sql.Conn, Options, *Plan, map[string]*sourceThread) error{
		checkActiveWork,
		checkCollisions,
		countRows,
		planSessions,
		planAttachments,
		planDuplicates,
		planScheduledTasks,
		planSourceCleanup,
		warnLegacyAndWorktrees,
	}
	for _, step := range steps {
		if err := step(ctx, conn, opts, plan, threads); err != nil {
			return err
		}
	}
	return nil
}

func loadProjects(ctx context.Context, conn *sql.Conn, db string) ([]project, error) {
	rows, err := conn.QueryContext(ctx,
		"SELECT project_id, workspace_root FROM "+db+".projection_projects WHERE deleted_at IS NULL ORDER BY workspace_root, project_id")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var projects []project
	for rows.Next() {
		var p project
		if err := rows.Scan(&p.id, &p.root); err != nil {
			return nil, err
		}
		p.real, p.resolved = realPath(p.root)
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

// realPath resolves symlinks so a symlinked workspace root matches its
// physical path. Missing paths fall back to their cleaned form.
func realPath(path string) (string, bool) {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return filepath.Clean(path), false
	}
	abs, err := filepath.Abs(real)
	if err != nil {
		return filepath.Clean(real), true
	}
	return abs, true
}

func selectProjects(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan) error {
	sources, err := loadProjects(ctx, conn, "src")
	if err != nil {
		return err
	}
	targets, err := loadProjects(ctx, conn, "main")
	if err != nil {
		return err
	}

	var selected []project
	if len(opts.Projects) == 0 {
		for _, source := range sources {
			if len(matchingProjects(targets, source.real)) == 0 {
				plan.Unmatched = append(plan.Unmatched, source.root)
				continue
			}
			selected = append(selected, source)
		}
	} else {
		seen := map[string]bool{}
		for _, arg := range opts.Projects {
			expanded, err := instance.ExpandHome(arg)
			if err != nil {
				return err
			}
			abs, err := filepath.Abs(expanded)
			if err != nil {
				return err
			}
			real, _ := realPath(abs)
			matches := matchingProjects(sources, real)
			if len(matches) == 0 {
				return fmt.Errorf("no source project has workspace root %s", arg)
			}
			for _, match := range matches {
				if !seen[match.id] {
					seen[match.id] = true
					selected = append(selected, match)
				}
			}
		}
	}

	for _, source := range selected {
		pp := &ProjectPlan{SourceID: source.id, SourceRoot: source.root, RealPath: source.real}
		if !source.resolved {
			pp.Warnings = append(pp.Warnings, "workspace root does not exist on this host; matched by its recorded path")
		}
		matches := matchingProjects(targets, source.real)
		switch len(matches) {
		case 0:
			pp.AddCommand = "t3 project add " + shellQuote(source.real) + " --base-dir " + shellQuote(opts.To.BaseDir)
			pp.Blockers = append(pp.Blockers, "the target has no project for this workspace; create it with: "+pp.AddCommand)
		case 1:
			pp.TargetID = matches[0].id
			pp.TargetRoot = matches[0].root
		default:
			ids := make([]string, len(matches))
			for i, m := range matches {
				ids[i] = m.id
			}
			pp.Blockers = append(pp.Blockers, "several target projects share this workspace: "+strings.Join(ids, ", "))
		}
		plan.Projects = append(plan.Projects, pp)
	}
	return nil
}

func matchingProjects(projects []project, real string) []project {
	var matches []project
	for _, p := range projects {
		if p.real == real {
			matches = append(matches, p)
		}
	}
	return matches
}

func loadThreads(ctx context.Context, conn *sql.Conn) (map[string]*sourceThread, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT thread_id, project_id, deleted_at IS NOT NULL, archived_at IS NOT NULL,
			COALESCE(json_extract(payload_json, '$.worktreePath'), '')
		FROM src.orchestration_v2_projection_threads`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	threads := map[string]*sourceThread{}
	for rows.Next() {
		t := &sourceThread{}
		if err := rows.Scan(&t.id, &t.projectID, &t.deleted, &t.archived, &t.worktree); err != nil {
			return nil, err
		}
		threads[t.id] = t
	}
	return threads, rows.Err()
}

// loadEdges returns every relationship between source threads that requires
// them to live on the same server.
func loadEdges(ctx context.Context, conn *sql.Conn) (map[string][]edge, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT a, b, kind FROM (
			SELECT thread_id AS a, json_extract(payload_json, '$.lineage.parentThreadId') AS b, 'lineage parent' AS kind
				FROM src.orchestration_v2_projection_threads
			UNION ALL
			SELECT thread_id, json_extract(payload_json, '$.lineage.rootThreadId'), 'lineage root'
				FROM src.orchestration_v2_projection_threads
			UNION ALL
			SELECT thread_id, json_extract(payload_json, '$.forkedFrom.threadId'), 'fork source'
				FROM src.orchestration_v2_projection_threads
			UNION ALL
			SELECT t.thread_id, n.thread_id, 'fork source'
				FROM src.orchestration_v2_projection_threads t
				JOIN src.orchestration_v2_projection_nodes n
					ON n.node_id = json_extract(t.payload_json, '$.forkedFrom.nodeId')
			UNION ALL
			SELECT t.thread_id, COALESCE(p.thread_id, n.thread_id), 'fork source'
				FROM src.orchestration_v2_projection_threads t
				JOIN src.orchestration_v2_projection_provider_threads p
					ON p.provider_thread_id = json_extract(t.payload_json, '$.forkedFrom.providerThreadId')
				LEFT JOIN src.orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
			UNION ALL
			SELECT thread_id, child_thread_id, 'subagent'
				FROM src.orchestration_v2_projection_subagents
			UNION ALL
			SELECT source_thread_id, target_thread_id, 'context transfer'
				FROM src.orchestration_v2_projection_context_transfers
			UNION ALL
			SELECT p.thread_id, n.thread_id, 'provider thread owner'
				FROM src.orchestration_v2_projection_provider_threads p
				JOIN src.orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
		)
		WHERE a IS NOT NULL AND b IS NOT NULL AND a <> b`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	edges := map[string][]edge{}
	for rows.Next() {
		var a, b, kind string
		if err := rows.Scan(&a, &b, &kind); err != nil {
			return nil, err
		}
		edges[a] = append(edges[a], edge{other: b, kind: kind})
		edges[b] = append(edges[b], edge{other: a, kind: kind})
	}
	return edges, rows.Err()
}

// closeLineage selects the seed threads of every selected project and adds
// every thread they are linked to. Links into projects outside the move block
// the owning project.
func closeLineage(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, threads map[string]*sourceThread) (map[string]bool, error) {
	byProject := map[string]*ProjectPlan{}
	for _, pp := range plan.Projects {
		byProject[pp.SourceID] = pp
	}

	moved := map[string]bool{}
	var queue []string
	ids := make([]string, 0, len(threads))
	for id := range threads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := threads[id]
		pp := byProject[t.projectID]
		if pp == nil {
			continue
		}
		if t.deleted && !opts.IncludeDeleted {
			continue
		}
		moved[id] = true
		queue = append(queue, id)
	}

	edges, err := loadEdges(ctx, conn)
	if err != nil {
		return nil, err
	}
	dangling := map[string]bool{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		owner := byProject[threads[id].projectID]
		for _, e := range edges[id] {
			if moved[e.other] {
				continue
			}
			linked, ok := threads[e.other]
			if !ok {
				if !dangling[e.other] {
					dangling[e.other] = true
					owner.Warnings = append(owner.Warnings, fmt.Sprintf(
						"thread %s refers to missing thread %s (%s); the reference stays dangling", id, e.other, e.kind))
				}
				continue
			}
			linkedProject := byProject[linked.projectID]
			if linkedProject == nil {
				owner.Blockers = append(owner.Blockers, fmt.Sprintf(
					"thread %s is linked to thread %s (%s) in project %s, which this move does not include",
					id, e.other, e.kind, linked.projectID))
				continue
			}
			moved[e.other] = true
			linkedProject.LineageAdded++
			queue = append(queue, e.other)
		}
	}
	for _, id := range ids {
		t := threads[id]
		pp := byProject[t.projectID]
		switch {
		case pp == nil:
		case moved[id]:
			pp.Threads++
			if t.deleted {
				pp.DeletedIncluded++
			}
		case t.deleted:
			pp.DeletedSkipped++
		}
	}
	return moved, nil
}

func fillMovedTable(ctx context.Context, conn *sql.Conn, plan *Plan, threads map[string]*sourceThread, moved map[string]bool) error {
	targetOf := map[string]string{}
	for _, pp := range plan.Projects {
		targetOf[pp.SourceID] = pp.TargetID
	}
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS temp.t3rry_moved"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `CREATE TEMP TABLE t3rry_moved (
		thread_id TEXT PRIMARY KEY,
		source_project_id TEXT NOT NULL,
		target_project_id TEXT NOT NULL)`); err != nil {
		return err
	}
	stmt, err := conn.PrepareContext(ctx, "INSERT INTO temp.t3rry_moved VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	ids := make([]string, 0, len(moved))
	for id := range moved {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := threads[id]
		if _, err := stmt.ExecContext(ctx, id, t.projectID, targetOf[t.projectID]); err != nil {
			return err
		}
	}
	plan.threads = ids
	return nil
}

// excludeAlreadyMoved drops threads an earlier run already copied, so a move
// can be repeated. A thread that exists in the target for another reason, or
// that changed in the source since it was copied, blocks the move.
func excludeAlreadyMoved(ctx context.Context, conn *sql.Conn, _ Options, plan *Plan, threads map[string]*sourceThread) error {
	present, err := queryStrings(ctx, conn, `
		SELECT m.thread_id FROM temp.t3rry_moved m
		WHERE EXISTS (SELECT 1 FROM main.orchestration_v2_projection_threads t WHERE t.thread_id = m.thread_id)
			OR EXISTS (SELECT 1 FROM main.orchestration_events e
				WHERE e.aggregate_kind = 'thread' AND e.stream_id = m.thread_id)
		ORDER BY m.thread_id`)
	if err != nil {
		return err
	}
	byProject := projectIndex(plan)
	var remaining []string
	drop := map[string]bool{}
	blocked := map[string]bool{}
	for _, id := range present {
		pp := byProject[threads[id].projectID]
		var sameOrigin, diverged, original bool
		err := conn.QueryRowContext(ctx, `
			SELECT
				EXISTS (SELECT 1 FROM src.orchestration_events s
					JOIN main.orchestration_events e ON e.event_id = s.event_id
					WHERE s.application_event_version = 2 AND s.aggregate_kind = 'thread'
						AND s.stream_id = ?1 AND s.event_type = 'thread.created'),
				EXISTS (SELECT 1 FROM src.orchestration_events s
					WHERE s.application_event_version = 2 AND s.aggregate_kind = 'thread' AND s.stream_id = ?1
						AND (s.command_id IS NULL OR s.command_id NOT LIKE ?2)
						AND NOT EXISTS (SELECT 1 FROM main.orchestration_events e WHERE e.event_id = s.event_id)),
				EXISTS (SELECT 1 FROM main.orchestration_events e
					WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread' AND e.stream_id = ?1
						AND `+markerCondition("e")+`)`,
			id, ownCommandPattern).Scan(&sameOrigin, &diverged, &original)
		if err != nil {
			return err
		}
		switch {
		case !sameOrigin:
			blocked[id] = true
			pp.Blockers = append(pp.Blockers, fmt.Sprintf("thread %s already exists in the target", id))
		case original:
			// t3rry archived the target copy as the source of an earlier
			// move: this run would move the thread back onto its original.
			blocked[id] = true
			pp.Blockers = append(pp.Blockers, fmt.Sprintf(
				"thread %s was moved out of the target earlier and its original is archived there; "+
					"unarchive it in the target's T3 Code and archive this copy instead of moving it back", id))
		case diverged:
			blocked[id] = true
			pp.Blockers = append(pp.Blockers, fmt.Sprintf(
				"thread %s was moved before but has changed in the source since", id))
		default:
			drop[id] = true
			pp.AlreadyMoved++
			pp.Threads--
		}
	}
	// Present threads leave the working set: already moved ones need only
	// source cleanup, and blocked ones already explain why.
	for _, id := range plan.threads {
		if drop[id] || blocked[id] {
			if _, err := conn.ExecContext(ctx, "DELETE FROM temp.t3rry_moved WHERE thread_id = ?", id); err != nil {
				return err
			}
		} else {
			remaining = append(remaining, id)
		}
		// Every selected thread that is or will be in the target gets its
		// source cleanup, so a retry after a failed cleanup finishes it.
		if !blocked[id] {
			plan.selected = append(plan.selected, id)
		}
	}
	plan.threads = remaining
	return fillSelectedTable(ctx, conn, plan, threads)
}

// fillSelectedTable records every selected thread, including those an
// earlier run already copied, so the target cleanup of their imports also
// finishes on a rerun.
func fillSelectedTable(ctx context.Context, conn *sql.Conn, plan *Plan, threads map[string]*sourceThread) error {
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS temp.t3rry_selected"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `CREATE TEMP TABLE t3rry_selected (
		thread_id TEXT PRIMARY KEY,
		source_project_id TEXT NOT NULL)`); err != nil {
		return err
	}
	stmt, err := conn.PrepareContext(ctx, "INSERT INTO temp.t3rry_selected VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, id := range plan.selected {
		if _, err := stmt.ExecContext(ctx, id, threads[id].projectID); err != nil {
			return err
		}
	}
	return nil
}

// planSourceCleanup counts the source cleanup still due for selected threads,
// including threads an earlier run already copied: threads without a
// source-archive marker, enabled scheduled tasks and live session bindings.
// Without archiving, enabled tasks would run on both servers.
func planSourceCleanup(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, threads map[string]*sourceThread) error {
	if len(plan.selected) == 0 {
		return nil
	}
	ids, err := json.Marshal(plan.selected)
	if err != nil {
		return err
	}
	// Tasks of deleted threads stay as they are in the source; the target
	// receives them disabled.
	rows, err := conn.QueryContext(ctx, `
		SELECT s.task_id, s.thread_id FROM src.scheduled_tasks s
		JOIN src.`+tableThreads+` t ON t.thread_id = s.thread_id
		WHERE s.enabled <> 0 AND t.deleted_at IS NULL AND s.thread_id IN (SELECT value FROM json_each(?))
		ORDER BY s.task_id`, string(ids))
	if err != nil {
		return err
	}
	byProject := projectIndex(plan)
	for rows.Next() {
		var taskID, threadID string
		if err := rows.Scan(&taskID, &threadID); err != nil {
			_ = rows.Close()
			return err
		}
		plan.sourceTasks++
		if !opts.ArchiveSource {
			pp := byProject[threads[threadID].projectID]
			pp.Blockers = append(pp.Blockers, fmt.Sprintf(
				"scheduled task %s would run on both servers; it is disabled in the source only when the source is archived", taskID))
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}

	// Source cleanup works from each thread's current state: it archives
	// every open thread, even one marked by an earlier move, and marks every
	// thread without a marker, deleted ones included. The marker lets a
	// later move in the opposite direction recognise the original.
	if opts.ArchiveSource {
		rows, err := conn.QueryContext(ctx, `
			SELECT t.thread_id, t.project_id FROM src.orchestration_v2_projection_threads t
			WHERE t.thread_id IN (SELECT value FROM json_each(?1))
				AND ((t.archived_at IS NULL AND t.deleted_at IS NULL)
					OR NOT EXISTS (SELECT 1 FROM src.orchestration_events e
						WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread'
							AND e.stream_id = t.thread_id AND `+markerCondition("e")+`))
			ORDER BY t.thread_id`, string(ids))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, projectID string
			if err := rows.Scan(&id, &projectID); err != nil {
				_ = rows.Close()
				return err
			}
			plan.archive = append(plan.archive, id)
			if pp := byProject[projectID]; pp != nil {
				pp.Archive++
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}

	return conn.QueryRowContext(ctx, `
		SELECT count(*) FROM src.orchestration_v2_projection_provider_session_bindings b
		JOIN src.orchestration_v2_projection_provider_sessions s ON s.provider_session_id = b.provider_session_id
		WHERE b.thread_id IN (SELECT value FROM json_each(?)) AND s.status NOT IN ('stopped', 'error')`,
		string(ids)).Scan(&plan.sourceSessions)
}

// checkActiveWork blocks threads the source server is still driving.
func checkActiveWork(ctx context.Context, conn *sql.Conn, _ Options, plan *Plan, threads map[string]*sourceThread) error {
	checks := []struct {
		query   string
		message string
	}{
		{`SELECT thread_id, count(*) FROM src.orchestration_v2_projection_runs
			WHERE thread_id IN ` + movedThreads + ` AND status IN ` + sqlList(activeRunStatuses) + `
			GROUP BY thread_id ORDER BY thread_id`,
			"thread %s has %d run(s) that are not finished"},
		{`SELECT COALESCE(n.thread_id, p.thread_id), count(*)
			FROM src.orchestration_v2_projection_provider_threads p
			LEFT JOIN src.orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
			WHERE (p.thread_id IN ` + movedThreads + ` OR n.thread_id IN ` + movedThreads + `)
				AND (p.status = 'active' OR (json_valid(p.payload_json)
					AND json_array_length(p.payload_json, '$.pendingBackgroundTasks') > 0))
			GROUP BY 1 ORDER BY 1`,
			"thread %s has %d provider thread(s) that are active or have background tasks"},
		{`SELECT thread_id, count(*) FROM src.orchestration_v2_projection_runtime_requests
			WHERE thread_id IN ` + movedThreads + ` AND status = 'pending'
			GROUP BY thread_id ORDER BY thread_id`,
			"thread %s has %d pending approval or input request(s)"},
		{`SELECT thread_id, count(*) FROM src.orchestration_v2_effect_outbox
			WHERE thread_id IN ` + movedThreads + ` AND status IN ('pending', 'running')
			GROUP BY thread_id ORDER BY thread_id`,
			"thread %s has %d queued server effect(s)"},
		{`SELECT thread_id, 1 FROM src.orchestration_v2_legacy_imports
			WHERE thread_id IN ` + movedThreads + ` AND transcript_imported_at IS NULL
			ORDER BY thread_id`,
			"thread %[1]s still keeps its transcript in legacy tables; open it once in the source server first"},
		{`SELECT m.thread_id, 0 FROM temp.t3rry_moved m
			WHERE NOT EXISTS (SELECT 1 FROM src.orchestration_events e
				WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread'
					AND e.stream_id = m.thread_id AND e.event_type = 'thread.created')
			ORDER BY m.thread_id`,
			"thread %[1]s has no thread.created event and would fail the target's projection check"},
	}
	byProject := projectIndex(plan)
	for _, check := range checks {
		rows, err := conn.QueryContext(ctx, check.query)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			var count int
			if err := rows.Scan(&id, &count); err != nil {
				_ = rows.Close()
				return err
			}
			if t := threads[id]; t != nil {
				pp := byProject[t.projectID]
				pp.Blockers = append(pp.Blockers, fmt.Sprintf(check.message, id, count))
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}

	warnings := []struct {
		query   string
		message string
	}{
		{`SELECT thread_id, count(*) FROM src.orchestration_v2_projection_turn_items
			WHERE thread_id IN ` + movedThreads + `
				AND type IN ('command_execution', 'dynamic_tool', 'subagent')
				AND status IN ('pending', 'running', 'waiting')
			GROUP BY thread_id ORDER BY thread_id`,
			"thread %s has %d unfinished tool item(s); the target server will recover them on start"},
		{`SELECT b.thread_id, count(*) FROM src.orchestration_v2_projection_provider_session_bindings b
			JOIN src.orchestration_v2_projection_provider_sessions s ON s.provider_session_id = b.provider_session_id
			WHERE b.thread_id IN ` + movedThreads + ` AND s.status NOT IN ('stopped', 'error')
			GROUP BY b.thread_id ORDER BY b.thread_id`,
			"thread %s is bound to %d provider session(s) not marked stopped; the target server will recover them on start"},
	}
	for _, warning := range warnings {
		rows, err := conn.QueryContext(ctx, warning.query)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			var count int
			if err := rows.Scan(&id, &count); err != nil {
				_ = rows.Close()
				return err
			}
			if t := threads[id]; t != nil {
				pp := byProject[t.projectID]
				pp.Warnings = append(pp.Warnings, fmt.Sprintf(warning.message, id, count))
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

// checkCollisions blocks rows whose keys already exist in the target.
func checkCollisions(ctx context.Context, conn *sql.Conn, _ Options, plan *Plan, _ map[string]*sourceThread) error {
	var count int64
	err := conn.QueryRowContext(ctx, `
		SELECT count(*) FROM src.orchestration_events s
		JOIN temp.t3rry_moved m ON m.thread_id = s.stream_id
		WHERE s.application_event_version = 2 AND s.aggregate_kind = 'thread'
			AND EXISTS (SELECT 1 FROM main.orchestration_events e WHERE e.event_id = s.event_id)`).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf("%d event id(s) already exist in the target", count))
	}
	for _, rc := range rowCopies {
		table := plan.schema.Table(rc.table)
		if table == nil || len(table.PrimaryKey) == 0 {
			return fmt.Errorf("supported schema lacks a primary key for %s", rc.table)
		}
		var match []string
		for _, column := range table.PrimaryKey {
			match = append(match, "t."+quoteIdent(column)+" = s."+quoteIdent(column))
		}
		query := "SELECT count(*) FROM src." + rc.table + " s WHERE (" + rc.filter + ") AND EXISTS (SELECT 1 FROM main." +
			rc.table + " t WHERE " + strings.Join(match, " AND ") + ")"
		if err := conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("check %s collisions: %w", rc.table, err)
		}
		if count > 0 {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("%d row(s) of %s already exist in the target", count, rc.table))
		}
	}
	return nil
}

// countRows records how many rows each table should receive, so the copy can
// verify itself, and the events per project for the report.
func countRows(ctx context.Context, conn *sql.Conn, _ Options, plan *Plan, _ map[string]*sourceThread) error {
	plan.counts[tableThreads] = int64(len(plan.threads))
	rows, err := conn.QueryContext(ctx, `
		SELECT m.source_project_id, count(*) FROM src.orchestration_events e
		JOIN temp.t3rry_moved m ON m.thread_id = e.stream_id
		WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread'
		GROUP BY m.source_project_id`)
	if err != nil {
		return err
	}
	byProject := projectIndex(plan)
	for rows.Next() {
		var projectID string
		var count int64
		if err := rows.Scan(&projectID, &count); err != nil {
			_ = rows.Close()
			return err
		}
		if pp := byProject[projectID]; pp != nil {
			pp.Events += count
		}
		plan.counts[tableEvents] += count
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, rc := range rowCopies {
		var count int64
		if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM src."+rc.table+" WHERE "+rc.filter).Scan(&count); err != nil {
			return fmt.Errorf("count %s: %w", rc.table, err)
		}
		plan.counts[rc.table] = count
	}
	return nil
}

// planSessions classifies the provider sessions bound to moved threads.
func planSessions(ctx context.Context, conn *sql.Conn, _ Options, plan *Plan, _ map[string]*sourceThread) error {
	rows, err := conn.QueryContext(ctx, `
		SELECT s.provider_session_id,
			EXISTS (SELECT 1 FROM src.orchestration_v2_projection_provider_session_bindings b
				WHERE b.provider_session_id = s.provider_session_id AND b.thread_id NOT IN `+movedThreads+`)
				OR (s.thread_id IS NOT NULL AND s.thread_id NOT IN `+movedThreads+`),
			EXISTS (SELECT 1 FROM main.orchestration_v2_projection_provider_sessions t
				WHERE t.provider_session_id = s.provider_session_id),
			EXISTS (SELECT 1 FROM main.orchestration_v2_projection_provider_sessions t
				WHERE t.provider_session_id = s.provider_session_id AND t.status = 'stopped'
					AND t.provider = s.provider AND t.provider_instance_id IS s.provider_instance_id),
			COALESCE((SELECT MIN(b.thread_id) FROM src.orchestration_v2_projection_provider_session_bindings b
				WHERE b.provider_session_id = s.provider_session_id AND b.thread_id IN `+movedThreads+`), s.thread_id)
		FROM src.orchestration_v2_projection_provider_sessions s
		WHERE s.provider_session_id IN (SELECT provider_session_id
				FROM src.orchestration_v2_projection_provider_session_bindings WHERE thread_id IN `+movedThreads+`)
			OR s.thread_id IN `+movedThreads+`
		ORDER BY s.provider_session_id`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	collisions := 0
	for rows.Next() {
		var s sessionCopy
		var stoppedCopy bool
		if err := rows.Scan(&s.id, &s.shared, &s.inTarget, &stoppedCopy, &s.boundThread); err != nil {
			return err
		}
		// A stopped copy of the same session is what an earlier move of a
		// thread sharing it inserted; only the bindings are missing.
		if !s.shared && s.inTarget && !stoppedCopy {
			collisions++
		}
		plan.sessions = append(plan.sessions, s)
		if !s.inTarget {
			plan.counts[tableSessions]++
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if collisions > 0 {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf(
			"%d provider session(s) owned by moved threads already exist in the target", collisions))
	}
	return nil
}

// planDuplicates finds target threads that "import recent sessions" created
// for the provider sessions of selected threads and, per ADR-002, for the
// Codex subagent sessions those sessions spawned.
func planDuplicates(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, _ map[string]*sourceThread) error {
	rows, err := conn.QueryContext(ctx, `
		SELECT p.provider_instance_id, json_extract(p.payload_json, '$.nativeThreadRef.driver'),
			json_extract(p.payload_json, '$.nativeThreadRef.nativeId'), m.source_project_id
		FROM src.orchestration_v2_projection_provider_threads p
		LEFT JOIN src.orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
		JOIN temp.t3rry_selected m ON m.thread_id = COALESCE(p.thread_id, n.thread_id)
		WHERE p.provider_instance_id IS NOT NULL AND json_valid(p.payload_json)
			AND json_extract(p.payload_json, '$.nativeThreadRef.nativeId') IS NOT NULL
		UNION
		SELECT COALESCE(r.provider_instance_id, r.provider_name), r.provider_name,
			CASE WHEN r.provider_name = 'codex'
				THEN json_extract(r.resume_cursor_json, '$.threadId')
				ELSE json_extract(r.resume_cursor_json, '$.resume') END,
			m.source_project_id
		FROM src.provider_session_runtime r
		JOIN temp.t3rry_selected m ON m.thread_id = r.thread_id
		WHERE json_valid(r.resume_cursor_json)
		ORDER BY 1, 3, 4`)
	if err != nil {
		return err
	}
	// sessionProject maps the import id of every session a selected thread
	// owns to the thread's source project; codexOwned does the same for the
	// Codex session ids alone.
	sessionProject := map[string]string{}
	codexOwned := map[string]string{}
	for rows.Next() {
		var instanceID, driver, nativeID sql.NullString
		var projectID string
		if err := rows.Scan(&instanceID, &driver, &nativeID, &projectID); err != nil {
			_ = rows.Close()
			return err
		}
		if !instanceID.Valid || !nativeID.Valid || nativeID.String == "" {
			continue
		}
		id := "import:" + instanceID.String + ":" + nativeID.String
		if _, ok := sessionProject[id]; !ok {
			sessionProject[id] = projectID
		}
		if driver.String == "codex" {
			if _, ok := codexOwned[nativeID.String]; !ok {
				codexOwned[nativeID.String] = projectID
			}
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}

	byProject := projectIndex(plan)
	duplicates, err := queryStrings(ctx, conn, `
		SELECT t.thread_id FROM main.orchestration_v2_projection_threads t
		WHERE t.deleted_at IS NULL AND t.thread_id IN (SELECT value FROM json_each(?1))
			AND t.thread_id NOT IN (SELECT thread_id FROM temp.t3rry_selected)
		ORDER BY t.thread_id`, jsonKeys(sessionProject))
	if err != nil {
		return err
	}
	for _, id := range duplicates {
		pp := byProject[sessionProject[id]]
		keep, err := importHasActivity(ctx, conn, id)
		if err != nil {
			return err
		}
		switch {
		case keep:
			pp.Warnings = append(pp.Warnings, fmt.Sprintf(
				"target thread %s duplicates a moved provider session but has its own activity; it is kept", id))
		case opts.KeepDuplicates:
			pp.Warnings = append(pp.Warnings, fmt.Sprintf(
				"target thread %s duplicates a moved provider session; kept because of --keep-duplicates", id))
		default:
			plan.duplicates = append(plan.duplicates, id)
			pp.Duplicates++
		}
	}
	return planSubagentImports(ctx, conn, opts, plan, codexOwned, duplicates)
}

// planSubagentImports finds target imports of Codex sessions that a moved
// session spawned as a subagent, directly or through other subagents. Codex
// records the parent only in the session_meta line of the child's rollout.
func planSubagentImports(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, codexOwned map[string]string, duplicates []string) error {
	if len(codexOwned) == 0 || opts.CodexHome == "" {
		return nil
	}
	var targets []string
	for _, pp := range plan.Projects {
		if pp.TargetID != "" {
			targets = append(targets, pp.TargetID)
		}
	}
	targetJSON, err := json.Marshal(targets)
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `
		SELECT t.thread_id, COALESCE(t.provider_instance_id, '') FROM main.orchestration_v2_projection_threads t
		WHERE t.deleted_at IS NULL AND t.default_provider = 'codex' AND t.thread_id LIKE 'import:%'
			AND t.project_id IN (SELECT value FROM json_each(?1))
			AND t.thread_id NOT IN (SELECT thread_id FROM temp.t3rry_selected)
		ORDER BY t.thread_id`, string(targetJSON))
	if err != nil {
		return err
	}
	skip := map[string]bool{}
	for _, id := range duplicates {
		skip[id] = true
	}
	// threadOf maps a candidate's Codex session id to its target thread.
	threadOf := map[string]string{}
	for rows.Next() {
		var id, instanceID string
		if err := rows.Scan(&id, &instanceID); err != nil {
			_ = rows.Close()
			return err
		}
		prefix := "import:" + instanceID + ":"
		if skip[id] || instanceID == "" || !strings.HasPrefix(id, prefix) {
			continue
		}
		threadOf[strings.TrimPrefix(id, prefix)] = id
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(threadOf) == 0 {
		return nil
	}
	if _, err := os.Stat(opts.CodexHome); err != nil {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"Codex home %s is not readable (%v); imported subagent sessions were not checked", opts.CodexHome, err))
		return nil
	}

	ids := map[string]bool{}
	for nativeID := range threadOf {
		ids[nativeID] = true
	}
	parents, missing, err := codexParents(opts.CodexHome, ids)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"%d imported Codex thread(s) in the target have no rollout under %s and were not checked", len(missing), opts.CodexHome))
	}

	// Resolve each candidate to the moved session at the root of its spawn
	// chain, if any. Chains are short; the depth bound guards against cycles.
	spawned := map[string]string{}
	for nativeID := range threadOf {
		current := nativeID
		for depth := 0; depth <= len(parents); depth++ {
			parent, ok := parents[current]
			if !ok {
				break
			}
			if projectID, owned := codexOwned[parent]; owned {
				spawned[nativeID] = projectID
				break
			}
			current = parent
		}
	}

	byProject := projectIndex(plan)
	found := make([]string, 0, len(spawned))
	for nativeID := range spawned {
		found = append(found, nativeID)
	}
	sort.Strings(found)
	for _, nativeID := range found {
		id := threadOf[nativeID]
		pp := byProject[spawned[nativeID]]
		keep, err := importHasActivity(ctx, conn, id)
		if err != nil {
			return err
		}
		switch {
		case keep:
			pp.Warnings = append(pp.Warnings, fmt.Sprintf(
				"target thread %s imports a subagent session of a moved thread but has its own activity; it is kept", id))
		case opts.KeepSubagentImports:
			pp.Warnings = append(pp.Warnings, fmt.Sprintf(
				"target thread %s imports a subagent session of a moved thread; kept because of --keep-subagent-imports", id))
		default:
			plan.subagentImports = append(plan.subagentImports, id)
			pp.SubagentImports++
		}
	}
	return nil
}

// importHasActivity reports whether a target thread is more than an untouched
// import: it was not imported, has runs of its own, or holds a live session.
func importHasActivity(ctx context.Context, conn *sql.Conn, id string) (bool, error) {
	var origin string
	var runs, liveSessions int
	err := conn.QueryRowContext(ctx, `
		SELECT COALESCE(json_extract(t.payload_json, '$.historyOrigin'), ''),
			(SELECT count(*) FROM main.orchestration_v2_projection_runs r WHERE r.thread_id = t.thread_id),
			(SELECT count(*) FROM main.orchestration_v2_projection_provider_session_bindings b
				JOIN main.orchestration_v2_projection_provider_sessions s ON s.provider_session_id = b.provider_session_id
				WHERE b.thread_id = t.thread_id AND s.status NOT IN ('stopped', 'error'))
		FROM main.orchestration_v2_projection_threads t WHERE t.thread_id = ?`, id).Scan(&origin, &runs, &liveSessions)
	if err != nil {
		return false, err
	}
	return origin != "v1_import" || runs > 0 || liveSessions > 0, nil
}

// jsonKeys returns the keys of m as a JSON array for json_each.
func jsonKeys(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	data, err := json.Marshal(keys)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// planScheduledTasks moves scheduled tasks bound to moved threads and warns
// about project-level tasks, which stay in the source.
func planScheduledTasks(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, _ map[string]*sourceThread) error {
	rows, err := conn.QueryContext(ctx, `
		SELECT s.task_id, m.source_project_id,
			EXISTS (SELECT 1 FROM main.scheduled_tasks t WHERE t.task_id = s.task_id)
		FROM src.scheduled_tasks s
		JOIN temp.t3rry_moved m ON m.thread_id = s.thread_id
		ORDER BY s.task_id`)
	if err != nil {
		return err
	}
	byProject := projectIndex(plan)
	for rows.Next() {
		var id, projectID string
		var exists bool
		if err := rows.Scan(&id, &projectID, &exists); err != nil {
			_ = rows.Close()
			return err
		}
		pp := byProject[projectID]
		pp.ScheduledTasks++
		plan.counts[tableTasks]++
		if exists {
			pp.Blockers = append(pp.Blockers, fmt.Sprintf("scheduled task %s already exists in the target", id))
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}

	rows, err = conn.QueryContext(ctx, `
		SELECT project_id, count(*) FROM src.scheduled_tasks
		WHERE project_id IN (SELECT source_project_id FROM temp.t3rry_moved)
			AND (thread_id IS NULL OR thread_id NOT IN `+movedThreads+`)
		GROUP BY project_id`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var projectID string
		var count int
		if err := rows.Scan(&projectID, &count); err != nil {
			return err
		}
		if pp := byProject[projectID]; pp != nil {
			pp.Warnings = append(pp.Warnings, fmt.Sprintf(
				"%d scheduled task(s) of this project are not bound to a moved thread and stay in the source", count))
		}
	}
	return rows.Err()
}

func warnLegacyAndWorktrees(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, threads map[string]*sourceThread) error {
	byProject := projectIndex(plan)
	for _, pp := range plan.Projects {
		var count int
		err := conn.QueryRowContext(ctx, `
			SELECT count(*) FROM src.projection_threads t
			WHERE t.project_id = ? AND t.deleted_at IS NULL
				AND NOT EXISTS (SELECT 1 FROM src.orchestration_events e
					WHERE e.application_event_version = 2 AND e.aggregate_kind = 'thread'
						AND e.stream_id = t.thread_id AND e.event_type = 'thread.created')`,
			pp.SourceID).Scan(&count)
		if err != nil {
			return err
		}
		if count > 0 {
			pp.Warnings = append(pp.Warnings, fmt.Sprintf(
				"%d legacy thread(s) were never imported into the source's current storage and are not moved", count))
		}
	}

	worktrees := filepath.Join(opts.From.BaseDir, "worktrees") + string(filepath.Separator)
	counts := map[string]int{}
	for _, id := range plan.threads {
		t := threads[id]
		if t.worktree != "" && strings.HasPrefix(filepath.Clean(t.worktree)+string(filepath.Separator), worktrees) {
			counts[t.projectID]++
		}
	}
	for projectID, count := range counts {
		byProject[projectID].Warnings = append(byProject[projectID].Warnings, fmt.Sprintf(
			"%d moved thread(s) use worktrees under the source base directory; they stay there and the source server's worktree cleanup may remove them",
			count))
	}
	return nil
}

func projectIndex(plan *Plan) map[string]*ProjectPlan {
	index := map[string]*ProjectPlan{}
	for _, pp := range plan.Projects {
		index[pp.SourceID] = pp
	}
	return index
}

func queryStrings(ctx context.Context, conn *sql.Conn, query string, args ...any) ([]string, error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, rows.Err()
}
