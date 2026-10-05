package move

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPlanSelectsProjectByRealPath(t *testing.T) {
	f := newFixture(t)
	plan := analyzeOK(t, f.options())
	if plan.Blocked() {
		t.Fatalf("plan is blocked: %+v %+v", plan.Blockers, plan.Projects[0].Blockers)
	}
	if len(plan.Projects) != 1 {
		t.Fatalf("got %d projects, want 1", len(plan.Projects))
	}
	pp := plan.Projects[0]
	if pp.SourceID != "p-src" || pp.TargetID != "p-dst" {
		t.Fatalf("matched %s -> %s, want p-src -> p-dst", pp.SourceID, pp.TargetID)
	}
	if pp.Threads != 2 || pp.DeletedSkipped != 1 || pp.Duplicates != 1 || pp.Attachments != 1 || pp.ScheduledTasks != 1 {
		t.Fatalf("unexpected plan %+v", pp)
	}
	if pp.Events != 5 {
		t.Fatalf("events = %d, want 5", pp.Events)
	}
	if len(plan.Unmatched) != 1 {
		t.Fatalf("unmatched = %v, want the other project", plan.Unmatched)
	}

	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"threads  2 (1 deleted skipped)", "duplicates to soft-delete 1", "result: ready to move 2 thread(s)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("plan output lacks %q:\n%s", want, out.String())
		}
	}
}

func TestMoveCopiesRemapsAndArchives(t *testing.T) {
	f := newFixture(t)
	targetEventsBefore := f.dst.count("SELECT MAX(sequence) FROM orchestration_events")
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	opts := f.options()
	opts.Now = func() time.Time { return now }
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")

	plan, result, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if plan.ThreadCount() != 2 || result.SourceError != nil {
		t.Fatalf("threads = %d, source error = %v", plan.ThreadCount(), result.SourceError)
	}
	dst, src := f.dst, f.src
	stamp := timestamp(now)

	// Threads land in the target project, in both the row and its payload.
	if got := dst.count("SELECT count(*) FROM orchestration_v2_projection_threads WHERE project_id = 'p-dst' AND thread_id IN ('thread-a', 'thread-b')"); got != 2 {
		t.Fatalf("moved thread rows in p-dst = %d, want 2", got)
	}
	if got := dst.str("SELECT json_extract(payload_json, '$.projectId') FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-a'"); got != "p-dst" {
		t.Fatalf("thread payload projectId = %q", got)
	}
	if got := dst.count("SELECT count(*) FROM orchestration_events WHERE stream_id = 'thread-a' AND event_type LIKE 'thread.%' AND json_extract(payload_json, '$.projectId') <> 'p-dst'"); got != 0 {
		t.Fatalf("%d thread events still carry the source project", got)
	}
	// Non-snapshot events are untouched.
	if got := dst.str("SELECT payload_json FROM orchestration_events WHERE stream_id = 'thread-a' AND event_type = 'message.updated'"); got != src.str("SELECT payload_json FROM orchestration_events WHERE stream_id = 'thread-a' AND event_type = 'message.updated'") {
		t.Fatalf("message event payload changed: %s", got)
	}
	// Events are appended after the target's own, in source order, keeping ids and stream versions.
	if got := dst.count("SELECT MIN(sequence) FROM orchestration_events WHERE stream_id IN ('thread-a', 'thread-b')"); got <= targetEventsBefore {
		t.Fatalf("moved events start at %d, not after %d", got, targetEventsBefore)
	}
	if got := dst.str("SELECT group_concat(event_type || ':' || stream_version, ',') FROM (SELECT * FROM orchestration_events WHERE stream_id = 'thread-a' ORDER BY sequence)"); got != "thread.created:0,thread.visited:1,message.updated:2" {
		t.Fatalf("thread-a events = %s", got)
	}
	// The projection cursor follows the newest thread event.
	if got, want := dst.count("SELECT last_sequence FROM orchestration_v2_projection_metadata"), dst.count("SELECT MAX(sequence) FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'"); got != want {
		t.Fatalf("target cursor = %d, newest thread event = %d", got, want)
	}
	// Related rows are copied.
	for query, want := range map[string]int{
		"SELECT count(*) FROM orchestration_v2_projection_runs WHERE thread_id = 'thread-a'":                      1,
		"SELECT count(*) FROM orchestration_v2_projection_messages WHERE thread_id = 'thread-a'":                  1,
		"SELECT count(*) FROM orchestration_v2_projection_subagents WHERE child_thread_id = 'thread-b'":           1,
		"SELECT count(*) FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = 'pt-a'":     1,
		"SELECT count(*) FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = 'thread-a'": 2,
		"SELECT count(*) FROM orchestration_v2_projection_threads WHERE thread_id IN ('thread-d', 'thread-x')":    0,
	} {
		if got := dst.count(query); got != want {
			t.Errorf("%s = %d, want %d", query, got, want)
		}
	}
	// The shared session is added stopped and bound to a moved thread; the owned one is verbatim.
	if got := dst.str("SELECT thread_id || ':' || status || ':' || json_extract(payload_json, '$.status') FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id LIKE '%:shared'"); got != "thread-a:stopped:stopped" {
		t.Fatalf("shared session = %s", got)
	}
	if got := dst.str("SELECT status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id = 'session-a'"); got != "stopped" {
		t.Fatalf("owned session status = %s", got)
	}
	// Scheduled task follows its thread into the target project and is disabled in the source.
	if got := dst.str("SELECT project_id || ':' || enabled FROM scheduled_tasks WHERE task_id = 'task-a'"); got != "p-dst:1" {
		t.Fatalf("target task = %s", got)
	}
	if got := src.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-a'"); got != 0 {
		t.Fatalf("source task still enabled")
	}

	// The import duplicate is soft-deleted with a full snapshot event.
	dup := "import:codex:native-1"
	if got := dst.str("SELECT deleted_at FROM orchestration_v2_projection_threads WHERE thread_id = ?", dup); got != stamp {
		t.Fatalf("duplicate deleted_at = %q, want %q", got, stamp)
	}
	if got := dst.str("SELECT json_extract(payload_json, '$.deletedAt') || '|' || json_extract(payload_json, '$.updatedAt') || '|' || json_extract(payload_json, '$.title') || '|' || json_type(payload_json, '$.titleRegeneration') FROM orchestration_events WHERE stream_id = ? AND event_type = 'thread.deleted'", dup); got != stamp+"|"+stamp+"|Thread "+dup+"|null" {
		t.Fatalf("thread.deleted payload = %s", got)
	}
	if got := dst.str("SELECT metadata_json FROM orchestration_events WHERE stream_id = ? AND event_type = 'thread.deleted'", dup); got != `{"providerInstanceId":"codex"}` {
		t.Fatalf("thread.deleted metadata = %s", got)
	}
	if got := dst.count("SELECT stream_version FROM orchestration_events WHERE stream_id = ? AND event_type = 'thread.deleted'", dup); got != 2 {
		t.Fatalf("thread.deleted stream_version = %d, want 2", got)
	}

	// Attachments are copied byte for byte.
	data, err := os.ReadFile(filepath.Join(dst.inst.AttachmentsDir, "thread-a-11111111-2222-4333-8444-555555555555.png"))
	if err != nil || string(data) != "png" {
		t.Fatalf("attachment copy: %q, %v", data, err)
	}

	// The source keeps the threads but archives them, and its cursor advances.
	for _, id := range []string{"thread-a", "thread-b"} {
		if got := src.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = ?", id); got != stamp {
			t.Fatalf("%s archived_at = %q", id, got)
		}
		if got := src.str("SELECT json_extract(payload_json, '$.archivedAt') FROM orchestration_events WHERE stream_id = ? AND event_type = 'thread.archived'", id); got != stamp {
			t.Fatalf("%s thread.archived payload archivedAt = %q", id, got)
		}
	}
	if got := src.str("SELECT json_extract(payload_json, '$.reason') FROM orchestration_events WHERE stream_id = 'thread-a' AND event_type = 'provider-session.detached'"); got != "Thread archived." {
		t.Fatalf("live shared session was not detached from the archived thread: %q", got)
	}
	if got := src.count("SELECT count(*) FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = 'thread-a' AND provider_session_id LIKE '%:shared'"); got != 0 {
		t.Fatalf("source binding of the detached session remains")
	}
	if got, want := src.count("SELECT last_sequence FROM orchestration_v2_projection_metadata"), src.count("SELECT MAX(sequence) FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'"); got != want {
		t.Fatalf("source cursor = %d, newest thread event = %d", got, want)
	}
	if got := src.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-x'"); got != "" {
		t.Fatalf("unrelated source thread was archived")
	}

	// The backup holds the pre-move target.
	if _, err := os.Stat(filepath.Join(opts.BackupDir, "statev2.sqlite")); err != nil {
		t.Fatalf("backup database: %v", err)
	}

	// A second run finds the threads already moved and changes nothing.
	again := analyzeOK(t, f.options())
	if again.Blocked() || again.ThreadCount() != 0 || again.Projects[0].AlreadyMoved != 2 {
		t.Fatalf("rerun: blocked=%v threads=%d project=%+v", again.Blocked(), again.ThreadCount(), again.Projects[0])
	}
}

func TestMoveWithoutArchiveKeepsSource(t *testing.T) {
	f := newFixture(t)
	f.src.exec("UPDATE scheduled_tasks SET enabled = 0")
	opts := f.options()
	opts.ArchiveSource = false
	opts.KeepDuplicates = true
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	before := f.src.count("SELECT count(*) FROM orchestration_events")
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	if got := f.src.count("SELECT count(*) FROM orchestration_events"); got != before {
		t.Fatalf("source events changed: %d -> %d", before, got)
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_v2_projection_threads WHERE deleted_at IS NOT NULL"); got != 0 {
		t.Fatalf("duplicate was deleted despite --keep-duplicates")
	}
}

func TestBlockers(t *testing.T) {
	tests := []struct {
		name  string
		setup func(f *fixture)
		opts  func(f *fixture, o *Options)
		want  string
	}{
		{
			name:  "running run",
			setup: func(f *fixture) { f.src.run("run-a2", "thread-a", 2, "running"); f.src.syncCursor() },
			want:  "thread thread-a has 1 run(s) that are not finished",
		},
		{
			name: "pending runtime request",
			setup: func(f *fixture) {
				f.src.exec(`INSERT INTO orchestration_v2_projection_runtime_requests (runtime_request_id, thread_id, node_id, kind, status, created_at, payload_json)
					VALUES ('req-1', 'thread-b', 'node-1', 'approval', 'pending', ?, '{}')`, testStamp)
			},
			want: "thread thread-b has 1 pending approval",
		},
		{
			name: "active provider thread",
			setup: func(f *fixture) {
				f.src.exec("UPDATE orchestration_v2_projection_provider_threads SET status = 'active'")
			},
			want: "provider thread(s) that are active",
		},
		{
			name: "message id collision",
			setup: func(f *fixture) {
				f.dst.message("msg-a1", "import:codex:native-1")
				f.dst.syncCursor()
			},
			want: "1 row(s) of orchestration_v2_projection_messages already exist in the target",
		},
		{
			name: "thread already in target",
			setup: func(f *fixture) {
				f.dst.thread(threadSpec{id: "thread-b", projectID: "p-dst"})
				f.dst.syncCursor()
			},
			want: "thread thread-b already exists in the target",
		},
		{
			name: "lineage into another project",
			setup: func(f *fixture) {
				f.src.thread(threadSpec{id: "thread-y", projectID: "p-other", parentID: "thread-a", relationship: "fork"})
				f.src.syncCursor()
			},
			want: "is linked to thread thread-y (lineage parent) in project p-other",
		},
		{
			name: "missing target project",
			setup: func(f *fixture) {
				f.dst.exec("UPDATE projection_projects SET deleted_at = ?", testStamp)
			},
			opts: func(f *fixture, o *Options) { o.Projects = []string{f.workspace} },
			want: "t3 project add",
		},
		{
			name: "source schema mismatch",
			setup: func(f *fixture) {
				f.src.exec("INSERT INTO effect_sql_migrations (migration_id, name) VALUES (57, 'Future')")
			},
			want: "source schema: unsupported schema: highest migration is 57",
		},
		{
			name:  "target column mismatch",
			setup: func(f *fixture) { f.dst.exec("ALTER TABLE orchestration_v2_projection_messages ADD COLUMN extra TEXT") },
			want:  "target schema: table orchestration_v2_projection_messages differs: unexpected columns extra",
		},
		{
			name:  "stale projection cursor",
			setup: func(f *fixture) { f.dst.exec("UPDATE orchestration_v2_projection_metadata SET last_sequence = 1") },
			want:  "does not match the newest thread event",
		},
		{
			name: "running server",
			setup: func(f *fixture) {
				state := `{"version":1,"pid":` + strconv.Itoa(os.Getpid()) + `,"port":1,"origin":"x","startedAt":"x"}`
				if err := os.WriteFile(f.dst.inst.RuntimePath, []byte(state), 0o644); err != nil {
					panic(err)
				}
			},
			want: "target server appears to be running",
		},
		{
			name: "conflicting attachment",
			setup: func(f *fixture) {
				f.dst.file("thread-a-11111111-2222-4333-8444-555555555555.png", "different")
			},
			want: "exists in the target with different content",
		},
		{
			name: "legacy transcript not imported",
			setup: func(f *fixture) {
				f.src.exec(`INSERT INTO orchestration_v2_legacy_imports (thread_id, source_updated_at, shell_imported_at)
					VALUES ('thread-a', ?, ?)`, testStamp, testStamp)
			},
			want: "thread thread-a still keeps its transcript in legacy tables",
		},
		{
			name: "missing thread.created",
			setup: func(f *fixture) {
				f.src.exec("DELETE FROM orchestration_events WHERE stream_id = 'thread-b' AND event_type = 'thread.created'")
			},
			want: "thread thread-b has no thread.created event",
		},
		{
			name:  "scheduled task without archiving",
			setup: func(*fixture) {},
			opts:  func(_ *fixture, o *Options) { o.ArchiveSource = false },
			want:  "scheduled task task-a would run on both servers",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			test.setup(f)
			opts := f.options()
			opts.BackupDir = filepath.Join(t.TempDir(), "backup")
			if test.opts != nil {
				test.opts(f, &opts)
			}
			before := f.dst.count("SELECT count(*) FROM orchestration_events")
			plan, _, err := Run(context.Background(), opts)
			if !errors.Is(err, ErrBlocked) {
				t.Fatalf("Run error = %v, want ErrBlocked", err)
			}
			var out bytes.Buffer
			if err := plan.Write(&out); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), test.want) {
				t.Fatalf("plan lacks %q:\n%s", test.want, out.String())
			}
			if got := f.dst.count("SELECT count(*) FROM orchestration_events"); got != before {
				t.Fatalf("blocked move wrote %d target events", got-before)
			}
		})
	}
}

func TestProjectFlagAcceptsTargetPath(t *testing.T) {
	f := newFixture(t)
	opts := f.options()
	opts.Projects = []string{f.dst.str("SELECT workspace_root FROM projection_projects WHERE project_id = 'p-dst'")}
	plan := analyzeOK(t, opts)
	if len(plan.Projects) != 1 || plan.Projects[0].SourceID != "p-src" {
		t.Fatalf("selected %+v", plan.Projects)
	}
	opts.Projects = []string{t.TempDir()}
	if _, err := Analyze(context.Background(), opts); err == nil {
		t.Fatal("unknown project path was accepted")
	}
}

func TestIncludeDeleted(t *testing.T) {
	f := newFixture(t)
	opts := f.options()
	opts.IncludeDeleted = true
	plan := analyzeOK(t, opts)
	pp := plan.Projects[0]
	if pp.Threads != 3 || pp.DeletedIncluded != 1 || pp.Archive != 2 {
		t.Fatalf("plan %+v", pp)
	}
}

func TestEventIDFormat(t *testing.T) {
	id, err := newEventID("import:codex:x", "server:t3rry-move:1")
	if err != nil {
		t.Fatal(err)
	}
	prefix := "event:thread:import%3Acodex%3Ax:command:server%3At3rry-move%3A1:"
	if !strings.HasPrefix(id, prefix) || len(id) != len(prefix)+36 {
		t.Fatalf("event id = %s", id)
	}
	if got := encodeURIComponent("a b/é~*'()"); got != "a%20b%2F%C3%A9~*'()" {
		t.Fatalf("encodeURIComponent = %s", got)
	}
}
