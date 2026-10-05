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
	// Source cleanup archives the two open threads and marks the deleted one.
	if pp.Threads != 3 || pp.DeletedIncluded != 1 || pp.Archive != 3 {
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

func TestRetryFinishesSourceCleanup(t *testing.T) {
	f := newFixture(t)
	opts := f.options()
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	// Make the source cleanup fail after the target commit.
	f.src.exec(`CREATE TRIGGER fail_cleanup BEFORE UPDATE ON scheduled_tasks
		BEGIN SELECT RAISE(ABORT, 'injected failure'); END`)
	_, result, err := Run(context.Background(), opts)
	if err != nil || result.SourceError == nil {
		t.Fatalf("first run: err=%v source error=%v, want a source error", err, result.SourceError)
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_v2_projection_threads WHERE thread_id IN ('thread-a', 'thread-b')"); got != 2 {
		t.Fatalf("target holds %d moved threads, want 2", got)
	}
	if got := f.src.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-a'"); got != 1 {
		t.Fatal("failed cleanup left partial source changes")
	}

	f.src.exec("DROP TRIGGER fail_cleanup")
	retry := analyzeOK(t, f.options())
	if retry.Blocked() || retry.ThreadCount() != 0 || !retry.sourcePending() {
		t.Fatalf("retry plan: blocked=%v threads=%d pending=%v", retry.Blocked(), retry.ThreadCount(), retry.sourcePending())
	}
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	_, result, err = Run(context.Background(), opts)
	if err != nil || result.SourceError != nil {
		t.Fatalf("retry: err=%v source error=%v", err, result.SourceError)
	}
	if result.Archived != 2 || result.TasksDisabled != 1 {
		t.Fatalf("retry archived %d and disabled %d task(s), want 2 and 1", result.Archived, result.TasksDisabled)
	}
	if got := f.src.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-a'"); got != 0 {
		t.Fatal("source task still enabled after retry")
	}
	if got := f.src.count("SELECT count(*) FROM orchestration_v2_projection_threads WHERE thread_id IN ('thread-a', 'thread-b') AND archived_at IS NOT NULL"); got != 2 {
		t.Fatalf("%d of 2 source threads archived after retry", got)
	}
	if got, want := f.src.count("SELECT last_sequence FROM orchestration_v2_projection_metadata"), f.src.count("SELECT MAX(sequence) FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'"); got != want {
		t.Fatalf("source cursor = %d, newest thread event = %d", got, want)
	}

	// Nothing is left to do, and another run changes nothing.
	before := f.src.count("SELECT count(*) FROM orchestration_events")
	if final := analyzeOK(t, f.options()); final.sourcePending() {
		t.Fatal("source cleanup still pending after it completed")
	}
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	if got := f.src.count("SELECT count(*) FROM orchestration_events"); got != before {
		t.Fatalf("idle rerun appended %d source events", got-before)
	}
}

func TestReverseMoveIsRefused(t *testing.T) {
	f := newFixture(t)
	opts := f.options()
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}

	// Moving straight back finds the archived originals in the old source.
	reverse := Options{From: f.dst.inst, To: f.src.inst, ArchiveSource: true, BackupDir: filepath.Join(t.TempDir(), "backup")}
	srcEvents := f.src.count("SELECT count(*) FROM orchestration_events")
	dstEvents := f.dst.count("SELECT count(*) FROM orchestration_events")
	plan, _, err := Run(context.Background(), reverse)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("reverse move error = %v, want ErrBlocked", err)
	}
	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	want := "thread thread-a was moved out of the target earlier and its original is archived there"
	if !strings.Contains(out.String(), want) || !strings.Contains(out.String(), "unarchive it in the target's T3 Code") {
		t.Fatalf("plan lacks %q:\n%s", want, out.String())
	}
	if f.src.count("SELECT count(*) FROM orchestration_events") != srcEvents || f.dst.count("SELECT count(*) FROM orchestration_events") != dstEvents {
		t.Fatal("refused reverse move wrote events")
	}
	if got := f.dst.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-a'"); got != "" {
		t.Fatal("refused reverse move archived the live copy")
	}
}

func TestBackupDirInsideAttachmentsIsRejected(t *testing.T) {
	f := newFixture(t)
	link := filepath.Join(t.TempDir(), "attachments-link")
	if err := os.Symlink(f.dst.inst.AttachmentsDir, link); err != nil {
		t.Fatal(err)
	}
	before := f.dst.count("SELECT count(*) FROM orchestration_events")
	for _, dir := range []string{
		filepath.Join(f.dst.inst.AttachmentsDir, "backup"),
		filepath.Join(link, "nested", "backup"),
		f.dst.inst.UserData,
	} {
		opts := f.options()
		opts.BackupDir = dir
		_, _, err := Run(context.Background(), opts)
		if err == nil || !strings.Contains(err.Error(), "attachments") {
			t.Fatalf("backup dir %s: err = %v, want a rejection", dir, err)
		}
		if _, err := os.Stat(filepath.Join(f.dst.inst.AttachmentsDir, "backup")); !os.IsNotExist(err) {
			t.Fatal("rejected backup dir was created")
		}
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_events"); got != before {
		t.Fatal("rejected backup still wrote the target")
	}
}

func TestSharedSessionAcrossProjectRuns(t *testing.T) {
	f := newFixture(t)
	f.dst.project("p-dst-other", f.other)
	first := f.options()
	first.Projects = []string{f.workspace}
	first.BackupDir = filepath.Join(t.TempDir(), "backup")
	if _, _, err := Run(context.Background(), first); err != nil {
		t.Fatalf("first project: %v", err)
	}

	// The second project owns the shared session the first run inserted.
	second := f.options()
	second.Projects = []string{f.other}
	second.BackupDir = filepath.Join(t.TempDir(), "backup")
	plan, result, err := Run(context.Background(), second)
	if err != nil {
		var out bytes.Buffer
		_ = plan.Write(&out)
		t.Fatalf("second project: %v\n%s", err, out.String())
	}
	if got := result.Rows[tableSessions]; got != 0 {
		t.Fatalf("second run inserted %d session rows, want 0", got)
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id LIKE '%:shared'"); got != 2 {
		t.Fatalf("shared session has %d target bindings, want 2", got)
	}
	if got := f.dst.str("SELECT project_id FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-x'"); got != "p-dst-other" {
		t.Fatalf("thread-x project = %q", got)
	}

	// A live session with the same id is still a collision.
	g := newFixture(t)
	g.dst.project("p-dst-other", g.other)
	g.src.exec("DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = 'thread-a' AND provider_session_id LIKE '%:shared'")
	g.dst.exec(`INSERT INTO orchestration_v2_projection_provider_sessions (provider_session_id, provider, status, updated_at, payload_json, provider_instance_id)
		VALUES ('provider-session:provider-instance:codex:shared', 'codex', 'ready', ?, '{}', 'codex')`, testStamp)
	opts := g.options()
	opts.Projects = []string{g.other}
	if _, _, err := Run(context.Background(), opts); !errors.Is(err, ErrBlocked) {
		t.Fatalf("live target session: err = %v, want ErrBlocked", err)
	}
}

func TestReverseMoveOfArchivedThreadWithTask(t *testing.T) {
	f := newFixture(t)
	// thread-a is archived by the user before the move and keeps an enabled task.
	f.src.exec(`UPDATE orchestration_v2_projection_threads SET archived_at = ?1,
		payload_json = json_set(payload_json, '$.archivedAt', ?1) WHERE thread_id = 'thread-a'`, testStamp)
	opts := f.options()
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	if got := f.src.count(`SELECT count(*) FROM orchestration_events WHERE stream_id = 'thread-a'
		AND event_type = 'thread.archived' AND command_id LIKE 'server:t3rry-archive:%'`); got != 1 {
		t.Fatalf("source-archive markers on the already archived original = %d, want 1", got)
	}
	if got := f.src.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-a'"); got != testStamp {
		t.Fatalf("marking changed archived_at to %q", got)
	}
	if got := f.dst.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-a'"); got != 1 {
		t.Fatal("target task should stay enabled")
	}

	reverse := Options{From: f.dst.inst, To: f.src.inst, ArchiveSource: true, BackupDir: filepath.Join(t.TempDir(), "backup")}
	plan, _, err := Run(context.Background(), reverse)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("reverse move error = %v, want ErrBlocked", err)
	}
	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "thread thread-a was moved out of the target earlier") {
		t.Fatalf("plan:\n%s", out.String())
	}
	if got := f.dst.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-a'"); got != 1 {
		t.Fatal("refused reverse move disabled the only enabled task")
	}
}

func TestUserArchivedTargetCopyIsARetry(t *testing.T) {
	f := newFixture(t)
	opts := f.options()
	opts.ArchiveSource = false
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	f.src.exec("UPDATE scheduled_tasks SET enabled = 0")
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	// The user archives the moved copy in the target, then finishes the
	// source cleanup with a normal run.
	f.dst.exec(`UPDATE orchestration_v2_projection_threads SET archived_at = ?1,
		payload_json = json_set(payload_json, '$.archivedAt', ?1) WHERE thread_id = 'thread-a'`, testStamp)
	opts.ArchiveSource = true
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	plan, result, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if plan.Blocked() || result.SourceError != nil || result.Archived != 2 {
		t.Fatalf("retry: blocked=%v source error=%v archived=%d", plan.Blocked(), result.SourceError, result.Archived)
	}
}

func TestBackupAttachmentsDestinationIsResolved(t *testing.T) {
	f := newFixture(t)
	before := f.dst.count("SELECT count(*) FROM orchestration_events")

	// <backup>/attachments already exists as a link into the target's attachments.
	linked := t.TempDir()
	if err := os.Symlink(f.dst.inst.AttachmentsDir, filepath.Join(linked, "attachments")); err != nil {
		t.Fatal(err)
	}
	// The backup dir links to the target's userdata, so its attachments are the target's.
	userdataLink := filepath.Join(t.TempDir(), "userdata-link")
	if err := os.Symlink(f.dst.inst.UserData, userdataLink); err != nil {
		t.Fatal(err)
	}
	// A not yet existing backup dir below a link to the target's attachments.
	attachmentsLink := filepath.Join(t.TempDir(), "attachments-link")
	if err := os.Symlink(f.dst.inst.AttachmentsDir, attachmentsLink); err != nil {
		t.Fatal(err)
	}
	for dir, want := range map[string]string{
		linked:                                   "already exists",
		userdataLink:                             "already exists",
		filepath.Join(attachmentsLink, "nested"): "overlaps the target's attachments",
	} {
		opts := f.options()
		opts.BackupDir = dir
		_, result, err := Run(context.Background(), opts)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("backup dir %s: err = %v, want %q", dir, err, want)
		}
		if result != nil && result.BackupDir != "" {
			t.Fatalf("failed backup is reported as %s", result.BackupDir)
		}
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_events"); got != before {
		t.Fatal("rejected backup still wrote the target")
	}

	// The target's attachments tree lies inside <backup>/attachments.
	g := newFixture(t)
	outer := t.TempDir()
	inner := filepath.Join(outer, "attachments", "store")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(g.dst.inst.AttachmentsDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(inner, g.dst.inst.AttachmentsDir); err != nil {
		t.Fatal(err)
	}
	if err := checkBackupDir(g.dst.inst.AttachmentsDir, outer); err == nil {
		t.Fatal("backup whose attachments copy contains the target's attachments was accepted")
	}
}

func TestSymlinkedAttachmentsRoot(t *testing.T) {
	f := newFixture(t)
	real := t.TempDir()
	if err := os.WriteFile(filepath.Join(real, "existing-file.png"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(f.dst.inst.AttachmentsDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, f.dst.inst.AttachmentsDir); err != nil {
		t.Fatal(err)
	}
	opts := f.options()
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	_, result, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(opts.BackupDir, "attachments", "existing-file.png")); err != nil || string(data) != "old" {
		t.Fatalf("backup of a symlinked attachments root: %q, %v", data, err)
	}
	if result.AttachmentsCopied != 1 {
		t.Fatalf("attachments copied = %d, want 1", result.AttachmentsCopied)
	}
	if _, err := os.Lstat(filepath.Join(real, "thread-a-11111111-2222-4333-8444-555555555555.png")); err != nil {
		t.Fatalf("moved attachment is not in the resolved attachments dir: %v", err)
	}
}

func TestFailedTargetCopyReportsRollback(t *testing.T) {
	f := newFixture(t)
	f.dst.exec(`CREATE TRIGGER fail_copy BEFORE INSERT ON orchestration_v2_projection_messages
		BEGIN SELECT RAISE(ABORT, 'injected failure'); END`)
	before := f.dst.count("SELECT count(*) FROM orchestration_events")
	opts := f.options()
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	_, result, err := Run(context.Background(), opts)
	if err == nil || !strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("err = %v, want a rolled back copy", err)
	}
	if result == nil || !result.RolledBack || len(result.Rows) != 0 || result.AttachmentsCopied != 0 {
		t.Fatalf("result = %+v, want an empty rolled back result", result)
	}
	var out bytes.Buffer
	if err := result.Write(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "copied") || !strings.Contains(out.String(), "rolled back") {
		t.Fatalf("report:\n%s", out.String())
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_events"); got != before {
		t.Fatal("failed copy left target events")
	}
	if _, err := os.Stat(filepath.Join(f.dst.inst.AttachmentsDir, "thread-a-11111111-2222-4333-8444-555555555555.png")); !os.IsNotExist(err) {
		t.Fatal("failed copy left an attachment")
	}
	if got := f.src.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-a'"); got != "" {
		t.Fatal("failed copy archived the source")
	}
}

func TestReopenedOriginalIsArchivedAgain(t *testing.T) {
	f := newFixture(t)
	opts := f.options()
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	// The user archives the B copy and reopens the original in A.
	f.dst.exec(`UPDATE orchestration_v2_projection_threads SET archived_at = ?1,
		payload_json = json_set(payload_json, '$.archivedAt', ?1) WHERE thread_id = 'thread-a'`, testStamp)
	f.src.exec(`UPDATE orchestration_v2_projection_threads SET archived_at = NULL,
		payload_json = json_set(payload_json, '$.archivedAt', NULL) WHERE thread_id = 'thread-a'`)
	f.src.event("thread-a", "thread.unarchived", f.src.str("SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-a'"))
	f.src.syncCursor()

	// A fresh target C.
	c := newBase(t)
	c.project("p-c", f.workspace)
	c.syncCursor()
	toC := Options{From: f.src.inst, To: c.inst, ArchiveSource: true, BackupDir: filepath.Join(t.TempDir(), "backup")}
	plan, result, err := Run(context.Background(), toC)
	if err != nil {
		var out bytes.Buffer
		_ = plan.Write(&out)
		t.Fatalf("A->C: %v\n%s", err, out.String())
	}
	if result.SourceError != nil || result.Archived != 1 {
		t.Fatalf("A->C archived %d thread(s), source error %v; want the reopened original archived", result.Archived, result.SourceError)
	}
	if got := f.src.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-a'"); got == "" {
		t.Fatal("reopened original is still open in A")
	}
	if got := f.src.count("SELECT count(*) FROM orchestration_events WHERE stream_id = 'thread-a' AND " + markerCondition("orchestration_events")); got != 2 {
		t.Fatalf("A holds %d markers for thread-a, want 2", got)
	}
	// C received A's earlier marker as an ordinary archive event.
	if got := c.count("SELECT count(*) FROM orchestration_events WHERE stream_id = 'thread-a' AND event_type = 'thread.archived'"); got != 1 {
		t.Fatalf("C holds %d archive events for thread-a, want 1", got)
	}
	if got := c.count("SELECT count(*) FROM orchestration_events WHERE stream_id = 'thread-a' AND " + markerCondition("orchestration_events")); got != 0 {
		t.Fatal("the copied archive event still carries the marker")
	}
}

func TestDeletedThreadMarkerAndTask(t *testing.T) {
	f := newFixture(t)
	f.src.exec(`INSERT INTO scheduled_tasks (task_id, title, prompt, enabled, schedule_json, project_id, thread_id,
			workspace_strategy_json, model_selection_json, runtime_mode, interaction_mode, created_by, creation_source,
			created_at, updated_at, last_run_status, run_count)
		VALUES ('task-d', 'Weekly', 'go', 1, '{}', 'p-src', 'thread-d', '{}', '{}', 'full-access', 'default', 'user', 'web', ?, ?, 'never', 0)`,
		testStamp, testStamp)
	opts := f.options()
	opts.IncludeDeleted = true
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	if _, _, err := Run(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	if got := f.src.count("SELECT count(*) FROM orchestration_events WHERE stream_id = 'thread-d' AND " + markerCondition("orchestration_events")); got != 1 {
		t.Fatalf("deleted original has %d markers, want 1", got)
	}
	if got := f.src.str("SELECT deleted_at || '|' || COALESCE(archived_at, '') FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-d'"); got != testStamp+"|" {
		t.Fatalf("marking changed the deleted thread: %s", got)
	}
	if got := f.dst.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-d'"); got != 0 {
		t.Fatal("the deleted thread's task arrived enabled")
	}
	if got := f.src.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-d'"); got != 1 {
		t.Fatal("the source task of the deleted thread was disabled")
	}

	reverse := Options{From: f.dst.inst, To: f.src.inst, IncludeDeleted: true, ArchiveSource: true,
		BackupDir: filepath.Join(t.TempDir(), "backup")}
	plan, _, err := Run(context.Background(), reverse)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("reverse move error = %v, want ErrBlocked", err)
	}
	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "thread thread-d was moved out of the target earlier") {
		t.Fatalf("plan:\n%s", out.String())
	}
	if got := f.src.count("SELECT enabled FROM scheduled_tasks WHERE task_id = 'task-d'"); got != 1 {
		t.Fatal("the task ended up disabled on both servers")
	}
}
