package move

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// importsBase has a project with a native thread, a live import bound to a
// ready session, a deleted import and an import with an unfinished run.
func importsBase(t *testing.T) *base {
	t.Helper()
	b := newBase(t)
	b.project("p-1", t.TempDir())
	b.thread(threadSpec{id: "thread-native", projectID: "p-1"})
	b.thread(threadSpec{id: "import:codex:live", projectID: "p-1", origin: "v1_import"})
	b.thread(threadSpec{id: "import:codex:gone", projectID: "p-1", origin: "v1_import", deleted: true})
	b.thread(threadSpec{id: "import:codex:busy", projectID: "p-1", origin: "v1_import"})
	b.run("run-live", "import:codex:live", 1, "completed")
	b.run("run-busy", "import:codex:busy", 1, "running")
	b.exec(`INSERT INTO orchestration_v2_projection_provider_sessions (provider_session_id, thread_id, provider, status, updated_at, payload_json, provider_instance_id, driver)
		VALUES ('session-1', 'import:codex:live', 'codex', 'ready', ?, '{"status":"ready"}', 'codex', 'codex')`, testStamp)
	b.exec(`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('session-1', 'import:codex:live')`)
	b.syncCursor()
	return b
}

func TestListImports(t *testing.T) {
	b := importsBase(t)
	plan, err := AnalyzeImports(context.Background(), ImportOptions{Base: b.inst})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, it := range plan.Imports {
		ids = append(ids, it.ID)
	}
	if strings.Join(ids, ",") != "import:codex:busy,import:codex:live" {
		t.Fatalf("listed %v", ids)
	}
	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "2 live import(s)") {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestDeleteImportsBlockers(t *testing.T) {
	b := importsBase(t)
	for id, want := range map[string]string{
		"thread-native":     "was not created by \"import recent sessions\"",
		"import:codex:busy": "has 1 run(s) that are not finished",
		"import:codex:none": "does not exist",
	} {
		plan, result, err := DeleteImports(context.Background(), ImportOptions{Base: b.inst, Threads: []string{id}})
		if !errors.Is(err, ErrBlocked) || result != nil {
			t.Fatalf("%s: err = %v, result = %+v", id, err, result)
		}
		if !strings.Contains(strings.Join(plan.Blockers, "\n"), want) {
			t.Errorf("%s: blockers %v lack %q", id, plan.Blockers, want)
		}
	}
	if got := b.count("SELECT count(*) FROM orchestration_events WHERE command_id LIKE 'server:t3rry-%'"); got != 0 {
		t.Fatalf("blocked runs wrote %d event(s)", got)
	}
}

func TestDeleteImports(t *testing.T) {
	b := importsBase(t)
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	opts := ImportOptions{
		Base:      b.inst,
		Threads:   []string{"import:codex:live", "import:codex:gone", "import:codex:live"},
		BackupDir: filepath.Join(t.TempDir(), "backup"),
		Now:       func() time.Time { return now },
	}
	plan, result, err := DeleteImports(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 1 || result.Detached != 1 || len(plan.Skipped) != 1 {
		t.Fatalf("result %+v, skipped %v", result, plan.Skipped)
	}
	stamp := timestamp(now)
	if got := b.str("SELECT deleted_at FROM orchestration_v2_projection_threads WHERE thread_id = 'import:codex:live'"); got != stamp {
		t.Fatalf("deleted_at = %q", got)
	}
	if got := b.str("SELECT group_concat(event_type, ',') FROM (SELECT event_type FROM orchestration_events WHERE stream_id = 'import:codex:live' AND command_id LIKE 'server:t3rry-delete-imports:%' ORDER BY sequence)"); got != "provider-session.detached,thread.deleted" {
		t.Fatalf("events = %s", got)
	}
	if got := b.str("SELECT json_extract(payload_json, '$.reason') FROM orchestration_events WHERE event_type = 'provider-session.detached'"); got != "Thread deleted." {
		t.Fatalf("detach reason = %q", got)
	}
	if got := b.count("SELECT count(*) FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = 'import:codex:live'"); got != 0 {
		t.Fatalf("binding remains")
	}
	if got := b.str("SELECT archived_at FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-native'"); got != "" {
		t.Fatalf("native thread changed")
	}
	if got, want := b.count("SELECT last_sequence FROM orchestration_v2_projection_metadata"), b.count("SELECT MAX(sequence) FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'"); got != want {
		t.Fatalf("cursor = %d, newest thread event = %d", got, want)
	}
	if len(result.Verification) != 1 || !strings.HasPrefix(result.Verification[0], "projection cursor matches") {
		t.Fatalf("verification %v", result.Verification)
	}

	// A rerun finds nothing to do and writes nothing.
	again, result, err := DeleteImports(context.Background(), ImportOptions{Base: b.inst, Threads: []string{"import:codex:live"}})
	if err != nil || result.Deleted != 0 || len(again.Skipped) != 1 || result.BackupDir != "" {
		t.Fatalf("rerun: err=%v result=%+v skipped=%v", err, result, again.Skipped)
	}
}
