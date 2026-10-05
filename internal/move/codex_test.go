package move

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Codex session ids of the target imports in subagentFixture.
const (
	codexChild      = "01a00000-0000-7000-8000-000000000001"
	codexGrandchild = "01a00000-0000-7000-8000-000000000002"
	codexExec       = "01a00000-0000-7000-8000-000000000003"
	codexBusy       = "01a00000-0000-7000-8000-000000000004"
	codexNoRollout  = "01a00000-0000-7000-8000-000000000005"
	codexReview     = "01a00000-0000-7000-8000-000000000006"
)

// writeRollout writes a Codex rollout whose session_meta records source.
func writeRollout(t *testing.T, home, dir, id, source string) {
	t.Helper()
	path := filepath.Join(home, dir, "2026", "01", "02", "rollout-2026-01-02T03-04-05-"+id+".jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	meta := fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"source":%s,"cwd":"/w"}}`, testStamp, id, source)
	if err := os.WriteFile(path, []byte(meta+"\n"+`{"type":"response_item","payload":{}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func spawnedBy(parent string) string {
	return fmt.Sprintf(`{"subagent":{"thread_spawn":{"parent_thread_id":%q,"depth":1}}}`, parent)
}

// subagentFixture extends newFixture with target imports of Codex sessions:
// a subagent of thread A's session native-1 and its own subagent, a
// standalone exec session, a review subagent, a subagent with a run of its
// own and one without a rollout.
func subagentFixture(t *testing.T) (*fixture, string) {
	t.Helper()
	f := newFixture(t)
	home := t.TempDir()
	writeRollout(t, home, "sessions", codexChild, spawnedBy("native-1"))
	writeRollout(t, home, "archived_sessions", codexGrandchild, spawnedBy(codexChild))
	writeRollout(t, home, "sessions", codexExec, `"exec"`)
	writeRollout(t, home, "sessions", codexReview, `{"subagent":"review"}`)
	writeRollout(t, home, "sessions", codexBusy, spawnedBy("native-1"))
	for _, id := range []string{codexChild, codexGrandchild, codexExec, codexReview, codexBusy, codexNoRollout} {
		f.dst.thread(threadSpec{id: "import:codex:" + id, projectID: "p-dst", origin: "v1_import"})
	}
	f.dst.run("run-busy", "import:codex:"+codexBusy, 1, "completed")
	f.dst.syncCursor()
	return f, home
}

func TestSubagentImportsAreSoftDeleted(t *testing.T) {
	f, home := subagentFixture(t)
	opts := f.options()
	opts.CodexHome = home
	plan := analyzeOK(t, opts)
	if plan.Blocked() {
		t.Fatalf("plan is blocked: %+v %+v", plan.Blockers, plan.Projects[0].Blockers)
	}
	pp := plan.Projects[0]
	if pp.Duplicates != 1 || pp.SubagentImports != 2 {
		t.Fatalf("duplicates = %d, subagent imports = %d, want 1 and 2", pp.Duplicates, pp.SubagentImports)
	}
	warnings := strings.Join(append(append([]string(nil), plan.Warnings...), pp.Warnings...), "\n")
	for _, want := range []string{
		"import:codex:" + codexBusy + " imports a subagent session of a moved thread but has its own activity",
		"1 imported Codex thread(s) in the target have no rollout",
	} {
		if !strings.Contains(warnings, want) {
			t.Errorf("warnings lack %q:\n%s", want, warnings)
		}
	}
	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "subagent imports to soft-delete 2") {
		t.Fatalf("plan output:\n%s", out.String())
	}

	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	_, result, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Duplicates != 1 || result.SubagentImports != 2 {
		t.Fatalf("result duplicates = %d, subagent imports = %d", result.Duplicates, result.SubagentImports)
	}
	for id, deleted := range map[string]bool{
		codexChild: true, codexGrandchild: true,
		codexExec: false, codexReview: false, codexBusy: false, codexNoRollout: false,
	} {
		got := f.dst.count("SELECT count(*) FROM orchestration_v2_projection_threads WHERE thread_id = ? AND deleted_at IS NOT NULL", "import:codex:"+id)
		if (got == 1) != deleted {
			t.Errorf("import:codex:%s deleted = %v, want %v", id, got == 1, deleted)
		}
	}
	if got := f.dst.count("SELECT count(*) FROM orchestration_events WHERE stream_id = ? AND event_type = 'thread.deleted'", "import:codex:"+codexGrandchild); got != 1 {
		t.Fatalf("grandchild thread.deleted events = %d", got)
	}
}

func TestKeepSubagentImports(t *testing.T) {
	f, home := subagentFixture(t)
	opts := f.options()
	opts.CodexHome = home
	opts.KeepSubagentImports = true
	opts.BackupDir = filepath.Join(t.TempDir(), "backup")
	plan, result, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Projects[0].SubagentImports != 0 || result.SubagentImports != 0 || result.Duplicates != 1 {
		t.Fatalf("plan %+v, result %+v", plan.Projects[0], result)
	}
	if !strings.Contains(strings.Join(plan.Projects[0].Warnings, "\n"), "kept because of --keep-subagent-imports") {
		t.Fatalf("warnings: %v", plan.Projects[0].Warnings)
	}
}

func TestRerunFinishesImportCleanup(t *testing.T) {
	f, home := subagentFixture(t)
	// An earlier release moved the threads but left every import behind.
	first := f.options()
	first.KeepDuplicates = true
	first.BackupDir = filepath.Join(t.TempDir(), "backup-1")
	if _, _, err := Run(context.Background(), first); err != nil {
		t.Fatal(err)
	}

	opts := f.options()
	opts.CodexHome = home
	plan := analyzeOK(t, opts)
	if plan.Blocked() || plan.ThreadCount() != 0 || !plan.targetPending() {
		t.Fatalf("rerun: blocked=%v threads=%d duplicates=%v subagent imports=%v",
			plan.Blocked(), plan.ThreadCount(), plan.duplicates, plan.subagentImports)
	}
	var out bytes.Buffer
	if err := plan.Write(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "result: ready to finish target cleanup of already moved threads (1 duplicate(s) and 2 subagent import(s) to soft-delete)") {
		t.Fatalf("plan output:\n%s", out.String())
	}

	opts.BackupDir = filepath.Join(t.TempDir(), "backup-2")
	_, result, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Duplicates != 1 || result.SubagentImports != 2 || result.BackupDir == "" {
		t.Fatalf("result %+v", result)
	}
	if got, want := f.dst.count("SELECT last_sequence FROM orchestration_v2_projection_metadata"), f.dst.count("SELECT MAX(sequence) FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'"); got != want {
		t.Fatalf("target cursor = %d, newest thread event = %d", got, want)
	}
	again := analyzeOK(t, opts)
	if again.targetPending() || again.ThreadCount() != 0 {
		t.Fatalf("third run still has work: %v %v", again.duplicates, again.subagentImports)
	}
}

func TestMissingCodexHomeWarns(t *testing.T) {
	f, _ := subagentFixture(t)
	opts := f.options()
	opts.CodexHome = filepath.Join(t.TempDir(), "absent")
	plan := analyzeOK(t, opts)
	if plan.Blocked() || plan.Projects[0].SubagentImports != 0 {
		t.Fatalf("blocked=%v project=%+v", plan.Blocked(), plan.Projects[0])
	}
	if !strings.Contains(strings.Join(plan.Warnings, "\n"), "imported subagent sessions were not checked") {
		t.Fatalf("warnings: %v", plan.Warnings)
	}
}

func TestReadCodexParent(t *testing.T) {
	home := t.TempDir()
	writeRollout(t, home, "sessions", codexChild, spawnedBy("parent-1"))
	writeRollout(t, home, "sessions", codexExec, `"exec"`)
	writeRollout(t, home, "sessions", codexReview, `{"subagent":"review"}`)
	parents, missing, err := codexParents(home, map[string]bool{codexChild: true, codexExec: true, codexReview: true, codexNoRollout: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[codexChild] != "parent-1" {
		t.Fatalf("parents = %v", parents)
	}
	if len(missing) != 1 || missing[0] != codexNoRollout {
		t.Fatalf("missing = %v", missing)
	}

	// A rollout whose first line belongs to another session is an error.
	path := filepath.Join(home, "sessions", "rollout-x-"+codexBusy+".jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"session_meta","payload":{"id":"other"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := codexParents(home, map[string]bool{codexBusy: true}); err == nil {
		t.Fatal("mismatched session_meta was accepted")
	}
}
