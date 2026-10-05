package move

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

const testStamp = "2026-01-02T03:04:05.000Z"

// base is a synthetic T3 Code base directory built from the v56 fixture.
type base struct {
	t    *testing.T
	inst instance.Instance
	db   *sql.DB
}

func newBase(t *testing.T) *base {
	t.Helper()
	dir := t.TempDir()
	userData := filepath.Join(dir, "userdata")
	if err := os.MkdirAll(filepath.Join(userData, "attachments"), 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(userData, "statev2.sqlite")
	schemaSQL, err := os.ReadFile("../store/testdata/schema-v56.sql")
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := os.ReadFile("../store/testdata/migrations-v56.txt")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := store.ParseLedger(string(ledger))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	b := &base{t: t, db: db}
	b.exec(string(schemaSQL))
	b.exec("PRAGMA journal_mode = WAL")
	for _, m := range migrations {
		b.exec("INSERT INTO effect_sql_migrations (migration_id, name) VALUES (?, ?)", m.ID, m.Name)
	}
	b.exec(`INSERT INTO orchestration_v2_projection_metadata VALUES ('thread-projections', 2, 0, ?)`, testStamp)
	inst, err := instance.Resolve(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.inst = inst
	return b
}

func (b *base) exec(query string, args ...any) {
	b.t.Helper()
	if _, err := b.db.Exec(query, args...); err != nil {
		b.t.Fatalf("%s: %v", query, err)
	}
}

func (b *base) count(query string, args ...any) int {
	b.t.Helper()
	var n int
	if err := b.db.QueryRow(query, args...).Scan(&n); err != nil {
		b.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (b *base) str(query string, args ...any) string {
	b.t.Helper()
	var s sql.NullString
	if err := b.db.QueryRow(query, args...).Scan(&s); err != nil {
		b.t.Fatalf("%s: %v", query, err)
	}
	return s.String
}

// syncCursor sets the projection cursor to the newest thread event, as the
// server leaves it after every commit.
func (b *base) syncCursor() {
	b.exec(`UPDATE orchestration_v2_projection_metadata SET last_sequence =
		(SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events
			WHERE application_event_version = 2 AND aggregate_kind = 'thread')`)
}

func (b *base) project(id, root string) {
	b.exec(`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
		VALUES (?, ?, ?, '[]', ?, ?)`, id, filepath.Base(root), root, testStamp, testStamp)
	payload := fmt.Sprintf(`{"projectId":%q,"workspaceRoot":%q}`, id, root)
	b.exec(`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type,
			occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
		VALUES (?, 'project', ?, 0, 'project.created', ?, 'client', ?, '{}', 2)`,
		"event:project:"+id, id, testStamp, payload)
}

type threadSpec struct {
	id, projectID string
	parentID      string
	relationship  string
	deleted       bool
	origin        string
}

func threadPayload(spec threadSpec) string {
	root := spec.id
	parent := any(nil)
	relationship := any(nil)
	if spec.parentID != "" {
		root = spec.parentID
		parent = spec.parentID
		relationship = spec.relationship
	}
	payload := map[string]any{
		"createdBy":              "user",
		"creationSource":         "web",
		"id":                     spec.id,
		"projectId":              spec.projectID,
		"title":                  "Thread " + spec.id,
		"providerInstanceId":     "codex",
		"modelSelection":         map[string]any{"instanceId": "codex", "model": "gpt"},
		"runtimeMode":            "full-access",
		"interactionMode":        "default",
		"branch":                 nil,
		"worktreePath":           nil,
		"activeProviderThreadId": nil,
		"lineage":                map[string]any{"parentThreadId": parent, "relationshipToParent": relationship, "rootThreadId": root},
		"forkedFrom":             nil,
		"createdAt":              testStamp,
		"updatedAt":              testStamp,
		"archivedAt":             nil,
		"deletedAt":              nil,
	}
	if spec.origin != "" {
		payload["historyOrigin"] = spec.origin
	}
	if spec.deleted {
		payload["deletedAt"] = testStamp
	}
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// thread inserts a thread's projection row and its thread.created event.
func (b *base) thread(spec threadSpec) {
	payload := threadPayload(spec)
	var deletedAt any
	if spec.deleted {
		deletedAt = testStamp
	}
	b.exec(`INSERT INTO orchestration_v2_projection_threads (thread_id, project_id, title, default_provider,
			runtime_mode, interaction_mode, created_at, updated_at, deleted_at, payload_json, provider_instance_id)
		VALUES (?, ?, ?, 'codex', 'full-access', 'default', ?, ?, ?, ?, 'codex')`,
		spec.id, spec.projectID, "Thread "+spec.id, testStamp, testStamp, deletedAt, payload)
	b.event(spec.id, "thread.created", payload)
	b.event(spec.id, "thread.visited", payload)
}

func (b *base) event(threadID, eventType, payload string) {
	b.exec(`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type,
			occurred_at, command_id, correlation_id, actor_kind, payload_json, metadata_json, application_event_version)
		VALUES (?1, 'thread', ?2, COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
				WHERE aggregate_kind = 'thread' AND stream_id = ?2), 0),
			?3, ?4, 'command:test', 'command:test', 'server', ?5, '{"providerInstanceId":"codex"}', 2)`,
		fmt.Sprintf("event:%s:%s:%d", threadID, eventType, b.count("SELECT count(*) FROM orchestration_events")),
		threadID, eventType, testStamp, payload)
}

func (b *base) message(id, threadID string, attachmentIDs ...string) {
	attachments := []map[string]any{}
	for _, a := range attachmentIDs {
		attachments = append(attachments, map[string]any{"type": "image", "id": a, "name": "shot.png", "mimeType": "image/png", "sizeBytes": 3})
	}
	data, _ := json.Marshal(map[string]any{"id": id, "threadId": threadID, "role": "user", "text": "hi", "attachments": attachments})
	b.exec(`INSERT INTO orchestration_v2_projection_messages (message_id, thread_id, role, streaming, created_at, updated_at, payload_json)
		VALUES (?, ?, 'user', 0, ?, ?, ?)`, id, threadID, testStamp, testStamp, string(data))
	b.event(threadID, "message.updated", string(data))
}

func (b *base) run(id, threadID string, ordinal int, status string) {
	b.exec(`INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, status, requested_at, payload_json, provider_instance_id)
		VALUES (?, ?, ?, 'codex', ?, ?, '{}', 'codex')`, id, threadID, ordinal, status, testStamp)
}

func (b *base) file(name, content string) {
	if err := os.WriteFile(filepath.Join(b.inst.AttachmentsDir, name), []byte(content), 0o644); err != nil {
		b.t.Fatal(err)
	}
}

// fixture is a source and target pair with one project to move.
type fixture struct {
	src, dst  *base
	workspace string
	other     string
}

// newFixture builds:
//   - source project p-src (workspace) with root thread A, its subagent child
//     B, a deleted thread D, and project p-other with thread X;
//   - target project p-dst whose root is a symlink to the same workspace, and
//     an import thread that duplicates A's provider session.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	workspace := t.TempDir()
	link := filepath.Join(t.TempDir(), "ws-link")
	if err := os.Symlink(workspace, link); err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	src, dst := newBase(t), newBase(t)

	src.project("p-src", workspace)
	src.project("p-other", other)
	src.thread(threadSpec{id: "thread-a", projectID: "p-src"})
	src.thread(threadSpec{id: "thread-b", projectID: "p-src", parentID: "thread-a", relationship: "subagent"})
	src.thread(threadSpec{id: "thread-d", projectID: "p-src", deleted: true})
	src.thread(threadSpec{id: "thread-x", projectID: "p-other"})
	src.run("run-a1", "thread-a", 1, "completed")
	src.message("msg-a1", "thread-a", "thread-a-11111111-2222-4333-8444-555555555555")
	src.file("thread-a-11111111-2222-4333-8444-555555555555.png", "png")
	src.exec(`INSERT INTO orchestration_v2_projection_subagents (subagent_id, thread_id, parent_node_id, provider, child_thread_id, origin, status, updated_at, payload_json)
		VALUES ('sub-1', 'thread-a', 'node-1', 'codex', 'thread-b', 'app_owned', 'completed', ?, '{}')`, testStamp)
	src.exec(`INSERT INTO orchestration_v2_projection_provider_threads (provider_thread_id, thread_id, provider, provider_session_id, status, updated_at, payload_json, provider_instance_id)
		VALUES ('pt-a', 'thread-a', 'codex', 'provider-session:provider-instance:codex:shared', 'idle', ?, ?, 'codex')`,
		testStamp, `{"id":"pt-a","nativeThreadRef":{"driver":"codex","nativeId":"native-1","strength":"strong"}}`)
	src.exec(`INSERT INTO orchestration_v2_projection_provider_sessions (provider_session_id, thread_id, provider, status, updated_at, payload_json, provider_instance_id)
		VALUES ('provider-session:provider-instance:codex:shared', 'thread-x', 'codex', 'ready', ?, '{"status":"ready"}', 'codex'),
			('session-a', 'thread-a', 'codex', 'stopped', ?, '{"status":"stopped"}', 'codex')`, testStamp, testStamp)
	src.exec(`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES
		('provider-session:provider-instance:codex:shared', 'thread-a'),
		('provider-session:provider-instance:codex:shared', 'thread-x'),
		('session-a', 'thread-a')`)
	src.exec(`INSERT INTO scheduled_tasks (task_id, title, prompt, enabled, schedule_json, project_id, thread_id,
			workspace_strategy_json, model_selection_json, runtime_mode, interaction_mode, created_by, creation_source,
			created_at, updated_at, last_run_status, run_count)
		VALUES ('task-a', 'Nightly', 'go', 1, '{}', 'p-src', 'thread-a', '{}', '{}', 'full-access', 'default', 'user', 'web', ?, ?, 'never', 0)`,
		testStamp, testStamp)
	src.syncCursor()

	dst.project("p-dst", link)
	dst.thread(threadSpec{id: "import:codex:native-1", projectID: "p-dst", origin: "v1_import"})
	dst.syncCursor()
	return &fixture{src: src, dst: dst, workspace: workspace, other: other}
}

func (f *fixture) options() Options {
	return Options{From: f.src.inst, To: f.dst.inst, ArchiveSource: true}
}

func analyzeOK(t *testing.T, opts Options) *Plan {
	t.Helper()
	plan, err := Analyze(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
