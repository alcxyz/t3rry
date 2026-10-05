package instance

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestExpandHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip(err)
	}
	for in, want := range map[string]string{
		"~":          home,
		"~/.t3":      filepath.Join(home, ".t3"),
		"/srv/t3":    "/srv/t3",
		"~other/.t3": "~other/.t3",
		"relative":   "relative",
	} {
		got, err := ExpandHome(in)
		if err != nil || got != want {
			t.Errorf("ExpandHome(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func newInstance(t *testing.T) Instance {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "userdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "userdata", "statev2.sqlite"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	inst, err := Resolve(dir)
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

func TestResolve(t *testing.T) {
	inst := newInstance(t)
	if filepath.Base(inst.DBPath) != "statev2.sqlite" || filepath.Base(inst.AttachmentsDir) != "attachments" ||
		filepath.Base(inst.RuntimePath) != "server-runtime.json" {
		t.Fatalf("paths = %+v", inst)
	}
	if _, err := Resolve(t.TempDir()); err == nil || !strings.Contains(err.Error(), "no T3 Code state database") {
		t.Fatalf("Resolve without a database: %v", err)
	}
	other := newInstance(t)
	if SameDatabase(inst, other) || !SameDatabase(inst, inst) {
		t.Fatal("SameDatabase is wrong")
	}
}

func TestActivity(t *testing.T) {
	inst := newInstance(t)
	if activity := inst.Activity(); activity.Running() {
		t.Fatalf("idle instance reported running: %v", activity.Reasons)
	}

	write := func(content string) {
		if err := os.WriteFile(inst.RuntimePath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"version":1,"pid":` + strconv.Itoa(os.Getpid()) + `}`)
	if activity := inst.Activity(); !activity.Running() || !strings.Contains(activity.Reasons[0], "live pid") {
		t.Fatalf("live pid not detected: %+v", activity)
	}
	write(`{"version":1,"pid":2147483646}`)
	if activity := inst.Activity(); activity.Running() {
		t.Fatalf("dead pid reported running: %v", activity.Reasons)
	}
	write(`not json`)
	if activity := inst.Activity(); !activity.Running() {
		t.Fatal("unreadable runtime state should block")
	}
}
