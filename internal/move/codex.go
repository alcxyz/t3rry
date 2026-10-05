package move

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// maxSessionMetaLine bounds the first rollout line, which carries the
// session's base instructions and can be large.
const maxSessionMetaLine = 64 << 20

// codexSessionIDLen is the length of the session id that ends every rollout
// file name: rollout-<timestamp>-<uuid>.jsonl.
const codexSessionIDLen = 36

// codexRollouts indexes the Codex rollouts under a Codex home by session id
// and reads their parents on demand. Problems reading a directory or file are
// collected rather than returned, so one bad rollout only leaves the sessions
// it concerns unchecked.
type codexRollouts struct {
	paths    map[string]string
	parents  map[string]string
	read     map[string]bool
	problems []string
}

// indexCodexRollouts walks the sessions and archived_sessions directories of
// home, following a symlink at either root.
func indexCodexRollouts(home string) *codexRollouts {
	r := &codexRollouts{paths: map[string]string{}, parents: map[string]string{}, read: map[string]bool{}}
	for _, dir := range []string{"sessions", "archived_sessions"} {
		root, err := filepath.EvalSymlinks(filepath.Join(home, dir))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			r.problems = append(r.problems, err.Error())
			continue
		}
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				r.problems = append(r.problems, err.Error())
				if d != nil && d.IsDir() && path != root {
					return fs.SkipDir
				}
				return nil
			}
			name := d.Name()
			if d.IsDir() || !strings.HasPrefix(name, "rollout-") || !strings.HasSuffix(name, ".jsonl") {
				return nil
			}
			stem := strings.TrimSuffix(name, ".jsonl")
			if len(stem) < codexSessionIDLen {
				return nil
			}
			id := stem[len(stem)-codexSessionIDLen:]
			if _, seen := r.paths[id]; !seen {
				r.paths[id] = path
			}
			return nil
		})
		if err != nil {
			r.problems = append(r.problems, err.Error())
		}
	}
	return r
}

// has reports whether a rollout exists for the session.
func (r *codexRollouts) has(id string) bool {
	_, ok := r.paths[id]
	return ok
}

// parent returns the session that spawned id, or "" when id is a top-level
// session, has no rollout, or its rollout cannot be read.
func (r *codexRollouts) parent(id string) string {
	if r.read[id] {
		return r.parents[id]
	}
	r.read[id] = true
	path, ok := r.paths[id]
	if !ok {
		return ""
	}
	parent, err := readCodexParent(path, id)
	if err != nil {
		r.problems = append(r.problems, err.Error())
		return ""
	}
	r.parents[id] = parent
	return parent
}

// readCodexParent returns the parent session recorded in a rollout's
// session_meta line, or "" when the session was not spawned by another.
func readCodexParent(path, id string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	line, err := bufio.NewReader(io.LimitReader(f, maxSessionMetaLine)).ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	var meta struct {
		Type    string `json:"type"`
		Payload struct {
			ID     string          `json:"id"`
			Source json.RawMessage `json:"source"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(line, &meta); err != nil {
		return "", fmt.Errorf("parse session_meta of %s: %w", path, err)
	}
	if meta.Type != "session_meta" || meta.Payload.ID != id {
		return "", fmt.Errorf("%s does not start with the session_meta of %s", path, id)
	}
	// source is a string such as "cli" or "exec" for top-level sessions, and
	// {"subagent": {"thread_spawn": {"parent_thread_id": ...}}} for spawned
	// ones. Other subagent kinds, such as reviews, have no parent session.
	var source struct {
		Subagent json.RawMessage `json:"subagent"`
	}
	if json.Unmarshal(meta.Payload.Source, &source) != nil || len(source.Subagent) == 0 {
		return "", nil
	}
	var subagent struct {
		ThreadSpawn struct {
			ParentThreadID string `json:"parent_thread_id"`
		} `json:"thread_spawn"`
	}
	if json.Unmarshal(source.Subagent, &subagent) != nil {
		return "", nil
	}
	return subagent.ThreadSpawn.ParentThreadID, nil
}
