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

// codexParents reads the session_meta line of the Codex rollouts of the given
// session ids under home and returns the parent session of every one that a
// thread spawned as a subagent. Sessions without a readable rollout are
// returned in missing.
func codexParents(home string, ids map[string]bool) (parents map[string]string, missing []string, err error) {
	paths := map[string]string{}
	for _, dir := range []string{"sessions", "archived_sessions"} {
		root := filepath.Join(home, dir)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) && path == root {
					return fs.SkipDir
				}
				return err
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
			if ids[id] {
				if _, seen := paths[id]; !seen {
					paths[id] = path
				}
			}
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("read Codex sessions: %w", err)
		}
	}

	parents = map[string]string{}
	for id := range ids {
		path, ok := paths[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		parent, err := readCodexParent(path, id)
		if err != nil {
			return nil, nil, err
		}
		if parent != "" {
			parents[id] = parent
		}
	}
	return parents, missing, nil
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
