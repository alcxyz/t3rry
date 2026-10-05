// Package instance locates a T3 Code server's state under its base directory
// and detects whether a server is using it.
package instance

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Instance is a T3 Code base directory and the state paths derived from it,
// mirroring the server's deriveServerPaths for an explicit --base-dir.
type Instance struct {
	BaseDir        string
	UserData       string
	DBPath         string
	AttachmentsDir string
	RuntimePath    string
}

// Resolve expands ~ in baseDir, makes it absolute and requires its state
// database to exist.
func Resolve(baseDir string) (Instance, error) {
	if strings.TrimSpace(baseDir) == "" {
		return Instance{}, errors.New("base directory is empty")
	}
	expanded, err := ExpandHome(baseDir)
	if err != nil {
		return Instance{}, err
	}
	abs, err := filepath.Abs(expanded)
	if err != nil {
		return Instance{}, err
	}
	userData := filepath.Join(abs, "userdata")
	inst := Instance{
		BaseDir:        abs,
		UserData:       userData,
		DBPath:         filepath.Join(userData, "statev2.sqlite"),
		AttachmentsDir: filepath.Join(userData, "attachments"),
		RuntimePath:    filepath.Join(userData, "server-runtime.json"),
	}
	info, err := os.Stat(inst.DBPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Instance{}, fmt.Errorf("%s has no T3 Code state database (%s)", baseDir, inst.DBPath)
		}
		return Instance{}, err
	}
	if !info.Mode().IsRegular() {
		return Instance{}, fmt.Errorf("%s is not a regular file", inst.DBPath)
	}
	return inst, nil
}

// ExpandHome replaces a leading ~ or ~/ with the current user's home directory.
func ExpandHome(path string) (string, error) {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if path == "~" {
		return home, nil
	}
	return filepath.Join(home, path[2:]), nil
}

// SameDatabase reports whether a and b resolve to the same state database.
func SameDatabase(a, b Instance) bool {
	ai, errA := os.Stat(a.DBPath)
	bi, errB := os.Stat(b.DBPath)
	if errA != nil || errB != nil {
		return a.DBPath == b.DBPath
	}
	return os.SameFile(ai, bi)
}

// Activity reports why a server appears to be using an instance.
type Activity struct {
	// Reasons are evidence of a running server; any reason blocks a move.
	Reasons []string
	// Notes describe checks that could not run.
	Notes []string
}

// Running reports whether any evidence of a running server was found.
func (a Activity) Running() bool {
	return len(a.Reasons) > 0
}

type runtimeState struct {
	PID int `json:"pid"`
}

// Activity checks server-runtime.json for a live pid and, where /proc exists,
// looks for processes holding the state database open.
func (i Instance) Activity() Activity {
	var activity Activity
	data, err := os.ReadFile(i.RuntimePath)
	switch {
	case err == nil:
		var state runtimeState
		if jsonErr := json.Unmarshal(data, &state); jsonErr != nil || state.PID <= 0 {
			activity.Reasons = append(activity.Reasons,
				"server-runtime.json exists but has no readable pid; remove it if no server is running")
			break
		}
		alive, aliveErr := processAlive(state.PID)
		switch {
		case aliveErr != nil:
			activity.Notes = append(activity.Notes, "could not check pid "+strconv.Itoa(state.PID)+": "+aliveErr.Error())
		case alive:
			activity.Reasons = append(activity.Reasons,
				fmt.Sprintf("server-runtime.json names live pid %d", state.PID))
		}
	case !errors.Is(err, os.ErrNotExist):
		activity.Notes = append(activity.Notes, "could not read server-runtime.json: "+err.Error())
	}

	pids, err := openers(i.DBPath)
	if err != nil {
		activity.Notes = append(activity.Notes, err.Error())
	}
	for _, pid := range pids {
		activity.Reasons = append(activity.Reasons, fmt.Sprintf("process %d has the state database open", pid))
	}
	return activity
}

// openers returns processes other than this one with dbPath, or its WAL or
// shared-memory file, open. It needs /proc and silently skips processes it may
// not inspect.
func openers(dbPath string) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("open-file check skipped: /proc is unavailable on this system")
		}
		return nil, fmt.Errorf("open-file check skipped: %w", err)
	}
	targets := map[string]bool{}
	paths := []string{dbPath}
	if real, err := filepath.EvalSymlinks(dbPath); err == nil && real != dbPath {
		paths = append(paths, real)
	}
	for _, p := range paths {
		targets[p] = true
		targets[p+"-wal"] = true
		targets[p+"-shm"] = true
	}

	self := os.Getpid()
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		fdDir := filepath.Join("/proc", entry.Name(), "fd")
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue
		}
		for _, fd := range fds {
			link, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
			if err != nil {
				continue
			}
			if targets[link] {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids, nil
}
