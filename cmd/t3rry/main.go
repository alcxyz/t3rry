// Command t3rry moves T3 Code projects between server base directories on
// the same host while both servers are stopped.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alcxyz/t3rry/internal/buildinfo"
	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/move"
	"github.com/alcxyz/t3rry/internal/store"
)

// version is injected at build time via -ldflags "-X main.version=<version>".
// Ordinary go builds derive a development version from VCS metadata.
var version = "dev"

const usage = `t3rry moves T3 Code projects between server base directories.

Usage:
  t3rry [plan] --from <base-dir> --to <base-dir> [--project <path>]... [--include-deleted]
               [--codex-home <dir>]
  t3rry move   --from <base-dir> --to <base-dir> [--project <path>]... --yes
               [--include-deleted] [--codex-home <dir>] [--keep-duplicates]
               [--keep-subagent-imports] [--no-archive-source] [--backup-dir <dir>]
  t3rry check  --base-dir <base-dir>
  t3rry delete-imports --base-dir <base-dir> [--thread <id>]... [--yes] [--backup-dir <dir>]
  t3rry version

Both servers must be stopped. plan is read-only and is the default command.
Without --project, every source project with a matching target project is
selected; --project accepts a source or target workspace path. A move also
soft-deletes the target's imports of the moved sessions and of the Codex
subagent sessions they spawned, read from --codex-home (default $CODEX_HOME
or ~/.codex).

delete-imports lists the threads "import recent sessions" created in a base
directory, and soft-deletes the ones named with --thread when given --yes,
for imports the move's own cleanup keeps. Its server must be stopped.
`

// exitError carries a process exit status.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }

func (e *exitError) Unwrap() error { return e.err }

func main() {
	err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)
	if err == nil {
		return
	}
	code := 1
	var exit *exitError
	if errors.As(err, &exit) {
		code = exit.code
	}
	if !errors.Is(err, errSilent) {
		fmt.Fprintf(os.Stderr, "t3rry: %v\n", err)
	}
	os.Exit(code)
}

// errSilent reports a failure that was already printed.
var errSilent = errors.New("failed")

func usageError(format string, args ...any) error {
	return &exitError{code: 2, err: fmt.Errorf(format, args...)}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	command := "plan"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	switch command {
	case "plan":
		return runPlan(ctx, args, stdout, stderr, false)
	case "move":
		return runPlan(ctx, args, stdout, stderr, true)
	case "check":
		return runCheck(ctx, args, stdout, stderr)
	case "delete-imports":
		return runDeleteImports(ctx, args, stdout, stderr)
	case "version":
		_, err := fmt.Fprintf(stdout, "t3rry %s\nsupported T3 Code schemas: %s\n",
			buildinfo.Resolve(version), joinInts(store.SupportedMigrations()))
		return err
	case "help", "-h", "--help":
		_, err := fmt.Fprint(stdout, usage)
		return err
	default:
		_, _ = fmt.Fprint(stderr, usage)
		return usageError("unknown command %q", command)
	}
}

// stringList collects a repeatable flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ", ") }

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func runPlan(ctx context.Context, args []string, stdout, stderr io.Writer, write bool) error {
	name := "plan"
	if write {
		name = "move"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { _, _ = fmt.Fprint(stderr, usage) }
	from := fs.String("from", "", "source T3 Code base directory")
	to := fs.String("to", "", "target T3 Code base directory")
	var projects stringList
	fs.Var(&projects, "project", "workspace path of a project to move (repeatable)")
	includeDeleted := fs.Bool("include-deleted", false, "also move deleted threads")
	codexHome := fs.String("codex-home", "", "Codex home with session rollouts (default $CODEX_HOME or ~/.codex)")
	var yes, keepDuplicates, keepSubagentImports, noArchive *bool
	var backupDir *string
	if write {
		yes = fs.Bool("yes", false, "perform the move")
		keepDuplicates = fs.Bool("keep-duplicates", false, "do not soft-delete target threads imported from the same sessions")
		keepSubagentImports = fs.Bool("keep-subagent-imports", false,
			"do not soft-delete target threads imported from subagent sessions of moved threads")
		noArchive = fs.Bool("no-archive-source", false, "do not archive moved threads in the source")
		backupDir = fs.String("backup-dir", "", "target backup directory (default <to>/userdata/t3rry-backups/<timestamp>)")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{code: 2, err: errSilent}
	}
	if fs.NArg() > 0 {
		return usageError("unexpected argument %q", fs.Arg(0))
	}
	if *from == "" || *to == "" {
		return usageError("--from and --to are required")
	}
	if write && !*yes {
		return usageError("move writes to both servers' state; review `t3rry plan` and add --yes")
	}

	source, err := instance.Resolve(*from)
	if err != nil {
		return fmt.Errorf("--from: %w", err)
	}
	target, err := instance.Resolve(*to)
	if err != nil {
		return fmt.Errorf("--to: %w", err)
	}
	home, err := resolveCodexHome(*codexHome)
	if err != nil {
		return fmt.Errorf("--codex-home: %w", err)
	}
	opts := move.Options{
		From:           source,
		To:             target,
		Projects:       projects,
		IncludeDeleted: *includeDeleted,
		ArchiveSource:  true,
		CodexHome:      home,
	}
	if write {
		opts.KeepDuplicates = *keepDuplicates
		opts.KeepSubagentImports = *keepSubagentImports
		opts.ArchiveSource = !*noArchive
		if *backupDir != "" {
			dir, err := instance.ExpandHome(*backupDir)
			if err != nil {
				return err
			}
			opts.BackupDir = dir
		}
	}

	if !write {
		plan, err := move.Analyze(ctx, opts)
		if err != nil {
			return err
		}
		if err := plan.Write(stdout); err != nil {
			return err
		}
		if plan.Blocked() {
			return errSilent
		}
		return nil
	}

	plan, result, err := move.Run(ctx, opts)
	if plan != nil {
		if werr := plan.Write(stdout); werr != nil && err == nil {
			err = werr
		}
	}
	if result != nil {
		if _, werr := fmt.Fprintln(stdout); werr != nil && err == nil {
			err = werr
		}
		if werr := result.Write(stdout); werr != nil && err == nil {
			err = werr
		}
	}
	if errors.Is(err, move.ErrBlocked) {
		return errSilent
	}
	if err != nil {
		return err
	}
	if result != nil && result.SourceError != nil {
		return errSilent
	}
	return nil
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { _, _ = fmt.Fprint(stderr, usage) }
	baseDir := fs.String("base-dir", "", "T3 Code base directory")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{code: 2, err: errSilent}
	}
	if fs.NArg() > 0 {
		return usageError("unexpected argument %q", fs.Arg(0))
	}
	if *baseDir == "" {
		return usageError("--base-dir is required")
	}
	inst, err := instance.Resolve(*baseDir)
	if err != nil {
		return err
	}
	activity := inst.Activity()
	db, conn, err := store.Open(ctx, inst.DBPath, true)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	defer func() { _ = conn.Close() }()
	report, err := store.Check(ctx, conn, "main")
	if err != nil {
		return err
	}
	if err := move.WriteCheck(stdout, inst, report, activity); err != nil {
		return err
	}
	if !report.OK() || activity.Running() {
		return errSilent
	}
	return nil
}

// resolveCodexHome returns the Codex home to read rollouts from: the flag,
// else $CODEX_HOME, else ~/.codex.
func resolveCodexHome(flagValue string) (string, error) {
	home := flagValue
	if home == "" {
		home = os.Getenv("CODEX_HOME")
	}
	if home == "" {
		home = "~/.codex"
	}
	expanded, err := instance.ExpandHome(home)
	if err != nil {
		return "", err
	}
	return filepath.Abs(expanded)
}

func runDeleteImports(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("delete-imports", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { _, _ = fmt.Fprint(stderr, usage) }
	baseDir := fs.String("base-dir", "", "T3 Code base directory")
	var threads stringList
	fs.Var(&threads, "thread", "id of an imported thread to soft-delete (repeatable)")
	yes := fs.Bool("yes", false, "perform the deletion")
	backupDir := fs.String("backup-dir", "", "backup directory (default <base-dir>/userdata/t3rry-backups/<timestamp>)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{code: 2, err: errSilent}
	}
	if fs.NArg() > 0 {
		return usageError("unexpected argument %q", fs.Arg(0))
	}
	if *baseDir == "" {
		return usageError("--base-dir is required")
	}
	if *yes && len(threads) == 0 {
		return usageError("--yes needs at least one --thread")
	}
	base, err := instance.Resolve(*baseDir)
	if err != nil {
		return err
	}
	opts := move.ImportOptions{Base: base, Threads: threads}
	if *backupDir != "" {
		dir, err := instance.ExpandHome(*backupDir)
		if err != nil {
			return err
		}
		opts.BackupDir = dir
	}

	if !*yes {
		plan, err := move.AnalyzeImports(ctx, opts)
		if err != nil {
			return err
		}
		if err := plan.Write(stdout); err != nil {
			return err
		}
		if plan.Blocked() {
			return errSilent
		}
		return nil
	}
	plan, result, err := move.DeleteImports(ctx, opts)
	if plan != nil {
		if werr := plan.Write(stdout); werr != nil && err == nil {
			err = werr
		}
	}
	if result != nil {
		if _, werr := fmt.Fprintln(stdout); werr != nil && err == nil {
			err = werr
		}
		if werr := result.Write(stdout); werr != nil && err == nil {
			err = werr
		}
	}
	if errors.Is(err, move.ErrBlocked) {
		return errSilent
	}
	return err
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ", ")
}
