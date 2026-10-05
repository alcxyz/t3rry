// Package move plans and performs offline moves of T3 Code projects between
// two server base directories, as decided in ADR-001: copy the threads' v2
// events and projection rows, remap their project, and advance the projection
// cursor inside one target transaction.
package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

// ErrBlocked reports a plan with blockers.
var ErrBlocked = errors.New("move is blocked")

// Options select what to move and how.
type Options struct {
	From, To instance.Instance
	// Projects are source or target workspace roots; empty selects every
	// source project with a target match.
	Projects []string
	// IncludeDeleted also moves deleted threads of the selected projects.
	IncludeDeleted bool
	// KeepDuplicates leaves target threads imported from the same provider
	// sessions untouched.
	KeepDuplicates bool
	// ArchiveSource archives moved threads in the source after the target
	// commit.
	ArchiveSource bool
	// BackupDir receives the target backup; empty uses
	// <to>/userdata/t3rry-backups/<timestamp>.
	BackupDir string
	// Now is the clock; nil uses time.Now.
	Now func() time.Time
}

func (o Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// Plan describes a move without performing it.
type Plan struct {
	From, To                 instance.Instance
	FromActivity, ToActivity instance.Activity
	FromSchema, ToSchema     store.Report
	Projects                 []*ProjectPlan
	// Unmatched lists source workspace roots without a target project when
	// no project was selected explicitly.
	Unmatched []string
	Blockers  []string
	Warnings  []string

	schema      *store.Schema
	threads     []string
	archive     []string
	duplicates  []string
	sessions    []sessionCopy
	attachments []attachmentFile
	counts      map[string]int64
}

// ProjectPlan is the part of a plan for one source project.
type ProjectPlan struct {
	SourceID   string
	SourceRoot string
	RealPath   string
	TargetID   string
	TargetRoot string
	// AddCommand creates the missing target project.
	AddCommand string

	Threads         int
	LineageAdded    int
	DeletedSkipped  int
	DeletedIncluded int
	AlreadyMoved    int
	Archive         int
	Events          int64
	Attachments     int
	AttachmentBytes int64
	Duplicates      int
	ScheduledTasks  int
	Blockers        []string
	Warnings        []string
}

// Blocked reports whether anything prevents the move.
func (p *Plan) Blocked() bool {
	if len(p.Blockers) > 0 {
		return true
	}
	for _, project := range p.Projects {
		if len(project.Blockers) > 0 {
			return true
		}
	}
	return false
}

// ThreadCount returns the number of threads the move copies.
func (p *Plan) ThreadCount() int {
	return len(p.threads)
}

// Result reports a completed move.
type Result struct {
	BackupDir         string
	Rows              map[string]int64
	Duplicates        int
	AttachmentsCopied int
	Archived          int
	TasksDisabled     int
	// SourceError is set when the target commit succeeded but finishing the
	// source failed.
	SourceError  error
	Verification []string
}

// Analyze builds a plan without writing to either database.
func Analyze(ctx context.Context, opts Options) (*Plan, error) {
	if instance.SameDatabase(opts.From, opts.To) {
		return nil, errors.New("--from and --to use the same state database")
	}
	conn, closeAll, err := openPair(ctx, opts, true)
	if err != nil {
		return nil, err
	}
	defer closeAll()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return nil, err
	}
	defer store.Rollback(conn)

	plan := newPlan(opts)
	if err := analyze(ctx, conn, opts, plan); err != nil {
		return nil, err
	}
	return plan, nil
}

// Run plans the move and, when nothing blocks it, backs up the target, copies
// the threads in one target transaction, copies attachments and finally
// archives the moved threads in the source.
func Run(ctx context.Context, opts Options) (*Plan, *Result, error) {
	plan, err := Analyze(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	if plan.Blocked() {
		return plan, nil, ErrBlocked
	}
	if len(plan.threads) == 0 && len(plan.archive) == 0 {
		return plan, &Result{}, nil
	}

	now := opts.now()
	result := &Result{Rows: map[string]int64{}}
	result.BackupDir = opts.BackupDir
	if result.BackupDir == "" {
		result.BackupDir = filepath.Join(opts.To.UserData, "t3rry-backups", now.UTC().Format("20060102T150405Z"))
	}
	if err := backupTarget(ctx, opts.To, result.BackupDir); err != nil {
		return plan, nil, fmt.Errorf("back up target: %w", err)
	}

	if err := requireStopped(opts); err != nil {
		return plan, result, err
	}
	conn, closeAll, err := openPair(ctx, opts, false)
	if err != nil {
		return plan, result, err
	}
	defer closeAll()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return plan, result, fmt.Errorf("lock target: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			store.Rollback(conn)
		}
	}()

	// Re-plan under the write lock so the copy matches the committed state.
	locked := newPlan(opts)
	if err := analyze(ctx, conn, opts, locked); err != nil {
		return plan, result, err
	}
	if locked.Blocked() {
		return locked, result, ErrBlocked
	}
	plan = locked

	commandID, err := newCommandID()
	if err != nil {
		return plan, result, err
	}
	if err := applyTarget(ctx, conn, plan, now, commandID, result); err != nil {
		return plan, result, fmt.Errorf("copy into target (rolled back): %w", err)
	}
	created, err := copyAttachments(plan.attachments)
	result.AttachmentsCopied = len(created)
	if err != nil {
		removeFiles(created)
		return plan, result, fmt.Errorf("copy attachments (target rolled back): %w", err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		removeFiles(created)
		return plan, result, fmt.Errorf("commit target: %w", err)
	}
	committed = true

	result.SourceError = finishSource(ctx, opts, plan, now, result)
	result.Verification = verifyTarget(ctx, opts, plan)
	return plan, result, nil
}

func newPlan(opts Options) *Plan {
	return &Plan{
		From:         opts.From,
		To:           opts.To,
		FromActivity: opts.From.Activity(),
		ToActivity:   opts.To.Activity(),
		counts:       map[string]int64{},
	}
}

func requireStopped(opts Options) error {
	for _, side := range []struct {
		name string
		inst instance.Instance
	}{{"source", opts.From}, {"target", opts.To}} {
		activity := side.inst.Activity()
		if activity.Running() {
			return fmt.Errorf("%s server appears to be running: %s", side.name, activity.Reasons[0])
		}
	}
	return nil
}

// openPair opens the target as main with the source attached read-only as
// src, on one dedicated connection.
func openPair(ctx context.Context, opts Options, readOnly bool) (*sql.Conn, func(), error) {
	db, conn, err := store.Open(ctx, opts.To.DBPath, readOnly)
	if err != nil {
		return nil, nil, fmt.Errorf("open target database: %w", err)
	}
	closeAll := func() {
		_ = conn.Close()
		_ = db.Close()
	}
	if err := store.Attach(ctx, conn, opts.From.DBPath, "src"); err != nil {
		closeAll()
		return nil, nil, fmt.Errorf("open source database: %w", err)
	}
	return conn, closeAll, nil
}

func removeFiles(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}
