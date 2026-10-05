package move

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/alcxyz/t3rry/internal/instance"
	"github.com/alcxyz/t3rry/internal/store"
)

// Write prints the plan for people.
func (p *Plan) Write(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "from %s  (%s; %s)\n", p.From.BaseDir, schemaSummary(p.FromSchema), activitySummary(p.FromActivity))
	fmt.Fprintf(&b, "to   %s  (%s; %s)\n", p.To.BaseDir, schemaSummary(p.ToSchema), activitySummary(p.ToActivity))
	writeList(&b, "", "blocked", p.Blockers)
	writeList(&b, "", "warning", p.Warnings)

	for _, pp := range p.Projects {
		fmt.Fprintf(&b, "\nproject %s\n", pp.RealPath)
		fmt.Fprintf(&b, "  source   %s  %s\n", pp.SourceID, pp.SourceRoot)
		switch {
		case pp.TargetID != "":
			fmt.Fprintf(&b, "  target   %s  %s\n", pp.TargetID, pp.TargetRoot)
		case pp.AddCommand != "":
			fmt.Fprintf(&b, "  target   missing; run: %s\n", pp.AddCommand)
		default:
			fmt.Fprintf(&b, "  target   ambiguous\n")
		}
		fmt.Fprintf(&b, "  threads  %d", pp.Threads)
		var notes []string
		if pp.LineageAdded > 0 {
			notes = append(notes, fmt.Sprintf("%d added by lineage", pp.LineageAdded))
		}
		if pp.DeletedIncluded > 0 {
			notes = append(notes, fmt.Sprintf("%d deleted", pp.DeletedIncluded))
		}
		if pp.DeletedSkipped > 0 {
			notes = append(notes, fmt.Sprintf("%d deleted skipped", pp.DeletedSkipped))
		}
		if pp.AlreadyMoved > 0 {
			notes = append(notes, fmt.Sprintf("%d already in target", pp.AlreadyMoved))
		}
		if len(notes) > 0 {
			fmt.Fprintf(&b, " (%s)", strings.Join(notes, ", "))
		}
		fmt.Fprintln(&b)
		fmt.Fprintf(&b, "  events   %d\n", pp.Events)
		fmt.Fprintf(&b, "  attachments %d (%s)\n", pp.Attachments, byteSize(pp.AttachmentBytes))
		fmt.Fprintf(&b, "  duplicates to soft-delete %d\n", pp.Duplicates)
		if pp.ScheduledTasks > 0 {
			fmt.Fprintf(&b, "  scheduled tasks %d\n", pp.ScheduledTasks)
		}
		fmt.Fprintf(&b, "  archive in source %d\n", pp.Archive)
		writeList(&b, "  ", "blocked", pp.Blockers)
		writeList(&b, "  ", "warning", pp.Warnings)
	}

	if len(p.Unmatched) > 0 {
		fmt.Fprintf(&b, "\n%d source project(s) have no target project and are not selected:\n", len(p.Unmatched))
		roots := append([]string(nil), p.Unmatched...)
		sort.Strings(roots)
		for _, root := range roots {
			fmt.Fprintf(&b, "  %s\n", root)
		}
	}

	fmt.Fprintln(&b)
	switch {
	case p.Blocked():
		fmt.Fprintln(&b, "result: blocked")
	case len(p.Projects) == 0 || len(p.threads) == 0 && !p.sourcePending():
		fmt.Fprintln(&b, "result: nothing to move")
	case len(p.threads) == 0:
		fmt.Fprintf(&b, "result: ready to finish source cleanup of already moved threads "+
			"(%d to archive, %d scheduled task(s) to disable, %d session binding(s) to detach)\n",
			len(p.archive), p.sourceTasks, p.sourceSessions)
	default:
		fmt.Fprintf(&b, "result: ready to move %d thread(s) from %d project(s)\n", len(p.threads), len(p.Projects))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// Write prints a completed move.
func (r *Result) Write(w io.Writer) error {
	var b strings.Builder
	if r.BackupDir != "" {
		fmt.Fprintf(&b, "backup     %s\n", r.BackupDir)
	}
	tables := make([]string, 0, len(r.Rows))
	for table, n := range r.Rows {
		if n > 0 {
			tables = append(tables, table)
		}
	}
	sort.Strings(tables)
	for _, table := range tables {
		fmt.Fprintf(&b, "copied     %6d  %s\n", r.Rows[table], table)
	}
	fmt.Fprintf(&b, "attachments copied %d\n", r.AttachmentsCopied)
	fmt.Fprintf(&b, "duplicates soft-deleted %d\n", r.Duplicates)
	if r.SourceError != nil {
		fmt.Fprintf(&b, "source NOT finished: %v\n", r.SourceError)
		fmt.Fprintln(&b, "  the target move is committed; archive the moved threads in the source server by hand")
	} else {
		fmt.Fprintf(&b, "archived in source %d\n", r.Archived)
		if r.TasksDisabled > 0 {
			fmt.Fprintf(&b, "scheduled tasks disabled in source %d\n", r.TasksDisabled)
		}
	}
	for _, line := range r.Verification {
		fmt.Fprintf(&b, "verify     %s\n", line)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteCheck prints the result of `t3rry check`.
func WriteCheck(w io.Writer, inst instance.Instance, report store.Report, activity instance.Activity) error {
	var b strings.Builder
	fmt.Fprintf(&b, "base dir   %s\n", inst.BaseDir)
	fmt.Fprintf(&b, "database   %s\n", inst.DBPath)
	fmt.Fprintf(&b, "schema     %s\n", schemaSummary(report))
	if report.Schema != nil || report.ProjectionVersion != 0 {
		fmt.Fprintf(&b, "projection version %d, cursor %d, newest thread event %d\n",
			report.ProjectionVersion, report.ProjectionSequence, report.EventSequence)
	}
	fmt.Fprintf(&b, "server     %s\n", activitySummary(activity))
	writeList(&b, "", "problem", report.Problems)
	writeList(&b, "", "problem", activity.Reasons)
	writeList(&b, "", "warning", report.Warnings)
	writeList(&b, "", "note", activity.Notes)
	_, err := io.WriteString(w, b.String())
	return err
}

func schemaSummary(r store.Report) string {
	switch {
	case r.OK():
		return fmt.Sprintf("schema %d supported", r.MaxMigration)
	case r.MaxMigration > 0:
		return fmt.Sprintf("schema %d NOT supported", r.MaxMigration)
	default:
		return "schema unknown"
	}
}

func activitySummary(a instance.Activity) string {
	if a.Running() {
		return "server RUNNING"
	}
	return "server stopped"
}

func writeList(b *strings.Builder, indent, label string, items []string) {
	for _, item := range items {
		fmt.Fprintf(b, "%s%s: %s\n", indent, label, item)
	}
}

func byteSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	for _, suffix := range []string{"KiB", "MiB", "GiB"} {
		value /= unit
		if value < unit || suffix == "GiB" {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%d B", n)
}
