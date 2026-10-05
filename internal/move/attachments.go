package move

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// attachmentFile is one stored attachment file referenced by a moved thread.
// Files are named <attachment id><extension> in the attachments directory.
type attachmentFile struct {
	name string
	src  string
	dst  string
	size int64
	// present means the target already holds an identical file.
	present bool
}

// planAttachments finds the files behind every attachment id referenced by
// the moved threads' messages and user turn items.
func planAttachments(ctx context.Context, conn *sql.Conn, opts Options, plan *Plan, _ map[string]*sourceThread) error {
	rows, err := conn.QueryContext(ctx, `
		SELECT DISTINCT m.source_project_id, json_extract(a.value, '$.id')
		FROM src.orchestration_v2_projection_messages x
		JOIN temp.t3rry_moved m ON m.thread_id = x.thread_id,
			json_each(x.payload_json, '$.attachments') a
		WHERE json_valid(x.payload_json) AND json_type(x.payload_json, '$.attachments') = 'array'
		UNION
		SELECT DISTINCT m.source_project_id, json_extract(a.value, '$.id')
		FROM src.orchestration_v2_projection_turn_items x
		JOIN temp.t3rry_moved m ON m.thread_id = x.thread_id,
			json_each(x.payload_json, '$.attachments') a
		WHERE json_valid(x.payload_json) AND json_type(x.payload_json, '$.attachments') = 'array'
		ORDER BY 2, 1`)
	if err != nil {
		return err
	}
	type reference struct{ projectID, id string }
	var refs []reference
	for rows.Next() {
		var ref reference
		var id sql.NullString
		if err := rows.Scan(&ref.projectID, &id); err != nil {
			_ = rows.Close()
			return err
		}
		ref.id = id.String
		refs = append(refs, ref)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}

	names, err := listFiles(opts.From.AttachmentsDir)
	if err != nil {
		return fmt.Errorf("read source attachments: %w", err)
	}
	byProject := projectIndex(plan)
	planned := map[string]bool{}
	missing := map[string]int{}
	for _, ref := range refs {
		pp := byProject[ref.projectID]
		if !validAttachmentID(ref.id) {
			pp.Warnings = append(pp.Warnings, fmt.Sprintf("ignoring malformed attachment id %q", ref.id))
			continue
		}
		matches := attachmentNames(names, ref.id)
		if len(matches) == 0 {
			missing[ref.projectID]++
			continue
		}
		for _, name := range matches {
			if planned[name] {
				continue
			}
			planned[name] = true
			file, conflict, err := inspectAttachment(opts, name)
			if err != nil {
				return err
			}
			if conflict {
				pp.Blockers = append(pp.Blockers, fmt.Sprintf(
					"attachment %s exists in the target with different content", name))
				continue
			}
			pp.Attachments++
			pp.AttachmentBytes += file.size
			plan.attachments = append(plan.attachments, file)
		}
	}
	for projectID, count := range missing {
		byProject[projectID].Warnings = append(byProject[projectID].Warnings, fmt.Sprintf(
			"%d referenced attachment(s) have no file in the source; they stay missing", count))
	}
	sort.Slice(plan.attachments, func(i, j int) bool { return plan.attachments[i].name < plan.attachments[j].name })
	return nil
}

// validAttachmentID accepts ids that name a file directly inside the
// attachments directory.
func validAttachmentID(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, "/\\\x00") && !strings.HasPrefix(id, ".")
}

// attachmentNames returns the stored files for id: the id itself or the id
// followed by an extension.
func attachmentNames(names []string, id string) []string {
	var matches []string
	for _, name := range names {
		if name == id || strings.HasPrefix(name, id+".") {
			matches = append(matches, name)
		}
	}
	return matches
}

func listFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

// inspectAttachment plans one file copy between the resolved attachments
// directories, so symlinked attachment roots are read and written in place.
func inspectAttachment(opts Options, name string) (attachmentFile, bool, error) {
	file := attachmentFile{
		name: name,
		src:  filepath.Join(resolveExisting(opts.From.AttachmentsDir), name),
		dst:  filepath.Join(resolveExisting(opts.To.AttachmentsDir), name),
	}
	info, err := os.Stat(file.src)
	if err != nil {
		return file, false, err
	}
	file.size = info.Size()
	dstInfo, err := os.Stat(file.dst)
	if errors.Is(err, os.ErrNotExist) {
		return file, false, nil
	}
	if err != nil {
		return file, false, err
	}
	if !dstInfo.Mode().IsRegular() || dstInfo.Size() != file.size {
		return file, true, nil
	}
	same, err := sameContent(file.src, file.dst)
	if err != nil {
		return file, false, err
	}
	file.present = same
	return file, !same, nil
}

func sameContent(a, b string) (bool, error) {
	ha, _, err := hashFile(a)
	if err != nil {
		return false, err
	}
	hb, _, err := hashFile(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ha, hb), nil
}

func hashFile(path string) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
}

// copyAttachments copies files that the target lacks without overwriting
// anything, verifying size and SHA-256. It returns the files it created.
func copyAttachments(files []attachmentFile) ([]string, error) {
	var created []string
	for _, file := range files {
		if file.present {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(file.dst), 0o755); err != nil {
			return created, err
		}
		if err := copyNoClobber(file.src, file.dst); err != nil {
			return created, fmt.Errorf("%s: %w", file.name, err)
		}
		created = append(created, file.dst)
		srcHash, srcSize, err := hashFile(file.src)
		if err != nil {
			return created, err
		}
		dstHash, dstSize, err := hashFile(file.dst)
		if err != nil {
			return created, err
		}
		if srcSize != dstSize || !bytes.Equal(srcHash, dstHash) {
			return created, fmt.Errorf("%s: copy does not match the source", file.name)
		}
	}
	return created, nil
}

// copyNoClobber copies src to a temporary file next to dst and links it into
// place, failing if dst already exists.
func copyNoClobber(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".t3rry-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if info, err := in.Stat(); err == nil {
		_ = os.Chmod(tmpName, info.Mode().Perm())
	}
	err = os.Link(tmpName, dst)
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%s appeared in the target during the move", dst)
	}
	if err == nil {
		return nil
	}
	// Some filesystems lack hard links; create the file exclusively instead.
	return copyExclusive(tmpName, dst)
}

func copyExclusive(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	return out.Close()
}
