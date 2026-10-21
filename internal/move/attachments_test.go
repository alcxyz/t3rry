package move

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestCopyNoClobberPreservesPermissions(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, mode := range []os.FileMode{0o400, 0o600, 0o640, 0o644} {
			t.Run(fmt.Sprintf("fallback=%t/mode=%04o", fallback, mode), func(t *testing.T) {
				source, target := newBase(t), newBase(t)
				const name = "synthetic-attachment.png"
				source.file(name, "synthetic image content")
				src := filepath.Join(source.inst.AttachmentsDir, name)
				dst := filepath.Join(target.inst.AttachmentsDir, name)
				if err := os.Chmod(src, mode); err != nil {
					t.Fatal(err)
				}
				var err error
				if fallback {
					called := false
					err = copyNoClobberWithLink(src, dst, func(_, _ string) error {
						called = true
						return syscall.EOPNOTSUPP
					})
					if !called {
						t.Fatal("hard-link attempt was not reached")
					}
				} else {
					err = copyNoClobber(src, dst)
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{src, dst} {
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != mode {
						t.Errorf("%s mode = %04o, want %04o", filepath.Base(path), info.Mode().Perm(), mode)
					}
					data, err := os.ReadFile(path)
					if err != nil || string(data) != "synthetic image content" {
						t.Errorf("attachment content differs: %v", err)
					}
				}
				entries, err := os.ReadDir(target.inst.AttachmentsDir)
				if err != nil || len(entries) != 1 || entries[0].Name() != name {
					t.Errorf("unexpected attachment files: %v, %v", entries, err)
				}
			})
		}
	}
}

func TestCopyNoClobberFallbackKeepsExistingDestination(t *testing.T) {
	source, target := newBase(t), newBase(t)
	const name = "synthetic-attachment.png"
	source.file(name, "new synthetic image")
	target.file(name, "existing synthetic image")
	src := filepath.Join(source.inst.AttachmentsDir, name)
	dst := filepath.Join(target.inst.AttachmentsDir, name)
	called := false
	err := copyNoClobberWithLink(src, dst, func(_, _ string) error {
		called = true
		return syscall.EOPNOTSUPP
	})
	if !called || err == nil {
		t.Fatal("exclusive-copy fallback should fail on an existing destination")
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "existing synthetic image" {
		t.Errorf("existing attachment changed: %v", err)
	}
	entries, err := os.ReadDir(target.inst.AttachmentsDir)
	if err != nil || len(entries) != 1 {
		t.Errorf("temporary file left behind: %v, %v", entries, err)
	}
}
