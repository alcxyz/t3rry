package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunArguments(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		code   int
		stdout string
	}{
		{name: "version", args: []string{"version"}, stdout: "supported T3 Code schemas: 56"},
		{name: "help", args: []string{"help"}, stdout: "t3rry [plan]"},
		{name: "unknown command", args: []string{"frobnicate"}, code: 2},
		{name: "plan needs both dirs", args: []string{"--from", "/x"}, code: 2},
		{name: "move needs --yes", args: []string{"move", "--from", "/x", "--to", "/y"}, code: 2},
		{name: "check needs base dir", args: []string{"check"}, code: 2},
		{name: "missing base dir", args: []string{"plan", "--from", "/nonexistent-t3rry", "--to", "/nonexistent-t3rry-2"}, code: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(context.Background(), test.args, &stdout, &stderr)
			code := 0
			if err != nil {
				code = 1
				var exit *exitError
				if errors.As(err, &exit) {
					code = exit.code
				}
			}
			if code != test.code {
				t.Fatalf("exit code = %d (%v), want %d", code, err, test.code)
			}
			if !strings.Contains(stdout.String(), test.stdout) {
				t.Fatalf("stdout lacks %q:\n%s", test.stdout, stdout.String())
			}
		})
	}
}
