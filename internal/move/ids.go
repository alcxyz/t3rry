package move

import (
	"crypto/rand"
	"fmt"
	"strings"
	"time"
)

// commandPrefix marks the command ids of events t3rry appends, so a later run
// can tell its own source events from user activity.
const commandPrefix = "server:t3rry-move:"

// threadSnapshotEventTypes are the v2 event types whose payload is the full
// app thread, including projectId (OrchestrationV2DomainEvent upstream).
var threadSnapshotEventTypes = []string{
	"thread.created",
	"thread.archived",
	"thread.unarchived",
	"thread.deleted",
	"thread.settled",
	"thread.unsettled",
	"thread.snoozed",
	"thread.unsnoozed",
	"thread.pinned",
	"thread.auto-settle-set",
	"thread.unpinned",
	"thread.pin-reordered",
	"thread.active-reordered",
	"thread.visited",
	"thread.marked-unread",
	"thread.metadata-updated",
	"thread.pull-request-synced",
	"thread.runtime-mode-updated",
	"thread.interaction-mode-updated",
	"thread.model-selection-updated",
	"thread.provider-switched",
}

// activeRunStatuses are run states the server still drives.
var activeRunStatuses = []string{"preparing", "queued", "starting", "running", "waiting"}

// timestamp formats t like JavaScript's Date.toISOString, which T3 Code uses
// for every stored DateTimeUtc.
func timestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// newUUID returns a random RFC 4122 version 4 UUID.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// newCommandID returns the command id for one t3rry write.
func newCommandID() (string, error) {
	id, err := newUUID()
	if err != nil {
		return "", err
	}
	return commandPrefix + id, nil
}

// newEventID mirrors the server's IdAllocator event ids:
// event:thread:<threadId>:command:<commandId>:<uuid>, parts URI-encoded.
func newEventID(threadID, commandID string) (string, error) {
	id, err := newUUID()
	if err != nil {
		return "", err
	}
	return "event:thread:" + encodeURIComponent(threadID) + ":command:" + encodeURIComponent(commandID) + ":" + id, nil
}

// encodeURIComponent matches JavaScript's encodeURIComponent.
func encodeURIComponent(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*'()"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// sqlList returns a parenthesized list of SQL string literals.
func sqlList(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

// quoteIdent quotes a SQL identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// shellQuote quotes s for display in a copyable shell command.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	safe := true
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune("-_./~:@%+=,", r) {
			safe = false
			break
		}
	}
	if safe {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
