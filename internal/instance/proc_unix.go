//go:build unix

package instance

import (
	"errors"
	"syscall"
)

// processAlive reports whether pid names a running process. A process owned by
// another user still counts as alive.
func processAlive(pid int) (bool, error) {
	err := syscall.Kill(pid, 0)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	case errors.Is(err, syscall.EPERM):
		return true, nil
	default:
		return false, err
	}
}
