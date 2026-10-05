//go:build !unix

package instance

import "errors"

// processAlive cannot check pids on this platform.
func processAlive(int) (bool, error) {
	return false, errors.New("pid liveness checks are not supported on this platform")
}
