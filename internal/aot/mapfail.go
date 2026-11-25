package aot

import (
	"errors"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/shibukawa/pgmem/internal/aot/pgaot/base"
	"github.com/shibukawa/pgmem/internal/host"
)

// mapFailureReports bounds the diagnostics: the first few failures of a process tell why, the rest repeat them.
var mapFailureReports atomic.Int32

// mapImage maps one instance's linear memory. The kernel can refuse the 2 GiB mapping (ENOMEM) for a moment when
// many instances come and go at once; it is retried for a few seconds, and the first failures log the state of the
// process and the host (Linux) so that the cause can be told.
func mapImage(h *host.Host, img *base.SharedImage) ([]byte, error) {
	mem, err := img.Memory(memoryMax)
	if err == nil || !errors.Is(err, syscall.ENOMEM) {
		return mem, err
	}
	if h.Log != nil && mapFailureReports.Add(1) <= 5 {
		h.Log("pgmem: mapping the data image failed (%v); process and host state:\n%s", err, mapFailureState())
	}
	start := time.Now()
	delay := 20 * time.Millisecond
	for time.Since(start) < 3*time.Second {
		time.Sleep(delay)
		if delay < 320*time.Millisecond {
			delay *= 2
		}
		mem, err = img.Memory(memoryMax)
		if err == nil {
			if h.Log != nil {
				h.Log("pgmem: mapping the data image succeeded after %v", time.Since(start).Round(time.Millisecond))
			}
			return mem, nil
		}
		if !errors.Is(err, syscall.ENOMEM) {
			break
		}
	}
	return nil, err
}
