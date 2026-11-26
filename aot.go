package pgmem

import (
	"fmt"

	"github.com/shibukawa/pgmem/internal/aot"
	"github.com/shibukawa/pgmem/internal/engine"
)

// The backend is Go code generated ahead of time by wasm2go from the
// PostgreSQL wasm module; no wasm runtime is involved at run time.
func init() {
	engine.PostgresFactory = &aot.Factory{}
}

// SetMemory sets the address space every PostgreSQL process reserves, in MiB:
// heapMiB of private heap plus shmMiB for the shared segments (the defaults
// are 512 and 512). Only touched pages are resident, but a Linux before 5.2
// with vm.overcommit_memory=0 refuses a mapping larger than the memory it
// counts as free, so a smaller reservation avoids "mapping the data image
// failed" on a loaded host. Call it before Start; values outside 64..1792
// (heap) and 16..1792 (shm), or beyond 2 GiB together, are rejected.
func SetMemory(heapMiB, shmMiB int) error {
	if heapMiB < 64 || heapMiB > 1792 || shmMiB < 16 || shmMiB > 1792 || heapMiB+shmMiB > 2048 {
		return fmt.Errorf("pgmem: heap %d MiB and shm %d MiB out of range", heapMiB, shmMiB)
	}
	heap, shm := uint32(heapMiB)<<20, uint32(shmMiB)<<20
	engine.SetSharedWindow(heap, heap+shm-0x10000)
	aot.SetMemoryMax(int(heap + shm))
	return nil
}
