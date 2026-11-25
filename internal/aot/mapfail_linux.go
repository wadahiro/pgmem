package aot

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

// mapFailureState describes the process and the host the way the kernel accounts a mapping: the process's address
// space and mappings, the memory the kernel counts as available, and the overcommit settings.
func mapFailureState() string {
	var b strings.Builder
	pick := func(path string, keys ...string) {
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(&b, "  %s: %v\n", path, err)
			return
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			for _, k := range keys {
				if strings.HasPrefix(line, k) {
					fmt.Fprintf(&b, "  %s\n", strings.Join(strings.Fields(line), " "))
					break
				}
			}
		}
	}
	pick("/proc/self/status", "VmPeak", "VmSize", "VmData", "VmRSS", "Threads")
	if data, err := os.ReadFile("/proc/self/maps"); err == nil {
		fmt.Fprintf(&b, "  mappings: %d\n", bytes.Count(data, []byte("\n")))
	}
	pick("/proc/meminfo", "MemTotal", "MemFree", "MemAvailable", "Cached", "Shmem", "CommitLimit", "Committed_AS", "SReclaimable")
	pick("/proc/self/limits", "Max address space", "Max data size", "Max open files")
	for _, p := range []string{"/proc/sys/vm/overcommit_memory", "/proc/sys/vm/overcommit_ratio", "/proc/sys/vm/max_map_count"} {
		if data, err := os.ReadFile(p); err == nil {
			fmt.Fprintf(&b, "  %s: %s\n", strings.TrimPrefix(p, "/proc/sys/"), strings.TrimSpace(string(data)))
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
