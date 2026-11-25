package aot

import (
	"strings"
	"testing"
)

func TestMapFailureState(t *testing.T) {
	s := mapFailureState()
	for _, want := range []string{"VmSize", "mappings:", "MemAvailable", "vm/overcommit_memory"} {
		if !strings.Contains(s, want) {
			t.Errorf("state lacks %q:\n%s", want, s)
		}
	}
	t.Log("\n" + s)
}
