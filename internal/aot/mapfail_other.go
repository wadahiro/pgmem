//go:build !linux

package aot

func mapFailureState() string { return "  (not available on this platform)" }
