//go:build unit
// +build unit

package gocql

import "testing"

// TestControlConnConfigIsMarked pins the wiring that makes the control
// connection agree on its connection-specific settings everywhere it is
// (re)established. Every path which (re)establishes it must go through
// Session.controlConnConfig.
func TestControlConnConfigIsMarked(t *testing.T) {
	s := &Session{connCfg: &ConnConfig{}}

	cfg := s.controlConnConfig()
	if !cfg.disableCoalesce {
		t.Error("expected the control connection config to disable write coalescing")
	}
	if s.connCfg.disableCoalesce {
		t.Error("expected the session-wide connection config to be left untouched")
	}
}
