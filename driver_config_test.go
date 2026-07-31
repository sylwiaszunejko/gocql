//go:build unit
// +build unit

package gocql

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

// TestControlConnConfigIsMarked pins the wiring that makes the control connection,
// and only the control connection, report the driver configuration. Every path
// which (re)establishes the long-lived control connection must go through
// Session.controlConnConfig. discoverProtocol's throwaway probe connections are a
// deliberate exception, documented on controlConnConfig itself.
func TestControlConnConfigIsMarked(t *testing.T) {
	s := &Session{connCfg: &ConnConfig{}}

	cfg := s.controlConnConfig()
	if !cfg.isControlConn {
		t.Error("expected the control connection config to be marked as such")
	}
	if !cfg.disableCoalesce {
		t.Error("expected the control connection config to disable write coalescing")
	}
	if s.connCfg.isControlConn || s.connCfg.disableCoalesce {
		t.Error("expected the session-wide connection config to be left untouched")
	}
}

func TestDriverConfigReporterStartupOptions(t *testing.T) {
	reporter := newDriverConfigReporter(&Session{logger: &defaultLogger{}})

	opts := map[string]string{}
	reporter.updateStartupOptions(opts)

	if got, want := opts[driverConfigStartupKey], `{"version":1}`; got != want {
		t.Errorf("expected %s to be %q, got %q", driverConfigStartupKey, want, got)
	}
}

// TestDriverConfigReportingStartupFrame checks what actually reaches the wire
// for the connections of a session pool.
func TestDriverConfigReportingStartupFrame(t *testing.T) {
	// Enough connections that the absence asserted below is a claim about the
	// pool rather than about a single sample: only the first connection is
	// opened during session initialization, so a test that did not set
	// NumConns and wait for the pool would routinely observe just that one.
	const connsPerSession = 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu       sync.Mutex
		configs  []string
		startups int
	)

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			if f.header.Op != frm.OpStartup {
				return
			}
			// Consuming the frame body here is only safe because the fake
			// server does not read the body of a STARTUP request.
			opts := readStartupOptions(t, f)

			mu.Lock()
			defer mu.Unlock()
			startups++
			if cfg, ok := opts[driverConfigStartupKey]; ok {
				configs = append(configs, cfg)
			}
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = connsPerSession
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	// A connection only joins the pool once its handshake has completed, which
	// is strictly after the server has run recvHook on its STARTUP, so waiting
	// here is what makes the whole pool visible to the snapshot below.
	if err := waitForPoolSize(session, connsPerSession, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Without this, the absence asserted below would hold just as well on a
	// run that observed no STARTUP frame at all.
	if startups < connsPerSession {
		t.Fatalf("expected the whole pool to be observed: got %d STARTUP frames for %d connections", startups, connsPerSession)
	}

	// The control connection is disabled by testCluster, so no connection
	// of this session may report DRIVER_CONFIG.
	if len(configs) != 0 {
		t.Errorf("expected no %s outside of the control connection, got %v", driverConfigStartupKey, configs)
	}
}

// TestDriverConfigReportingDial exercises both sides of the gate in Conn.init
// which decides that a connection reports DRIVER_CONFIG, by dialing the fake
// server by hand with each of the two ConnConfigs a session builds.
//
// Neither side is covered elsewhere in the unit tests.
// TestDriverConfigReporterStartupOptions calls updateStartupOptions directly,
// bypassing ConnConfig entirely, so it says nothing about which connections
// hold a reporter. TestDriverConfigReportingStartupFrame runs with
// disableControlConn true, which leaves "a connection marked as the control
// connection puts DRIVER_CONFIG on the wire" to the integration test, and that
// skips on any server without client_options; and while it does assert the
// absence over a pool of regular connections, it never exercises the gate
// against a config it could have decided the other way on.
func TestDriverConfigReportingDial(t *testing.T) {
	tests := []struct {
		name string
		// connConfig picks the ConnConfig to dial with: the one every path
		// (re)establishing the control connection goes through, or the
		// session-wide one every pool connection is dialed with.
		connConfig  func(*Session) *ConnConfig
		wantConfigs []string
	}{
		{
			name:        "control connection",
			connConfig:  (*Session).controlConnConfig,
			wantConfigs: []string{`{"version":1}`},
		},
		{
			name:        "regular connection",
			connConfig:  func(s *Session) *ConnConfig { return s.connCfg },
			wantConfigs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var (
				mu       sync.Mutex
				configs  []string
				startups int
			)

			srv := newTestServerOpts{
				addr:     "127.0.0.1:0",
				protocol: defaultProto,
				recvHook: func(f *framer) {
					if f.header.Op != frm.OpStartup {
						return
					}
					// Consuming the frame body here is only safe because the
					// fake server does not read the body of a STARTUP request.
					opts := readStartupOptions(t, f)

					mu.Lock()
					defer mu.Unlock()
					startups++
					if cfg, ok := opts[driverConfigStartupKey]; ok {
						configs = append(configs, cfg)
					}
				},
			}.newServer(t, ctx)
			defer srv.Stop()

			// disableControlConn, set by testCluster, keeps the session's own
			// control connection out of the way, so any DRIVER_CONFIG captured
			// below can only have come from the connection dialed by hand. A
			// single pool connection, waited for below, keeps the STARTUP count
			// deterministic: the rest of a larger pool is filled
			// asynchronously and could land a frame mid-test.
			cluster := testCluster(defaultProto, srv.Address)
			cluster.NumConns = 1
			session, err := cluster.CreateSession()
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()

			if err := waitForPoolSize(session, 1, 10*time.Second); err != nil {
				t.Fatal(err)
			}

			hosts := session.GetHosts()
			if len(hosts) == 0 {
				t.Fatal("expected at least one host in the session")
			}

			mu.Lock()
			startupsBeforeDial := startups
			mu.Unlock()

			conn, err := session.dial(session.ctx, hosts[0], tt.connConfig(session), connErrorHandlerFn(func(*Conn, error, bool) {}))
			if err != nil {
				t.Fatal(err)
			}
			// This connection only exists to observe what reaches the wire, so
			// it is discarded right away without calling
			// conn.finalizeConnection, the same way
			// controlConn.discoverProtocol does for its throwaway connections.
			conn.Close()

			mu.Lock()
			defer mu.Unlock()

			// The connection dialed above is the only one the assertion below
			// is about, so pin its STARTUP frame down: without this the
			// "regular connection" case would pass just as well on a frame
			// that never reached the server.
			if got := startups - startupsBeforeDial; got != 1 {
				t.Fatalf("expected the connection dialed by hand to send exactly one STARTUP frame, got %d", got)
			}
			if !slices.Equal(configs, tt.wantConfigs) {
				t.Errorf("expected %s %q on a %s, got %q", driverConfigStartupKey, tt.wantConfigs, tt.name, configs)
			}
		})
	}
}

// TestDriverConfigReportingDisabled pins DisableDriverConfigReporting on the
// wire: with the option set, not even a connection dialed with the config that
// marks it as the control connection reports DRIVER_CONFIG, while SESSION_ID,
// which the option is documented not to affect, is still reported by every
// connection.
//
// Nothing else in the tree sets the option, so without this test the guard in
// newSessionCommon could be deleted, or inverted, and every test would still
// pass.
func TestDriverConfigReportingDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu       sync.Mutex
		startups []map[string]string
	)

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		recvHook: func(f *framer) {
			if f.header.Op != frm.OpStartup {
				return
			}
			// Consuming the frame body here is only safe because the fake
			// server does not read the body of a STARTUP request.
			opts := readStartupOptions(t, f)

			mu.Lock()
			defer mu.Unlock()
			startups = append(startups, opts)
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.DisableDriverConfigReporting = true
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if session.driverConfigReporter != nil {
		t.Error("expected a session with reporting disabled to hold no reporter")
	}

	if err := waitForPoolSize(session, 1, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	hosts := session.GetHosts()
	if len(hosts) == 0 {
		t.Fatal("expected at least one host in the session")
	}

	// Dial with the config that reports when the option is left at its
	// default, so that the option is the only reason DRIVER_CONFIG is absent
	// below rather than the connection simply not being a control connection.
	conn, err := session.dial(session.ctx, hosts[0], session.controlConnConfig(), connErrorHandlerFn(func(*Conn, error, bool) {}))
	if err != nil {
		t.Fatal(err)
	}
	// This connection only exists to observe what reaches the wire, so it is
	// discarded right away without calling conn.finalizeConnection, the same
	// way controlConn.discoverProtocol does for its throwaway connections.
	conn.Close()

	mu.Lock()
	defer mu.Unlock()

	// Without this the absence asserted below would hold just as well on a run
	// that observed no STARTUP frame at all.
	if len(startups) == 0 {
		t.Fatal("expected at least one STARTUP frame to be observed")
	}
	for i, opts := range startups {
		if config, ok := opts[driverConfigStartupKey]; ok {
			t.Errorf("STARTUP %d: expected no %s when reporting is disabled, got %q", i, driverConfigStartupKey, config)
		}
		if got := opts[sessionIDStartupKey]; got != session.ID() {
			t.Errorf("STARTUP %d: expected %s %q to be reported whatever the option is set to, got %q",
				i, sessionIDStartupKey, session.ID(), got)
		}
	}
}

// readStartupOptions decodes the string map carried by a STARTUP frame body.
// The framer read helpers panic on a truncated buffer and this runs on the fake
// server's read goroutine, so a malformed frame is turned into a test failure
// instead of taking the whole test binary down.
func readStartupOptions(t *testing.T, f *framer) (opts map[string]string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("malformed STARTUP frame: %v", r)
			opts = nil
		}
	}()

	opts = make(map[string]string)
	for n := f.readShort(); n > 0; n-- {
		key := f.readString()
		opts[key] = f.readString()
	}
	return opts
}
