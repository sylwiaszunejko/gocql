//go:build unit
// +build unit

package gocql

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

// dnsResolverFunc adapts a function to the DNSResolver interface.
type dnsResolverFunc func(host string) ([]net.IP, error)

func (f dnsResolverFunc) LookupIP(host string) ([]net.IP, error) {
	return f(host)
}

type fakeControlConn struct {
	statement string
	values    []any
}

func (f *fakeControlConn) getConn() *connHost          { return nil }
func (f *fakeControlConn) awaitSchemaAgreement() error { return nil }
func (f *fakeControlConn) query(statement string, values ...any) *Iter {
	f.statement = statement
	f.values = values
	return &Iter{}
}
func (f *fakeControlConn) querySystem(statement string, values ...any) *Iter {
	return &Iter{}
}
func (f *fakeControlConn) discoverProtocol(hosts []*HostInfo) (int, error) { return 0, nil }
func (f *fakeControlConn) connect(hosts []*HostInfo) error                 { return nil }
func (f *fakeControlConn) close()                                          {}
func (f *fakeControlConn) getSession() *Session                            { return nil }
func (f *fakeControlConn) reconnect() error                                { return nil }

type testHostInfo struct {
	hostID string
}

func (t testHostInfo) HostID() string                     { return t.hostID }
func (t testHostInfo) Rack() string                       { return "" }
func (t testHostInfo) DataCenter() string                 { return "" }
func (t testHostInfo) BroadcastAddress() net.IP           { return nil }
func (t testHostInfo) ListenAddress() net.IP              { return nil }
func (t testHostInfo) RPCAddress() net.IP                 { return nil }
func (t testHostInfo) PreferredIP() net.IP                { return nil }
func (t testHostInfo) Peer() net.IP                       { return nil }
func (t testHostInfo) UntranslatedConnectAddress() net.IP { return nil }
func (t testHostInfo) Port() int                          { return 0 }
func (t testHostInfo) Partitioner() string                { return "" }
func (t testHostInfo) ClusterName() string                { return "" }
func (t testHostInfo) ScyllaShardAwarePort() uint16       { return 0 }
func (t testHostInfo) ScyllaShardAwarePortTLS() uint16    { return 0 }
func (t testHostInfo) ScyllaShardCount() int              { return 0 }

func TestMerge(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
	}

	// Same record: no change expected.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042},
	}, []string{"c1"}, nil)
	if len(m) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m))
	}

	// Updated address: record should be replaced.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a2", cqlPort: 9043},
	}, []string{"c1"}, nil)
	rec := m["h1"]["c1"]
	if rec.address != "a2" || rec.cqlPort != 9043 {
		t.Fatalf("expected record to update")
	}

	// New record: should be added.
	m = make(clientRouteMap)
	m.merge([]clientRoute{
		{connectionID: "c2", hostID: "h2", address: "a3", cqlPort: 9044},
	}, []string{"c2"}, nil)
	if len(m) != 1 {
		t.Fatalf("expected new record to be added")
	}
}

func TestClientRoutesHandlerTranslateHost(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	noHost := testHostInfo{hostID: ""}
	missingHost := testHostInfo{hostID: "missing"}

	resolver := dnsResolverFunc(func(host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.1")}, nil
	})

	handler := &ClientRoutesHandler{
		stickyRoute: make(map[string]string),
		resolver:    resolver,
		routes:      make(clientRouteMap),
	}

	res, err := handler.TranslateHost(noHost, addr)
	if err != nil {
		t.Fatalf("unexpected error for empty hostID: %v", err)
	}
	if !res.Equal(addr) {
		t.Fatalf("expected address to pass through when hostID is empty")
	}

	_, err = handler.TranslateHost(missingHost, addr)
	if err == nil {
		t.Fatalf("expected error for missing host entry")
	}

	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", cqlPort: 9042, secureCQLPort: 9142}},
	}

	handler.pickTLSPorts = false
	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Port != 9042 {
		t.Fatalf("expected non-TLS port, got %d", res.Port)
	}

	handler.pickTLSPorts = true
	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Port != 9142 {
		t.Fatalf("expected TLS port, got %d", res.Port)
	}

	errorHandler := &ClientRoutesHandler{
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			return nil, errors.New("lookup failed")
		}),
		stickyRoute: make(map[string]string),
		routes:      clientRouteMap{"h2": {"c2": {connectionID: "c2", hostID: "h2", address: "host", cqlPort: 9042}}},
	}
	_, err = errorHandler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err == nil {
		t.Fatalf("expected resolver error to bubble up")
	}
}

func TestTranslateHost_StickyRoute(t *testing.T) {
	addr := AddressPort{Address: net.ParseIP("1.1.1.1"), Port: 9042}
	resolvedIPs := map[string]net.IP{
		"addr-c1": net.ParseIP("10.0.0.1"),
		"addr-c2": net.ParseIP("10.0.0.2"),
	}
	handler := &ClientRoutesHandler{
		pickTLSPorts: false,
		stickyRoute:  make(map[string]string),
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			if ip, ok := resolvedIPs[host]; ok {
				return []net.IP{ip}, nil
			}
			return nil, fmt.Errorf("unknown host %s", host)
		}),
		routes: clientRouteMap{
			"h1": {
				"c1": {connectionID: "c1", hostID: "h1", address: "addr-c1", cqlPort: 9042},
				"c2": {connectionID: "c2", hostID: "h1", address: "addr-c2", cqlPort: 9042},
			},
		},
	}

	// First call picks an arbitrary route and records it as sticky.
	res1, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	firstIP := res1.Address

	// Determine which connection was picked so we can verify stickiness.
	var pickedConn, otherConn string
	var otherIP net.IP
	if firstIP.Equal(resolvedIPs["addr-c1"]) {
		pickedConn, otherConn, otherIP = "c1", "c2", resolvedIPs["addr-c2"]
	} else if firstIP.Equal(resolvedIPs["addr-c2"]) {
		pickedConn, otherConn, otherIP = "c2", "c1", resolvedIPs["addr-c1"]
	} else {
		t.Fatalf("unexpected IP %v", firstIP)
	}
	_ = otherConn // used implicitly via otherIP

	// Second call should stick to the same connection.
	res2, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res2.Address.Equal(firstIP) {
		t.Fatalf("expected sticky route IP %v, got %v", firstIP, res2.Address)
	}

	// Remove the picked route; sticky route should fall back to the other.
	handler.mu.Lock()
	delete(handler.routes["h1"], pickedConn)
	handler.mu.Unlock()

	res3, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res3.Address.Equal(otherIP) {
		t.Fatalf("expected fallback IP %v, got %v", otherIP, res3.Address)
	}
}

func TestGetHostPortMappingFromClusterQuery(t *testing.T) {
	tcases := []struct {
		name          string
		connectionIDs []string
		hostIDs       []string
		expectedStmt  string
		expectedVals  []any
	}{
		{
			name:         "all",
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes allow filtering",
		},
		{
			name:          "connections-only",
			connectionIDs: []string{"c1", "c2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in (?,?) allow filtering",
			expectedVals:  []any{"c1", "c2"},
		},
		{
			name:         "hosts-only",
			hostIDs:      []string{"h1"},
			expectedStmt: "select connection_id, host_id, address, port, tls_port from system.client_routes where host_id in (?) allow filtering",
			expectedVals: []any{"h1"},
		},
		{
			name:          "connections-and-hosts",
			connectionIDs: []string{"c1"},
			hostIDs:       []string{"h1", "h2"},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes where connection_id in (?) and host_id in (?,?)",
			expectedVals:  []any{"c1", "h1", "h2"},
		},
		{
			name:          "empty-slices",
			connectionIDs: []string{},
			hostIDs:       []string{},
			expectedStmt:  "select connection_id, host_id, address, port, tls_port from system.client_routes allow filtering",
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := &fakeControlConn{}
			_, err := getHostPortMappingFromCluster(ctrl, "system.client_routes", tc.connectionIDs, tc.hostIDs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ctrl.statement != tc.expectedStmt {
				t.Fatalf("statement mismatch: got %q want %q", ctrl.statement, tc.expectedStmt)
			}
			if fmt.Sprint(ctrl.values) != fmt.Sprint(tc.expectedVals) {
				t.Fatalf("values mismatch: got %v want %v", ctrl.values, tc.expectedVals)
			}
		})
	}
}

func TestMerge_DeletedHost(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
	}

	// Simulate event for (c1, h1) where query returned nothing → (c1,h1) should be removed.
	m.merge(nil, []string{"c1"}, []string{"h1"})

	if len(m) != 1 {
		t.Fatalf("expected 1 entry after pruning deleted host, got %d", len(m))
	}
	if _, ok := m["h2"]; !ok {
		t.Fatalf("expected h2 to survive")
	}
}

func TestMerge_UpdatedHost(t *testing.T) {
	m := clientRouteMap{
		"h1": {
			"c1": {connectionID: "c1", hostID: "h1", address: "old-addr", cqlPort: 9042},
			"c2": {connectionID: "c2", hostID: "h1", address: "old-addr2", cqlPort: 9042},
		},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "keep", cqlPort: 9042}},
	}

	// h1 address changed; fresh query returns new data for h1. h2 is not affected.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "new-addr", cqlPort: 9043},
		{connectionID: "c2", hostID: "h1", address: "new-addr2", cqlPort: 9043},
	}, []string{"c1", "c2"}, []string{"h1"})

	if len(m) != 2 || len(m["h1"]) != 2 {
		t.Fatalf("expected 2 hosts with h1 having 2 connections, got %d hosts", len(m))
	}
	if r := m["h1"]["c1"]; r.address != "new-addr" {
		t.Fatalf("expected c1/h1 to have new address, got %s", r.address)
	}
	if r := m["h1"]["c2"]; r.address != "new-addr2" {
		t.Fatalf("expected c2/h1 to have new address, got %s", r.address)
	}
	if r := m["h2"]["c1"]; r.address != "keep" {
		t.Fatalf("expected h2 entry to be preserved unchanged")
	}
}

func TestMerge_FullRefresh_PrunesAllStale(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
		"h3": {"c1": {connectionID: "c1", hostID: "h3", address: "a3", cqlPort: 9042}},
	}

	// Full refresh for connection c1: all entries for c1 are pruned, only h1 and h2 returned.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042},
		{connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042},
	}, []string{"c1"}, nil)

	if len(m) != 2 {
		t.Fatalf("expected 2 entries after full refresh prune, got %d", len(m))
	}
	if _, ok := m["h3"]; ok {
		t.Fatalf("expected h3 to be pruned")
	}
}

func TestPruneStickyRoutes(t *testing.T) {
	handler := &ClientRoutesHandler{
		stickyRoute: map[string]string{
			"h1": "c1",
			"h2": "c1",
			"h3": "c1",
		},
		routes: clientRouteMap{
			"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
			"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
		},
	}

	handler.pruneStickyRoutes()

	if _, ok := handler.stickyRoute["h1"]; !ok {
		t.Fatalf("expected h1 to remain in stickyRoute")
	}
	if _, ok := handler.stickyRoute["h2"]; !ok {
		t.Fatalf("expected h2 to remain in stickyRoute")
	}
	if _, ok := handler.stickyRoute["h3"]; ok {
		t.Fatalf("expected h3 to be pruned from stickyRoute")
	}
}

// TestUpdateHostPortMapping_FullRefresh_PrunesStaleEntries simulates the same
// sequence of operations that updateHostPortMapping performs (lock → Merge → unlock)
// to verify that a full refresh correctly prunes a host that disappeared.
func TestUpdateHostPortMapping_FullRefresh_PrunesStaleEntries(t *testing.T) {
	// Existing routes: h1, h2, h3.
	routes := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042}},
		"h3": {"c1": {connectionID: "c1", hostID: "h3", address: "a3", cqlPort: 9042}},
	}

	// Cluster now returns only h1 and h2 (h3 was decommissioned).
	incoming := []clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042},
		{connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9042},
	}

	routes.merge(incoming, []string{"c1"}, nil)

	if len(routes) != 2 {
		t.Fatalf("expected 2 entries after full-refresh prune, got %d", len(routes))
	}
	if _, ok := routes["h3"]; ok {
		t.Fatalf("h3 should have been pruned by full refresh")
	}
}

// newTestHandler creates a ClientRoutesHandler suitable for unit tests.
// The resolver maps IP-like hostnames to themselves (e.g. "127.0.0.1" →  127.0.0.1).
func newTestHandler(pickTLS bool) *ClientRoutesHandler {
	return &ClientRoutesHandler{
		pickTLSPorts: pickTLS,
		stickyRoute:  make(map[string]string),
		resolver: dnsResolverFunc(func(host string) ([]net.IP, error) {
			ip := net.ParseIP(host)
			if ip == nil {
				return nil, fmt.Errorf("cannot parse %q as IP", host)
			}
			return []net.IP{ip}, nil
		}),
		routes: make(clientRouteMap),
	}
}

func addrPort(ip string, port uint16) AddressPort {
	return AddressPort{Address: net.ParseIP(ip), Port: port}
}

// When routes haven't been fetched yet (empty map), TranslateHost returns an
// error for a known host. This differs from the Rust driver which falls back
// to the original address; the Go driver surfaces the error so the caller can
// decide what to do.
func TestTranslateHost_NoRoutesYet_ReturnsError(t *testing.T) {
	handler := newTestHandler(false)
	addr := addrPort("1.1.1.1", 9042)

	_, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err == nil {
		t.Fatal("expected error when no routes are present")
	}
}

// When routes exist but the specific host ID is not found, TranslateHost
// returns an error.
func TestTranslateHost_HostIDNotInRoutes(t *testing.T) {
	handler := newTestHandler(false)
	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042}},
	}
	addr := addrPort("1.1.1.1", 9042)

	_, err := handler.TranslateHost(testHostInfo{hostID: "h-missing"}, addr)
	if err == nil {
		t.Fatal("expected error when host ID is not in routes")
	}
}

// When a matching route exists and TLS is not enabled, the plaintext cqlPort
// is used.
func TestTranslateHost_ResolvesHostnameAndPort(t *testing.T) {
	handler := newTestHandler(false)
	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042, secureCQLPort: 9142}},
	}
	addr := addrPort("1.1.1.1", 19999)

	res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := addrPort("127.0.0.1", 9042)
	if !res.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, res)
	}
}

// Multiple nodes with different connection IDs and addresses are each
// translated independently.
func TestTranslateHost_MultipleNodesResolvedIndependently(t *testing.T) {
	handler := newTestHandler(false)
	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042}},
		"h2": {"c2": {connectionID: "c2", hostID: "h2", address: "127.0.0.2", cqlPort: 9043}},
	}
	addr := addrPort("1.1.1.1", 19999)

	res1, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error for h1: %v", err)
	}
	if !res1.Equal(addrPort("127.0.0.1", 9042)) {
		t.Fatalf("h1: expected 127.0.0.1:9042, got %v", res1)
	}

	res2, err := handler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err != nil {
		t.Fatalf("unexpected error for h2: %v", err)
	}
	if !res2.Equal(addrPort("127.0.0.2", 9043)) {
		t.Fatalf("h2: expected 127.0.0.2:9043, got %v", res2)
	}
}

// When TLS is enabled, the secureCQLPort is used instead of cqlPort.
func TestTranslateHost_UsesTLSPort(t *testing.T) {
	handler := newTestHandler(true)
	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042, secureCQLPort: 9142}},
	}
	addr := addrPort("1.1.1.1", 19999)

	res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9142)) {
		t.Fatalf("expected TLS port 9142, got %v", res)
	}
}

// When TLS is not enabled and cqlPort is 0 (missing), TranslateHost returns an error.
func TestTranslateHost_ErrorWhenCQLPortMissing(t *testing.T) {
	handler := newTestHandler(false)
	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 0, secureCQLPort: 9142}},
	}
	addr := addrPort("1.1.1.1", 19999)

	_, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err == nil {
		t.Fatal("expected error when cqlPort is 0")
	}
}

// When TLS is enabled and secureCQLPort is 0 (missing), TranslateHost returns an error.
func TestTranslateHost_ErrorWhenTLSPortMissing(t *testing.T) {
	handler := newTestHandler(true)
	handler.routes = clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042, secureCQLPort: 0}},
	}
	addr := addrPort("1.1.1.1", 19999)

	_, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err == nil {
		t.Fatal("expected error when secureCQLPort is 0")
	}
}

// Full lifecycle: no routes →  error; add routes →  translated; update routes →  new translation.
func TestTranslateHost_FullLifecycle(t *testing.T) {
	handler := newTestHandler(false)
	addr := addrPort("1.1.1.1", 19999)

	// Before any routes: error.
	_, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err == nil {
		t.Fatal("expected error before routes are set")
	}

	// Add routes.
	handler.mu.Lock()
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042},
	}, []string{"c1"}, nil)
	handler.mu.Unlock()

	res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9042)) {
		t.Fatalf("expected 127.0.0.1:9042, got %v", res)
	}

	// Update routes with new port.
	handler.mu.Lock()
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9043},
	}, []string{"c1"}, nil)
	handler.mu.Unlock()

	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9043)) {
		t.Fatalf("expected updated port 9043, got %v", res)
	}
}

// Non-key property updates (address, port) are reflected via both full
// replacement and partial merge.
func TestTranslateHost_NonKeyPropertyUpdatesReflected(t *testing.T) {
	handler := newTestHandler(false)
	addr := addrPort("1.1.1.1", 19999)

	// Initial state.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042},
	}, []string{"c1"}, nil)

	assertTranslation := func(expected AddressPort) {
		t.Helper()
		res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Equal(expected) {
			t.Fatalf("expected %v, got %v", expected, res)
		}
	}

	assertTranslation(addrPort("127.0.0.1", 9042))

	// Port changes via full replacement.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9043},
	}, []string{"c1"}, nil)
	assertTranslation(addrPort("127.0.0.1", 9043))

	// Address changes via full replacement.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.2", cqlPort: 9043},
	}, []string{"c1"}, nil)
	assertTranslation(addrPort("127.0.0.2", 9043))

	// Both change via full replacement.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.3", cqlPort: 9044},
	}, []string{"c1"}, nil)
	assertTranslation(addrPort("127.0.0.3", 9044))

	// Port changes via partial merge.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.3", cqlPort: 9050},
	}, []string{"c1"}, []string{"h1"})
	assertTranslation(addrPort("127.0.0.3", 9050))

	// Address changes via partial merge.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.4", cqlPort: 9050},
	}, []string{"c1"}, []string{"h1"})
	assertTranslation(addrPort("127.0.0.4", 9050))

	// Both change via partial merge.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.5", cqlPort: 9060},
	}, []string{"c1"}, []string{"h1"})
	assertTranslation(addrPort("127.0.0.5", 9060))
}

// Full replacement removes hosts absent from the snapshot. The removed host
// should no longer translate.
func TestTranslateHost_FullReplacementRemovesAbsentHosts(t *testing.T) {
	handler := newTestHandler(false)
	addr := addrPort("1.1.1.1", 19999)

	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042},
		{connectionID: "c1", hostID: "h2", address: "127.0.0.1", cqlPort: 9043},
	}, []string{"c1"}, nil)

	// Both translate.
	if _, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr); err != nil {
		t.Fatalf("h1 should translate: %v", err)
	}
	if _, err := handler.TranslateHost(testHostInfo{hostID: "h2"}, addr); err != nil {
		t.Fatalf("h2 should translate: %v", err)
	}

	// Second snapshot: only h1 remains with updated port.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9044},
	}, []string{"c1"}, nil)

	res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("h1 should still translate: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9044)) {
		t.Fatalf("h1: expected port 9044, got %v", res)
	}

	// h2 was removed →  error.
	_, err = handler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err == nil {
		t.Fatal("h2 should fail after being removed from routes")
	}
}

// Partial merge preserves hosts not mentioned in the update.
func TestTranslateHost_PartialMergePreservesUnaffectedHosts(t *testing.T) {
	handler := newTestHandler(false)
	addr := addrPort("1.1.1.1", 19999)

	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042},
		{connectionID: "c1", hostID: "h2", address: "127.0.0.1", cqlPort: 9043},
	}, []string{"c1"}, nil)

	// Partial update: only h1 changes.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9044},
	}, []string{"c1"}, []string{"h1"})

	// h1 updated.
	res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("h1 unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9044)) {
		t.Fatalf("h1: expected port 9044, got %v", res)
	}

	// h2 preserved.
	res, err = handler.TranslateHost(testHostInfo{hostID: "h2"}, addr)
	if err != nil {
		t.Fatalf("h2 unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9043)) {
		t.Fatalf("h2: expected port 9043, got %v", res)
	}
}

// Partial merge removes dangling entries: event mentions (c1, h1), (c1, h2),
// (c1, h3) but re-fetch returns only h1 (updated) and h3 under c2.
// h2 becomes dangling and should be removed.
func TestMerge_RemovesDanglingEntries(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9042}},
		"h2": {"c1": {connectionID: "c1", hostID: "h2", address: "a2", cqlPort: 9043}},
		"h3": {"c1": {connectionID: "c1", hostID: "h3", address: "a3", cqlPort: 9044}},
	}

	// Event scope: c1 for h1, h2, h3 AND c2 for h3.
	// Re-fetch returns: h1 under c1 (updated port), h3 under c2 (new connection).
	// h2 has no entry →  dangling, removed.
	// h3's c1 entry is pruned (in scope), but c2 entry is added.
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "a1", cqlPort: 9050},
		{connectionID: "c2", hostID: "h3", address: "a3", cqlPort: 9060},
	}, []string{"c1", "c2"}, []string{"h1", "h2", "h3"})

	// h1 resilient: updated to port 9050.
	if r := m["h1"]["c1"]; r.cqlPort != 9050 {
		t.Fatalf("h1: expected port 9050, got %d", r.cqlPort)
	}
	// h2 dangling: removed entirely.
	if _, ok := m["h2"]; ok {
		t.Fatal("h2 should have been removed")
	}
	// h3: c1 pruned, re-added under c2.
	if _, ok := m["h3"]["c1"]; ok {
		t.Fatal("h3/c1 should have been pruned")
	}
	if r := m["h3"]["c2"]; r.cqlPort != 9060 {
		t.Fatalf("h3/c2: expected port 9060, got %d", r.cqlPort)
	}
}

// Sticky route preference is preserved across updates when the sticky
// connection ID is still available.
func TestTranslateHost_StickyRoutePreservedAcrossUpdates(t *testing.T) {
	handler := newTestHandler(false)
	addr := addrPort("1.1.1.1", 19999)

	// Initial: h1 via c1.
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042},
	}, []string{"c1"}, nil)

	// Establish stickiness to c1.
	res, err := handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9042)) {
		t.Fatalf("expected 127.0.0.1:9042, got %v", res)
	}

	// Update: both c1 (port 9043) and c0 (port 9044) available.
	handler.mu.Lock()
	handler.routes = make(clientRouteMap)
	handler.routes.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9043},
		{connectionID: "c0", hostID: "h1", address: "127.0.0.1", cqlPort: 9044},
	}, []string{"c1", "c0"}, nil)
	handler.mu.Unlock()

	// Should stick to c1 (port 9043), not pick c0 (port 9044).
	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9043)) {
		t.Fatalf("expected sticky c1 port 9043, got %v", res)
	}

	// When sticky connection disappears via full replacement, falls back.
	handler.mu.Lock()
	handler.routes = make(clientRouteMap)
	handler.routes.merge([]clientRoute{
		{connectionID: "c0", hostID: "h1", address: "127.0.0.1", cqlPort: 9050},
	}, []string{"c0"}, nil)
	// Also prune sticky since c1 is gone from the map for h1... but the sticky
	// entry still says c1. findPreferredRoute will fail the sticky lookup and
	// fall back to the remaining route.
	handler.mu.Unlock()

	res, err = handler.TranslateHost(testHostInfo{hostID: "h1"}, addr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Equal(addrPort("127.0.0.1", 9050)) {
		t.Fatalf("expected fallback to c0 port 9050, got %v", res)
	}
}

// Cross-product artifact: merge event mentions only host_y, but re-fetch
// includes extra data for host_x. host_x's existing route must not be clobbered.
func TestMerge_CrossProductDoesNotClobberUnrelatedHost(t *testing.T) {
	m := clientRouteMap{
		"hx": {"c1": {connectionID: "c1", hostID: "hx", address: "127.0.0.1", cqlPort: 9042}},
	}

	// Event scope: c2 for hy only.
	// Re-fetch returns hy (expected) AND hx under c2 (cross-product artifact).
	// Since scope is (c2, hy), hx should not be affected.
	m.merge([]clientRoute{
		{connectionID: "c2", hostID: "hy", address: "127.0.0.1", cqlPort: 9099},
		{connectionID: "c2", hostID: "hx", address: "127.0.0.1", cqlPort: 9088},
	}, []string{"c2"}, []string{"hy"})

	// hx's c1 route must be preserved.
	if r := m["hx"]["c1"]; r.cqlPort != 9042 {
		t.Fatalf("hx/c1: expected port 9042 preserved, got %d", r.cqlPort)
	}
	// hy should have the new c2 route.
	if r := m["hy"]["c2"]; r.cqlPort != 9099 {
		t.Fatalf("hy/c2: expected port 9099, got %d", r.cqlPort)
	}
	// hx also gets c2 because the incoming data includes it and hx is not in
	// scopeHostIDs so c2 won't be pruned for hx — but it IS added via upsert.
	// This is expected: the cross-product artifact adds an extra route but
	// does not remove the existing c1 route.
	if r := m["hx"]["c2"]; r.cqlPort != 9088 {
		t.Fatalf("hx/c2: expected port 9088, got %d", r.cqlPort)
	}
}

// Deletion of one connection ID for a host should not lose the host when
// another connection ID's route is still held.
func TestMerge_DeletionDoesNotLoseHostWithOtherConnID(t *testing.T) {
	m := clientRouteMap{
		"hx": {
			"c1": {connectionID: "c1", hostID: "hx", address: "127.0.0.1", cqlPort: 9042},
			"c2": {connectionID: "c2", hostID: "hx", address: "127.0.0.1", cqlPort: 9043},
		},
	}

	// Event: c1 for hx changed (deleted in DB). Re-fetch returns nothing for c1+hx.
	m.merge(nil, []string{"c1"}, []string{"hx"})

	// c1 should be removed, but c2 should remain.
	if _, ok := m["hx"]["c1"]; ok {
		t.Fatal("hx/c1 should have been deleted")
	}
	if r := m["hx"]["c2"]; r.cqlPort != 9043 {
		t.Fatalf("hx/c2 should be preserved with port 9043, got %d", r.cqlPort)
	}
}

// Same host under two connection IDs in one event: the second iteration
// must not delete the route inserted by the first.
func TestMerge_SameHostTwoConnIDs_SecondIterationDoesNotDelete(t *testing.T) {
	m := clientRouteMap{
		"hx": {"c1": {connectionID: "c1", hostID: "hx", address: "127.0.0.1", cqlPort: 9042}},
	}

	// Event: both c1 and c2 changed for hx.
	// Re-fetch: c1 deleted, c2 created with port 9099.
	m.merge([]clientRoute{
		{connectionID: "c2", hostID: "hx", address: "127.0.0.1", cqlPort: 9099},
	}, []string{"c1", "c2"}, []string{"hx"})

	// hx should be reachable via c2.
	if _, ok := m["hx"]["c1"]; ok {
		t.Fatal("hx/c1 should have been pruned")
	}
	if r := m["hx"]["c2"]; r.cqlPort != 9099 {
		t.Fatalf("hx/c2: expected port 9099, got %d", r.cqlPort)
	}
}

// Duplicate entries in a merge event (same connection_id and host_id appearing
// multiple times in scope) should be handled correctly: the route is applied
// once and the duplicate occurrence is a no-op.
func TestMerge_DuplicateEntriesAreIdempotent(t *testing.T) {
	m := clientRouteMap{
		"h1": {"c1": {connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9042}},
	}

	// Scope lists h1 twice under c1 (simulating duplicate event entries).
	m.merge([]clientRoute{
		{connectionID: "c1", hostID: "h1", address: "127.0.0.1", cqlPort: 9099},
	}, []string{"c1", "c1"}, []string{"h1", "h1"})

	// The route should be updated, not deleted.
	if r := m["h1"]["c1"]; r.cqlPort != 9099 {
		t.Fatalf("expected port 9099, got %d", r.cqlPort)
	}
}
