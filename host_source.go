/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrCannotFindHost    = errors.New("cannot find host")
	ErrHostAlreadyExists = errors.New("host already exists")
)

type nodeState int32

func (n nodeState) String() string {
	if n == NodeUp {
		return "UP"
	} else if n == NodeDown {
		return "DOWN"
	}
	return fmt.Sprintf("UNKNOWN_%d", n)
}

const (
	NodeUp nodeState = iota
	NodeDown
)

type cassVersion struct {
	Major, Minor, Patch int
	Qualifier           string
}

func (c *cassVersion) Set(v string) error {
	if v == "" {
		return nil
	}

	return c.UnmarshalCQL(nil, []byte(v))
}

func (c *cassVersion) UnmarshalCQL(info TypeInfo, data []byte) error {
	return c.unmarshal(data)
}

func (c *cassVersion) unmarshal(data []byte) error {
	v := strings.SplitN(strings.TrimPrefix(strings.TrimSuffix(string(data), "-SNAPSHOT"), "v"), ".", 3)

	if len(v) < 2 {
		return fmt.Errorf("invalid version string: %s", data)
	}

	var err error
	c.Major, err = strconv.Atoi(v[0])
	if err != nil {
		return fmt.Errorf("invalid major version %v: %v", v[0], err)
	}

	if len(v) == 2 {
		vMinor := strings.SplitN(v[1], "-", 2)
		c.Minor, err = strconv.Atoi(vMinor[0])
		if err != nil {
			return fmt.Errorf("invalid minor version %v: %v", vMinor[0], err)
		}
		if len(vMinor) == 2 {
			c.Qualifier = vMinor[1]
		}
		return nil
	}

	c.Minor, err = strconv.Atoi(v[1])
	if err != nil {
		return fmt.Errorf("invalid minor version %v: %v", v[1], err)
	}

	vPatch := strings.SplitN(v[2], "-", 2)
	c.Patch, err = strconv.Atoi(vPatch[0])
	if err != nil {
		return fmt.Errorf("invalid patch version %v: %v", vPatch[0], err)
	}
	if len(vPatch) == 2 {
		c.Qualifier = vPatch[1]
	}
	return nil
}

func (c cassVersion) Before(major, minor, patch int) bool {
	// We're comparing us (cassVersion) with the provided version (major, minor, patch)
	// We return true if our version is lower (comes before) than the provided one.
	if c.Major < major {
		return true
	} else if c.Major == major {
		if c.Minor < minor {
			return true
		} else if c.Minor == minor && c.Patch < patch {
			return true
		}
	}
	return false
}

func (c cassVersion) AtLeast(major, minor, patch int) bool {
	return !c.Before(major, minor, patch)
}

func (c cassVersion) String() string {
	if c.Qualifier != "" {
		return fmt.Sprintf("%d.%d.%d-%v", c.Major, c.Minor, c.Patch, c.Qualifier)
	}
	return fmt.Sprintf("v%d.%d.%d", c.Major, c.Minor, c.Patch)
}

func (c cassVersion) nodeUpDelay() time.Duration {
	if c.Major >= 2 && c.Minor >= 2 {
		// CASSANDRA-8236
		return 0
	}

	return 10 * time.Second
}

type HostInfo struct {
	// TODO(zariel): reduce locking maybe, not all values will change, but to ensure
	// that we are thread safe use a mutex to access all fields.
	mu                         sync.RWMutex
	hostname                   string
	peer                       net.IP
	broadcastAddress           net.IP
	listenAddress              net.IP
	rpcAddress                 net.IP
	preferredIP                net.IP
	connectAddress             net.IP
	untranslatedConnectAddress net.IP
	port                       int
	dataCenter                 string
	rack                       string
	hostId                     string
	workload                   string
	graph                      bool
	dseVersion                 string
	partitioner                string
	clusterName                string
	version                    cassVersion
	state                      nodeState
	schemaVersion              string
	tokens                     []string

	scyllaShardAwarePort    uint16
	scyllaShardAwarePortTLS uint16
}

func (h *HostInfo) Equal(host *HostInfo) bool {
	if h == host {
		// prevent rlock reentry
		return true
	}

	return h.HostID() == host.HostID() && h.ConnectAddressAndPort() == host.ConnectAddressAndPort()
}

func (h *HostInfo) Peer() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.peer
}

func (h *HostInfo) invalidConnectAddr() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	addr, _ := h.connectAddressLocked()
	return !validIpAddr(addr)
}

func validIpAddr(addr net.IP) bool {
	return addr != nil && !addr.IsUnspecified()
}

func (h *HostInfo) connectAddressLocked() (net.IP, string) {
	if validIpAddr(h.connectAddress) {
		return h.connectAddress, "connect_address"
	} else if validIpAddr(h.rpcAddress) {
		return h.rpcAddress, "rpc_adress"
	} else if validIpAddr(h.preferredIP) {
		// where does perferred_ip get set?
		return h.preferredIP, "preferred_ip"
	} else if validIpAddr(h.broadcastAddress) {
		return h.broadcastAddress, "broadcast_address"
	} else if validIpAddr(h.peer) {
		return h.peer, "peer"
	}
	return net.IPv4zero, "invalid"
}

// nodeToNodeAddress returns address broadcasted between node to nodes.
// It's either `broadcast_address` if host info is read from system.local or `peer` if read from system.peers.
// This IP address is also part of CQL Event emitted on topology/status changes,
// but does not uniquely identify the node in case multiple nodes use the same IP address.
func (h *HostInfo) nodeToNodeAddress() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if validIpAddr(h.broadcastAddress) {
		return h.broadcastAddress
	} else if validIpAddr(h.peer) {
		return h.peer
	}
	return net.IPv4zero
}

// Returns the address that should be used to connect to the host.
// If you wish to override this, use an AddressTranslator or
// use a HostFilter to SetConnectAddress()
func (h *HostInfo) ConnectAddress() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if addr, _ := h.connectAddressLocked(); validIpAddr(addr) {
		return addr
	}
	panic(fmt.Sprintf("no valid connect address for host: %v. Is your cluster configured correctly?", h))
}

// ConnectAddressWithError same as ConnectAddress, but an error instead of panic.
func (h *HostInfo) ConnectAddressWithError() (net.IP, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if addr, source := h.connectAddressLocked(); source != "invalid" {
		return addr, nil
	}
	return nil, fmt.Errorf("no valid connect address for host: %v", h)
}

func (h *HostInfo) UntranslatedConnectAddress() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.untranslatedConnectAddress) != 0 {
		return h.untranslatedConnectAddress
	}

	if addr, _ := h.connectAddressLocked(); validIpAddr(addr) {
		return addr
	}
	panic(fmt.Sprintf("no valid connect address for host: %v. Is your cluster configured correctly?", h))
}

func (h *HostInfo) SetConnectAddress(address net.IP) *HostInfo {
	// TODO(zariel): should this not be exported?
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connectAddress = address
	return h
}

func (h *HostInfo) BroadcastAddress() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.broadcastAddress
}

func (h *HostInfo) ListenAddress() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.listenAddress
}

func (h *HostInfo) RPCAddress() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rpcAddress
}

func (h *HostInfo) PreferredIP() net.IP {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.preferredIP
}

func (h *HostInfo) DataCenter() string {
	h.mu.RLock()
	dc := h.dataCenter
	h.mu.RUnlock()
	return dc
}

func (h *HostInfo) Rack() string {
	h.mu.RLock()
	rack := h.rack
	h.mu.RUnlock()
	return rack
}

func (h *HostInfo) HostID() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.hostId
}

func (h *HostInfo) SetHostID(hostID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hostId = hostID
}

func (h *HostInfo) WorkLoad() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.workload
}

func (h *HostInfo) Graph() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.graph
}

func (h *HostInfo) DSEVersion() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.dseVersion
}

func (h *HostInfo) Partitioner() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.partitioner
}

func (h *HostInfo) ClusterName() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clusterName
}

func (h *HostInfo) Version() cassVersion {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.version
}

func (h *HostInfo) State() nodeState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

func (h *HostInfo) setState(state nodeState) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state = state
	return h
}

func (h *HostInfo) Tokens() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.tokens
}

func (h *HostInfo) Port() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.port
}

func (h *HostInfo) update(from *HostInfo) {
	if h == from {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	from.mu.RLock()
	defer from.mu.RUnlock()

	// autogenerated do not update
	if h.peer == nil {
		h.peer = from.peer
	}
	if h.broadcastAddress == nil {
		h.broadcastAddress = from.broadcastAddress
	}
	if h.listenAddress == nil {
		h.listenAddress = from.listenAddress
	}
	if h.rpcAddress == nil {
		h.rpcAddress = from.rpcAddress
	}
	if h.preferredIP == nil {
		h.preferredIP = from.preferredIP
	}
	if h.connectAddress == nil {
		h.connectAddress = from.connectAddress
	}
	if h.port == 0 {
		h.port = from.port
	}
	if h.dataCenter == "" {
		h.dataCenter = from.dataCenter
	}
	if h.rack == "" {
		h.rack = from.rack
	}
	if h.hostId == "" {
		h.hostId = from.hostId
	}
	if h.workload == "" {
		h.workload = from.workload
	}
	if h.dseVersion == "" {
		h.dseVersion = from.dseVersion
	}
	if h.partitioner == "" {
		h.partitioner = from.partitioner
	}
	if h.clusterName == "" {
		h.clusterName = from.clusterName
	}
	if h.version == (cassVersion{}) {
		h.version = from.version
	}
	if h.tokens == nil {
		h.tokens = from.tokens
	}
}

func (h *HostInfo) IsUp() bool {
	return h != nil && h.State() == NodeUp
}

func (h *HostInfo) IsBusy(s *Session) bool {
	pool, ok := s.pool.getPool(h)
	return ok && h != nil && pool.InFlight() >= MAX_IN_FLIGHT_THRESHOLD
}

func (h *HostInfo) ConnectAddressAndPort() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	addr, _ := h.connectAddressLocked()
	return net.JoinHostPort(addr.String(), strconv.Itoa(h.port))
}

func (h *HostInfo) String() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	connectAddr, source := h.connectAddressLocked()
	return fmt.Sprintf("[HostInfo hostname=%q connectAddress=%q peer=%q rpc_address=%q broadcast_address=%q "+
		"preferred_ip=%q connect_addr=%q connect_addr_source=%q "+
		"port=%d data_center=%q rack=%q host_id=%q version=%q state=%s num_tokens=%d]",
		h.hostname, h.connectAddress, h.peer, h.rpcAddress, h.broadcastAddress, h.preferredIP,
		connectAddr, source,
		h.port, h.dataCenter, h.rack, h.hostId, h.version, h.state, len(h.tokens))
}

func (h *HostInfo) setScyllaSupported(s scyllaSupported) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.scyllaShardAwarePort = s.shardAwarePort
	h.scyllaShardAwarePortTLS = s.shardAwarePortSSL
}

// ScyllaShardAwarePort returns the shard aware port of this host.
// Returns zero if the shard aware port is not known.
func (h *HostInfo) ScyllaShardAwarePort() uint16 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.scyllaShardAwarePort
}

// ScyllaShardAwarePortTLS returns the TLS-enabled shard aware port of this host.
// Returns zero if the shard aware port is not known.
func (h *HostInfo) ScyllaShardAwarePortTLS() uint16 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.scyllaShardAwarePortTLS
}

// Returns true if we are using system_schema.keyspaces instead of system.schema_keyspaces
func checkSystemSchema(control controlConnection) (bool, error) {
	iter := control.query("SELECT * FROM system_schema.keyspaces" + control.getSession().usingTimeoutClause)
	if err := iter.err; err != nil {
		if errf, ok := err.(*errorFrame); ok {
			if errf.code == ErrCodeSyntax {
				return false, nil
			}
		}

		return false, err
	}

	return true, nil
}

// Given a map that represents a row from either system.local or system.peers
// return as much information as we can in *HostInfo
func hostInfoFromMap(row map[string]interface{}, host *HostInfo, translateAddressPort func(addr net.IP, port int) (net.IP, int)) (*HostInfo, error) {
	const assertErrorMsg = "Assertion failed for %s"
	var ok bool

	// Default to our connected port if the cluster doesn't have port information
	for key, value := range row {
		switch key {
		case "data_center":
			host.dataCenter, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "data_center")
			}
		case "rack":
			host.rack, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "rack")
			}
		case "host_id":
			hostId, ok := value.(UUID)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "host_id")
			}
			host.hostId = hostId.String()
		case "release_version":
			version, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "release_version")
			}
			host.version.Set(version)
		case "peer":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "peer")
			}
			host.peer = net.ParseIP(ip)
		case "cluster_name":
			host.clusterName, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "cluster_name")
			}
		case "partitioner":
			host.partitioner, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "partitioner")
			}
		case "broadcast_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "broadcast_address")
			}
			host.broadcastAddress = net.ParseIP(ip)
		case "preferred_ip":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "preferred_ip")
			}
			host.preferredIP = net.ParseIP(ip)
		case "rpc_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "rpc_address")
			}
			host.rpcAddress = net.ParseIP(ip)
		case "native_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "native_address")
			}
			host.rpcAddress = net.ParseIP(ip)
		case "listen_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "listen_address")
			}
			host.listenAddress = net.ParseIP(ip)
		case "native_port":
			native_port, ok := value.(int)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "native_port")
			}
			host.port = native_port
		case "workload":
			host.workload, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "workload")
			}
		case "graph":
			host.graph, ok = value.(bool)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "graph")
			}
		case "tokens":
			host.tokens, ok = value.([]string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "tokens")
			}
		case "dse_version":
			host.dseVersion, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "dse_version")
			}
		case "schema_version":
			schemaVersion, ok := value.(UUID)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "schema_version")
			}
			host.schemaVersion = schemaVersion.String()
		}
		// TODO(thrawn01): Add 'port'? once CASSANDRA-7544 is complete
		// Not sure what the port field will be called until the JIRA issue is complete
	}

	host.untranslatedConnectAddress = host.ConnectAddress()
	ip, port := translateAddressPort(host.untranslatedConnectAddress, host.port)
	host.connectAddress = ip
	host.port = port

	return host, nil
}

func hostInfoFromIter(iter *Iter, connectAddress net.IP, defaultPort int, translateAddressPort func(addr net.IP, port int) (net.IP, int)) (*HostInfo, error) {
	rows, err := iter.SliceMap()
	if err != nil {
		// TODO(zariel): make typed error
		return nil, err
	}

	if len(rows) == 0 {
		return nil, errors.New("query returned 0 rows")
	}

	host, err := hostInfoFromMap(rows[0], &HostInfo{connectAddress: connectAddress, port: defaultPort}, translateAddressPort)
	if err != nil {
		return nil, err
	}
	return host, nil
}

// debounceRingRefresh submits a ring refresh request to the ring refresh debouncer.
func (s *Session) debounceRingRefresh() {
	s.ringRefresher.Debounce()
}

// refreshRing executes a ring refresh immediately and cancels pending debounce ring refresh requests.
func (s *Session) refreshRingNow() error {
	err, ok := <-s.ringRefresher.RefreshNow()
	if !ok {
		return errors.New("could not refresh ring because stop was requested")
	}

	return err
}

func (s *Session) refreshRing() error {
	hosts, partitioner, err := s.hostSource.GetHostsFromSystem()
	if err != nil {
		return err
	}
	prevHosts := s.hostSource.getHostsMap()

	for _, h := range hosts {
		if s.cfg.filterHost(h) {
			continue
		}

		if host, ok := s.hostSource.addHostIfMissing(h); !ok {
			s.startPoolFill(h)
		} else {
			// host (by hostID) already exists; determine if IP has changed
			newHostID := h.HostID()
			existing, ok := prevHosts[newHostID]
			if !ok {
				return fmt.Errorf("get existing host=%s from prevHosts: %w", h, ErrCannotFindHost)
			}
			if h.connectAddress.Equal(existing.connectAddress) && h.nodeToNodeAddress().Equal(existing.nodeToNodeAddress()) {
				// no host IP change
				host.update(h)
			} else {
				// host IP has changed
				// remove old HostInfo (w/old IP)
				s.removeHost(existing)
				if _, alreadyExists := s.hostSource.addHostIfMissing(h); alreadyExists {
					return fmt.Errorf("add new host=%s after removal: %w", h, ErrHostAlreadyExists)
				}
				// add new HostInfo (same hostID, new IP)
				s.startPoolFill(h)
			}
		}
		delete(prevHosts, h.HostID())
	}

	for _, host := range prevHosts {
		s.metadataDescriber.RemoveTabletsWithHost(host)
		s.removeHost(host)
	}
	s.policy.SetPartitioner(partitioner)

	return nil
}
