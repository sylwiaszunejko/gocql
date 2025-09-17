//go:build unit
// +build unit

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
	"net"
	"testing"
)

func TestUnmarshalCassVersion(t *testing.T) {
	t.Parallel()

	tests := [...]struct {
		data    string
		version cassVersion
	}{
		{"3.2", cassVersion{Major: 3, Minor: 2, Patch: 0, Qualifier: ""}},
		{"2.10.1-SNAPSHOT", cassVersion{Major: 2, Minor: 10, Patch: 1, Qualifier: ""}},
		{"1.2.3", cassVersion{Major: 1, Minor: 2, Patch: 3, Qualifier: ""}},
		{"4.0-rc2", cassVersion{Major: 4, Minor: 0, Patch: 0, Qualifier: "rc2"}},
		{"4.3.2-rc1", cassVersion{Major: 4, Minor: 3, Patch: 2, Qualifier: "rc1"}},
		{"4.3.2-rc1-qualifier1", cassVersion{Major: 4, Minor: 3, Patch: 2, Qualifier: "rc1-qualifier1"}},
		{"4.3-rc1-qualifier1", cassVersion{Major: 4, Minor: 3, Patch: 0, Qualifier: "rc1-qualifier1"}},
	}

	for i, test := range tests {
		v := &cassVersion{}
		if err := v.UnmarshalCQL(nil, []byte(test.data)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if *v != test.version {
			t.Errorf("%d: expected %#+v got %#+v", i, test.version, *v)
		}
	}
}

func TestCassVersionBefore(t *testing.T) {
	t.Parallel()

	tests := [...]struct {
		version             cassVersion
		major, minor, patch int
		Qualifier           string
	}{
		{cassVersion{Major: 1, Minor: 0, Patch: 0, Qualifier: ""}, 0, 0, 0, ""},
		{cassVersion{Major: 0, Minor: 1, Patch: 0, Qualifier: ""}, 0, 0, 0, ""},
		{cassVersion{Major: 0, Minor: 0, Patch: 1, Qualifier: ""}, 0, 0, 0, ""},

		{cassVersion{Major: 1, Minor: 0, Patch: 0, Qualifier: ""}, 0, 1, 0, ""},
		{cassVersion{Major: 0, Minor: 1, Patch: 0, Qualifier: ""}, 0, 0, 1, ""},
		{cassVersion{Major: 4, Minor: 1, Patch: 0, Qualifier: ""}, 3, 1, 2, ""},

		{cassVersion{Major: 4, Minor: 1, Patch: 0, Qualifier: ""}, 3, 1, 2, ""},
	}

	for i, test := range tests {
		if test.version.Before(test.major, test.minor, test.patch) {
			t.Errorf("%d: expected v%d.%d.%d to be before %v", i, test.major, test.minor, test.patch, test.version)
		}
	}

}

func TestIsValidPeer(t *testing.T) {
	t.Parallel()

	host := &HostInfo{
		rpcAddress: net.ParseIP("0.0.0.0"),
		rack:       "myRack",
		hostId:     "0",
		dataCenter: "datacenter",
		tokens:     []string{"0", "1"},
	}

	if !isValidPeer(host) {
		t.Errorf("expected %+v to be a valid peer", host)
	}

	host.rack = ""
	if isValidPeer(host) {
		t.Errorf("expected %+v to NOT be a valid peer", host)
	}
}

func TestIsZeroToken(t *testing.T) {
	t.Parallel()

	host := &HostInfo{
		rpcAddress: net.ParseIP("0.0.0.0"),
		rack:       "myRack",
		hostId:     "0",
		dataCenter: "datacenter",
		tokens:     []string{"0", "1"},
	}

	if isZeroToken(host) {
		t.Errorf("expected %+v to NOT be a zero-token host", host)
	}

	host.tokens = []string{}
	if !isZeroToken(host) {
		t.Errorf("expected %+v to be a zero-token host", host)
	}
}

func TestHostInfo_ConnectAddress(t *testing.T) {
	t.Parallel()

	var localhost = net.IPv4(127, 0, 0, 1)
	tests := []struct {
		name          string
		connectAddr   net.IP
		rpcAddr       net.IP
		broadcastAddr net.IP
		peer          net.IP
	}{
		{name: "rpc_address", rpcAddr: localhost},
		{name: "connect_address", connectAddr: localhost},
		{name: "broadcast_address", broadcastAddr: localhost},
		{name: "peer", peer: localhost},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &HostInfo{
				connectAddress:   test.connectAddr,
				rpcAddress:       test.rpcAddr,
				broadcastAddress: test.broadcastAddr,
				peer:             test.peer,
			}

			if addr := host.ConnectAddress(); !addr.Equal(localhost) {
				t.Fatalf("expected ConnectAddress to be %s got %s", localhost, addr)
			}
		})
	}
}
