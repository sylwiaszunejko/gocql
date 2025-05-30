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
	"github.com/gocql/gocql/internal/tests"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestNewCluster_Defaults(t *testing.T) {
	t.Parallel()

	cfg := NewCluster()
	tests.AssertEqual(t, "cluster config cql version", "3.0.0", cfg.CQLVersion)
	tests.AssertEqual(t, "cluster config timeout", 11*time.Second, cfg.Timeout)
	tests.AssertEqual(t, "cluster config port", 9042, cfg.Port)
	tests.AssertEqual(t, "cluster config num-conns", 2, cfg.NumConns)
	tests.AssertEqual(t, "cluster config consistency", Quorum, cfg.Consistency)
	tests.AssertEqual(t, "cluster config max prepared statements", defaultMaxPreparedStmts, cfg.MaxPreparedStmts)
	tests.AssertEqual(t, "cluster config max routing key info", 1000, cfg.MaxRoutingKeyInfo)
	tests.AssertEqual(t, "cluster config page-size", 5000, cfg.PageSize)
	tests.AssertEqual(t, "cluster config default timestamp", true, cfg.DefaultTimestamp)
	tests.AssertEqual(t, "cluster config max wait schema agreement", 60*time.Second, cfg.MaxWaitSchemaAgreement)
	tests.AssertEqual(t, "cluster config reconnect interval", 60*time.Second, cfg.ReconnectInterval)
	tests.AssertTrue(t, "cluster config conviction policy",
		reflect.DeepEqual(&SimpleConvictionPolicy{}, cfg.ConvictionPolicy))
	tests.AssertTrue(t, "cluster config reconnection policy",
		reflect.DeepEqual(&ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second}, cfg.ReconnectionPolicy))
}

func TestNewCluster_WithHosts(t *testing.T) {
	t.Parallel()

	cfg := NewCluster("addr1", "addr2")
	tests.AssertEqual(t, "cluster config hosts length", 2, len(cfg.Hosts))
	tests.AssertEqual(t, "cluster config host 0", "addr1", cfg.Hosts[0])
	tests.AssertEqual(t, "cluster config host 1", "addr2", cfg.Hosts[1])
}

func TestClusterConfig_translateAddressAndPort_NilTranslator(t *testing.T) {
	t.Parallel()

	cfg := NewCluster()
	tests.AssertNil(t, "cluster config address translator", cfg.AddressTranslator)
	newAddr, newPort := cfg.translateAddressPort(net.ParseIP("10.0.0.1"), 1234)
	tests.AssertTrue(t, "same address as provided", net.ParseIP("10.0.0.1").Equal(newAddr))
	tests.AssertEqual(t, "translated host and port", 1234, newPort)
}

func TestClusterConfig_translateAddressAndPort_EmptyAddr(t *testing.T) {
	t.Parallel()

	cfg := NewCluster()
	cfg.AddressTranslator = staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
	newAddr, newPort := cfg.translateAddressPort(net.IP([]byte{}), 0)
	tests.AssertTrue(t, "translated address is still empty", len(newAddr) == 0)
	tests.AssertEqual(t, "translated port", 0, newPort)
}

func TestClusterConfig_translateAddressAndPort_Success(t *testing.T) {
	t.Parallel()

	cfg := NewCluster()
	cfg.AddressTranslator = staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
	newAddr, newPort := cfg.translateAddressPort(net.ParseIP("10.0.0.1"), 2345)
	tests.AssertTrue(t, "translated address", net.ParseIP("10.10.10.10").Equal(newAddr))
	tests.AssertEqual(t, "translated port", 5432, newPort)
}
