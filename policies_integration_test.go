//go:build integration
// +build integration

package gocql

import (
	"testing"
)

// Check if session fail to start if DC name provided in the policy is wrong
func TestDCValidationTokenAware(t *testing.T) {
	cluster := createCluster()

	fallback := DCAwareRoundRobinPolicy("WRONG_DC")
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(fallback)

	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("createSession was expected to fail with wrong DC name provided.")
	}
}

func TestDCValidationDCAware(t *testing.T) {
	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = DCAwareRoundRobinPolicy("WRONG_DC")

	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("createSession was expected to fail with wrong DC name provided.")
	}
}

func TestDCValidationRackAware(t *testing.T) {
	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = RackAwareRoundRobinPolicy("WRONG_DC", "RACK")

	_, err := cluster.CreateSession()
	if err == nil {
		t.Fatal("createSession was expected to fail with wrong DC name provided.")
	}
}
