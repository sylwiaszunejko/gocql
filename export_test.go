//go:build integration
// +build integration

package gocql

var FlagRunSslTest = flagRunSslTest
var CreateCluster = createCluster
var TestLogger = &testLogger{}
var WaitUntilPoolsStopFilling = waitUntilPoolsStopFilling

func GetRingAllHosts(sess *Session) []*HostInfo {
	return sess.hostSource.getHostsList()
}
