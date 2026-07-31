package gocql

import (
	"encoding/json"
)

// driverConfigStartupKey is the STARTUP option carrying the JSON description
// of the effective driver configuration. The configuration is identical for
// every connection of a session, so it is only sent on the control connection
// to keep the other STARTUP frames small.
const driverConfigStartupKey = "DRIVER_CONFIG"

// driverConfigVersion is the major version of the reported configuration schema.
// Adding keys to the report is backwards compatible and does not bump it, only
// changing or removing the meaning of an existing key does.
const driverConfigVersion = 1

// driverConfigReport is the value sent under driverConfigStartupKey.
// For now only the schema version is reported; configuration groups will be
// added in a follow-up change.
type driverConfigReport struct {
	Version int `json:"version"`
}

// driverConfigReporter builds the DRIVER_CONFIG STARTUP option describing a
// session's effective configuration to the cluster. It is created once per
// session and shared by all of its connections, but only ever contributes to
// the STARTUP options of the control connection.
//
// The report is rebuilt on every control connection (re-)establishment
// rather than cached, since future configuration groups may describe state
// only known after ring and topology setup (e.g. an inferred local DC).
//
// The reporter holds the *Session itself and builds the report lazily, at
// first use, rather than at construction time: newDriverConfigReporter runs
// inside newSessionCommon before fields such as s.policy are assigned, so a
// report built eagerly would see a partially initialized Session. For the
// same reason, a future report describing the host selection policy must
// read it off s.policy, not s.cfg.PoolConfig.HostSelectionPolicy: the latter
// is never assigned the default policy that newSessionCommon applies.
type driverConfigReporter struct {
	session *Session
}

func newDriverConfigReporter(s *Session) *driverConfigReporter {
	return &driverConfigReporter{session: s}
}

// updateStartupOptions adds the DRIVER_CONFIG STARTUP option.
//
// Only the control connection's startup holds a reporter, so this is not the
// place that decides which connections report: see Conn.init.
//
// Reporting is best effort: it must never prevent a connection from being
// established, so a report that cannot be built is logged and left out.
func (r *driverConfigReporter) updateStartupOptions(opts map[string]string) {
	report, err := r.buildReport()
	if err != nil {
		r.session.logger.Printf("gocql: unable to report driver configuration: %v", err)
		return
	}
	opts[driverConfigStartupKey] = report
}

// buildReport returns the JSON configuration report of the session, marshalled
// fresh on every call so that it reflects the session's current state rather
// than a snapshot from whenever it was first requested.
//
// The error is unreachable while the report only carries the schema version, but
// it is the fail-safe boundary for the configuration groups added later: those
// describe user-supplied values, such as the names and fields of custom
// policies, whose marshalling can genuinely fail.
func (r *driverConfigReporter) buildReport() (string, error) {
	report, err := json.Marshal(driverConfigReport{Version: driverConfigVersion})
	return string(report), err
}
