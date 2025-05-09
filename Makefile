SHELL := bash
MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
KEY_PATH = ${MAKEFILE_PATH}/testdata/pki

CASSANDRA_VERSION ?= 4.1.6
SCYLLA_VERSION ?= release:6.1.1

TEST_CQL_PROTOCOL ?= 4
TEST_COMPRESSOR ?= snappy
TEST_OPTS ?=
TEST_INTEGRATION_TAGS ?= integration gocql_debug

CCM_SCYLLA_CLUSTER_NAME = gocql_scylla_integration_test
CCM_SCYLLA_IP_PREFIX = 127.0.2.
CCM_SCYLLA_REPO ?= github.com/scylladb/scylla-ccm
CCM_SCYLLA_VERSION ?= master

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

SCYLLA_CONFIG = "native_transport_port_ssl: 9142" \
"native_transport_port: 9042" \
"native_shard_aware_transport_port: 19042" \
"native_shard_aware_transport_port_ssl: 19142" \
"client_encryption_options.enabled: true" \
"client_encryption_options.certificate: ${KEY_PATH}/cassandra.crt" \
"client_encryption_options.keyfile: ${KEY_PATH}/cassandra.key" \
"client_encryption_options.truststore: ${KEY_PATH}/ca.crt" \
"client_encryption_options.require_client_auth: true" \
"maintenance_socket: workdir" \
"enable_tablets: true" \
"enable_user_defined_functions: true" \
"experimental_features: [udf]"

export JVM_EXTRA_OPTS
export JAVA11_HOME=${JAVA_HOME_11_X64}
export JAVA17_HOME=${JAVA_HOME_17_X64}
export JAVA_HOME=${JAVA_HOME_11_X64}

scylla-start: .prepare-scylla-ccm .prepare-java
	@if [ -d ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME} ] && ccm switch ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 && ccm status | grep UP 2>/dev/null 1>&2; then \
		echo "Scylla cluster is already started"; \
  	else \
		echo "Start scylla ${SCYLLA_VERSION} cluster"; \
		ccm stop ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true; \
		ccm remove ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true; \
		ccm create ${CCM_SCYLLA_CLUSTER_NAME} -i ${CCM_SCYLLA_IP_PREFIX} --scylla -v ${SCYLLA_VERSION} -n 3 -d --jvm_arg="--smp 2 --memory 1G --experimental-features udf --enable-user-defined-functions true" && \
		ccm updateconf ${SCYLLA_CONFIG} && \
		ccm start --wait-for-binary-proto --wait-other-notice --verbose && \
		ccm status && \
		ccm node1 nodetool status && \
		sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node1/cql.m && \
		sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node2/cql.m && \
		sudo chmod 0777 ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node3/cql.m; \
	fi

scylla-stop: .prepare-scylla-ccm
	@echo "Stop scylla cluster"
	@ccm stop --not-gently ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true
	@ccm remove ${CCM_SCYLLA_CLUSTER_NAME} 2>/dev/null 1>&2 || true

test-integration-scylla: scylla-start
	@echo "Run integration tests for proto ${TEST_CQL_PROTOCOL} on scylla ${SCYLLA_IMAGE}"
	go test -v ${TEST_OPTS} -tags "${TEST_INTEGRATION_TAGS}" -cluster-socket ${CCM_CONFIG_DIR}/${CCM_SCYLLA_CLUSTER_NAME}/node1/cql.m -timeout=5m -gocql.timeout=60s -proto=${TEST_CQL_PROTOCOL} -rf=3 -clusterSize=3 -autowait=2000ms -compressor=${TEST_COMPRESSOR} -gocql.cversion=$$(ccm node1 versionfrombuild) -cluster=$$(ccm liveset) ./...

test-unit:
	@echo "Run unit tests"
	go test -v -tags unit -timeout=5m -race ./...

check:
	@echo "Run go vet linter"
	go vet --tags "unit all ccm cassandra integration" ./...

.prepare-java:
ifeq ($(shell if [ -f ~/.sdkman/bin/sdkman-init.sh ]; then echo "installed"; else echo "not-installed"; fi), not-installed)
	@$(MAKE) install-java
endif

install-java:
	@echo "Installing SDKMAN..."
	@curl -s "https://get.sdkman.io" | bash
	@echo "sdkman_auto_answer=true" >> ~/.sdkman/etc/config
	@( \
		source ~/.sdkman/bin/sdkman-init.sh; \
		export PATH=${PATH}:~/.sdkman/bin; \
		echo "Installing Java versions..."; \
		sdk install java 11.0.24-zulu; \
		sdk install java 17.0.12-zulu; \
		sdk default java 11.0.24-zulu; \
		sdk use java 11.0.24-zulu \
	)

.prepare-scylla-ccm:
	@ccm --help 2>/dev/null 1>&2; if [[ $$? -lt 127 ]] && grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 && grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev//null  1>&2; then \
		echo "Scylla CCM ${CCM_SCYLLA_VERSION} is already installed"; \
  	else \
		echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"; \
		pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"; \
		mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true; \
		echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type; \
		echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version; \
  	fi

install-scylla-ccm:
	@echo "Installing Scylla CCM ${CCM_SCYLLA_VERSION}"
	@pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	@mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	@echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	@echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version
