.PHONY: help test test-all test-basic test-replication test-cli test-datanode-failure clean install-kuttl

help:
	@echo "Pillar HDFS System Tests"
	@echo ""
	@echo "Available targets:"
	@echo "  make test              - Run all standard tests in parallel"
	@echo "  make test-all          - Run all tests including datanode-failure"
	@echo "  make test-basic        - Run basic HDFS operations test only"
	@echo "  make test-replication  - Run replication test only"
	@echo "  make test-cli          - Run Hadoop CLI compatibility test only"
	@echo "  make test-datanode-failure - Run DataNode failure test (runs separately)"
	@echo "  make clean             - Clean up test namespaces"
	@echo "  make install-kuttl     - Install KUTTL testing framework"
	@echo ""
	@echo "Requirements:"
	@echo "  - Kubernetes cluster with kubectl configured"
	@echo "  - KUTTL installed (run 'make install-kuttl')"
	@echo "  - Pillar Docker images built and available"
	@echo ""

# Install KUTTL if not already installed
install-kuttl:
	@echo "Installing KUTTL..."
	@if command -v kubectl-kuttl >/dev/null 2>&1; then \
		echo "KUTTL is already installed"; \
	else \
		echo "Installing KUTTL via krew..."; \
		kubectl krew install kuttl || \
		(echo "Krew not found. Installing KUTTL binary directly..."; \
		curl -LO https://github.com/kudobuilder/kuttl/releases/download/v0.15.0/kubectl-kuttl_0.15.0_$$(uname -s | tr '[:upper:]' '[:lower:]')_$$(uname -m | sed 's/x86_64/amd64/'); \
		chmod +x kubectl-kuttl_*; \
		sudo mv kubectl-kuttl_* /usr/local/bin/kubectl-kuttl); \
	fi

# Run all standard tests (runs in parallel)
test:
	@echo "Running all standard Pillar HDFS tests..."
	kubectl kuttl test --config kuttl-testsuite.yaml

# Run all tests including those that must run separately
test-all: test test-datanode-failure
	@echo "All tests completed!"

# Run specific test suites
test-basic:
	@echo "Running basic operations test..."
	kubectl kuttl test --config kuttl-testsuite.yaml --test basic-operations

test-replication:
	@echo "Running replication test..."
	kubectl kuttl test --config kuttl-testsuite.yaml --test replication

test-cli:
	@echo "Running Hadoop CLI compatibility test..."
	kubectl kuttl test --config kuttl-testsuite.yaml --test hadoop-cli-compat

# DataNode failure test runs separately
test-datanode-failure:
	@echo "Running DataNode failure test (isolation mode)..."
	kubectl kuttl test --config kuttl-testsuite-datanode-failure.yaml

# Clean up test resources
clean:
	@echo "Cleaning up test namespaces..."
	@kubectl get ns | grep kuttl-test | awk '{print $$1}' | xargs -r kubectl delete ns --wait=false || echo "No test namespaces to clean"
	@echo "Clean up initiated (namespaces will be deleted in background)"

# Verbose test run (for debugging)
test-verbose:
	@echo "Running tests with verbose output..."
	kubectl kuttl test --config kuttl-testsuite.yaml --suppress=

# Run tests and keep resources on failure (for debugging)
test-debug:
	@echo "Running tests in debug mode (resources will not be deleted on failure)..."
	kubectl kuttl test --config kuttl-testsuite.yaml --skip-delete

# Check prerequisites
check-prereqs:
	@echo "Checking prerequisites..."
	@command -v kubectl >/dev/null 2>&1 || (echo "ERROR: kubectl not found"; exit 1)
	@command -v kubectl-kuttl >/dev/null 2>&1 || (echo "ERROR: kuttl not found. Run 'make install-kuttl'"; exit 1)
	@kubectl cluster-info >/dev/null 2>&1 || (echo "ERROR: Cannot connect to Kubernetes cluster"; exit 1)
	@echo "All prerequisites satisfied!"
