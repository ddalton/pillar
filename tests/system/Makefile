# Pillar system tests (Chainsaw). See README.md.
.PHONY: help test test-one keep

help:
	@echo "make test               build images, create a kind cluster, run every test"
	@echo "make test-one T=<name>  run tests/<name> only (e.g. T=namenode-restart)"
	@echo "make keep               like test, but leave the cluster up for debugging"

test:
	./kind.sh

test-one:
	TESTS=tests/$(T) ./kind.sh

keep:
	KEEP=1 ./kind.sh
