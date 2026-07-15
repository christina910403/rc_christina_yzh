GO ?= go
CONFIG ?= configs/config.yaml

.PHONY: fmt test build config-validate migrate api worker mock

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...

build:
	mkdir -p bin
	$(GO) build -o bin/notifyhub ./cmd/notifyhub

config-validate:
	$(GO) run ./cmd/notifyhub config-validate -config $(CONFIG)

migrate:
	$(GO) run ./cmd/notifyhub migrate -config $(CONFIG)

api:
	$(GO) run ./cmd/notifyhub api -config $(CONFIG)

worker:
	$(GO) run ./cmd/notifyhub worker -config $(CONFIG)

mock:
	$(GO) run ./cmd/mockvendor
