BIN_DIR ?= ./.artifacts
BUILD_OPTS ?= -ldflags "-s -w" -trimpath

.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build $(BUILD_OPTS) -o $(BIN_DIR)/kafka .

.PHONY: test
test:
	go test -failfast -race ./...

.PHONY: test/integration
test/integration:
	go test -tags integration -count=1 ./provider/...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: vet
vet:
	go vet ./...

.PHONY: up
up:
	docker compose up -d --wait

.PHONY: down
down:
	docker compose down -v

.PHONY: precommit
precommit: lint vet test build
