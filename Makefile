.PHONY: build test test-int test-chaos lint bench proto cluster-up cluster-down load-test clean

BIN_DIR := ./bin
CMD_DIR := ./cmd

build:
	@mkdir -p $(BIN_DIR)
	@for dir in $(CMD_DIR)/*; do \
		if [ -d "$$dir" ]; then \
			name=$$(basename $$dir); \
			echo "building $$name..."; \
			go build -o $(BIN_DIR)/$$name ./cmd/$$name; \
		fi; \
	done

test:
	go test ./...

test-int:
	go test -tags=integration ./test/integration/...

test-chaos:
	go test -tags=chaos ./test/chaos/...

lint:
	golangci-lint run ./...

bench:
	go test -bench=. -benchmem ./...

proto:
	@which protoc > /dev/null || (echo "protoc not installed"; exit 1)
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/*.proto

cluster-up:
	docker compose -f deploy/docker-compose.yml up -d

cluster-down:
	docker compose -f deploy/docker-compose.yml down

load-test:
	go run ./cmd/loadgen

clean:
	rm -rf $(BIN_DIR)
	go clean -testcache
