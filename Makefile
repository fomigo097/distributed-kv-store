APP_NAME := distributed-kv-store

.PHONY: test lint run-http run-tcp run-node run-router compose-up compose-down

test:
	go test ./...

lint:
	golangci-lint run ./...

run-http:
	go run ./cmd/server

run-tcp:
	go run ./cmd/tcpserver

run-node:
	go run ./cmd/raftnode

run-router:
	go run ./cmd/router

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

