# Distributed Key-Value Store

This repository now covers the full six-phase roadmap for building a Redis-like distributed key-value store in Go with Raft consensus.

The current implementation now includes:

- In-memory key-value store
- HTTP API for `GET`, `PUT`, and `DELETE`
- Binary TCP protocol for `SET`, `GET`, and `DEL`
- Write-ahead log (WAL) for durability
- Restart recovery by replaying the WAL
- Pure Raft state machine for elections, voting, and log replication rules
- Multi-node HTTP transport for Raft vote and append RPCs
- Three-node integration test covering election and replicated writes
- Consistent-hash shard ring and a router that forwards keys to shard leaders
- Status and Prometheus-style metrics endpoints on Raft nodes
- Docker packaging and Compose-based local demo topology
- GitHub Actions CI and lint configuration
- Architecture notes for interview-ready explanation
- Concurrency-safe storage layer
- Unit tests for the store and handlers

## Current HTTP API

### Write a value

```bash
curl -X PUT http://localhost:8080/kv/my-key \
  -H "Content-Type: application/json" \
  -d '{"value":"hello"}'
```

### Read a value

```bash
curl http://localhost:8080/kv/my-key
```

### Delete a value

```bash
curl -X DELETE http://localhost:8080/kv/my-key
```

## Current TCP API

The TCP server uses a compact binary protocol:

```text
[1 byte command][4 byte key length][key bytes][4 byte value length][value bytes]
```

Commands:

- `1` = `SET`
- `2` = `GET`
- `3` = `DEL`

Responses use:

```text
[1 byte status][4 byte payload length][payload bytes]
```

Statuses:

- `0` = success
- `1` = key not found
- `2` = server error

## Project layout

```text
cmd/server/           HTTP server entrypoint
cmd/tcpserver/        Persistent TCP server entrypoint
cmd/raftnode/         HTTP Raft node entrypoint
cmd/router/           Shard router entrypoint
internal/httpapi/     HTTP handlers and routing
internal/store/       In-memory concurrency-safe KV engine
internal/persistence/ WAL-backed storage layer
internal/raft/        Pure Raft consensus state machine
internal/raftnode/    Multi-node Raft transport and replicated KV behavior
internal/router/      Shard router for directing keys to shard leaders
internal/sharding/    Consistent hashing primitives
internal/tcpapi/      Binary TCP protocol and server
docs/                 Roadmap and architecture notes
```

## Roadmap

The full project plan is captured in [docs/roadmap.md](./docs/roadmap.md).
The architecture write-up is in [docs/architecture.md](./docs/architecture.md).

High-level phases:

1. Learn Go fundamentals and build a simple in-memory KV store
2. Replace HTTP-first storage with a persistent single-node TCP server and WAL
3. Implement the Raft state machine with strong unit-test coverage
4. Add distributed consensus, replication, snapshots, and fault tolerance
5. Add sharding, observability, and production-facing admin surfaces
6. Package, deploy, document, and polish the system

## Local run

HTTP server:

```bash
go run ./cmd/server
```

The server listens on `:8080` by default. Override with `PORT`.

TCP server:

```bash
go run ./cmd/tcpserver
```

The TCP server listens on `:9090` by default. Override with `TCP_PORT`.
The WAL file defaults to `data/kv.wal`. Override with `WAL_PATH`.

Raft node:

```bash
NODE_ID=node-1 \
HTTP_PORT=7001 \
PEERS="node-2=http://127.0.0.1:7002,node-3=http://127.0.0.1:7003" \
go run ./cmd/raftnode
```

This node exposes:

- `POST /admin/campaign` to trigger an election manually
- `GET /admin/status` for node state
- `GET /metrics` for Prometheus-style counters
- `POST /raft/request-vote` for vote RPCs
- `POST /raft/append-entries` for append RPCs
- `PUT /kv/{key}` and `GET /kv/{key}` for replicated client access

Shard router:

```bash
SHARD_LEADERS="shard-a=http://127.0.0.1:7001,shard-b=http://127.0.0.1:7011" \
go run ./cmd/router
```

The router exposes:

- `PUT /kv/{key}`, `GET /kv/{key}`, and `DELETE /kv/{key}` for shard-aware client access
- `GET /admin/shards` for shard inventory
- `GET /healthz` for health checks

## Docker demo

Start a local sharded demo:

```bash
docker compose up --build
```

Then trigger leadership for the single-node shards:

```bash
curl -X POST http://localhost:7001/admin/campaign
curl -X POST http://localhost:7011/admin/campaign
```

Now write through the router:

```bash
curl -X PUT http://localhost:7100/kv/pet \
  -H "Content-Type: application/json" \
  -d '{"value":"cat"}'
```

Inspect node status and metrics:

```bash
curl http://localhost:7001/admin/status
curl http://localhost:7001/metrics
curl http://localhost:7100/admin/shards
```

Stop the stack:

```bash
docker compose down -v
```

## Test

```bash
go test ./...
```

Lint:

```bash
golangci-lint run ./...
```

## What comes next

This repo now has a complete end-to-end learning path and a demoable local setup. The most meaningful future upgrades would be automatic elections, gRPC transport, snapshots, and full Prometheus/Grafana integration.
