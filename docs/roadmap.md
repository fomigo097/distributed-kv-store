# Distributed KV Store Roadmap

This roadmap is based on the six-phase plan you shared for building a distributed key-value store with Go and Raft.

## Phase 1: Go fundamentals

Goal: build fluency with Go and ship a simple in-memory key-value store.

Deliverable:

- Simple in-memory KV store with an HTTP API

Key skills:

- Goroutines and channels
- Structs and interfaces
- Error handling
- `net/http`
- Testing with race detection

## Phase 2: Single-node KV store

Goal: move from a toy service to a durable storage engine.

Deliverable:

- Persistent TCP KV server with write-ahead logging and restart recovery

Key skills:

- TCP sockets
- Binary protocol design
- WAL durability
- File I/O and recovery

## Phase 3: Raft fundamentals

Goal: understand consensus deeply before wiring networked nodes together.

Deliverable:

- Raft state machine with extensive unit tests

Key skills:

- Leader election
- Terms and votes
- Log replication
- Safety properties
- Quorum reasoning

## Phase 4: Distributed consensus

Goal: turn the storage engine into a replicated, fault-tolerant cluster.

Deliverable:

- Three-node distributed KV store that survives failures and partitions

Key skills:

- gRPC and protobuf
- RPC handlers
- Timers and goroutines
- Replication and commits
- Snapshots

## Phase 5: Scale and observe

Goal: make the project feel production-credible.

Deliverable:

- Multi-shard cluster with observability and routing

Key skills:

- Consistent hashing
- Prometheus metrics
- Grafana dashboards
- Admin endpoints
- mTLS

## Phase 6: Deploy and polish

Goal: package the project for demos, interviews, and portfolio value.

Deliverable:

- Dockerized cluster, polished README, CI, architecture docs, and demo flow

Key skills:

- Docker
- Compose
- GitHub Actions
- Technical writing
- Architecture communication

## Suggested implementation order in this repo

1. Stabilize the in-memory store and test surface
2. Add a TCP command server alongside the HTTP server
3. Add WAL append and replay
4. Model Raft state transitions in isolation
5. Introduce protobuf contracts and node RPC
6. Connect committed log entries to the KV state machine

## Current repo status

Completed:

- Phase 1 foundation: in-memory store plus HTTP API
- Phase 2 foundation: TCP protocol, WAL, and restart recovery
- Phase 3 foundation: pure Raft state machine with election and log-replication tests
- Phase 4 foundation: multi-node HTTP transport with live leader election and replicated writes
- Phase 5 foundation: consistent-hash shard router plus basic metrics and node status endpoints
- Phase 6 foundation: Docker packaging, Compose demo flow, CI, linting, and architecture documentation

Next:

- Automatic elections and heartbeats
- Snapshotting and log compaction
- gRPC/protobuf transport
- Prometheus and Grafana integration
