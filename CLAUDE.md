# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TinyKV is an educational distributed key-value storage system inspired by MIT 6.824 and TiKV. It implements a horizontally scalable, highly available KV store with Raft consensus and transaction support. The course is structured as 4 progressive projects:

- **Project 1**: StandaloneKV - Single-node storage engine
- **Project 2**: RaftKV - Fault-tolerant distributed KV using Raft consensus
- **Project 3**: Multi-raftKV - Membership changes, region splitting, and scheduler
- **Project 4**: Transaction - MVCC (Multi-Version Concurrency Control)

## Common Commands

### Build
```bash
make                    # Build both tinykv-server and tinyscheduler-server
make kv                 # Build only tinykv-server
make scheduler          # Build only tinyscheduler-server
make proto              # Generate protobuf Go code
```

### Testing
```bash
make test               # Run all tests with coverage
make project1           # Run Project 1 tests (standalone KV)
make project2           # Run all Project 2 tests (Raft: 2A, 2B, 2C)
make project2a          # Run Project 2A tests (Raft leader election & log replication)
make project2b          # Run Project 2B tests (Raft-based KV store)
make project2c          # Run Project 2C tests (Raft log GC & snapshots)
make project3           # Run all Project 3 tests (Multi-raft)
make project4           # Run all Project 4 tests (Transactions)
```

### Code Quality
```bash
make format             # Format Go code with gofmt
make ci                 # Run format and vet checks
```

### Running the System
```bash
mkdir -p data
./bin/tinyscheduler-server              # Start scheduler
./bin/tinykv-server -path=data          # Start KV server
```

## Architecture

TinyKV follows a microservices architecture similar to TiDB + TiKV + PD:

```
Clients (TinySQL)
    ↓ gRPC
TinyKV Server (kv/)
    ↓ Storage Layer
├── StandaloneStorage (kv/storage/standalone_storage/)
└── RaftStorage (kv/storage/raft_storage/)
    ↓ Raft Consensus
Raft Implementation (raft/)
    ↓ Replication
Multiple TinyKV Nodes
    ←→ Heartbeats
TinyScheduler (scheduler/)
```

### Core Components

**kv/** - Key-Value Storage Core
- `server/` - gRPC server and request handlers
- `storage/` - Storage engine implementations (StandaloneStorage, RaftStorage)
- `raftstore/` - Raft store management, peer handling, message routing
- `transaction/` - MVCC layer, two-phase commit, lock management
- `coprocessor/` - Distributed query processing
- `test_raftstore/` - Integration tests

**raft/** - Raft Consensus Implementation
- Core Raft algorithm (leader election, log replication, state management)
- Based on etcd's Raft implementation
- Handles snapshots and log compaction

**scheduler/** - Cluster Management
- `server/` - Scheduler server implementation
- `tso/` - Timestamp Oracle for global timestamps
- `schedulers/` - Scheduling algorithms (region splitting, balancing)

**proto/** - Protocol Definitions
- Protocol Buffer definitions for all RPC communications
- Generated Go code in `proto/pkg/`
- Key packages: `tinykvpb`, `kvrpcpb`, `eraftpb`, `raft_cmdpb`, `metapb`, `schedulerpb`

## Key Interfaces and Patterns

### Storage Interface
```go
type Storage interface {
    Start() error
    Stop() error
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

### Raftstore Message Passing
Raftstore uses a message-passing architecture:
- `router.go` - Routes messages between components
- `peer_msg_handler.go` - Handles Raft messages for each peer
- `raft_worker.go` - Processes Raft events
- `store_worker.go` - Handles store-level operations

### Transaction Layer (Project 4)
The transaction layer implements two types of transactions:
1. **TinySQL transactions** - Client-visible, multi-request coordination using locks
2. **mvcc transactions** - Internal single-request atomicity using latches

Key concepts:
- **Locks** - Written to `lock` CF for TinySQL transaction coordination
- **Latches** - In-memory locks for mvcc transaction atomicity
- **Encoded keys** - User key + timestamp for MVCC versioning
- **Column Families (CFs)**:
  - `default` - Encoded key → value
  - `lock` - User key → lock info (primary key, type, start_ts, ttl)
  - `write` - Encoded key (commit_ts) → write record (start_ts, type)

## Testing Strategy

Tests are organized by project and test case name:
- Individual test runs use `|| true` to continue on failure
- `TEST_CLEAN` removes temporary test files before/after tests
- Use `-run` flag to filter specific test cases
- Set `LOG_LEVEL=fatal` to reduce test noise

Example: Run a specific test
```bash
GOTEST="./raft -run 2A -run TestInitialElection2A"
```

## Development Notes

- Go 1.13+ required
- Uses BadgerDB as underlying storage engine
- gRPC with Protocol Buffers for all inter-node communication
- PingCAP error patterns for structured errors
- Timezone set to 'Asia/Shanghai' for tests
- Current branch: `course`
- Recent work: storage logic fixes in `raft/raft.go`
