# AhriMQ (AMQ)

[![Build Status](https://github.com/ahriroot/ahrimq/actions/workflows/release.yml/badge.svg)](https://github.com/ahriroot/ahrimq/actions)
[![GitHub Release](https://img.shields.io/github/v/release/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq/releases)
[![License](https://img.shields.io/github/license/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq)

> A high-performance message queue service supporting pub/sub, normal messages, ordered messages, delayed messages and dead letter queues.

## Usage

### Run AhriMQ Server

```bash
# run with default config
amqs

# run with config file
amqs config.toml
```

#### Default configuration

```toml
# Server configuration
host = "127.0.0.1"
port = 60001
access_key = "your_access_key"
access_secret = "your_access_secret"
retry_times = 3
retry_interval = 60

# Persistence configuration (optional, uses defaults if not specified)
# [persistence]
# wal_max_size = 104857600           # 100MB - WAL file max size
# wal_rotation_count = 10              # Number of WAL files to keep
# buffer_max_size = 1048576            # 1MB - Buffer max size
# buffer_max_entries = 1000             # Max entries in buffer
# flush_interval = 100                  # 100ms - Flush interval
# sync_interval = 1000                   # 1s - fsync interval
# snapshot_interval = 300                # 5min - Snapshot interval
# snapshot_wal_threshold = 10000         # WAL entries to trigger snapshot
# enable_compression = true             # Enable compression
# compression_level = 3                  # Compression level (1-21)
```

### Install by downloading binary

Download the latest binary from the [releases page](https://github.com/ahriroot/ahrimq/releases) and copy it to the desired location.

### Install from Crates.io

```bash
cargo install ahrimq
```

### Install from Source

```bash
git clone https://github.com/ahriroot/ahrimq.git
cd ahrimq
cargo build --release
```

### SDK

- [Golang](./clients/golang/v1/example)

## Features

- **Sub and Pub**: Support for publishing and subscribing to messages.
- **Normal Messages**: Support for normal messages.
- **Ordered Messages**: Support for ordered messages.
- **Delay Message**s: Support for delayed messages.
- **Dead Letter Queues**: Support for dead letter queues.
- **High-Performance Persistence**: WAL + Snapshot based persistence with async batch writes for minimal performance impact.

## Persistence

AhriMQ includes a high-performance persistence engine based on WAL (Write-Ahead Log) and periodic snapshots:

- **WAL**: Records all message operations with append-only writes for maximum performance
- **Snapshots**: Periodic full state snapshots for fast recovery
- **Async Batch Writes**: Background thread with memory buffer (1MB) and 100ms flush interval
- **Crash Recovery**: Automatic recovery from snapshots + WAL replay

For detailed information, see [PERSISTENCE.md](./PERSISTENCE.md).

## License

MIT
