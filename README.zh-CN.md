# AhriMQ (AMQ) 消息队列

[![Build Status](https://github.com/ahriroot/ahrimq/actions/workflows/release.yml/badge.svg)](https://github.com/ahriroot/ahrimq/actions)
[![GitHub Release](https://img.shields.io/github/v/release/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq/releases)
[![License](https://img.shields.io/github/license/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq)

> 支持发布/订阅、普通消息、有序消息、延迟消息和死信队列的高性能消息队列服务。

## 使用

### 运行 AhriMQ 服务

```bash
# 默认配置运行
amqs

# 指定配置文件运行
amqs config.toml
```

#### 默认配置

```toml
# 服务器配置
host = "127.0.0.1"                   # 监听地址
port = 60001                         # 监听端口
access_key = "your_access_key"       # 访问密钥
access_secret = "your_access_secret" # 访问密钥
retry_times = 3                      # 重试次数
retry_interval = 60                  # 重试间隔（秒）

# 持久化配置（可选，未指定时使用默认值）
# [persistence]
# wal_max_size = 104857600           # 100MB - WAL 文件最大大小
# wal_rotation_count = 10              # 保留的 WAL 文件数量
# buffer_max_size = 1048576            # 1MB - 缓冲区最大大小
# buffer_max_entries = 1000             # 缓冲区最大条目数
# flush_interval = 100                  # 100ms - 刷新间隔
# sync_interval = 1000                   # 1s - 同步间隔
# snapshot_interval = 300                # 5min - 快照间隔
# snapshot_wal_threshold = 10000         # 触发快照的 WAL 条目数
# enable_compression = true             # 启用压缩
# compression_level = 3                  # 压缩级别（1-21）
```

### 下载可执行文件

从 [发布页面](https://github.com/ahriroot/ahrimq/releases) 下载最新可执行文件，并将其复制到所需位置。

### 从 Crates.io 安装

```bash
cargo install ahrimq
```

### 从源码安装

```bash
git clone https://github.com/ahriroot/ahrimq.git
cd ahrimq
cargo build --release
```

### SDK

- [Golang](./clients/golang/v1/example)

## 特性

- **发布/订阅**：支持发布和订阅消息。
- **普通消息**：支持普通消息。
- **有序消息**：支持有序消息。
- **延迟消息**：支持延迟消息。
- **死信队列**：支持死信队列。
- **高性能持久化**：基于 WAL + 快照的持久化方案，异步批量写入，对性能影响最小。

## 持久化

AhriMQ 包含基于 WAL（预写日志）和定期快照的高性能持久化引擎：

- **WAL**：使用追加写入记录所有消息操作，实现最大性能
- **快照**：定期生成完整状态快照，实现快速恢复
- **异步批量写入**：后台线程配合内存缓冲区（1MB）和 100ms 刷新间隔
- **崩溃恢复**：自动从快照 + WAL 重放恢复

详细信息请参阅 [PERSISTENCE.md](./PERSISTENCE.md)。

## 许可证

MIT
