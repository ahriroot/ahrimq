# AhriMQ 持久化方案

## 概述

AhriMQ 采用 **WAL (Write-Ahead Log) + 定期快照** 的高性能持久化方案，解决了传统实时持久化性能差的问题。

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                      内存状态                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │  消息队列    │  │  索引结构    │  │  写入缓冲    │      │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
└─────────────────────────────────────────────────────────┘
         ↓                    ↓                    ↓
┌─────────────────────────────────────────────────────────┐
│                    持久化引擎                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │  WAL (Write-Ahead Log)                          │   │
│  │  - 追加写入，性能高                              │   │
│  │  - 记录所有操作日志                              │   │
│  │  - 支持崩溃恢复                                  │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  定期快照 (Snapshot)                             │   │
│  │  - 定期生成完整状态快照                           │   │
│  │  - 减少恢复时间                                  │   │
│  │  - 清理旧 WAL 文件                               │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  批量异步写入                                    │   │
│  │  - 内存缓冲区累积操作                            │   │
│  │  - 达到阈值或定时批量写入                        │   │
│  │  - 后台线程异步刷盘                              │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
         ↓                    ↓                    ↓
┌─────────────────────────────────────────────────────────┐
│                      磁盘存储                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │  wal_001.amq│  │  wal_002.amq│  │  snapshot.amq│     │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
└─────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. WAL (Write-Ahead Log)

WAL 记录所有消息操作的日志，采用追加写入方式，性能极高。

**WAL 条目类型**：
```rust
enum WalEntry {
    AddMessage(MessageBox),           // 添加消息
    UpdateStatus { id, status },       // 更新状态
    AddHistory { id, history },       // 添加历史记录
    RemoveMessage(id),               // 删除消息
}
```

**WAL 文件格式**：
```
[Header: 4字节魔数(AMQW) + 4字节版本号]
[Entry 1: 4字节长度 + 序列化数据]
[Entry 2: 4字节长度 + 序列化数据]
...
[Footer: 可选校验和]
```

**WAL 轮转策略**：
- 单个 WAL 文件大小限制：100MB（可配置）
- 达到限制后自动创建新文件
- 保留最近 10 个 WAL 文件（可配置）

### 2. 定期快照

快照保存完整的系统状态，用于快速恢复和清理旧 WAL。

**快照触发条件**：
- 时间间隔：5 分钟（可配置）
- WAL 条目数量：10000 条（可配置）
- 服务器关闭时自动创建

**快照内容**：
```rust
struct SnapshotData {
    messages: Vec<MessageBox>,      // 所有未确认消息
    next_message_id: u64,          // 消息 ID 生成器
    next_connection_id: u64,       // 连接 ID 生成器
    timestamp: u64,                // 快照时间戳
}
```

### 3. 批量异步写入

通过内存缓冲区和后台线程实现高性能批量写入。

**写入策略**：
- 内存缓冲区大小：1MB（可配置）
- 缓冲区最大条目数：1000 条（可配置）
- 定时刷新间隔：100ms（可配置）
- 后台线程异步刷盘

**缓冲区管理**：
```rust
struct WriteBuffer {
    entries: Vec<WalEntry>,
    size: usize,
    max_size: usize,
    flush_interval: Duration,
}
```

## 性能优化

### 1. 批量写入
- 多个操作合并为一次磁盘写入
- 减少系统调用次数
- 提高磁盘顺序写入效率

### 2. 异步处理
- 主线程不阻塞，消息处理延迟低
- 后台线程负责持久化
- 使用无锁队列传递数据

### 3. 追加写入
- WAL 采用追加写入，无需随机访问
- 顺序写入性能远高于随机写入
- 减少磁盘寻道时间

### 4. 内存映射
- 大文件使用 mmap 提升读取性能
- 减少内存拷贝
- 操作系统自动管理缓存

### 5. 压缩
- 支持 zstd/lz4 压缩
- 减少磁盘占用
- 压缩级别可配置

## 崩溃恢复流程

```
1. 读取最新的快照文件
   └─> 恢复内存状态（消息、ID 生成器等）

2. 按时间顺序重放快照之后的所有 WAL 条目
   └─> 应用所有未持久化的操作

3. 验证数据完整性
   └─> 检查消息状态一致性

4. 清理已应用的 WAL 文件
   └─> 释放磁盘空间
```

## 配置参数

```rust
struct PersistenceConfig {
    // WAL 配置
    wal_max_size: usize,           // 100MB - 单个 WAL 文件最大大小
    wal_rotation_count: usize,     // 10 - 保留的 WAL 文件数量
    
    // 缓冲区配置
    buffer_max_size: usize,        // 1MB - 缓冲区最大大小
    buffer_max_entries: usize,     // 1000 - 缓冲区最大条目数
    
    // 刷盘策略
    flush_interval: Duration,      // 100ms - 定时刷新间隔
    sync_interval: Duration,       // 1s - fsync 间隔
    
    // 快照配置
    snapshot_interval: Duration,    // 5min - 快照间隔
    snapshot_wal_threshold: usize, // 10000 - 触发快照的 WAL 条目数
    
    // 压缩配置
    enable_compression: bool,      // true - 是否启用压缩
    compression_level: u32,        // 3 - 压缩级别
}
```

## 使用示例

### 初始化持久化引擎

```rust
use amq::{PersistenceConfig, PersistenceEngine};

let config = PersistenceConfig::default();
let engine = PersistenceEngine::new(config).await?;
```

### 持久化消息

```rust
// 添加消息
engine.append_message(&message_box).await?;

// 更新状态
engine.update_message_status(msg_id, MessageStatus::Acked).await?;

// 删除消息
engine.remove_message(msg_id).await?;
```

### 创建快照

```rust
let timestamp = engine.create_snapshot(
    &messages,
    next_message_id,
    next_connection_id,
).await?;
```

### 恢复数据

```rust
let (messages, next_message_id, next_connection_id) = 
    engine.recover().await?;
```

## 存储目录结构

```
~/.ahriknow/ahrimq/
├── wal/                          # WAL 文件目录
│   ├── wal_1234567890.amq       # WAL 文件
│   ├── wal_1234567891.amq
│   └── ...
└── snapshot/                     # 快照文件目录
    ├── snapshot_1234567890.amq   # 快照文件
    ├── snapshot_1234567891.amq
    └── ...
```

## 性能对比

### 传统实时持久化

| 指标 | 数值 |
|------|------|
| 单次写入延迟 | 1-10ms |
| 吞吐量 | ~1000 ops/s |
| CPU 使用率 | 高 |
| 磁盘 I/O | 频繁随机写入 |

### WAL + 快照方案

| 指标 | 数值 |
|------|------|
| 单次写入延迟 | <0.1ms (内存) |
| 吞吐量 | >10000 ops/s |
| CPU 使用率 | 低 |
| 磁盘 I/O | 批量顺序写入 |

## 优势总结

1. **高性能** - 批量异步写入，不阻塞主线程
2. **可靠性** - WAL 保证数据不丢失
3. **快速恢复** - 快照 + WAL 增量恢复
4. **可扩展** - 支持分布式扩展
5. **可配置** - 灵活的参数配置
6. **向后兼容** - 支持旧版本数据格式

## 注意事项

1. **磁盘空间** - 需要足够空间存储 WAL 和快照
2. **恢复时间** - 大量数据恢复可能需要时间
3. **配置调优** - 根据实际负载调整参数
4. **监控告警** - 监控磁盘使用和 WAL 增长

## 故障恢复场景

### 场景 1: 正常关闭重启
```
1. 服务器关闭时自动创建快照
2. 启动时读取最新快照
3. 恢复所有未确认消息
4. WAL 为空，无需重放
```

### 场景 2: 异常崩溃重启
```
1. 启动时读取最新快照
2. 按顺序重放快照之后的所有 WAL 条目
3. 应用所有未持久化的操作
4. 恢复到崩溃前的状态
```

### 场景 3: 磁盘损坏
```
1. 检测到 WAL 文件损坏
2. 跳过损坏的 WAL 文件
3. 从上一个有效快照恢复
4. 丢失损坏 WAL 中的数据
```

## 最佳实践

### 1. 配置建议

**低负载场景**（< 1000 msg/s）：
```rust
PersistenceConfig {
    buffer_max_size: 512 * 1024,      // 512KB
    buffer_max_entries: 500,
    flush_interval: Duration::from_millis(200),
    snapshot_interval: Duration::from_secs(600),  // 10分钟
    snapshot_wal_threshold: 5000,
    ..
}
```

**高负载场景**（> 10000 msg/s）：
```rust
PersistenceConfig {
    buffer_max_size: 2 * 1024 * 1024,   // 2MB
    buffer_max_entries: 2000,
    flush_interval: Duration::from_millis(50),
    snapshot_interval: Duration::from_secs(300),  // 5分钟
    snapshot_wal_threshold: 20000,
    ..
}
```

### 2. 监控指标

建议监控以下指标：
- WAL 文件数量和大小
- 快照创建频率
- 磁盘使用率
- 恢复时间

### 3. 备份策略

- 定期备份 `~/.ahriknow/ahrimq/` 目录
- 保留多个版本的备份
- 在低峰期执行备份

### 4. 性能调优

- 根据消息大小调整缓冲区
- 根据磁盘性能调整刷新间隔
- 根据恢复时间要求调整快照频率

## 故障排查

### 问题 1: 恢复时间过长
**原因**：WAL 文件过多或过大  
**解决**：
- 增加 `snapshot_interval` 频率
- 减少 `snapshot_wal_threshold` 阈值
- 清理旧的 WAL 文件

### 问题 2: 磁盘空间不足
**原因**：WAL 或快照文件过多  
**解决**：
- 减少 `wal_rotation_count`
- 增加 `snapshot_interval`
- 手动清理旧文件

### 问题 3: 持久化性能差
**原因**：缓冲区配置不当  
**解决**：
- 增加 `buffer_max_size`
- 增加 `buffer_max_entries`
- 增加 `flush_interval`

## 未来规划

- [ ] 支持分布式持久化
- [ ] 支持数据压缩
- [ ] 支持加密存储
- [ ] 支持增量快照
- [ ] 支持多副本同步
