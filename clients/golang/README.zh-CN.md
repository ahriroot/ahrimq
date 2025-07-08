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
host = "127.0.0.1"                   # 监听地址
port = 60001                         # 监听端口
access_key = "your_access_key"       # 访问密钥
access_secret = "your_access_secret" # 访问密钥
retry_times = 3                      # 重试次数
retry_interval = 60                  # 重试间隔
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

- 发布/订阅：支持发布和订阅消息。
- 普通消息：支持普通消息。
- 有序消息：支持有序消息。
- 延迟消息：支持延迟消息。
- 死信队列：支持死信队列。

## 许可证

MIT
