# AhriMQ Golang SDK

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![License](https://img.shields.io/github/license/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq)

> AhriMQ (AMQ) Golang SDK - 支持自动重连的高性能消息队列客户端。

## 特性

- ✅ **自动重连** - 服务器断开时自动重连
- ✅ **发布/订阅** - 支持发布和订阅消息
- ✅ **普通消息** - 支持普通消息
- ✅ **有序消息** - 支持有序消息
- ✅ **延迟消息** - 支持延迟消息
- ✅ **死信队列** - 支持死信队列
- ✅ **消费者模式** - 从队列拉取消息
- ✅ **消息确认** - 支持 ACK 和批量 ACK

## 安装

```bash
go get github.com/ahriroot/ahrimq/clients/golang/v1
```

## 快速开始

### 基础使用

```go
package main

import (
    "log"
    "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
    // 创建配置
    config := v1.NewConfig()
    config.Host = "127.0.0.1"
    config.Port = 60001
    config.AccessKey = "your_access_key"
    config.AccessSecret = "your_access_secret"

    // 创建客户端
    client, err := v1.NewAhrimq(config)
    if err != nil {
        log.Fatal(err)
    }

    // 连接服务器
    err = client.Connect(func(msg interface{}) {
        log.Printf("收到消息: %v", msg)
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // 订阅主题
    err = client.Subscribe("test_topic", func(message []byte) error {
        log.Printf("收到: %s", string(message))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }

    // 保持运行
    select {}
}
```

### 发布消息

```go
// 发布普通消息
err := client.Publish("test_topic", []byte("你好, AhriMQ!"))

// 发布有序消息
err := client.PublishOrdered("test_topic", []byte("有序消息"))

// 发布延迟消息（延迟秒数）
err := client.PublishDelay("test_topic", []byte("延迟消息"), 60)
```

### 消费者模式

```go
// 添加消费者主题
err := client.ConsumerTopic("test_topic")

// 拉取消息
msg, err := client.Pull("test_topic", 1)
if err == nil {
    log.Printf("拉取消息: %s", string(msg.Message))

    // 确认消息
    client.Ack("test_topic", msg.ID)

    // 或批量确认
    client.AckMulti("test_topic", []uint64{msg.ID})
}

// 移除消费者主题
err := client.UnconsumerTopic("test_topic")
```

### 取消订阅

```go
err := client.Unsubscribe("test_topic")
```

## 自动重连

SDK 支持服务器断开时的自动重连：

```go
config := v1.NewConfig()

// 配置重连
config.ReconnectInterval = 5 * time.Second  // 每 5 秒尝试重连
config.MaxReconnectAttempts = 0           // 0 表示无限重连

client, _ := v1.NewAhrimq(config)
client.Connect(func(msg interface{}) {
    // 处理消息
})
```

当 AhriMQ 服务器挂掉时：
- ✅ 客户端自动重连
- ✅ 后端继续运行
- ✅ 服务不中断

详细信息请参阅 [RECONNECT.md](./RECONNECT.md)。

## 配置

### 配置选项

| 选项 | 类型 | 默认值 | 说明 |
|------|--------|---------|------|
| `Host` | string | "127.0.0.1" | 服务器地址 |
| `Port` | int | 60001 | 服务器端口 |
| `AccessKey` | string | "" | 访问密钥 |
| `AccessSecret` | string | "" | 访问密钥 |
| `Mode` | Mode | Active | 连接模式（Active/Passive）|
| `PingInterval` | time.Duration | 60s | 心跳间隔 |
| `ReconnectInterval` | time.Duration | 5s | 重连间隔 |
| `MaxReconnectAttempts` | int | 0 | 最大重连次数（0 = 无限）|

### Unix Socket 支持

```go
config := v1.NewConfig()
config.Path = "/tmp/ahrimq.sock"  // 使用 Unix Socket 代替 TCP

client, _ := v1.NewAhrimq(config)
```

## 示例

查看 [example](./example) 目录获取更多示例：

- `subscribe.go` - 订阅主题
- `publish.go` - 发布消息
- `consumer.go` - 消费者模式
- `reconnect_example.go` - 自动重连演示

## 错误处理

```go
err := client.Publish("test_topic", []byte("消息"))
if err != nil {
    // 处理错误
    log.Printf("发布失败: %v", err)
}
```

常见错误：
- `ErrInvalidAccessKeyOrSecret` - 无效的凭证
- `ErrTopicNotFound` - 主题未找到
- `ErrMessageNotFound` - 消息未找到
- 网络错误 - 由自动重连处理

## 最佳实践

1. **关闭时始终调用 `Close()`**
2. **根据网络条件设置合适的 `PingInterval`**
3. **生产环境设置 `MaxReconnectAttempts`** 防止无限重试
4. **处理所有操作的错误**
5. **使用批量 ACK** 提高性能

## 许可证

MIT
