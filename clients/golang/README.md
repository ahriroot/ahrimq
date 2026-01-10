# AhriMQ Golang SDK

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![License](https://img.shields.io/github/license/ahriroot/ahrimq?style=flat-square)](https://github.com/ahriroot/ahrimq)

> AhriMQ (AMQ) Golang SDK - High-performance message queue client with automatic reconnection support.

## Features

- ✅ **Automatic Reconnection** - Automatically reconnect when server goes down
- ✅ **Pub/Sub** - Support for publishing and subscribing to messages
- ✅ **Normal Messages** - Support for normal messages
- ✅ **Ordered Messages** - Support for ordered messages
- ✅ **Delay Messages** - Support for delayed messages
- ✅ **Dead Letter Queues** - Support for dead letter queues
- ✅ **Consumer Mode** - Pull messages from queue
- ✅ **Message Acknowledgment** - Support for ACK and batch ACK

## Installation

```bash
go get github.com/ahriroot/ahrimq/clients/golang/v1
```

## Quick Start

### Basic Usage

```go
package main

import (
    "log"
    "github.com/ahriroot/ahrimq/clients/golang/v1"
)

func main() {
    // Create config
    config := v1.NewConfig()
    config.Host = "127.0.0.1"
    config.Port = 60001
    config.AccessKey = "your_access_key"
    config.AccessSecret = "your_access_secret"

    // Create client
    client, err := v1.NewAhrimq(config)
    if err != nil {
        log.Fatal(err)
    }

    // Connect to server
    err = client.Connect(func(msg interface{}) {
        log.Printf("Received message: %v", msg)
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Subscribe to topic
    err = client.Subscribe("test_topic", func(message []byte) error {
        log.Printf("Received: %s", string(message))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }

    // Keep running
    select {}
}
```

### Publish Messages

```go
// Publish normal message
err := client.Publish("test_topic", []byte("Hello, AhriMQ!"))

// Publish ordered message
err := client.PublishOrdered("test_topic", []byte("Ordered message"))

// Publish delay message (delay in seconds)
err := client.PublishDelay("test_topic", []byte("Delayed message"), 60)
```

### Consumer Mode

```go
// Add consumer topic
err := client.ConsumerTopic("test_topic")

// Pull message
msg, err := client.Pull("test_topic", 1)
if err == nil {
    log.Printf("Pulled message: %s", string(msg.Message))

    // Acknowledge message
    client.Ack("test_topic", msg.ID)

    // Or batch acknowledge
    client.AckMulti("test_topic", []uint64{msg.ID})
}

// Remove consumer topic
err := client.UnconsumerTopic("test_topic")
```

### Unsubscribe

```go
err := client.Unsubscribe("test_topic")
```

## Auto Reconnection

The SDK supports automatic reconnection when the server goes down:

```go
config := v1.NewConfig()

// Configure reconnection
config.ReconnectInterval = 5 * time.Second  // Retry every 5 seconds
config.MaxReconnectAttempts = 0           // 0 = infinite retries

client, _ := v1.NewAhrimq(config)
client.Connect(func(msg interface{}) {
    // Handle messages
})
```

When AhriMQ server goes down:
- ✅ Client automatically reconnects
- ✅ Backend continues running
- ✅ No service interruption

For detailed information, see [RECONNECT.md](./RECONNECT.md).

## Configuration

### Config Options

| Option | Type | Default | Description |
|---------|--------|----------|-------------|
| `Host` | string | "127.0.0.1" | Server address |
| `Port` | int | 60001 | Server port |
| `AccessKey` | string | "" | Access key |
| `AccessSecret` | string | "" | Access secret |
| `Mode` | Mode | Active | Connection mode (Active/Passive) |
| `PingInterval` | time.Duration | 60s | Heartbeat interval |
| `ReconnectInterval` | time.Duration | 5s | Reconnection interval |
| `MaxReconnectAttempts` | int | 0 | Max reconnection attempts (0 = infinite) |

### Unix Socket Support

```go
config := v1.NewConfig()
config.Path = "/tmp/ahrimq.sock"  // Use Unix socket instead of TCP

client, _ := v1.NewAhrimq(config)
```

## Examples

See [example](./example) directory for more examples:

- `subscribe.go` - Subscribe to topic
- `publish.go` - Publish messages
- `consumer.go` - Consumer mode
- `reconnect_example.go` - Auto reconnection demo

## Error Handling

```go
err := client.Publish("test_topic", []byte("message"))
if err != nil {
    // Handle error
    log.Printf("Publish failed: %v", err)
}
```

Common errors:
- `ErrInvalidAccessKeyOrSecret` - Invalid credentials
- `ErrTopicNotFound` - Topic not found
- `ErrMessageNotFound` - Message not found
- Network errors - Handled by auto-reconnection

## Best Practices

1. **Always call `Close()`** when shutting down
2. **Use appropriate `PingInterval`** based on network conditions
3. **Set `MaxReconnectAttempts`** to prevent infinite retries in production
4. **Handle errors** from all operations
5. **Use batch ACK** for better performance

## License

MIT
