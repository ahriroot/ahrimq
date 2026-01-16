// Package v1 provides the Ahrimq Go client implementation.
// This client allows connecting to an Ahrimq server, publishing messages,
// subscribing to topics, and consuming messages.
package v1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"time"
)

// Message kind constants define the type of messages exchanged between client and server.
const (
	KIND_SUBSCRIBER      uint8 = 11 // Client subscription request
	KIND_UNSUBSCRIBER    uint8 = 12 // Client unsubscription request
	KIND_SUBSCRIBE       uint8 = 13 // Server subscription response
	KIND_PUBLISH         uint8 = 14 // Client publish request
	KIND_CONSUMER        uint8 = 21 // Client consumer registration
	KIND_UNCONSUMER      uint8 = 22 // Client consumer deregistration
	KIND_PRODUCE_NORMAL  uint8 = 24 // Client normal message production
	KIND_PRODUCE_ORDERED uint8 = 25 // Client ordered message production
	KIND_PRODUCE_DELAY   uint8 = 26 // Client delayed message production
	KIND_CONSUME_ACK     uint8 = 27 // Client consume acknowledgment
)

// Callback defines the function type for message handling.
// When a message is received, this callback is invoked with the message bytes.
// The callback can return an error to indicate message processing result:
// - nil: Message processed successfully
// - ConsumeAck: Acknowledge message consumption
// - ReconsumeLater: Request message reconsumption
// - ReconsumeDelay(uint64): Request delayed message reconsumption
// - Other errors: Treat as failure

type Callback func(message []byte) error

// NewAhrimq creates a new Ahrimq client instance with the given configuration.
// It initializes default values for missing configuration fields.
// Parameters:
//   - config: Client configuration
//
// Returns:
//   - *Ahrimq: New client instance
//   - error: Error if initialization fails
func NewAhrimq(config Config) (*Ahrimq, error) {
	// Set default mode if not specified
	if config.Mode != Active && config.Mode != Passive {
		config.Mode = Active
	}
	// Set default ping interval if not specified
	if config.PingInterval == 0 {
		config.PingInterval = 60 * time.Second
	}
	// Set default reconnect interval if not specified
	if config.ReconnectInterval == 0 {
		config.ReconnectInterval = 5 * time.Second
	}

	// Create and return new client instance
	client := &Ahrimq{
		config:         config,
		conn:           nil,
		connMu:         sync.RWMutex{},
		Channels:       make(map[string]Callback),
		Pending:        make(map[string]chan *RespMsgPullMessage),
		running:        false,
		reconnectCount: 0,
	}
	return client, nil
}

// Ahrimq represents the main client structure for interacting with Ahrimq server.
// It manages connections, message handling, and callback registration.
type Ahrimq struct {
	config         Config                              // Client configuration
	conn           net.Conn                            // Underlying network connection
	connMu         sync.RWMutex                        // Mutex for connection thread safety
	Channels       map[string]Callback                 // Topic to callback mapping for subscriptions
	Pending        map[string]chan *RespMsgPullMessage // Topic to channel mapping for pull messages
	running        bool                                // Connection status flag
	reconnectCount int                                 // Number of reconnect attempts
}

// dial establishes a network connection to the Ahrimq server.
// It supports both TCP and Unix socket connections.
// Returns:
//   - error: Error if connection fails
func (a *Ahrimq) dial() error {
	path := a.config.GetUnixPath()
	if path == "" {
		// Use TCP connection if no Unix socket path is configured
		addr := a.config.GetAddress()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		// Update connection with thread safety
		a.connMu.Lock()
		a.conn = conn
		a.connMu.Unlock()
	} else {
		// Use Unix socket connection if path is configured
		if runtime.GOOS == "windows" {
			return fmt.Errorf("Unix socket not supported on Windows")
		}
		conn, err := net.Dial("unix", path)
		if err != nil {
			return err
		}
		// Update connection with thread safety
		a.connMu.Lock()
		a.conn = conn
		a.connMu.Unlock()
	}
	return nil
}

// authenticate sends an authentication request to the Ahrimq server.
// It uses the access key and secret from the client configuration.
// Returns:
//   - error: Error if authentication fails
func (a *Ahrimq) authenticate() error {
	// Create authentication request
	req := ReqMsgAuthorizer{
		AccessKey:    a.config.AccessKey,
		AccessSecret: a.config.AccessSecret,
	}
	// Serialize request to bytes
	messageBytes, err := Serialize(req)
	if err != nil {
		return err
	}

	// Get connection with read lock for thread safety
	a.connMu.RLock()
	conn := a.conn
	a.connMu.RUnlock()

	if err := binary.Write(conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = conn.Write(messageBytes)
	if err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var respLen uint32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return err
	}

	message := make([]byte, respLen)
	if _, err := conn.Read(message); err != nil {
		return err
	}

	t, msg, err := Deserialize(message)
	if err != nil {
		return err
	}

	if t != TypeRespAuthorizer {
		return fmt.Errorf("Invalid response type: %s", t)
	}
	resp := msg.(RespMsgAuthorizer)
	if resp.Status != MsgStatusSuccess {
		return ErrInvalidAccessKeyOrSecret
	}

	conn.SetReadDeadline(time.Time{})
	return nil
}

// startPingLoop starts a background loop that sends periodic ping messages to the server.
// This helps maintain the connection alive and detect disconnections promptly.
// The ping interval is configured via the client's PingInterval setting.
func (a *Ahrimq) startPingLoop() {
	for a.running {
		time.Sleep(a.config.PingInterval)

		a.connMu.RLock()
		conn := a.conn
		a.connMu.RUnlock()

		if conn == nil {
			continue
		}

		message := ReqMsgPing{}
		messageBytes, err := Serialize(message)
		if err != nil {
			log.Printf("Failed to serialize ping: %v", err)
			a.handleDisconnect()
			continue
		}
		if err := binary.Write(conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
			log.Printf("Failed to send ping: %v", err)
			a.handleDisconnect()
			continue
		}
		_, err = conn.Write(messageBytes)
		if err != nil {
			log.Printf("Failed to write ping: %v", err)
			a.handleDisconnect()
			continue
		}
	}
}

// startMessageReceiver starts a background loop that receives messages from the server.
// It handles various types of messages and dispatches them to the appropriate callbacks.
// Parameters:
//   - callback: Optional fallback callback(s) for messages without a specific topic callback
func (a *Ahrimq) startMessageReceiver(callback ...func(message interface{})) {
	for a.running {
		a.connMu.RLock()
		conn := a.conn
		a.connMu.RUnlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var respLen uint32
		if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
			log.Printf("Read length error: %v", err)
			a.handleDisconnect()
			continue
		}

		message := make([]byte, respLen)
		if _, err := conn.Read(message); err != nil {
			log.Printf("Read data error: %v", err)
			a.handleDisconnect()
			continue
		}

		t, msg, err := Deserialize(message)
		if err != nil {
			log.Printf("Deserialize error: %v", err)
			continue
		}

		switch t {
		case TypeRespPing:
			// Ignore ping responses
		case TypeRespSubscribe:
			// Handle subscription responses
			resp := msg.(RespMsgSubscribe)
			if cb, ok := a.Channels[resp.Topic]; ok {
				go func() {
					result := cb(resp.Message)
					_ = result
				}()
			} else {
				for _, cback := range callback {
					go cback(msg)
				}
			}
		case TypeRespConsume:
			// Handle consumed messages
			resp := msg.(RespMsgConsume)
			if cb, ok := a.Channels[resp.Topic]; ok {
				go func() {
					result := cb(resp.Message)
					// Handle callback result for message acknowledgment or reconsumption
					if err, ok := result.(*AmqError); ok {
						switch err.Code {
						case ConsumeAckCode:
							// Acknowledge successful message consumption
							a.Ack(resp.Topic, resp.ID)
						case ReconsumeLaterCode:
							// Request message reconsumption later
							a.ReconsumeLater(resp.Topic, resp.ID)
						case ReconsumeDelayCode:
							// Request delayed message reconsumption
							a.ReconsumeDelay(resp.Topic, resp.ID, err.Delay)
						}
					}
				}()
			} else {
				for _, cback := range callback {
					go cback(msg)
				}
			}
		case TypeRespPullMessage:
			// Handle pulled messages
			resp := msg.(RespMsgPullMessage)
			if ch, ok := a.Pending[resp.Topic]; ok {
				go func() {
					ch <- &resp
				}()
			} else {
				for _, cback := range callback {
					go cback(msg)
				}
			}
		default:
			// Handle all other message types with fallback callbacks
			for _, cback := range callback {
				go cback(msg)
			}
		}
	}
}

// handleDisconnect handles the disconnection from the server.
// It closes the current connection, increments the reconnect count, and attempts to reconnect
// according to the client's reconnect policy.
func (a *Ahrimq) handleDisconnect() {
	// Close the current connection safely
	a.connMu.Lock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
	a.connMu.Unlock()

	// Increment reconnect attempt counter
	a.reconnectCount++

	// Check if maximum reconnect attempts reached
	if a.config.MaxReconnectAttempts > 0 && a.reconnectCount >= a.config.MaxReconnectAttempts {
		log.Printf("Max reconnection attempts (%d) reached, stopping", a.config.MaxReconnectAttempts)
		a.running = false
		return
	}

	log.Printf("Connection lost, attempting to reconnect... (attempt %d)", a.reconnectCount)

	// Attempt to reconnect with backoff
	for a.running {
		time.Sleep(a.config.ReconnectInterval)
		if !a.running {
			break
		}

		// Attempt to establish a new connection
		if err := a.dial(); err != nil {
			log.Printf("Reconnection failed: %v, retrying in %v...", err, a.config.ReconnectInterval)
			continue
		}

		// Attempt to authenticate with the server
		if err := a.authenticate(); err != nil {
			log.Printf("Authentication failed: %v, retrying...", err)
			// Close the connection if authentication fails
			a.connMu.Lock()
			if a.conn != nil {
				a.conn.Close()
				a.conn = nil
			}
			a.connMu.Unlock()
			continue
		}

		// Reconnection successful
		log.Printf("Successfully reconnected after %d attempts", a.reconnectCount)
		a.reconnectCount = 0
		break
	}
}

// Connect establishes a connection to the Ahrimq server and initializes the client.
// It performs the following steps:
// 1. Sets the client running flag
// 2. Resets reconnect counter
// 3. Establishes network connection
// 4. Authenticates with the server
// 5. Validates and sets ping interval
// 6. Starts background ping loop
// 7. Starts message receiver loop
// Parameters:
//   - callback: Optional fallback callbacks for message handling
//
// Returns:
//   - error: Error if connection fails at any stage
func (a *Ahrimq) Connect(callback ...func(message interface{})) error {
	a.running = true
	a.reconnectCount = 0

	// Establish network connection
	if err := a.dial(); err != nil {
		return fmt.Errorf("Failed to connect: %w", err)
	}

	// Authenticate with the server
	if err := a.authenticate(); err != nil {
		return fmt.Errorf("Failed to authenticate: %w", err)
	}

	// Ensure ping interval is at least 5 seconds
	if a.config.PingInterval < time.Second*5 {
		a.config.PingInterval = time.Second * 5
	}

	// Start background loops
	go a.startPingLoop()                   // Keep connection alive
	go a.startMessageReceiver(callback...) // Handle incoming messages

	return nil
}

// Close gracefully closes the connection to the Ahrimq server.
// It stops all background loops and closes the network connection.
func (a *Ahrimq) Close() {
	// Stop all background loops by setting running flag to false
	a.running = false

	// Close the network connection safely
	a.connMu.Lock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
	a.connMu.Unlock()
}

// Subscribe subscribes to a topic on the Ahrimq server.
// It sends a subscription request to the server and registers a callback
// to handle messages received on the specified topic.
// Parameters:
//   - topic: The name of the topic to subscribe to
//   - callback: The function to call when messages are received
//
// Returns:
//   - error: Error if subscription fails
func (a *Ahrimq) Subscribe(topic string, callback Callback) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create subscription request message
	message := ReqMsgSubscriber{
		Topic: topic,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}

	// Register the callback for this topic
	a.Channels[topic] = callback
	return nil
}

// Unsubscribe unsubscribes from a topic on the Ahrimq server.
// It sends an unsubscription request to the server but does not remove
// the callback from the client's Channels map. Use Close() to fully clean up.
// Parameters:
//   - topic: The name of the topic to unsubscribe from
//
// Returns:
//   - error: Error if unsubscription fails
func (a *Ahrimq) Unsubscribe(topic string) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create unsubscription request message
	message := ReqMsgUnsubscriber{
		Topic: topic,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// Publish publishes a message to a topic on the Ahrimq server.
// It sends a publish request to the server with the specified topic and content.
// Parameters:
//   - topic: The name of the topic to publish to
//   - content: The message content to publish
//
// Returns:
//   - error: Error if publishing fails
func (a *Ahrimq) Publish(topic string, content []byte) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create publish request message
	message := ReqMsgPublish{
		Topic:   topic,
		Message: content,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// Consume registers as a consumer for a topic on the Ahrimq server.
// It sends a consumer registration request to the server and registers a callback
// to handle messages received on the specified topic.
// Parameters:
//   - topic: The name of the topic to consume from
//   - callback: The function to call when messages are consumed
//
// Returns:
//   - error: Error if consumer registration fails
func (a *Ahrimq) Consume(topic string, callback Callback) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create consumer registration message
	message := ReqMsgConsumerTopic{
		Topic: topic,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}

	// Register the callback for this topic
	a.Channels[topic] = callback
	return nil
}

// Ack acknowledges successful consumption of a message.
// It sends an acknowledgment to the server for the specified message ID.
// Parameters:
//   - topic: The name of the topic the message was consumed from
//   - messageID: The unique identifier of the message to acknowledge
//
// Returns:
//   - error: Error if acknowledgment fails
func (a *Ahrimq) Ack(topic string, messageID uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create acknowledgment request message
	message := ReqMsgConsumeAck{
		ID: messageID,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// AckMulti acknowledges successful consumption of multiple messages.
// It sends a batch acknowledgment to the server for the specified message IDs.
// Parameters:
//   - topic: The name of the topic the messages were consumed from
//   - messageIDs: Slice of message IDs to acknowledge
//
// Returns:
//   - error: Error if batch acknowledgment fails
func (a *Ahrimq) AckMulti(topic string, messageIDs []uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create batch acknowledgment request message
	message := ReqMsgConsumeAckMulti{
		IDs: messageIDs,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// ReconsumeLater requests the server to reconsume a message at a later time.
// It sends a reconsumption request to the server with the specified message ID.
// The server will use its default delay for reconsumption.
// Parameters:
//   - topic: The name of the topic the message was consumed from
//   - messageID: The unique identifier of the message to reconsume
//
// Returns:
//   - error: Error if reconsumption request fails
func (a *Ahrimq) ReconsumeLater(topic string, messageID uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create reconsume later request message
	message := ReqReconsumeLater{
		ID: messageID,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// ReconsumeDelay requests the server to reconsume a message after a specified delay.
// It sends a delayed reconsumption request to the server with the specified message ID and delay.
// Parameters:
//   - topic: The name of the topic the message was consumed from
//   - messageID: The unique identifier of the message to reconsume
//   - delay: The delay in seconds before reconsuming the message
//
// Returns:
//   - error: Error if delayed reconsumption request fails
func (a *Ahrimq) ReconsumeDelay(topic string, messageID uint64, delay uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create delayed reconsumption request message
	message := ReqReconsumeDelay{
		ID:    messageID,
		Delay: delay,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// Unconsume deregisters as a consumer for a topic on the Ahrimq server.
// It sends an unregister request to the server, stopping message consumption
// for the specified topic. It does not remove the callback from the client's
// Channels map. Use Close() to fully clean up.
// Parameters:
//   - topic: The name of the topic to stop consuming from
//
// Returns:
//   - error: Error if consumer deregistration fails
func (a *Ahrimq) Unconsume(topic string) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create consumer deregistration request message
	message := ReqMsgUnConsumer{
		Topic: topic,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// PullMessage pulls messages from a topic on the Ahrimq server.
// It sends a pull request to the server and waits for a response with the specified timeout.
// Parameters:
//   - topic: The name of the topic to pull messages from
//   - total: The maximum number of messages to pull
//   - timeout: Optional timeout duration (default: 5 seconds)
//
// Returns:
//   - *RespMsgPullMessage: The pulled messages response
//   - error: Error if pull operation fails or times out
func (a *Ahrimq) PullMessage(topic string, total uint32, timeout ...time.Duration) (*RespMsgPullMessage, error) {
	if a.conn == nil {
		return nil, fmt.Errorf("Not connected to server")
	}

	// Create response channel for pull message response
	responseChan := make(chan *RespMsgPullMessage, 1)
	a.Pending[topic] = responseChan

	// Create pull message request
	message := ReqMsgPullMessage{
		Topic: topic,
		Total: total,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return nil, err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return nil, err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return nil, err
	}

	// Set default timeout if not specified
	timeoutDuration := 5 * time.Second
	if len(timeout) > 0 {
		timeoutDuration = timeout[0]
	}

	// Wait for response or timeout
	select {
	case resp := <-responseChan:
		return resp, nil
	case <-time.After(timeoutDuration):
		return nil, errors.New("请求超时")
	}
}

// ProduceNormal produces a normal message to a topic on the Ahrimq server.
// It sends a normal message production request to the server with the specified content.
// Parameters:
//   - topic: The name of the topic to produce to
//   - content: The message content to produce
//
// Returns:
//   - error: Error if production fails
func (a *Ahrimq) ProduceNormal(topic string, content []byte) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create normal message production request
	message := ReqMsgProduceNormal{
		Topic:   topic,
		Message: content,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// ProduceOrdered produces an ordered message to a topic on the Ahrimq server.
// It sends an ordered message production request to the server with the specified content.
// Ordered messages are guaranteed to be consumed in the order they were produced.
// Parameters:
//   - topic: The name of the topic to produce to
//   - content: The message content to produce
//
// Returns:
//   - error: Error if production fails
func (a *Ahrimq) ProduceOrdered(topic string, content []byte) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create ordered message production request
	message := ReqMsgProduceOrdered{
		Topic:   topic,
		Message: content,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}

// ProduceDelay produces a delayed message to a topic on the Ahrimq server.
// It sends a delayed message production request to the server with the specified
// content and delay time. The message will be delivered to consumers after the delay has elapsed.
// Parameters:
//   - topic: The name of the topic to produce to
//   - content: The message content to produce
//   - timestamp: The delay in seconds before the message becomes available for consumption
//
// Returns:
//   - error: Error if production fails
func (a *Ahrimq) ProduceDelay(topic string, content []byte, timestamp uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}

	// Create delayed message production request
	message := ReqMsgProduceDelay{
		Topic:   topic,
		Message: content,
		Delay:   timestamp,
	}

	// Serialize and send the request
	messageBytes, err := Serialize(message)
	if err != nil {
		return err
	}
	if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	_, err = a.conn.Write(messageBytes)
	if err != nil {
		return err
	}
	return nil
}
