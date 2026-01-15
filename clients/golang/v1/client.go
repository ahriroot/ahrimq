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

const (
	KIND_SUBSCRIBER      uint8 = 11
	KIND_UNSUBSCRIBER    uint8 = 12
	KIND_SUBSCRIBE       uint8 = 13
	KIND_PUBLISH         uint8 = 14
	KIND_CONSUMER        uint8 = 21
	KIND_UNCONSUMER      uint8 = 22
	KIND_PRODUCE_NORMAL  uint8 = 24
	KIND_PRODUCE_ORDERED uint8 = 25
	KIND_PRODUCE_DELAY   uint8 = 26
	KIND_CONSUME_ACK     uint8 = 27
)

type Callback func(message []byte) error

func NewAhrimq(config Config) (*Ahrimq, error) {
	if config.Mode != Active && config.Mode != Passive {
		config.Mode = Active
	}
	if config.PingInterval == 0 {
		config.PingInterval = 60 * time.Second
	}
	if config.ReconnectInterval == 0 {
		config.ReconnectInterval = 5 * time.Second
	}
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

type Ahrimq struct {
	config         Config
	conn           net.Conn
	connMu         sync.RWMutex
	Channels       map[string]Callback
	Pending        map[string]chan *RespMsgPullMessage
	running        bool
	reconnectCount int
}

func (a *Ahrimq) dial() error {
	path := a.config.GetUnixPath()
	if path == "" {
		addr := a.config.GetAddress()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		a.connMu.Lock()
		a.conn = conn
		a.connMu.Unlock()
	} else {
		if runtime.GOOS == "windows" {
			return fmt.Errorf("Unix socket not supported on Windows")
		}
		conn, err := net.Dial("unix", path)
		if err != nil {
			return err
		}
		a.connMu.Lock()
		a.conn = conn
		a.connMu.Unlock()
	}
	return nil
}

func (a *Ahrimq) authenticate() error {
	req := ReqMsgAuthorizer{
		AccessKey:    a.config.AccessKey,
		AccessSecret: a.config.AccessSecret,
	}
	messageBytes, err := Serialize(req)
	if err != nil {
		return err
	}

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
		case TypeRespSubscribe:
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
			resp := msg.(RespMsgConsume)
			if cb, ok := a.Channels[resp.Topic]; ok {
				go func() {
					result := cb(resp.Message)
					if errors.Is(result, ConsumeAck) {
						a.Ack(resp.Topic, resp.ID)
					}
				}()
			} else {
				for _, cback := range callback {
					go cback(msg)
				}
			}
		case TypeRespPullMessage:
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
			for _, cback := range callback {
				go cback(msg)
			}
		}
	}
}

func (a *Ahrimq) handleDisconnect() {
	a.connMu.Lock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
	a.connMu.Unlock()

	a.reconnectCount++
	if a.config.MaxReconnectAttempts > 0 && a.reconnectCount >= a.config.MaxReconnectAttempts {
		log.Printf("Max reconnection attempts (%d) reached, stopping", a.config.MaxReconnectAttempts)
		a.running = false
		return
	}

	log.Printf("Connection lost, attempting to reconnect... (attempt %d)", a.reconnectCount)

	for a.running {
		time.Sleep(a.config.ReconnectInterval)
		if !a.running {
			break
		}

		if err := a.dial(); err != nil {
			log.Printf("Reconnection failed: %v, retrying in %v...", err, a.config.ReconnectInterval)
			continue
		}

		if err := a.authenticate(); err != nil {
			log.Printf("Authentication failed: %v, retrying...", err)
			a.connMu.Lock()
			if a.conn != nil {
				a.conn.Close()
				a.conn = nil
			}
			a.connMu.Unlock()
			continue
		}

		log.Printf("Successfully reconnected after %d attempts", a.reconnectCount)
		a.reconnectCount = 0
		break
	}
}

func (a *Ahrimq) Connect(callback ...func(message interface{})) error {
	a.running = true
	a.reconnectCount = 0

	if err := a.dial(); err != nil {
		return fmt.Errorf("Failed to connect: %w", err)
	}

	if err := a.authenticate(); err != nil {
		return fmt.Errorf("Failed to authenticate: %w", err)
	}

	if a.config.PingInterval < time.Second*5 {
		a.config.PingInterval = time.Second * 5
	}

	go a.startPingLoop()
	go a.startMessageReceiver(callback...)

	return nil
}

func (a *Ahrimq) Close() {
	a.running = false
	a.connMu.Lock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
	a.connMu.Unlock()
}

func (a *Ahrimq) Subscribe(topic string, callback Callback) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgSubscriber{
		Topic: topic,
	}
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
	a.Channels[topic] = callback
	return nil
}

func (a *Ahrimq) Unsubscribe(topic string) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgUnsubscriber{
		Topic: topic,
	}
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

func (a *Ahrimq) Publish(topic string, content []byte) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgPublish{
		Topic:   topic,
		Message: content,
	}
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

func (a *Ahrimq) Consume(topic string, callback Callback) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgConsumerTopic{
		Topic: topic,
	}
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
	a.Channels[topic] = callback
	return nil
}

func (a *Ahrimq) Ack(topic string, messageID uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgConsumeAck{
		ID: messageID,
	}
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

func (a *Ahrimq) AckMulti(topic string, messageIDs []uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgConsumeAckMulti{
		IDs: messageIDs,
	}
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

func (a *Ahrimq) Reconsume(topic string, messageID uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqReconsumeLater{
		ID: messageID,
	}
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

func (a *Ahrimq) ReconsumeDelay(topic string, messageID uint64, delay uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqReconsumeDelay{
		ID:    messageID,
		Delay: delay,
	}
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

func (a *Ahrimq) Unconsume(topic string) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgUnConsumer{
		Topic: topic,
	}
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

func (a *Ahrimq) PullMessage(topic string, total uint32, timeout ...time.Duration) (*RespMsgPullMessage, error) {
	if a.conn == nil {
		return nil, fmt.Errorf("Not connected to server")
	}
	responseChan := make(chan *RespMsgPullMessage, 1)
	a.Pending[topic] = responseChan
	message := ReqMsgPullMessage{
		Topic: topic,
		Total: total,
	}
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
	timeoutDuration := 5 * time.Second
	if len(timeout) > 0 {
		timeoutDuration = timeout[0]
	}
	select {
	case resp := <-responseChan:
		return resp, nil
	case <-time.After(timeoutDuration):
		return nil, errors.New("请求超时")
	}
}

func (a *Ahrimq) ProduceNormal(topic string, content []byte) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgProduceNormal{
		Topic:   topic,
		Message: content,
	}
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

func (a *Ahrimq) ProduceOrdered(topic string, content []byte) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgProduceOrdered{
		Topic:   topic,
		Message: content,
	}
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

func (a *Ahrimq) ProduceDelay(topic string, content []byte, timestamp uint64) error {
	if a.conn == nil {
		return fmt.Errorf("Not connected to server")
	}
	message := ReqMsgProduceDelay{
		Topic:   topic,
		Message: content,
		Delay:   timestamp,
	}
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
