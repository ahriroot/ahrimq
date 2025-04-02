package v1

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
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

func NewAhrimq(config Config) *Ahrimq {
	return &Ahrimq{
		conn:     nil,
		Channels: make(map[string]Callback),
	}
}

type Ahrimq struct {
	conn     net.Conn
	Channels map[string]Callback
}

func (a *Ahrimq) Connect(callback ...func(message interface{})) error {
	conn, err := net.Dial("tcp", "127.0.0.1:60001")
	if err != nil {
		return err
	}
	a.conn = conn

	go func() error {
		for {
			time.Sleep(5 * time.Second)
			message := ReqMsgPing{}
			messageBytes, err := Serialize(message)
			if err != nil {
				break
			}
			if err := binary.Write(a.conn, binary.BigEndian, uint32(len(messageBytes))); err != nil {
				break
			}
			_, err = a.conn.Write(messageBytes)
			if err != nil {
				break
			}
		}
		return nil
	}()

	go func() {
		for {
			var respLen uint32
			if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
				fmt.Println("Read error: ", err)
				log.Fatal("Read length error: ", err)
			}

			// 3. 读取响应数据
			message := make([]byte, respLen)
			if _, err := conn.Read(message); err != nil {
				log.Fatal("Read data error: ", err)
			}

			t, msg, err := Deserialize(message)
			if err != nil {
				break
			}
			switch t {
			case TypeRespPing:
				fmt.Println("RespPing")
			case TypeRespSubscribe:
				resp := msg.(RespMsgSubscribe)
				if cb, ok := a.Channels[resp.Topic]; ok {
					go func() {
						result := cb(resp.Message)
						_ = result
					}()
				} else {
					for _, cb := range callback {
						go cb(msg)
					}
				}
			case TypeRespConsume:
				resp := msg.(RespMsgConsume)
				if cb, ok := a.Channels[resp.Topic]; ok {
					go func() {
						result := cb(resp.Message)
						_ = result
					}()
				} else {
					for _, cb := range callback {
						go cb(msg)
					}
				}
			default:
				for _, cb := range callback {
					go cb(msg)
				}
			}
		}
	}()
	return nil
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
