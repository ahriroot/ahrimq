package v1

import (
	"encoding/json"
	"fmt"
)

// MsgStatus 对应 Rust 的 MsgStatus 枚举
type MsgStatus string

const (
	MsgStatusSuccess MsgStatus = "Success"
	MsgStatusWarning MsgStatus = "Warning"
	MsgStatusFailure MsgStatus = "Failure"
	MsgStatusError   MsgStatus = "Error"
)

// MessageType 定义所有消息类型
const (
	TypeReqPing              = "ReqPing"
	TypeRespPing             = "RespPing"
	TypeReqSubscribeTopic    = "ReqSubscribeTopic"
	TypeRespSubscribeTopic   = "RespSubscribeTopic"
	TypeUnsubscribeTopic     = "UnsubscribeTopic"
	TypeRespUnsubscribeTopic = "RespUnsubscribeTopic"
	TypeReqPublish           = "ReqPublish"
	TypeRespPublish          = "RespPublish"
	TypeReqSubscribe         = "ReqSubscribe"
	TypeRespSubscribe        = "RespSubscribe"
	TypeReqConsumerTopic     = "ReqConsumerTopic"
	TypeRespConsumerTopic    = "RespConsumerTopic"
	TypeReqUnConsumer        = "ReqUnConsumer"
	TypeRespUnConsumer       = "RespUnConsumer"
	TypeReqProduceNormal     = "ReqProduceNormal"
	TypeReqProduceOrdered    = "ReqProduceOrdered"
	TypeReqProduceDelay      = "ReqProduceDelay"
	TypeRespProduceNormal    = "RespProduceNormal"
	TypeRespProduceOrdered   = "RespProduceOrdered"
	TypeRespProduceDelay     = "RespProduceDelay"
	TypeReqConsume           = "ReqConsume"
	TypeRespConsume          = "RespConsume"
	TypeReqConsumeAck        = "ReqConsumeAck"
	TypeRespConsumeAck       = "RespConsumeAck"
	TypeError                = "Error"
)

// 自定义类型替代 []byte
type ByteArray []byte

// 实现 json.Marshaler 接口
func (b ByteArray) MarshalJSON() ([]byte, error) {
	// 将字节切片转换为JSON数组格式
	result := make([]json.Number, len(b))
	for i, v := range b {
		result[i] = json.Number(fmt.Sprintf("%d", v))
	}
	return json.Marshal(result)
}

// Message 顶级消息结构
type Message struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ReqMsgPing 心跳请求
type ReqMsgPing struct {
}

// RespMsgPing 心跳响应
type RespMsgPing struct {
	Msg string `json:"msg"`
}

// ReqMsgSubscriber 订阅请求
type ReqMsgSubscriber struct {
	Topic string `json:"topic"`
}

// RespMsgSubscriber 订阅响应
type RespMsgSubscriber struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgUnsubscriber 取消订阅请求
type ReqMsgUnsubscriber struct {
	Topic string `json:"topic"`
}

// RespMsgUnsubscriber 取消订阅响应
type RespMsgUnsubscriber struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgPublish 发布请求
type ReqMsgPublish struct {
	Topic   string    `json:"topic"`
	Message ByteArray `json:"message"` // 实际使用可能需要Base64编码
}

// RespMsgPublish 发布响应
type RespMsgPublish struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgSubscribe 订阅请求
type ReqMsgSubscribe struct {
	Topic string `json:"topic"`
}

// RespMsgSubscribe 订阅响应
type RespMsgSubscribe struct {
	ID      uint64    `json:"id"`
	Status  MsgStatus `json:"status"`
	Topic   string    `json:"topic"`
	Message ByteArray `json:"message"`
}

// ReqMsgConsumerTopic 消费请求
type ReqMsgConsumerTopic struct {
	Topic string `json:"topic"`
}

// RespMsgConsumerTopic 消费响应
type RespMsgConsumerTopic struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgUnConsumer 取消消费请求
type ReqMsgUnConsumer struct {
	Topic string `json:"topic"`
}

// RespMsgUnConsumer 取消消费响应
type RespMsgUnConsumer struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgProduceNormal 普通生产请求
type ReqMsgProduceNormal struct {
	Topic   string    `json:"topic"`
	Message ByteArray `json:"message"`
}

// RespMsgProduceNormal 普通生产响应
type RespMsgProduceNormal struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgProduceOrdered 有序生产请求
type ReqMsgProduceOrdered struct {
	Topic   string    `json:"topic"`
	Message ByteArray `json:"message"`
}

// RespMsgProduceOrdered 有序生产响应
type RespMsgProduceOrdered struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
}

// ReqMsgProduceDelay 延时生产请求
type ReqMsgProduceDelay struct {
	Topic   string    `json:"topic"`
	Message ByteArray `json:"message"`
	Delay   uint64    `json:"delay"`
}

// RespMsgProduceDelay 延时生产响应
type RespMsgProduceDelay struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Topic  string    `json:"topic"`
	Msg    string    `json:"msg"`
	Delay  uint64    `json:"delay"`
}

type ReqMsgConsume struct {
	Topic string `json:"topic"`
}

type RespMsgConsume struct {
	ID      uint64    `json:"id"`
	Topic   string    `json:"topic"`
	Message ByteArray `json:"message"`
}

// ReqMsgConsumeAck 消费确认请求
type ReqMsgConsumeAck struct {
	ID uint64 `json:"id"`
}

// RespMsgConsumeAck 消费确认响应
type RespMsgConsumeAck struct {
	ID     uint64    `json:"id"`
	Status MsgStatus `json:"status"`
	Msg    string    `json:"msg"`
}

// SerializeMessage 序列化消息为JSON
func Serialize(msg interface{}) ([]byte, error) {
	// 获取消息类型
	var msgType string
	switch msg.(type) {
	case ReqMsgPing:
		msgType = TypeReqPing
	case RespMsgPing:
		msgType = TypeRespPing
	case ReqMsgSubscriber:
		msgType = TypeReqSubscribeTopic
	case RespMsgSubscriber:
		msgType = TypeRespSubscribeTopic
	case ReqMsgUnsubscriber:
		msgType = TypeUnsubscribeTopic
	case RespMsgUnsubscriber:
		msgType = TypeRespUnsubscribeTopic
	case ReqMsgPublish:
		msgType = TypeReqPublish
	case RespMsgPublish:
		msgType = TypeRespPublish
	case ReqMsgSubscribe:
		msgType = TypeReqSubscribe
	case RespMsgSubscribe:
		msgType = TypeRespSubscribe
	case ReqMsgConsumerTopic:
		msgType = TypeReqConsumerTopic
	case RespMsgConsumerTopic:
		msgType = TypeRespConsumerTopic
	case ReqMsgUnConsumer:
		msgType = TypeReqUnConsumer
	case RespMsgUnConsumer:
		msgType = TypeRespUnConsumer
	case ReqMsgProduceNormal:
		msgType = TypeReqProduceNormal
	case RespMsgProduceNormal:
		msgType = TypeRespProduceNormal
	case ReqMsgProduceOrdered:
		msgType = TypeReqProduceOrdered
	case RespMsgProduceOrdered:
		msgType = TypeRespProduceOrdered
	case ReqMsgProduceDelay:
		msgType = TypeReqProduceDelay
	case RespMsgProduceDelay:
		msgType = TypeRespProduceDelay
	case ReqMsgConsume:
		msgType = TypeReqConsume
	case RespMsgConsume:
		msgType = TypeRespConsume
	case ReqMsgConsumeAck:
		msgType = TypeReqConsumeAck
	case RespMsgConsumeAck:
		msgType = TypeRespConsumeAck
	case string: // Error 类型
		msgType = TypeError
	default:
		return nil, fmt.Errorf("unknown message type: %T", msg)
	}

	// 序列化数据部分
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message data: %w", err)
	}

	// 构建完整消息
	fullMsg := Message{
		Type: msgType,
		Data: data,
	}

	var fullMsgBytes []byte
	fullMsgBytes, err = json.Marshal(fullMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message envelope: %w", err)
	}

	return fullMsgBytes, nil
}

// MessageFromBytes 从字节数组反序列化消息
func MessageFromBytes(data []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return Message{}, fmt.Errorf("failed to unmarshal message envelope: %w", err)
	}
	return msg, nil
}

// DeserializeMessage 反序列化JSON消息
func Deserialize(data []byte) (string, interface{}, error) {
	msg, err := MessageFromBytes(data)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse message: %w", err)
	}

	switch msg.Type {
	case TypeReqPing:
		var req ReqMsgPing
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqPing, req, err
	case TypeRespPing:
		var resp RespMsgPing
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespPing, resp, err
	case TypeReqSubscribeTopic:
		var req ReqMsgSubscriber
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqSubscribeTopic, req, err
	case TypeRespSubscribeTopic:
		var resp RespMsgSubscriber
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespSubscribeTopic, resp, err
	case TypeUnsubscribeTopic:
		var req ReqMsgUnsubscriber
		err := json.Unmarshal(msg.Data, &req)
		return TypeUnsubscribeTopic, req, err
	case TypeRespUnsubscribeTopic:
		var resp RespMsgUnsubscriber
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespUnsubscribeTopic, resp, err
	case TypeReqPublish:
		var req ReqMsgPublish
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqPublish, req, err
	case TypeRespPublish:
		var resp RespMsgPublish
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespPublish, resp, err
	case TypeReqSubscribe:
		var req ReqMsgSubscribe
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqSubscribe, req, err
	case TypeRespSubscribe:
		var resp RespMsgSubscribe
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespSubscribe, resp, err
	case TypeReqConsumerTopic:
		var req ReqMsgConsumerTopic
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqConsumerTopic, req, err
	case TypeRespConsumerTopic:
		var resp RespMsgConsumerTopic
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespConsumerTopic, resp, err
	case TypeReqUnConsumer:
		var req ReqMsgUnConsumer
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqUnConsumer, req, err
	case TypeRespUnConsumer:
		var resp RespMsgUnConsumer
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespUnConsumer, resp, err
	case TypeReqProduceNormal:
		var req ReqMsgProduceNormal
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqProduceNormal, req, err
	case TypeRespProduceNormal:
		var resp RespMsgProduceNormal
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespProduceNormal, resp, err
	case TypeReqProduceOrdered:
		var req ReqMsgProduceOrdered
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqProduceOrdered, req, err
	case TypeRespProduceOrdered:
		var resp RespMsgProduceOrdered
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespProduceOrdered, resp, err
	case TypeReqProduceDelay:
		var req ReqMsgProduceDelay
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqProduceDelay, req, err
	case TypeRespProduceDelay:
		var resp RespMsgProduceDelay
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespProduceDelay, resp, err
	case TypeReqConsume:
		var req ReqMsgConsume
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqConsume, req, err
	case TypeRespConsume:
		var resp RespMsgConsume
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespConsume, resp, err
	case TypeReqConsumeAck:
		var req ReqMsgConsumeAck
		err := json.Unmarshal(msg.Data, &req)
		return TypeReqConsumeAck, req, err
	case TypeRespConsumeAck:
		var resp RespMsgConsumeAck
		err := json.Unmarshal(msg.Data, &resp)
		return TypeRespConsumeAck, resp, err
	case TypeError:
		var errMsg string
		err := json.Unmarshal(msg.Data, &errMsg)
		return TypeError, errMsg, err
	default:
		return "", nil, fmt.Errorf("unknown message type: %s", msg.Type)
	}
}
