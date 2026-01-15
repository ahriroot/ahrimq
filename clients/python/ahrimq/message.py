# -*- coding: utf-8 -*-
# @unittest.skip("Skipping message format tests - these are server data format definitions "
#                "that include currently unused types and exist primarily to maintain protocol "
#                "compatibility with the Rust implementation.")

import json
from enum import Enum
from typing import Any, Dict, List, Tuple, Union


class MsgStatus(str, Enum):
    SUCCESS = "Success"
    WARNING = "Warning"
    FAILURE = "Failure"
    ERROR = "Error"


# Message type constants
TYPE_REQ_PING = "ReqPing"
TYPE_RESP_PING = "RespPing"
TYPE_REQ_AUTHORIZER = "ReqAuthorizer"
TYPE_RESP_AUTHORIZER = "RespAuthorizer"
TYPE_REQ_SUBSCRIBE_TOPIC = "ReqSubscribeTopic"
TYPE_RESP_SUBSCRIBE_TOPIC = "RespSubscribeTopic"
TYPE_UNSUBSCRIBE_TOPIC = "ReqUnsubscribeTopic"
TYPE_RESP_UNSUBSCRIBE_TOPIC = "RespUnsubscribeTopic"
TYPE_REQ_PUBLISH = "ReqPublish"
TYPE_RESP_PUBLISH = "RespPublish"
TYPE_REQ_SUBSCRIBE = "ReqSubscribe"
TYPE_RESP_SUBSCRIBE = "RespSubscribe"
TYPE_REQ_CONSUMER_TOPIC = "ReqConsumerTopic"
TYPE_RESP_CONSUMER_TOPIC = "RespConsumerTopic"
TYPE_REQ_UN_CONSUMER = "ReqUnConsumer"
TYPE_RESP_UN_CONSUMER = "RespUnConsumer"
TYPE_REQ_PULL_MESSAGE = "ReqPullMessage"
TYPE_RESP_PULL_MESSAGE = "RespPullMessage"
TYPE_REQ_PRODUCE_NORMAL = "ReqProduceNormal"
TYPE_REQ_PRODUCE_ORDERED = "ReqProduceOrdered"
TYPE_REQ_PRODUCE_DELAY = "ReqProduceDelay"
TYPE_RESP_PRODUCE_NORMAL = "RespProduceNormal"
TYPE_RESP_PRODUCE_ORDERED = "RespProduceOrdered"
TYPE_RESP_PRODUCE_DELAY = "RespProduceDelay"
TYPE_REQ_CONSUME = "ReqConsume"
TYPE_RESP_CONSUME = "RespConsume"
TYPE_REQ_CONSUME_ACK = "ReqConsumeAck"
TYPE_RESP_CONSUME_ACK = "RespConsumeAck"
TYPE_REQ_RECONSUME_LATER = "ReqReconsumeLater"
TYPE_RESP_RECONSUME_LATER = "RespReconsumeLater"
TYPE_REQ_RECONSUME_DELAY = "ReqReconsumeDelay"
TYPE_RESP_RECONSUME_DELAY = "RespReconsumeDelay"
TYPE_ERROR = "Error"


class ByteArray(bytes):
    def to_json(self) -> List[int]:
        return list(self)

    def __json__(self) -> List[int]:
        return self.to_json()


class Message:
    def __init__(self, type: str, data: Any):
        self.type = type
        self.data = data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "data": self.data
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        return cls(data["type"], data["data"])


class ReqMsgPing:
    def __init__(self):
        pass

    def to_dict(self) -> Dict[str, Any]:
        return {}

    @classmethod
    def from_dict(cls) -> 'ReqMsgPing':
        return cls()


class RespMsgPing:
    def __init__(self, msg: str):
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {"msg": self.msg}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgPing':
        return cls(data["msg"])


class ReqMsgAuthorizer:
    def __init__(self, access_key: str, access_secret: str):
        self.access_key = access_key
        self.access_secret = access_secret

    def to_dict(self) -> Dict[str, Any]:
        return {
            "access_key": self.access_key,
            "access_secret": self.access_secret
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgAuthorizer':
        return cls(data["access_key"], data["access_secret"])


class RespMsgAuthorizer:
    def __init__(self, id: int, status: MsgStatus, msg: str):
        self.id = id
        self.status = status
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgAuthorizer':
        return cls(data["id"], MsgStatus(data["status"]), data["msg"])


class ReqMsgSubscriber:
    def __init__(self, topic: str):
        self.topic = topic

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgSubscriber':
        return cls(data["topic"])


class RespMsgSubscriber:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgSubscriber':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgUnsubscriber:
    def __init__(self, topic: str):
        self.topic = topic

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgUnsubscriber':
        return cls(data["topic"])


class RespMsgUnsubscriber:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgUnsubscriber':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgPublish:
    def __init__(self, topic: str, message: ByteArray):
        self.topic = topic
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "message": self.message
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgPublish':
        return cls(data["topic"], ByteArray(data["message"]))


class RespMsgPublish:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgPublish':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgSubscribe:
    def __init__(self, topic: str):
        self.topic = topic

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgSubscribe':
        return cls(data["topic"])


class RespMsgSubscribe:
    def __init__(self, id: int, status: MsgStatus, topic: str, message: ByteArray):
        self.id = id
        self.status = status
        self.topic = topic
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "message": self.message
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgSubscribe':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], ByteArray(data["message"]))


class ReqMsgConsumerTopic:
    def __init__(self, topic: str):
        self.topic = topic

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgConsumerTopic':
        return cls(data["topic"])


class RespMsgConsumerTopic:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgConsumerTopic':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgUnConsumer:
    def __init__(self, topic: str):
        self.topic = topic

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgUnConsumer':
        return cls(data["topic"])


class RespMsgUnConsumer:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgUnConsumer':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgPullMessage:
    def __init__(self, topic: str, total: int):
        self.topic = topic
        self.total = total

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "total": self.total
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgPullMessage':
        return cls(data["topic"], data["total"])


class RespMsgPullMsg:
    def __init__(self, id: int, message: ByteArray):
        self.id = id
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "message": self.message.to_json()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgPullMsg':
        return cls(data["id"], ByteArray(data["message"]))


class RespMsgPullMessage:
    def __init__(self, topic: str, messages: List[RespMsgPullMsg]):
        self.topic = topic
        self.messages = messages

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "messages": [msg.to_dict() for msg in self.messages]
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgPullMessage':
        return cls(data["topic"], [RespMsgPullMsg.from_dict(msg) for msg in data["messages"]])


class ReqMsgProduceNormal:
    def __init__(self, topic: str, message: ByteArray):
        self.topic = topic
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "message": self.message.to_json()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgProduceNormal':
        return cls(data["topic"], ByteArray(data["message"]))


class RespMsgProduceNormal:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgProduceNormal':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgProduceOrdered:
    def __init__(self, topic: str, message: ByteArray):
        self.topic = topic
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "message": self.message.to_json()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgProduceOrdered':
        return cls(data["topic"], ByteArray(data["message"]))


class RespMsgProduceOrdered:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgProduceOrdered':
        return cls(data["id"], MsgStatus(data["status"]), data["topic"], data["msg"])


class ReqMsgProduceDelay:
    def __init__(self, topic: str, message: ByteArray, delay: int):
        self.topic = topic
        self.message = message
        self.delay = delay

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "message": self.message.to_json(),
            "delay": self.delay
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgProduceDelay':
        return cls(data["topic"], ByteArray(data["message"]), data["delay"])


class RespMsgProduceDelay:
    def __init__(self, id: int, status: MsgStatus, topic: str, msg: str, delay: int):
        self.id = id
        self.status = status
        self.topic = topic
        self.msg = msg
        self.delay = delay

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "topic": self.topic,
            "msg": self.msg,
            "delay": self.delay
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgProduceDelay':
        return cls(
            data["id"],
            MsgStatus(data["status"]),
            data["topic"],
            data["msg"],
            data["delay"]
        )


class ReqMsgConsume:
    def __init__(self, topic: str):
        self.topic = topic

    def to_dict(self) -> Dict[str, Any]:
        return {"topic": self.topic}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgConsume':
        return cls(data["topic"])


class RespMsgConsume:
    def __init__(self, id: int, topic: str, message: ByteArray):
        self.id = id
        self.topic = topic
        self.message = message

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "topic": self.topic,
            "message": self.message.to_json()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgConsume':
        return cls(data["id"], data["topic"], ByteArray(data["message"]))


class ReqMsgConsumeAck:
    def __init__(self, id: int):
        self.id = id

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqMsgConsumeAck':
        return cls(data["id"])


class RespMsgConsumeAck:
    def __init__(self, id: int, status: MsgStatus, msg: str):
        self.id = id
        self.status = status
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespMsgConsumeAck':
        return cls(data["id"], MsgStatus(data["status"]), data["msg"])


class ReqReconsumeLater:
    def __init__(self, id: int):
        self.id = id

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqReconsumeLater':
        return cls(data["id"])


class RespReconsumeLater:
    def __init__(self, id: int, status: MsgStatus, msg: str):
        self.id = id
        self.status = status
        self.msg = msg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "msg": self.msg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespReconsumeLater':
        return cls(data["id"], MsgStatus(data["status"]), data["msg"])

class ReqReconsumeDelay:
    def __init__(self, id: int, delay: int):
        self.id = id
        self.delay = delay

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "delay": self.delay
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReqReconsumeDelay':
        return cls(data["id"], data["delay"])

class RespReconsumeDelay:
    def __init__(self, id: int, status: MsgStatus, msg: str, delay: int):
        self.id = id
        self.status = status
        self.msg = msg
        self.delay = delay

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "msg": self.msg,
            "delay": self.delay
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RespReconsumeDelay':
        return cls(data["id"], MsgStatus(data["status"]), data["msg"], data["delay"])


# 自定义编码函数
def custom_encoder(obj):
    if hasattr(obj, '__json__'):
        return obj.__json__()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def serialize(msg: Any) -> bytes:
    # Get message type
    msg_type = None

    if isinstance(msg, ReqMsgPing):
        msg_type = TYPE_REQ_PING
    elif isinstance(msg, RespMsgPing):
        msg_type = TYPE_RESP_PING
    elif isinstance(msg, ReqMsgAuthorizer):
        msg_type = TYPE_REQ_AUTHORIZER
    elif isinstance(msg, RespMsgAuthorizer):
        msg_type = TYPE_RESP_AUTHORIZER
    elif isinstance(msg, ReqMsgSubscriber):
        msg_type = TYPE_REQ_SUBSCRIBE_TOPIC
    elif isinstance(msg, RespMsgSubscriber):
        msg_type = TYPE_RESP_SUBSCRIBE_TOPIC
    elif isinstance(msg, ReqMsgUnsubscriber):
        msg_type = TYPE_UNSUBSCRIBE_TOPIC
    elif isinstance(msg, RespMsgUnsubscriber):
        msg_type = TYPE_RESP_UNSUBSCRIBE_TOPIC
    elif isinstance(msg, ReqMsgPublish):
        msg_type = TYPE_REQ_PUBLISH
    elif isinstance(msg, RespMsgPublish):
        msg_type = TYPE_RESP_PUBLISH
    elif isinstance(msg, ReqMsgSubscribe):
        msg_type = TYPE_REQ_SUBSCRIBE
    elif isinstance(msg, RespMsgSubscribe):
        msg_type = TYPE_RESP_SUBSCRIBE
    elif isinstance(msg, ReqMsgConsumerTopic):
        msg_type = TYPE_REQ_CONSUMER_TOPIC
    elif isinstance(msg, RespMsgConsumerTopic):
        msg_type = TYPE_RESP_CONSUMER_TOPIC
    elif isinstance(msg, ReqMsgUnConsumer):
        msg_type = TYPE_REQ_UN_CONSUMER
    elif isinstance(msg, RespMsgUnConsumer):
        msg_type = TYPE_RESP_UN_CONSUMER
    elif isinstance(msg, ReqMsgPullMessage):
        msg_type = TYPE_REQ_PULL_MESSAGE
    elif isinstance(msg, RespMsgPullMessage):
        msg_type = TYPE_RESP_PULL_MESSAGE
    elif isinstance(msg, ReqMsgProduceNormal):
        msg_type = TYPE_REQ_PRODUCE_NORMAL
    elif isinstance(msg, RespMsgProduceNormal):
        msg_type = TYPE_RESP_PRODUCE_NORMAL
    elif isinstance(msg, ReqMsgProduceOrdered):
        msg_type = TYPE_REQ_PRODUCE_ORDERED
    elif isinstance(msg, RespMsgProduceOrdered):
        msg_type = TYPE_RESP_PRODUCE_ORDERED
    elif isinstance(msg, ReqMsgProduceDelay):
        msg_type = TYPE_REQ_PRODUCE_DELAY
    elif isinstance(msg, RespMsgProduceDelay):
        msg_type = TYPE_RESP_PRODUCE_DELAY
    elif isinstance(msg, ReqMsgConsume):
        msg_type = TYPE_REQ_CONSUME
    elif isinstance(msg, RespMsgConsume):
        msg_type = TYPE_RESP_CONSUME
    elif isinstance(msg, ReqMsgConsumeAck):
        msg_type = TYPE_REQ_CONSUME_ACK
    elif isinstance(msg, RespMsgConsumeAck):
        msg_type = TYPE_RESP_CONSUME_ACK
    elif isinstance(msg, ReqReconsumeLater):
        msg_type = TYPE_REQ_RECONSUME_LATER
    elif isinstance(msg, RespReconsumeLater):
        msg_type = TYPE_RESP_RECONSUME_LATER
    elif isinstance(msg, ReqReconsumeDelay):
        msg_type = TYPE_REQ_RECONSUME_DELAY
    elif isinstance(msg, RespReconsumeDelay):
        msg_type = TYPE_RESP_RECONSUME_DELAY
    elif isinstance(msg, str):
        msg_type = TYPE_ERROR
    else:
        raise ValueError(f"Unknown message type: {type(msg)}")

    # Serialize data
    if hasattr(msg, 'to_dict') and not isinstance(msg, str):
        data = msg.to_dict()
    else:
        data = msg  # For simple types like str (error message)

    # Build full message
    full_msg = Message(msg_type, data)
    return json.dumps(full_msg.to_dict(), default=custom_encoder).encode('utf-8')


def message_from_bytes(data: bytes) -> Message:
    try:
        msg_dict = json.loads(data.decode('utf-8'))
        return Message.from_dict(msg_dict)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to decode message: {e}")


def deserialize(data: bytes) -> Tuple[str, Any | str]:
    msg = message_from_bytes(data)

    if msg.type == TYPE_REQ_PING:
        return msg.type, ReqMsgPing.from_dict()
    elif msg.type == TYPE_RESP_PING:
        return msg.type, RespMsgPing.from_dict(msg.data)
    elif msg.type == TYPE_REQ_AUTHORIZER:
        return msg.type, ReqMsgAuthorizer.from_dict(msg.data)
    elif msg.type == TYPE_RESP_AUTHORIZER:
        return msg.type, RespMsgAuthorizer.from_dict(msg.data)
    elif msg.type == TYPE_REQ_SUBSCRIBE_TOPIC:
        return msg.type, ReqMsgSubscriber.from_dict(msg.data)
    elif msg.type == TYPE_RESP_SUBSCRIBE_TOPIC:
        return msg.type, RespMsgSubscriber.from_dict(msg.data)
    elif msg.type == TYPE_UNSUBSCRIBE_TOPIC:
        return msg.type, ReqMsgUnsubscriber.from_dict(msg.data)
    elif msg.type == TYPE_RESP_UNSUBSCRIBE_TOPIC:
        return msg.type, RespMsgUnsubscriber.from_dict(msg.data)
    elif msg.type == TYPE_REQ_PUBLISH:
        return msg.type, ReqMsgPublish.from_dict(msg.data)
    elif msg.type == TYPE_RESP_PUBLISH:
        return msg.type, RespMsgPublish.from_dict(msg.data)
    elif msg.type == TYPE_REQ_SUBSCRIBE:
        return msg.type, ReqMsgSubscribe.from_dict(msg.data)
    elif msg.type == TYPE_RESP_SUBSCRIBE:
        return msg.type, RespMsgSubscribe.from_dict(msg.data)
    elif msg.type == TYPE_REQ_CONSUMER_TOPIC:
        return msg.type, ReqMsgConsumerTopic.from_dict(msg.data)
    elif msg.type == TYPE_RESP_CONSUMER_TOPIC:
        return msg.type, RespMsgConsumerTopic.from_dict(msg.data)
    elif msg.type == TYPE_REQ_UN_CONSUMER:
        return msg.type, ReqMsgUnConsumer.from_dict(msg.data)
    elif msg.type == TYPE_RESP_UN_CONSUMER:
        return msg.type, RespMsgUnConsumer.from_dict(msg.data)
    elif msg.type == TYPE_REQ_PULL_MESSAGE:
        return msg.type, ReqMsgPullMessage.from_dict(msg.data)
    elif msg.type == TYPE_RESP_PULL_MESSAGE:
        return msg.type, RespMsgPullMessage.from_dict(msg.data)
    elif msg.type == TYPE_REQ_PRODUCE_NORMAL:
        return msg.type, ReqMsgProduceNormal.from_dict(msg.data)
    elif msg.type == TYPE_RESP_PRODUCE_NORMAL:
        return msg.type, RespMsgProduceNormal.from_dict(msg.data)
    elif msg.type == TYPE_REQ_PRODUCE_ORDERED:
        return msg.type, ReqMsgProduceOrdered.from_dict(msg.data)
    elif msg.type == TYPE_RESP_PRODUCE_ORDERED:
        return msg.type, RespMsgProduceOrdered.from_dict(msg.data)
    elif msg.type == TYPE_REQ_PRODUCE_DELAY:
        return msg.type, ReqMsgProduceDelay.from_dict(msg.data)
    elif msg.type == TYPE_RESP_PRODUCE_DELAY:
        return msg.type, RespMsgProduceDelay.from_dict(msg.data)
    elif msg.type == TYPE_REQ_CONSUME:
        return msg.type, ReqMsgConsume.from_dict(msg.data)
    elif msg.type == TYPE_RESP_CONSUME:
        return msg.type, RespMsgConsume.from_dict(msg.data)
    elif msg.type == TYPE_REQ_CONSUME_ACK:
        return msg.type, ReqMsgConsumeAck.from_dict(msg.data)
    elif msg.type == TYPE_RESP_CONSUME_ACK:
        return msg.type, RespMsgConsumeAck.from_dict(msg.data)
    elif msg.type == TYPE_REQ_RECONSUME_LATER:
        return msg.type, ReqReconsumeLater.from_dict(msg.data)
    elif msg.type == TYPE_RESP_RECONSUME_LATER:
        return msg.type, RespReconsumeLater.from_dict(msg.data)
    elif msg.type == TYPE_REQ_RECONSUME_DELAY:
        return msg.type, ReqReconsumeDelay.from_dict(msg.data)
    elif msg.type == TYPE_RESP_RECONSUME_DELAY:
        return msg.type, RespReconsumeDelay.from_dict(msg.data)
    elif msg.type == TYPE_ERROR:
        return msg.type, msg.data
    else:
        raise ValueError(f"Unknown message type: {msg.type}")
