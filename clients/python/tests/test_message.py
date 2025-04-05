import unittest

from ahrimq.message import *


class TestMessage(unittest.TestCase):
    def test_serialize_deserialize(self):
        # Test ReqMsgPing
        req = ReqMsgPing()
        data = serialize(req)
        self.assertEqual(data, b'{"type": "ReqPing", "data": {}}')
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_PING)
        self.assertIsInstance(msg[1], ReqMsgPing)

        # Test RespMsgPing
        resp = RespMsgPing('msg')
        data = serialize(resp)
        self.assertEqual(data, b'{"type": "RespPing", "data": {"msg": "msg"}}')
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_PING)
        instance: RespMsgPing = msg[1]
        self.assertIsInstance(instance, RespMsgPing)
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgAuthorizer
        req = ReqMsgAuthorizer('access_key', 'access_secret')
        data = serialize(req)
        self.assertEqual(
            data, b'{"type": "ReqAuthorizer", "data": {"access_key": "access_key", "access_secret": "access_secret"}}')
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_AUTHORIZER)
        instance: ReqMsgAuthorizer = msg[1]
        self.assertIsInstance(instance, ReqMsgAuthorizer)
        self.assertEqual(instance.access_key, 'access_key')
        self.assertEqual(instance.access_secret, 'access_secret')

        # Test RespMsgAuthorizer
        resp = RespMsgAuthorizer(1, MsgStatus.SUCCESS, 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespAuthorizer", "data": {"id": 1, "status": "Success", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_AUTHORIZER)
        instance: RespMsgAuthorizer = msg[1]
        self.assertIsInstance(instance, RespMsgAuthorizer)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgSubscriber
        req = ReqMsgSubscriber('topic')
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqSubscribeTopic", "data": {"topic": "topic"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_SUBSCRIBE_TOPIC)
        instance: ReqMsgSubscriber = msg[1]
        self.assertIsInstance(instance, ReqMsgSubscriber)
        self.assertEqual(instance.topic, 'topic')

        # Test RespMsgSubscriber
        resp = RespMsgSubscriber(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespSubscribeTopic", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_SUBSCRIBE_TOPIC)
        instance: RespMsgSubscriber = msg[1]
        self.assertIsInstance(instance, RespMsgSubscriber)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgUnsubscriber
        req = ReqMsgUnsubscriber('topic')
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqUnsubscribeTopic", "data": {"topic": "topic"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_UNSUBSCRIBE_TOPIC)
        instance: ReqMsgUnsubscriber = msg[1]
        self.assertIsInstance(instance, ReqMsgUnsubscriber)
        self.assertEqual(instance.topic, 'topic')

        # Test RespMsgUnsubscriber
        resp = RespMsgUnsubscriber(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespUnsubscribeTopic", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_UNSUBSCRIBE_TOPIC)
        instance: RespMsgUnsubscriber = msg[1]
        self.assertIsInstance(instance, RespMsgUnsubscriber)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgPublish
        req = ReqMsgPublish('topic', ByteArray('message', encoding="utf-8"))
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqPublish", "data": {"topic": "topic", "message": [109, 101, 115, 115, 97, 103, 101]}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_PUBLISH)
        instance: ReqMsgPublish = msg[1]
        self.assertIsInstance(instance, ReqMsgPublish)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.message, b'message')

        # Test RespMsgPublish
        resp = RespMsgPublish(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespPublish", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_PUBLISH)
        instance: RespMsgPublish = msg[1]
        self.assertIsInstance(instance, RespMsgPublish)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgSubscribe
        req = ReqMsgSubscribe('topic')
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqSubscribe", "data": {"topic": "topic"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_SUBSCRIBE)
        instance: ReqMsgSubscribe = msg[1]
        self.assertIsInstance(instance, ReqMsgSubscribe)
        self.assertEqual(instance.topic, 'topic')

        # Test RespMsgSubscribe
        resp = RespMsgSubscribe(1, MsgStatus.SUCCESS, 'topic', ByteArray('message', encoding="utf-8"))
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespSubscribe", "data": {"id": 1, "status": "Success", "topic": "topic", "message": [109, 101, 115, 115, 97, 103, 101]}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_SUBSCRIBE)
        instance: RespMsgSubscribe = msg[1]
        self.assertIsInstance(instance, RespMsgSubscribe)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.message, b'message')

        # Test ReqMsgConsumerTopic
        req = ReqMsgConsumerTopic('topic')
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqConsumerTopic", "data": {"topic": "topic"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_CONSUMER_TOPIC)
        instance: ReqMsgConsumerTopic = msg[1]
        self.assertIsInstance(instance, ReqMsgConsumerTopic)
        self.assertEqual(instance.topic, 'topic')

        # Test RespMsgConsumerTopic
        resp = RespMsgConsumerTopic(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespConsumerTopic", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_CONSUMER_TOPIC)
        instance: RespMsgConsumerTopic = msg[1]
        self.assertIsInstance(instance, RespMsgConsumerTopic)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgUnConsumer
        req = ReqMsgUnConsumer('topic')
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqUnConsumer", "data": {"topic": "topic"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_UN_CONSUMER)
        instance: ReqMsgUnConsumer = msg[1]
        self.assertIsInstance(instance, ReqMsgUnConsumer)
        self.assertEqual(instance.topic, 'topic')

        # Test RespMsgUnConsumer
        resp = RespMsgUnConsumer(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespUnConsumer", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_UN_CONSUMER)
        instance: RespMsgUnConsumer = msg[1]
        self.assertIsInstance(instance, RespMsgUnConsumer)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgProduceNormal
        req = ReqMsgProduceNormal(
            "topic", ByteArray("message", encoding="utf-8")
        )
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqProduceNormal", "data": {"topic": "topic", "message": [109, 101, 115, 115, 97, 103, 101]}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_PRODUCE_NORMAL)
        instance: ReqMsgProduceNormal = msg[1]
        self.assertIsInstance(instance, ReqMsgProduceNormal)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.message, b'message')

        # Test RespMsgProduceNormal
        resp = RespMsgProduceNormal(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespProduceNormal", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_PRODUCE_NORMAL)
        instance: RespMsgProduceNormal = msg[1]
        self.assertIsInstance(instance, RespMsgProduceNormal)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgProduceOrdered
        req = ReqMsgProduceOrdered(
            "topic",
            ByteArray("message", encoding="utf-8")
        )
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqProduceOrdered", "data": {"topic": "topic", "message": [109, 101, 115, 115, 97, 103, 101]}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_PRODUCE_ORDERED)
        instance: ReqMsgProduceOrdered = msg[1]
        self.assertIsInstance(instance, ReqMsgProduceOrdered)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.message, b'message')

        # Test RespMsgProduceOrdered
        resp = RespMsgProduceOrdered(1, MsgStatus.SUCCESS, 'topic', 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespProduceOrdered", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_PRODUCE_ORDERED)
        instance: RespMsgProduceOrdered = msg[1]
        self.assertIsInstance(instance, RespMsgProduceOrdered)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')

        # Test ReqMsgProduceDelay
        req = ReqMsgProduceDelay(
            'topic',
            ByteArray('message', encoding="utf-8"), 10
        )
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqProduceDelay", "data": {"topic": "topic", "message": [109, 101, 115, 115, 97, 103, 101], "delay": 10}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_PRODUCE_DELAY)
        instance: ReqMsgProduceDelay = msg[1]
        self.assertIsInstance(instance, ReqMsgProduceDelay)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.message, b'message')
        self.assertEqual(instance.delay, 10)

        # Test RespMsgProduceDelay
        resp = RespMsgProduceDelay(
            1, MsgStatus.SUCCESS, 'topic', "msg", delay=10
        )
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespProduceDelay", "data": {"id": 1, "status": "Success", "topic": "topic", "msg": "msg", "delay": 10}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_PRODUCE_DELAY)
        instance: RespMsgProduceDelay = msg[1]
        self.assertIsInstance(instance, RespMsgProduceDelay)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.msg, 'msg')
        self.assertEqual(instance.delay, 10)

        # Test ReqMsgConsume
        req = ReqMsgConsume('topic')
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqConsume", "data": {"topic": "topic"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_CONSUME)
        instance: ReqMsgConsume = msg[1]
        self.assertIsInstance(instance, ReqMsgConsume)
        self.assertEqual(instance.topic, 'topic')

        # Test RespMsgConsume
        resp = RespMsgConsume(
            1,
            "topic",
            ByteArray('message', encoding="utf-8")
        )
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespConsume", "data": {"id": 1, "topic": "topic", "message": [109, 101, 115, 115, 97, 103, 101]}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_CONSUME)
        instance: RespMsgConsume = msg[1]
        self.assertIsInstance(instance, RespMsgConsume)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.topic, 'topic')
        self.assertEqual(instance.message, b'message')

        # Test ReqMsgConsumeAck
        req = ReqMsgConsumeAck(1)
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqConsumeAck", "data": {"id": 1}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_CONSUME_ACK)
        instance: ReqMsgConsumeAck = msg[1]
        self.assertIsInstance(instance, ReqMsgConsumeAck)
        self.assertEqual(instance.id, 1)

        # Test RespMsgConsumeAck
        resp = RespMsgConsumeAck(1, MsgStatus.SUCCESS, 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespConsumeAck", "data": {"id": 1, "status": "Success", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_CONSUME_ACK)
        instance: RespMsgConsumeAck = msg[1]
        self.assertIsInstance(instance, RespMsgConsumeAck)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.msg, 'msg')

        # Test ReqReconsumeLater
        req = ReqReconsumeLater(1)
        data = serialize(req)
        self.assertEqual(
            data,
            b'{"type": "ReqReconsumeLater", "data": {"id": 1}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_REQ_RECONSUME_LATER)
        instance: ReqReconsumeLater = msg[1]
        self.assertIsInstance(instance, ReqReconsumeLater)
        self.assertEqual(instance.id, 1)

        # Test RespReconsumeLater
        resp = RespReconsumeLater(1, MsgStatus.SUCCESS, 'msg')
        data = serialize(resp)
        self.assertEqual(
            data,
            b'{"type": "RespReconsumeLater", "data": {"id": 1, "status": "Success", "msg": "msg"}}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_RESP_RECONSUME_LATER)
        instance: RespReconsumeLater = msg[1]
        self.assertIsInstance(instance, RespReconsumeLater)
        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.status, MsgStatus.SUCCESS)
        self.assertEqual(instance.msg, 'msg')

        # Test error
        err = "error"
        data = serialize(err)
        self.assertEqual(
            data,
            b'{"type": "Error", "data": "error"}'
        )
        msg = deserialize(data)
        self.assertEqual(msg[0], TYPE_ERROR)
        instance: str = msg[1]
        self.assertIsInstance(instance, str)
        self.assertEqual(instance, 'error')

    def test_message_invalid(self):
        # Test invalid message type when serialize
        msg = 1
        with self.assertRaises(ValueError):
            serialize(msg)

        # Test invalid message type when deserialize
        data = b'{"type": "InvalidType", "data": "data"}'
        with self.assertRaises(ValueError):
            deserialize(data)

        # Test invalid message data
        msg = 1
        with self.assertRaises(TypeError):
            custom_encoder(msg)

        # Test json.JSONDecodeError
        data = b'{"type": "ReqConsumerTopic", "data": {"topic": "topic"'
        with self.assertRaises(ValueError):
            message_from_bytes(data)
