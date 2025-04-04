use std::error::Error;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum MessageStatus {
    New,
    Reconsume,
    Pending(u8, u64, u64),
    Dead,
    Acked,
}

#[derive(Debug, Clone)]
pub struct MessageBox {
    pub id: u64,
    pub status: MessageStatus,
    pub timestamp: u64,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum MsgStatus {
    Success,
    Warning,
    Failure,
    Error,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum Message {
    ReqPing(ReqMsgPing),
    RespPing(RespMsgPing),
    ReqAuthorizer(ReqMsgAuthorizer),
    RespAuthorizer(RespMsgAuthorizer),
    ReqSubscribeTopic(ReqMsgSubscriber),
    RespSubscribeTopic(RespMsgSubscriber),
    ReqUnsubscribeTopic(ReqMsgUnsubscriber),
    RespUnsubscribeTopic(RespMsgUnsubscriber),
    ReqPublish(ReqMsgPublish),
    RespPublish(RespMsgPublish),
    ReqSubscribe(ReqMsgSubscribe),
    RespSubscribe(RespMsgSubscribe),
    ReqConsumerTopic(ReqMsgConsumerTopic),
    RespConsumerTopic(RespMsgConsumerTopic),
    ReqUnconsumerTopic(ReqMsgUnconsumerTopic),
    RespUnconsumerTopic(RespMsgUnconsumerTopic),
    ReqProduceNormal(ReqMsgProduceNormal),
    ReqProduceOrdered(ReqMsgProduceOrdered),
    ReqProduceDelay(ReqMsgProduceDelay),
    RespProduceNormal(RespMsgProduceNormal),
    RespProduceOrdered(RespMsgProduceOrdered),
    RespProduceDelay(RespMsgProduceDelay),
    ReqConsume(ReqMsgConsume),
    RespConsume(RespMsgConsume),
    ReqConsumeAck(ReqMsgConsumeAck),
    RespConsumeAck(RespMsgConsumeAck),
    ReqReconsumeLater(ReqReconsumeLater),
    RespReconsumeLater(RespReconsumeLater),
    Error(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgPing {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgPing {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgAuthorizer {
    pub access_key: String,
    pub access_secret: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgAuthorizer {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgSubscriber {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgSubscriber {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgUnsubscriber {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgUnsubscriber {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgPublish {
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgPublish {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgSubscribe {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgSubscribe {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgConsumerTopic {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgConsumerTopic {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgUnconsumerTopic {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgUnconsumerTopic {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgProduceNormal {
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgProduceNormal {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgProduceOrdered {
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgProduceOrdered {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgProduceDelay {
    pub topic: String,
    pub message: Vec<u8>,
    pub delay: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgProduceDelay {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
    pub delay: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgConsume {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgConsume {
    pub id: u64,
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqMsgConsumeAck {
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespMsgConsumeAck {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqReconsumeLater {
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RespReconsumeLater {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

impl PartialEq for Message {
    fn eq(&self, _other: &Self) -> bool {
        match (self, _other) {
            (Message::ReqPing(_), Message::ReqPing(_)) => true,
            (Message::RespPing(_), Message::RespPing(_)) => true,
            (Message::ReqAuthorizer(req1), Message::ReqAuthorizer(req2)) => {
                req1.access_key == req2.access_key && req1.access_secret == req2.access_secret
            }
            (Message::RespAuthorizer(resp1), Message::RespAuthorizer(resp2)) => {
                resp1.id == resp2.id && resp1.status == resp2.status && resp1.msg == resp2.msg
            }
            (Message::ReqSubscribeTopic(req1), Message::ReqSubscribeTopic(req2)) => {
                req1.topic == req2.topic
            }
            (Message::RespSubscribeTopic(resp1), Message::RespSubscribeTopic(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.msg == resp2.msg
            }
            (Message::ReqUnsubscribeTopic(req1), Message::ReqUnsubscribeTopic(req2)) => {
                req1.topic == req2.topic
            }
            (Message::RespUnsubscribeTopic(resp1), Message::RespUnsubscribeTopic(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.msg == resp2.msg
            }
            (Message::ReqPublish(req1), Message::ReqPublish(req2)) => {
                req1.topic == req2.topic && req1.message == req2.message
            }
            (Message::RespPublish(resp1), Message::RespPublish(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.msg == resp2.msg
            }
            (Message::ReqSubscribe(req1), Message::ReqSubscribe(req2)) => req1.topic == req2.topic,
            (Message::RespSubscribe(resp1), Message::RespSubscribe(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.message == resp2.message
            }
            (Message::ReqProduceNormal(req1), Message::ReqProduceNormal(req2)) => {
                req1.topic == req2.topic && req1.message == req2.message
            }
            (Message::RespProduceNormal(resp1), Message::RespProduceNormal(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.msg == resp2.msg
            }
            (Message::ReqProduceOrdered(req1), Message::ReqProduceOrdered(req2)) => {
                req1.topic == req2.topic && req1.message == req2.message
            }
            (Message::RespProduceOrdered(resp1), Message::RespProduceOrdered(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.msg == resp2.msg
            }
            (Message::ReqProduceDelay(req1), Message::ReqProduceDelay(req2)) => {
                req1.topic == req2.topic && req1.message == req2.message && req1.delay == req2.delay
            }
            (Message::RespProduceDelay(resp1), Message::RespProduceDelay(resp2)) => {
                resp1.id == resp2.id
                    && resp1.status == resp2.status
                    && resp1.topic == resp2.topic
                    && resp1.msg == resp2.msg
                    && resp1.delay == resp2.delay
            }
            (Message::ReqConsume(req1), Message::ReqConsume(req2)) => req1.topic == req2.topic,
            (Message::RespConsume(resp1), Message::RespConsume(resp2)) => {
                resp1.id == resp2.id && resp1.topic == resp2.topic && resp1.message == resp2.message
            }
            (Message::ReqConsumeAck(req1), Message::ReqConsumeAck(req2)) => req1.id == req2.id,
            (Message::RespConsumeAck(resp1), Message::RespConsumeAck(resp2)) => {
                resp1.id == resp2.id && resp1.status == resp2.status && resp1.msg == resp2.msg
            }
            (Message::ReqReconsumeLater(req1), Message::ReqReconsumeLater(req2)) => {
                req1.id == req2.id
            }
            (Message::RespReconsumeLater(resp1), Message::RespReconsumeLater(resp2)) => {
                resp1.id == resp2.id && resp1.status == resp2.status && resp1.msg == resp2.msg
            }
            (Message::Error(err1), Message::Error(err2)) => err1 == err2,
            _ => false,
        }
    }
}

impl Eq for Message {}

impl Message {
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let bytes = serde_json::to_vec(self)?;
        Ok(bytes)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        let message: Message = serde_json::from_slice(bytes)?;
        Ok(message)
    }
}

mod tests {
    #[test]
    fn test_serialize_deserialize() {
        let messages = vec![
            super::Message::ReqSubscribeTopic(super::ReqMsgSubscriber {
                topic: "topic".to_string(),
            }),
            super::Message::RespSubscribeTopic(super::RespMsgSubscriber {
                id: 1,
                status: super::MsgStatus::Success,
                topic: "topic".to_string(),
                msg: "subscribe success".to_string(),
            }),
            super::Message::ReqPublish(super::ReqMsgPublish {
                topic: "topic".to_string(),
                message: "message".as_bytes().to_vec(),
            }),
        ];

        for message in messages.iter() {
            let serialized = message.serialize().unwrap();
            println!(
                "serialized: {}",
                String::from_utf8(serialized.clone()).unwrap()
            );
            let deserialized = super::Message::deserialize(&serialized).unwrap();
            assert_eq!(deserialized, *message);
        }
    }
}
