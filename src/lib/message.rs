use std::error::Error;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode, PartialEq, Eq)]
pub enum MessageStatus {
    New,
    Reconsume,
    Pending(u8, u64, u64),
    Dead,
    Acked,
}

impl MessageStatus {
    pub fn is_pending(&self) -> bool {
        match self {
            MessageStatus::Pending(_, _, _) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct MessageHistory {
    pub status: MessageStatus,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct MessageBox {
    pub id: u64,
    pub status: MessageStatus,
    pub timestamp: u64,
    pub message: Message,
    pub history: Vec<MessageHistory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Encode, Decode)]
pub enum MsgStatus {
    Success,
    Warning,
    Failure,
    Error,
}

/// Message for Amq.
///
/// # Variants
///
/// - `ReqPing`: Request ping.
/// - `RespPing`: Response ping.
/// - `ReqAuthorizer`: Request authorizer.
/// - `RespAuthorizer`: Response authorizer.
/// - `ReqSubscribeTopic`: Request subscribe topic.
/// - `RespSubscribeTopic`: Response subscribe topic.
/// - `ReqUnsubscribeTopic`: Request unsubscribe topic.
/// - `RespUnsubscribeTopic`: Response unsubscribe topic.
/// - `ReqPublish`: Request publish message.
/// - `RespPublish`: Response publish message.
/// - `ReqSubscribe`: Request subscribe message.
/// - `RespSubscribe`: Response subscribe message.
/// - `ReqConsumerTopic`: Request consumer topic.
/// - `RespConsumerTopic`: Response consumer topic.
/// - `ReqUnconsumerTopic`: Request unconsumer topic.
/// - `RespUnconsumerTopic`: Response unconsumer topic.
/// - `ReqPullMessage`: Request pull message.
/// - `RespPullMessage`: Response pull message.
/// - `ReqProduceNormal`: Request produce normal message.
/// - `ReqProduceOrdered`: Request produce ordered message.
/// - `ReqProduceDelay`: Request produce delay message.
/// - `RespProduceNormal`: Response produce normal message.
/// - `RespProduceOrdered`: Response produce ordered message.
/// - `RespProduceDelay`: Response produce delay message.
/// - `ReqConsume`: Request consume message.
/// - `RespConsume`: Response consume message.
/// - `ReqConsumeAck`: Request consume ack message.
/// - `RespConsumeAck`: Response consume ack message.
/// - `ReqConsumeAckMulti`: Request consume ack multi message.
/// - `RespConsumeAckMulti`: Response consume ack multi message.
/// - `ReqReconsumeLater`: Request reconsume later message.
/// - `RespReconsumeLater`: Response reconsume later message.
/// - `Error`: Error message.
/// - `ReqMessageList`: Request message list.
/// - `RespMessageList`: Response message list.
#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
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
    ReqPullMessage(ReqPullMessage),
    RespPullMessage(RespPullMessage),
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
    ReqConsumeAckMulti(ReqMsgConsumeAckMulti),
    RespConsumeAckMulti(RespMsgConsumeAckMulti),
    ReqReconsumeLater(ReqReconsumeLater),
    RespReconsumeLater(RespReconsumeLater),
    ReqReconsumeDelay(ReqReconsumeDelay),
    RespReconsumeDelay(RespReconsumeDelay),
    Error(String),
    ReqMessageList(ReqMsgList),
    RespMessageList(RespMsgList),
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgPing {}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgPing {}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgAuthorizer {
    pub access_key: String,
    pub access_secret: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgAuthorizer {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgSubscriber {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgSubscriber {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgUnsubscriber {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgUnsubscriber {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgPublish {
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgPublish {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgSubscribe {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgSubscribe {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgConsumerTopic {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgConsumerTopic {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgUnconsumerTopic {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgUnconsumerTopic {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqPullMessage {
    pub topic: String,
    pub total: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespPullMsg {
    pub id: u64,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespPullMessage {
    pub topic: String,
    pub messages: Vec<RespPullMsg>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgProduceNormal {
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgProduceNormal {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgProduceOrdered {
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgProduceOrdered {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgProduceDelay {
    pub topic: String,
    pub message: Vec<u8>,
    pub delay: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgProduceDelay {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub msg: String,
    pub delay: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgConsume {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgConsume {
    pub id: u64,
    pub topic: String,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgConsumeAck {
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgConsumeAck {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgConsumeAckMulti {
    pub ids: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgConsumeAckMulti {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqReconsumeLater {
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespReconsumeLater {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqReconsumeDelay {
    pub id: u64,
    pub delay: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespReconsumeDelay {
    pub id: u64,
    pub status: MsgStatus,
    pub msg: String,
    pub delay: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct ReqMsgList {
    pub topic: String,
    pub page_size: u32,
    pub page_num: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Encode, Decode)]
pub struct RespMsgList {
    pub id: u64,
    pub status: MsgStatus,
    pub topic: String,
    pub message_list: Vec<MessageBox>,
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
