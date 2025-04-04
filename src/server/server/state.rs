use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::{mpsc::Sender, RwLock};

use amq::{
    message::{
        Message, MessageBox, MessageStatus, MsgStatus, RespMsgConsume, RespMsgConsumeAck,
        RespMsgConsumerTopic, RespMsgProduceDelay, RespMsgProduceNormal, RespMsgPublish,
        RespMsgSubscribe, RespMsgSubscriber, RespMsgUnconsumerTopic, RespMsgUnsubscriber,
    },
    utils,
};

use super::config::Config;

type ConnectionId = u64;
type TopicId = String;
type Connections = Arc<RwLock<HashMap<ConnectionId, (HashSet<TopicId>, HashSet<TopicId>)>>>;
type Subscribers = Arc<RwLock<HashMap<TopicId, Vec<(ConnectionId, Sender<Vec<u8>>)>>>>;
type Consumers = Arc<RwLock<HashMap<TopicId, Vec<(ConnectionId, Sender<Vec<u8>>)>>>>;
type Messages = Arc<RwLock<Vec<MessageBox>>>;

pub struct State {
    pub config: Config,
    pub next_connection_id: Arc<RwLock<u64>>,
    pub connections: Connections,
    pub subscribers: Subscribers,
    pub consumers: Consumers,
    pub last_consumers_index: Arc<RwLock<usize>>,
    pub messages: Messages,
    pub task_notifier: Sender<()>,
    pub next_message_id: Arc<RwLock<u64>>,
}

impl State {
    pub fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            next_connection_id: Arc::clone(&self.next_connection_id),
            connections: Arc::clone(&self.connections),
            subscribers: Arc::clone(&self.subscribers),
            consumers: Arc::clone(&self.consumers),
            last_consumers_index: Arc::clone(&self.last_consumers_index),
            messages: Arc::clone(&self.messages),
            task_notifier: self.task_notifier.clone(),
            next_message_id: Arc::clone(&self.next_message_id),
        }
    }

    pub async fn get_connection_id(&self) -> ConnectionId {
        let mut id = self.next_connection_id.write().await;
        *id += 1;
        *id
    }

    pub async fn get_message_id(&self) -> u64 {
        let mut id = self.next_message_id.write().await;
        *id += 1;
        *id
    }

    pub async fn add_subscriber(
        &self,
        id: ConnectionId,
        topic: String,
        tx: Sender<Vec<u8>>,
    ) -> Vec<u8> {
        // 检查是否已订阅（读锁）
        {
            let conn_subs = self.connections.read().await;
            if conn_subs
                .get(&id)
                .map_or(false, |(s, _)| s.contains(&topic))
            {
                return Message::RespSubscribeTopic(RespMsgSubscriber {
                    id: 1,
                    status: MsgStatus::Failure,
                    topic: topic,
                    msg: "Already subscribed".to_string(),
                })
                .serialize()
                .unwrap();
            }
        }
        {
            let mut topics = self.subscribers.write().await;
            let mut conns = self.connections.write().await;

            // 添加到订阅列表
            topics
                .entry(topic.clone())
                .or_default()
                .push((id, tx.clone()));

            // 添加到连接集合
            conns.entry(id).or_default().0.insert(topic.clone());
            return Message::RespSubscribeTopic(RespMsgSubscriber {
                id: 1,
                status: MsgStatus::Success,
                topic: topic,
                msg: "Subscribed successfully".to_string(),
            })
            .serialize()
            .unwrap();
        }
    }

    pub async fn remove_subscriber(&self, id: ConnectionId, topic: String) -> Vec<u8> {
        {
            let conn_subs = self.connections.read().await;
            if !conn_subs
                .get(&id)
                .map_or(false, |(s, _)| s.contains(&topic))
            {
                return Message::RespUnsubscribeTopic(RespMsgUnsubscriber {
                    id: 1,
                    status: MsgStatus::Failure,
                    topic: topic,
                    msg: "Not subscribed".to_string(),
                })
                .serialize()
                .unwrap();
            }
        }

        // 获取写锁进行修改
        {
            let mut topics = self.subscribers.write().await;
            let mut conns = self.connections.write().await;

            // 从连接集合中移除
            if let Some(conn_subs) = conns.get_mut(&id) {
                conn_subs.0.remove(&topic);
            }

            // 从订阅列表中移除
            if let Some(subscribers) = topics.get_mut(&topic) {
                subscribers.retain(|(cid, _)| *cid != id);

                // 如果该列表没有订阅者了，移除条目
                if subscribers.is_empty() {
                    topics.remove(&topic);
                }
            }
            return Message::RespUnsubscribeTopic(RespMsgUnsubscriber {
                id: 1,
                status: MsgStatus::Success,
                topic: topic,
                msg: "Unsubscribed successfully".to_string(),
            })
            .serialize()
            .unwrap();
        }
    }

    pub async fn cleanup_connection(&self, id: ConnectionId) {
        // 获取该连接的所有订阅
        let subscribed_topics = {
            let mut conns = self.connections.write().await;
            conns.remove(&id)
        };

        if let Some((subscribed_topics, consummed_topics)) = subscribed_topics {
            // 从每个列表的订阅列表中移除该连接
            let mut topics = self.subscribers.write().await;

            for topic_id in subscribed_topics {
                if let Some(subscribers) = topics.get_mut(&topic_id) {
                    subscribers.retain(|(cid, _)| *cid != id);

                    // 清理空列表
                    if subscribers.is_empty() {
                        topics.remove(&topic_id);
                    }
                }
            }
            let mut topics = self.consumers.write().await;

            for topic_id in consummed_topics {
                if let Some(consumers) = topics.get_mut(&topic_id) {
                    consumers.retain(|(cid, _)| *cid != id);

                    // 清理空列表
                    if consumers.is_empty() {
                        topics.remove(&topic_id);
                    }
                }
            }
        }
    }

    pub async fn publish(&self, topic: String, message: Vec<u8>) -> Vec<u8> {
        let subscribers = self.subscribers.read().await;
        if let Some(txs) = subscribers.get(&topic) {
            for (_, tx) in txs {
                let resp = Message::RespSubscribe(RespMsgSubscribe {
                    id: 1,
                    status: MsgStatus::Success,
                    topic: topic.clone(),
                    message: message.clone(),
                });
                let _ = tx.send(resp.serialize().unwrap()).await;
            }
            Message::RespPublish(RespMsgPublish {
                id: 1,
                status: MsgStatus::Success,
                topic: topic,
                msg: "Publish successfully".to_string(),
            })
            .serialize()
            .unwrap()
        } else {
            Message::RespPublish(RespMsgPublish {
                id: 1,
                status: MsgStatus::Warning,
                topic: topic,
                msg: "No subscribers".to_string(),
            })
            .serialize()
            .unwrap()
        }
    }

    pub async fn add_consumer(
        &self,
        id: ConnectionId,
        topic: String,
        tx: Sender<Vec<u8>>,
    ) -> Vec<u8> {
        // 检查是否已添加消费者（读锁）
        {
            let conn_cons = self.connections.read().await;
            if conn_cons
                .get(&id)
                .map_or(false, |(_, s)| s.contains(&topic))
            {
                return Message::RespConsumerTopic(RespMsgConsumerTopic {
                    id: 1,
                    status: MsgStatus::Failure,
                    topic: topic,
                    msg: "Already added consumer".to_string(),
                })
                .serialize()
                .unwrap();
            }
        }
        {
            let mut topics = self.consumers.write().await;
            let mut conns = self.connections.write().await;

            // 添加到消费者列表
            topics
                .entry(topic.clone())
                .or_default()
                .push((id, tx.clone()));

            // 添加到连接集合
            conns.entry(id).or_default().1.insert(topic.clone());
            return Message::RespConsumerTopic(RespMsgConsumerTopic {
                id: 1,
                status: MsgStatus::Success,
                topic: topic,
                msg: "Added consumer successfully".to_string(),
            })
            .serialize()
            .unwrap();
        }
    }

    pub async fn remove_consumer(&self, id: ConnectionId, topic: String) -> Vec<u8> {
        {
            let conn_cons = self.connections.read().await;
            if !conn_cons
                .get(&id)
                .map_or(false, |(_, s)| s.contains(&topic))
            {
                return Message::RespUnconsumerTopic(RespMsgUnconsumerTopic {
                    id: 1,
                    status: MsgStatus::Failure,
                    topic: topic,
                    msg: "Not added consumer".to_string(),
                })
                .serialize()
                .unwrap();
            }
        }

        // 获取写锁进行修改
        {
            let mut topics = self.consumers.write().await;
            let mut conns = self.connections.write().await;

            // 从连接集合中移除
            if let Some(conn_cons) = conns.get_mut(&id) {
                conn_cons.1.remove(&topic);
            }

            // 从消费者列表中移除
            if let Some(consumers) = topics.get_mut(&topic) {
                consumers.retain(|(cid, _)| *cid != id);

                // 如果该列表没有消费者了，移除条目
                if consumers.is_empty() {
                    topics.remove(&topic);
                }
            }
            Message::RespUnconsumerTopic(RespMsgUnconsumerTopic {
                id: 1,
                status: MsgStatus::Success,
                topic: topic,
                msg: "Removed consumer successfully".to_string(),
            })
            .serialize()
            .unwrap()
        }
    }

    pub async fn produce_normal(&self, topic: String, message: Vec<u8>) -> Vec<u8> {
        {
            let mid = self.get_message_id().await;
            let mut messages = self.messages.write().await;
            messages.push(MessageBox {
                id: mid,
                status: MessageStatus::New,
                timestamp: 0,
                message: Message::RespConsume(RespMsgConsume {
                    id: mid,
                    topic: topic.clone(),
                    message: message.clone(),
                }),
            });
        }
        self.task_notifier.send(()).await.unwrap();
        Message::RespProduceNormal(RespMsgProduceNormal {
            id: 1,
            status: MsgStatus::Success,
            topic: topic,
            msg: "Produce normal message successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }

    pub async fn produce_ordered(&self, topic: String, message: Vec<u8>) -> Vec<u8> {
        {
            let mid = self.get_message_id().await;
            let mut messages = self.messages.write().await;
            messages.push(MessageBox {
                id: mid,
                status: MessageStatus::New,
                timestamp: 0,
                message: Message::RespConsume(RespMsgConsume {
                    id: mid,
                    topic: topic.clone(),
                    message: message.clone(),
                }),
            });
        }
        self.task_notifier.send(()).await.unwrap();
        Message::RespProduceNormal(RespMsgProduceNormal {
            id: 1,
            status: MsgStatus::Success,
            topic: topic,
            msg: "Produce ordered message successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }

    pub async fn produce_delay(&self, topic: String, message: Vec<u8>, delay: u64) -> Vec<u8> {
        {
            let mid = self.get_message_id().await;
            let mut messages = self.messages.write().await;
            messages.push(MessageBox {
                id: mid,
                status: MessageStatus::New,
                timestamp: utils::get_unix_timestamp() + delay,
                message: Message::RespConsume(RespMsgConsume {
                    id: mid,
                    topic: topic.clone(),
                    message: message.clone(),
                }),
            });
        }
        self.task_notifier.send(()).await.unwrap();
        Message::RespProduceDelay(RespMsgProduceDelay {
            id: 1,
            status: MsgStatus::Success,
            topic: topic,
            msg: "Produce delay message successfully".to_string(),
            delay: delay,
        })
        .serialize()
        .unwrap()
    }

    pub async fn consume(&self, message: Message) {
        let consumers = self.consumers.read().await;
        let topic = match &message {
            Message::RespConsume(msg) => msg.topic.clone(),
            _ => {
                return;
            }
        };
        if let Some(txs) = consumers.get(&topic) {
            let mut last_consumers_index = self.last_consumers_index.write().await;
            let mut current_consumers_index = *last_consumers_index + 1;
            if current_consumers_index >= txs.len() {
                *last_consumers_index = 0;
                current_consumers_index = 0;
            } else {
                *last_consumers_index = current_consumers_index;
            }
            let _ = txs[current_consumers_index]
                .1
                .send(message.serialize().unwrap())
                .await;
        }
    }

    pub async fn ack_message(&self, id: u64) -> Vec<u8> {
        let mut messages = self.messages.write().await;
        if let Some(message) = messages.iter_mut().find(|m| m.id == id) {
            message.status = MessageStatus::Acked;
        }
        Message::RespConsumeAck(RespMsgConsumeAck {
            id: 1,
            status: MsgStatus::Success,
            msg: "Ack message successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }

    pub async fn reconsume_message(&self, id: u64) -> Vec<u8> {
        let mut messages = self.messages.write().await;
        if let Some(message) = messages.iter_mut().find(|m| m.id == id) {
            message.status = MessageStatus::Reconsume;
        }
        Message::RespConsumeAck(RespMsgConsumeAck {
            id: 1,
            status: MsgStatus::Success,
            msg: "Reconsume message successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }
}
