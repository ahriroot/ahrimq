use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::{
    mpsc::{self, Sender},
    RwLock,
};

use amq::{
    message::{
        Message, MessageBox, MessageHistory, MessageStatus, MsgStatus, RespMsgConsume,
        RespMsgConsumeAck, RespMsgConsumerTopic, RespMsgProduceDelay, RespMsgProduceNormal,
        RespMsgPublish, RespMsgSubscribe, RespMsgSubscriber, RespMsgUnconsumerTopic,
        RespMsgUnsubscriber, RespPullMessage, RespPullMsg, RespReconsumeDelay, RespReconsumeLater,
    },
    persistence::PersistenceEngine,
    utils, Config,
};

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
    pub persistence: Option<PersistenceEngine>,
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
            persistence: self.persistence.as_ref().map(|p| p.clone()),
        }
    }

    pub async fn get_all_topics(&self) -> Vec<String> {
        let topics = self.consumers.read().await;
        topics.keys().cloned().collect()
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

    pub async fn pull_message(&self, topic: String, total: u32) -> Vec<u8> {
        let mut messages = self.messages.write().await;
        let mut result = Vec::new();
        let mut count = 0;
        let timestamp = utils::get_unix_timestamp();
        let mut status_updates = Vec::new();

        for message in messages.iter_mut() {
            if message.timestamp <= timestamp {
                if message.status == MessageStatus::New
                    || message.status == MessageStatus::Reconsume
                {
                    message.status = MessageStatus::Pending(0, timestamp, timestamp);
                    status_updates.push((message.id, message.status.clone()));
                    if let Message::RespConsume(msg) = &message.message {
                        if msg.topic == topic {
                            result.push(RespPullMsg {
                                id: message.id,
                                message: msg.message.clone(),
                            });
                            count += 1;
                            if count >= total {
                                break;
                            }
                        }
                    }
                } else if let MessageStatus::Pending(times, _, first_send) = message.status {
                    let interval = self.config.retry_interval * (times as u64 + 1);
                    if timestamp - first_send >= interval {
                        if times >= self.config.retry_times {
                            message.status = MessageStatus::Dead;
                            status_updates.push((message.id, message.status.clone()));
                        } else {
                            message.status =
                                MessageStatus::Pending(times + 1, timestamp, first_send);
                            status_updates.push((message.id, message.status.clone()));
                            if let Message::RespConsume(msg) = &message.message {
                                if msg.topic == topic {
                                    result.push(RespPullMsg {
                                        id: message.id,
                                        message: msg.message.clone(),
                                    });
                                    count += 1;
                                    if count >= total {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if let Some(ref persistence) = self.persistence {
            for (id, status) in status_updates {
                let _ = persistence.update_message_status(id, status).await;
            }
        }

        Message::RespPullMessage(RespPullMessage {
            topic: topic,
            messages: result,
        })
        .serialize()
        .unwrap()
    }

    pub async fn produce_normal(&self, topic: String, message: Vec<u8>) -> Vec<u8> {
        let msg_box = {
            let mid = self.get_message_id().await;
            let mut messages = self.messages.write().await;
            let msg_box = MessageBox {
                id: mid,
                status: MessageStatus::New,
                timestamp: 0,
                message: Message::RespConsume(RespMsgConsume {
                    id: mid,
                    topic: topic.clone(),
                    message: message.clone(),
                }),
                history: vec![MessageHistory {
                    status: MessageStatus::New,
                    timestamp: utils::get_unix_timestamp(),
                }],
            };
            messages.push(msg_box.clone());
            msg_box
        };

        if let Some(ref persistence) = self.persistence {
            let _ = persistence.append_message(&msg_box).await;
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
        let msg_box = {
            let mid = self.get_message_id().await;
            let mut messages = self.messages.write().await;
            let msg_box = MessageBox {
                id: mid,
                status: MessageStatus::New,
                timestamp: 0,
                message: Message::RespConsume(RespMsgConsume {
                    id: mid,
                    topic: topic.clone(),
                    message: message.clone(),
                }),
                history: vec![MessageHistory {
                    status: MessageStatus::New,
                    timestamp: utils::get_unix_timestamp(),
                }],
            };
            messages.push(msg_box.clone());
            msg_box
        };

        if let Some(ref persistence) = self.persistence {
            let _ = persistence.append_message(&msg_box).await;
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
        let msg_box = {
            let mid = self.get_message_id().await;
            let mut messages = self.messages.write().await;
            let msg_box = MessageBox {
                id: mid,
                status: MessageStatus::New,
                timestamp: utils::get_unix_timestamp() + delay,
                message: Message::RespConsume(RespMsgConsume {
                    id: mid,
                    topic: topic.clone(),
                    message: message.clone(),
                }),
                history: vec![MessageHistory {
                    status: MessageStatus::New,
                    timestamp: utils::get_unix_timestamp(),
                }],
            };
            messages.push(msg_box.clone());
            msg_box
        };

        if let Some(ref persistence) = self.persistence {
            let _ = persistence.append_message(&msg_box).await;
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

            if let Some(ref persistence) = self.persistence {
                let _ = persistence
                    .update_message_status(id, MessageStatus::Acked)
                    .await;
            }
        }
        Message::RespConsumeAck(RespMsgConsumeAck {
            id: 1,
            status: MsgStatus::Success,
            msg: "Ack message successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }

    pub async fn ack_message_multi(&self, ids: Vec<u64>) -> Vec<u8> {
        let mut messages = self.messages.write().await;
        for id in &ids {
            if let Some(message) = messages.iter_mut().find(|m| m.id == *id) {
                message.status = MessageStatus::Acked;
            }
        }

        if let Some(ref persistence) = self.persistence {
            for id in ids {
                let _ = persistence
                    .update_message_status(id, MessageStatus::Acked)
                    .await;
            }
        }

        Message::RespConsumeAck(RespMsgConsumeAck {
            id: 1,
            status: MsgStatus::Success,
            msg: "Ack messages successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }

    pub async fn reconsume_message(&self, id: u64) -> Vec<u8> {
        let mut messages = self.messages.write().await;
        if let Some(message) = messages.iter_mut().find(|m| m.id == id) {
            message.status = MessageStatus::Reconsume;

            if let Some(ref persistence) = self.persistence {
                let _ = persistence
                    .update_message_status(id, MessageStatus::Reconsume)
                    .await;
            }
        }

        Message::RespReconsumeLater(RespReconsumeLater {
            id: 1,
            status: MsgStatus::Success,
            msg: "Reconsume message successfully".to_string(),
        })
        .serialize()
        .unwrap()
    }

    pub async fn reconsume_delay_message(&self, id: u64, delay: u64) -> Vec<u8> {
        let mut messages = self.messages.write().await;
        if let Some(message) = messages.iter_mut().find(|m| m.id == id) {
            message.status = MessageStatus::Reconsume;
            message.timestamp = utils::get_unix_timestamp() + delay;

            if let Some(ref persistence) = self.persistence {
                let _ = persistence
                    .update_message_status(id, MessageStatus::Reconsume)
                    .await;

                let _ = persistence
                    .update_message_timestamp(id, message.timestamp)
                    .await;
            }
        }

        Message::RespReconsumeDelay(RespReconsumeDelay {
            id: 1,
            status: MsgStatus::Success,
            msg: "Reconsume delay message successfully".to_string(),
            delay: delay,
        })
        .serialize()
        .unwrap()
    }

    async fn cleanup_messages(&self, ids_to_ack: Vec<u64>) {
        if ids_to_ack.is_empty() {
            return;
        }

        let ids_set: HashSet<u64> = ids_to_ack.iter().cloned().collect();

        let mut messages = self.messages.write().await;
        messages.retain(|m| !ids_set.contains(&m.id));

        if let Some(ref persistence) = self.persistence {
            for id in ids_to_ack {
                let _ = persistence.remove_message(id).await;
            }
        }
    }
}

pub async fn interval(state: State, mut rx: mpsc::Receiver<()>) {
    tokio::spawn(async move {
        loop {
            let all_topics = state.get_all_topics().await;
            let timestamp = utils::get_unix_timestamp();
            let mut next_timestamp = timestamp;
            let mut wait_to_send = Vec::new();
            let mut wait_to_ack = Vec::new();
            let mut status_updates = Vec::new();
            {
                let mut messages = state.messages.write().await;
                if messages.len() == 0 {
                    next_timestamp = timestamp + 10;
                }
                for message in messages.iter_mut() {
                    match &message.message {
                        Message::RespConsume(msg) => {
                            if !all_topics.contains(&msg.topic) {
                                continue;
                            }
                        }
                        _ => {
                            continue;
                        }
                    }
                    match message.status {
                        MessageStatus::New | MessageStatus::Reconsume => {
                            if message.timestamp <= timestamp {
                                message.status = MessageStatus::Pending(0, timestamp, timestamp);
                                wait_to_send.push(message.clone());
                                status_updates.push((message.id, message.status.clone()));
                            } else {
                                if message.timestamp < next_timestamp {
                                    next_timestamp = message.timestamp;
                                }
                            }
                        }
                        MessageStatus::Pending(times, _, first_send) => {
                            let interval = state.config.retry_interval * (times as u64 + 1);
                            if timestamp - first_send >= interval {
                                if times >= state.config.retry_times {
                                    message.status = MessageStatus::Dead;
                                    status_updates.push((message.id, message.status.clone()));
                                } else {
                                    message.status =
                                        MessageStatus::Pending(times + 1, timestamp, first_send);
                                    wait_to_send.push(message.clone());
                                    status_updates.push((message.id, message.status.clone()));
                                }
                            }
                        }
                        MessageStatus::Dead => {}
                        MessageStatus::Acked => {
                            wait_to_ack.push(message.id);
                        }
                    }
                }
            }

            for message in wait_to_send {
                state.consume(message.message).await;
            }

            state.cleanup_messages(wait_to_ack).await;

            if let Some(ref persistence) = state.persistence {
                for (id, status) in status_updates {
                    let _ = persistence.update_message_status(id, status).await;
                }
            }

            let sleep_duration = if next_timestamp > timestamp {
                next_timestamp - timestamp + 1
            } else {
                1
            };

            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(sleep_duration)) => {}
                _ = rx.recv() => {
                    continue;
                }
            }
        }
    });
}
