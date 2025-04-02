use std::{collections::HashMap, sync::Arc};

use tokio::{net::TcpListener, sync::RwLock};

use amq::{message::MessageStatus, utils};

use crate::server::{config, handler::handler, state::State};

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::Config::new().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let state = State {
        next_connection_id: Arc::new(RwLock::new(0)),
        connections: Arc::new(RwLock::new(HashMap::new())),
        subscribers: Arc::new(RwLock::new(HashMap::new())),
        consumers: Arc::new(RwLock::new(HashMap::new())),
        messages: Arc::new(RwLock::new(Vec::new())),
        task_notifier: tx,
        next_message_id: Arc::new(RwLock::new(0)),
    };
    let st = state.clone();
    tokio::spawn(async move {
        loop {
            let timestamp = utils::get_unix_timestamp();
            let mut next_timestamp = timestamp;
            let mut wati_to_send = Vec::new();
            let mut wati_to_ack = Vec::new();
            {
                let mut messages = st.messages.write().await;
                for message in messages.iter_mut() {
                    // 处理状态为 0 的延时消息
                    match message.status {
                        MessageStatus::New => {
                            if message.timestamp <= timestamp {
                                message.status = MessageStatus::Pending(0, timestamp);
                                wati_to_send.push(message.clone());
                            } else {
                                if message.timestamp < next_timestamp || next_timestamp == timestamp
                                {
                                    next_timestamp = message.timestamp;
                                }
                            }
                        }
                        MessageStatus::Pending(times, at) => {
                            if times < 10 || at + 1000 < timestamp {
                                message.status = MessageStatus::Pending(times + 1, timestamp);
                            } else {
                                message.status = MessageStatus::Dead;
                            }
                        }
                        MessageStatus::Dead => {}
                        MessageStatus::Acked => {
                            // 已经被确认过了
                            wati_to_ack.push(message.id);
                        }
                    }
                }
                for message in wati_to_send {
                    st.consume(message.message).await;
                }
                for id in wati_to_ack {
                    messages.retain(|m| m.id != id);
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(next_timestamp - timestamp + 1)) => {
                    // 等到下个消息到期时间
                }
                _ = rx.recv() => {
                    // 有新的延时消息
                    continue;
                }
            }
        }
    });

    let addr = config.get_address();
    let listener = TcpListener::bind(&addr).await?;
    println!("Server running on {}", addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let s = state.clone();
        tokio::spawn(async move {
            handler(socket, s).await;
        });
    }
}
