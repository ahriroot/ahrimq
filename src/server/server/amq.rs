use std::{collections::HashMap, env, ffi::OsString, path::Path, sync::Arc};

use bincode::config::standard;
use tokio::{fs, net::TcpListener, signal, sync::RwLock};

use amq::{
    message::{MessageBox, MessageStatus},
    utils,
};

use crate::server::{config, handler::handler, state::State};

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::Config::new().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let mut state = State {
        config: config.clone(),
        next_connection_id: Arc::new(RwLock::new(0)),
        connections: Arc::new(RwLock::new(HashMap::new())),
        subscribers: Arc::new(RwLock::new(HashMap::new())),
        consumers: Arc::new(RwLock::new(HashMap::new())),
        last_consumers_index: Arc::new(RwLock::new(0)),
        messages: Arc::new(RwLock::new(Vec::new())),
        task_notifier: tx,
        next_message_id: Arc::new(RwLock::new(0)),
    };

    read_cache_file(&mut state).await;

    let sstop = state.clone();
    tokio::spawn(async move {
        stop(sstop).await;
    });

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
                        MessageStatus::New | MessageStatus::Reconsume => {
                            if message.timestamp <= timestamp {
                                message.status = MessageStatus::Pending(0, timestamp, timestamp);
                                wati_to_send.push(message.clone());
                            } else {
                                if message.timestamp < next_timestamp || next_timestamp == timestamp
                                {
                                    next_timestamp = message.timestamp;
                                }
                            }
                        }
                        MessageStatus::Pending(times, _, first_send) => {
                            if timestamp - first_send > state.config.retry_interval {
                                if times >= state.config.retry_times {
                                    message.status = MessageStatus::Dead;
                                } else {
                                    // 重新发送
                                    message.status =
                                        MessageStatus::Pending(times + 1, timestamp, first_send);
                                    wati_to_send.push(message.clone());
                                }
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
                    // 有新的消息
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

async fn read_cache_file(state: &mut State) {
    let config = standard().with_variable_int_encoding().with_little_endian();

    let home_dir = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE")) // Windows 兼容
        .unwrap_or(OsString::from("./"));
    let cache_dir = Path::new(&home_dir).join(".ahriknow/ahrimq");
    let cache_file = cache_dir.join("cache.akv");
    if cache_file.exists() {
        let encoded = fs::read(cache_file).await.unwrap();
        let msgs: Vec<MessageBox> = bincode::decode_from_slice(&encoded, config).unwrap().0;

        let mut messages = state.messages.write().await;
        *messages = msgs
            .iter()
            .filter(|m| m.status != MessageStatus::Acked)
            .cloned()
            .collect();
    }
}

async fn stop(state: State) {
    let config = standard().with_variable_int_encoding().with_little_endian();

    // 1. 监听终止信号
    signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");

    // 2. 保存数据到文件
    let home_dir = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE")) // Windows 兼容
        .unwrap_or(OsString::from("./"));
    let cache_dir = Path::new(&home_dir).join(".ahriknow/ahrimq");
    fs::create_dir_all(&cache_dir)
        .await
        .expect("Failed to create cache directory");

    let cache_file = cache_dir.join("cache.akv");
    let messages = state.messages.read().await.clone();
    let encoded = bincode::encode_to_vec(&messages, config).unwrap();
    fs::write(cache_file, encoded)
        .await
        .expect("Failed to write cache file");

    std::process::exit(0);
}
