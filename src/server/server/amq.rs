use std::{collections::HashMap, env, ffi::OsString, path::Path, sync::Arc};

use bincode::config::standard;
use tokio::{
    fs,
    net::TcpListener,
    sync::{oneshot, RwLock},
};

#[cfg(unix)]
use tokio::net::UnixListener;

use amq::message::{MessageBox, MessageStatus};

use crate::server::{
    config,
    handler::handler,
    state::{interval, State},
};

pub async fn start(
    shutdown_receiver: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = config::Config::new().unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel(32);
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
        stop(sstop, shutdown_receiver).await;
    });

    interval(state.clone(), rx).await;

    #[cfg(unix)]
    let path = config.get_unix_path();

    if path.is_empty() {
        let addr = config.get_address();
        let listener = TcpListener::bind(&addr).await?;
        println!("Server running on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let s = state.clone();
            let (reader, writer) = socket.into_split();
            tokio::spawn(async move {
                handler(reader, writer, s).await;
            });
        }
    } else {
        #[cfg(unix)]
        {
            if Path::new(&path).exists() {
                std::fs::remove_file(&path)?;
            }
            let listener = UnixListener::bind(&path)?;
            println!("Unix socket server listening on {}", path);

            loop {
                let (socket, _) = listener.accept().await?;
                let s = state.clone();
                let (reader, writer) = socket.into_split();
                tokio::spawn(async move {
                    handler(reader, writer, s).await;
                });
            }
        }
    };
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

async fn stop(state: State, shutdown_receiver: oneshot::Receiver<()>) {
    let _ = shutdown_receiver.await;

    let home_dir = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE")) // Windows 兼容
        .unwrap_or(OsString::from("./"));
    let cache_dir = Path::new(&home_dir).join(".ahriknow/ahrimq");
    fs::create_dir_all(&cache_dir)
        .await
        .expect("Failed to create cache directory");

    let cache_file = cache_dir.join("cache.akv");
    let messages = state.messages.read().await.clone();
    let config = standard().with_variable_int_encoding().with_little_endian();
    let encoded = bincode::encode_to_vec(&messages, config).unwrap();
    fs::write(cache_file, encoded)
        .await
        .expect("Failed to write cache file");

    std::process::exit(0);
}
