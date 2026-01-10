use std::{collections::HashMap, path::Path, sync::Arc};

use tokio::{
    net::TcpListener,
    sync::{oneshot, RwLock},
};

#[cfg(unix)]
use tokio::net::UnixListener;

use amq::{
    persistence::PersistenceEngine,
    Config,
};

use crate::server::{
    handler::handler,
    state::{interval, State},
};

pub async fn start(
    shutdown_receiver: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new().unwrap();

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
        persistence: None,
    };

    let persistence: Option<PersistenceEngine> = match PersistenceEngine::new(amq::PersistenceConfig::default()).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("Failed to initialize persistence engine: {}, using in-memory mode", e);
            None
        }
    };

    if let Some(ref persistence) = persistence {
        match persistence.recover().await {
            Ok((messages, next_message_id, next_connection_id)) => {
                let mut state_messages = state.messages.write().await;
                *state_messages = messages;
                *state.next_message_id.write().await = next_message_id;
                *state.next_connection_id.write().await = next_connection_id;
            }
            Err(e) => {
                eprintln!("Failed to recover from persistence: {}", e);
            }
        }
    }

    state.persistence = persistence;

    let sstop = state.clone();
    tokio::spawn(async move {
        stop(sstop, shutdown_receiver).await;
    });

    interval(state.clone(), rx).await;

    #[cfg(unix)]
    {
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
        };
    }
    #[cfg(not(unix))]
    {
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
    }
}

async fn stop(state: State, shutdown_receiver: oneshot::Receiver<()>) {
    let _ = shutdown_receiver.await;

    if let Some(ref persistence) = state.persistence {
        let messages = state.messages.read().await;
        let next_message_id = *state.next_message_id.read().await;
        let next_connection_id = *state.next_connection_id.read().await;

        if let Err(e) = persistence
            .create_snapshot(&messages, next_message_id, next_connection_id)
            .await
        {
            eprintln!("Failed to create snapshot: {}", e);
        }

        let _ = persistence.shutdown().await;
    }

    std::process::exit(0);
}
