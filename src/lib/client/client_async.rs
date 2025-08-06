use std::{collections::HashMap, error::Error, future::Future, pin::Pin, sync::Arc};

use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{
        tcp::{OwnedReadHalf as TcpOHR, OwnedWriteHalf as TcpOWH},
        TcpStream,
    },
    sync::{
        oneshot::{channel, Receiver},
        RwLock,
    },
    task::{spawn, JoinHandle},
};

#[cfg(unix)]
use tokio::net::{
    unix::{OwnedReadHalf as UnixOHR, OwnedWriteHalf as UnixOWH},
    UnixStream,
};

use crate::{
    error::AmqError,
    message::{
        Message, MsgStatus, ReqMsgAuthorizer, ReqMsgConsumeAck, ReqMsgConsumeAckMulti,
        ReqMsgConsumerTopic, ReqMsgPublish, ReqMsgSubscriber, ReqMsgUnconsumerTopic,
        ReqMsgUnsubscriber, RespMsgConsume, RespMsgSubscribe,
    },
    Config,
};

type OnRecvFn = Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// # Async Client
///
/// ```rust
/// loop {
///     let config = Config::new().unwrap();
///
///     let mut client = AsyncClient::new(config);
///
///     let rx = match client.connect().await {
///         Ok(rx) => rx,
///         Err(e) => {
///             println!("Connection error: {}", e);
///             sleep(Duration::from_secs(1)).await;
///             continue;
///         }
/// };
///
///     client
///         .subscribe("topic", |msg| async move {
///             println!("Received message: {:?}", msg);
///         })
///         .await?;
///
///     let should_exit = select! {
///         // Receive signal when connection closed
///         result = rx => {
///             client.shutdown().await;
///
///             match result {
///                 // Server closed connection
///                 Ok(AmqError::TcpServerClosed) => {
///                     true
///                 }
///                 // Other error
///                 Ok(e) => {
///                     println!("{}", e);
///                     false
///                 }
///                 Err(e) => {
///                     println!("Receive signal error: {:?}", e);
///                     false
///                 }
///             }
///         }
///
///         // Send message every 1s
///         _ = async {
///             loop {
///                 sleep(Duration::from_secs(1)).await;
///                 let _ = client.publish("topic", "Hello, world!".as_bytes().to_vec()).await;
///             }
///         } => {
///             false // Exit loop on error, and reconnect
///         }
///     };
///
///     if should_exit {
///         break;
///     }
///
///     println!("Reconnecting...");
/// }
/// ```
pub struct Client {
    config: Config,
    stream: Option<Owh>,
    recv_task: Option<JoinHandle<()>>,
    on_subscribes: Arc<RwLock<HashMap<String, OnRecvFn>>>,
    on_consumes: Arc<RwLock<HashMap<String, OnRecvFn>>>,
}

enum Owh {
    Tcp(TcpOWH),
    #[cfg(unix)]
    Unix(UnixOWH),
}

impl Client {
    /// # Create a new async client. (tokio)
    pub fn new(config: Config) -> Self {
        Self {
            config,
            stream: None,
            recv_task: None,
            on_subscribes: Arc::new(RwLock::new(HashMap::new())),
            on_consumes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// # Subscribe a topic.
    ///
    /// ```rust
    /// client.subscribe("topic", |msg| async move {
    ///     println!("Received message: {:?}", msg);
    /// }).await.unwrap();
    /// ```
    pub async fn subscribe<F, Fut>(&mut self, topic: &str, f: F) -> Result<(), AmqError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_subscribes
            .write()
            .await
            .insert(topic.to_string(), Arc::new(move |data| Box::pin(f(data))));
        let message = Message::ReqSubscribeTopic(ReqMsgSubscriber {
            topic: topic.to_string(),
        });
        self.send(message).await?;
        Ok(())
    }

    /// # Unsubscribe a topic.
    ///
    /// ```rust
    /// client.unsubscribe("topic").await.unwrap();
    /// ```
    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), AmqError> {
        let message = Message::ReqUnsubscribeTopic(ReqMsgUnsubscriber {
            topic: topic.to_string(),
        });
        self.send(message).await?;
        self.on_subscribes.write().await.remove(topic);
        Ok(())
    }

    /// # Publish a message.
    ///
    /// ```rust
    /// client.publish("topic", "Hello, world!".as_bytes().to_vec()).await.unwrap();
    /// ```
    pub async fn publish(&mut self, topic: &str, content: Vec<u8>) -> Result<(), AmqError> {
        let message = Message::ReqPublish(ReqMsgPublish {
            topic: topic.to_string(),
            message: content,
        });
        self.send(message).await?;
        Ok(())
    }

    /// # Add a consume callback for a topic.
    ///
    /// ```rust
    /// client.consume("topic", |msg| async move {
    ///     println!("Received message: {:?}", msg);
    /// }).await.unwrap();
    /// ```
    pub async fn consume<F, Fut>(&mut self, topic: &str, f: F) -> Result<(), AmqError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_consumes
            .write()
            .await
            .insert(topic.to_string(), Arc::new(move |data| Box::pin(f(data))));
        let message = Message::ReqConsumerTopic(ReqMsgConsumerTopic {
            topic: topic.to_string(),
        });
        self.send(message).await?;
        Ok(())
    }

    /// # Remove a consume callback for a topic.
    /// ```rust
    /// client.unconsume("topic").await.unwrap();
    /// ```
    pub async fn unconsume(&mut self, topic: &str) -> Result<(), AmqError> {
        let message = Message::ReqUnconsumerTopic(ReqMsgUnconsumerTopic {
            topic: topic.to_string(),
        });
        self.send(message).await?;
        self.on_consumes.write().await.remove(topic);
        Ok(())
    }

    /// # Acknowledge a message.
    ///
    /// ```rust
    /// client.ack(message_id).await.unwrap();
    /// ```
    pub async fn ack(&mut self, message_id: u64) -> Result<(), AmqError> {
        let message = Message::ReqConsumeAck(ReqMsgConsumeAck { id: message_id });
        self.send(message).await?;
        Ok(())
    }

    /// # Acknowledge multiple messages.
    ///
    /// ```rust
    /// client.ack_multi(vec![message_id1, message_id2]).await.unwrap();
    /// ```
    pub async fn ack_multi(&mut self, message_ids: Vec<u64>) -> Result<(), AmqError> {
        let message = Message::ReqConsumeAckMulti(ReqMsgConsumeAckMulti { ids: message_ids });
        self.send(message).await?;
        Ok(())
    }

    /// # Connect to the server and start the receive task.
    pub async fn connect(&mut self) -> Result<Receiver<AmqError>, Box<dyn Error>> {
        if self.config.path.is_empty() {
            let addr = self.config.get_address();
            let stream = TcpStream::connect(addr).await?;
            let (reader, writer) = stream.into_split();
            self.tcp_conn(reader, writer).await
        } else {
            #[cfg(unix)]
            {
                let stream = UnixStream::connect(&self.config.path).await?;
                let (reader, writer) = stream.into_split();
                self.unix_conn(reader, writer).await
            }
            #[cfg(not(unix))]
            {
                Err(Box::new(AmqError::UnsupportedPlatform))
            }
        }
    }

    async fn tcp_conn(
        &mut self,
        mut reader: TcpOHR,
        mut writer: TcpOWH,
    ) -> Result<Receiver<AmqError>, Box<dyn Error>> {
        let msg = Message::ReqAuthorizer(ReqMsgAuthorizer {
            access_key: self.config.access_key.clone(),
            access_secret: self.config.access_secret.clone(),
        });
        let message = &serde_json::to_vec(&msg)?;
        writer.write_u32(message.len() as u32).await?;
        writer.write_all(&message).await?;

        // Read mesage header (4 bytes)
        let len = reader.read_u32().await?;
        // Read message body (len bytes)
        let mut buf = vec![0; len as usize];
        let _ = reader.read(&mut buf).await?;
        match Message::deserialize(&buf)? {
            Message::RespAuthorizer(resp) => {
                if resp.status != MsgStatus::Success {
                    return Err(Box::new(AmqError::AuthorizationError(resp.msg)));
                }
            }
            _ => {
                return Err(Box::new(AmqError::AuthorizationError(
                    "Invalid response".to_string(),
                )));
            }
        }

        self.stream = Some(Owh::Tcp(writer));

        let (tx, rx) = channel::<AmqError>();

        let on_subscribes = self.on_subscribes.clone();
        let on_consumes = self.on_consumes.clone();
        let recv_task = spawn(async move {
            loop {
                // Read mesage header (4 bytes)
                let len = match reader.read_u32().await {
                    Ok(l) => l as usize,
                    Err(e) => {
                        tx.send(AmqError::TcpReceiveError(e.to_string())).unwrap();
                        break;
                    }
                };
                // Read message body (len bytes)
                let mut buf = vec![0; len];
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        tx.send(AmqError::TcpServerClosed).unwrap();
                        break;
                    }
                    Ok(_) => {
                        let msg = match Message::deserialize(&buf) {
                            Ok(m) => m,
                            Err(_) => {
                                continue;
                            }
                        };
                        match &msg {
                            Message::RespSubscribe(RespMsgSubscribe { topic, message, .. }) => {
                                if let Some(cb) = on_subscribes.read().await.get(topic) {
                                    cb(message.clone()).await;
                                }
                            }
                            Message::RespConsume(RespMsgConsume { topic, message, .. }) => {
                                if let Some(cb) = on_consumes.read().await.get(topic) {
                                    cb(message.clone()).await;
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tx.send(AmqError::TcpServerError(e.to_string())).unwrap();
                        break;
                    }
                }
            }
        });

        self.recv_task = Some(recv_task);

        Ok(rx)
    }

    #[cfg(unix)]
    async fn unix_conn(
        &mut self,
        mut reader: UnixOHR,
        mut writer: UnixOWH,
    ) -> Result<Receiver<AmqError>, Box<dyn Error>> {
        let msg = Message::ReqAuthorizer(ReqMsgAuthorizer {
            access_key: self.config.access_key.clone(),
            access_secret: self.config.access_secret.clone(),
        });
        let message = &serde_json::to_vec(&msg)?;
        writer.write_u32(message.len() as u32).await?;
        writer.write_all(&message).await?;

        // Read mesage header (4 bytes)
        let len = reader.read_u32().await?;
        // Read message body (len bytes)
        let mut buf = vec![0; len as usize];
        let _ = reader.read(&mut buf).await?;
        match Message::deserialize(&buf)? {
            Message::RespAuthorizer(resp) => {
                if resp.status != MsgStatus::Success {
                    return Err(Box::new(AmqError::AuthorizationError(resp.msg)));
                }
            }
            _ => {
                return Err(Box::new(AmqError::AuthorizationError(
                    "Invalid response".to_string(),
                )));
            }
        }

        self.stream = Some(Owh::Unix(writer));

        let (tx, rx) = channel::<AmqError>();

        let on_subscribes = self.on_subscribes.clone();
        let on_consumes = self.on_consumes.clone();
        let recv_task = spawn(async move {
            loop {
                // Read mesage header (4 bytes)
                let len = match reader.read_u32().await {
                    Ok(l) => l as usize,
                    Err(e) => {
                        tx.send(AmqError::TcpReceiveError(e.to_string())).unwrap();
                        break;
                    }
                };
                // Read message body (len bytes)
                let mut buf = vec![0; len];
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        tx.send(AmqError::TcpServerClosed).unwrap();
                        break;
                    }
                    Ok(_) => {
                        let msg = match Message::deserialize(&buf) {
                            Ok(m) => m,
                            Err(_) => {
                                continue;
                            }
                        };
                        match &msg {
                            Message::RespSubscribe(RespMsgSubscribe { topic, message, .. }) => {
                                if let Some(cb) = on_subscribes.read().await.get(topic) {
                                    cb(message.clone()).await;
                                }
                            }
                            Message::RespConsume(RespMsgConsume { topic, message, .. }) => {
                                if let Some(cb) = on_consumes.read().await.get(topic) {
                                    cb(message.clone()).await;
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tx.send(AmqError::TcpServerError(e.to_string())).unwrap();
                        break;
                    }
                }
            }
        });

        self.recv_task = Some(recv_task);

        Ok(rx)
    }

    /// # Shutdown the client.
    pub async fn shutdown(&mut self) {
        if let Some(task) = self.recv_task.take() {
            task.abort();
        }

        self.stream = None;
    }

    async fn send(&mut self, msg: Message) -> Result<(), AmqError> {
        match &mut self.stream {
            Some(Owh::Tcp(writer)) => {
                let message = &serde_json::to_vec(&msg)
                    .map_err(|e| AmqError::TcpSendDataError(e.to_string()))?;
                let _ = writer.write_u32(message.len() as u32).await;
                writer
                    .write_all(&message)
                    .await
                    .map_err(|e| AmqError::TcpReceiveError(e.to_string()))?;
            }
            #[cfg(unix)]
            Some(Owh::Unix(writer)) => {
                let message = &serde_json::to_vec(&msg)
                    .map_err(|e| AmqError::TcpSendDataError(e.to_string()))?;
                let _ = writer.write_u32(message.len() as u32).await;
                writer
                    .write_all(&message)
                    .await
                    .map_err(|e| AmqError::TcpReceiveError(e.to_string()))?;
            }
            None => {
                return Err(AmqError::TcpSendError("not connected".to_string()));
            }
        }
        Ok(())
    }
}
