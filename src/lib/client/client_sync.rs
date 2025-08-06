use std::{
    collections::HashMap,
    error::Error,
    io::{Read, Write},
    net::TcpStream,
    sync::{
        mpsc::{channel, Receiver},
        Arc, RwLock,
    },
    thread::{spawn, JoinHandle},
};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::{
    error::AmqError,
    message::{
        Message, MsgStatus, ReqMsgAuthorizer, ReqMsgConsumeAck, ReqMsgConsumeAckMulti,
        ReqMsgConsumerTopic, ReqMsgPublish, ReqMsgSubscriber, ReqMsgUnconsumerTopic,
        ReqMsgUnsubscriber, RespMsgConsume, RespMsgSubscribe,
    },
    Config,
};

type OnRecvFn = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

/// # Sync Client
///
/// ```rust
/// loop {
///     let config = Config::new().unwrap();
///
///     let mut client = SyncClient::new(config);
///
///     let rx = match client.connect() {
///         Ok(rx) => rx,
///         Err(e) => {
///             println!("Connection error: {}", e);
///             sleep(Duration::from_secs(1));
///             continue;
///         }
///     };
///
///     client.subscribe("topic", |msg| {
///         println!("Received message: {:?}", msg);
///     })?;
///
///     let should_exit;
///     loop {
///         let result = rx.try_recv();
///         match result {
///             Ok(AmqError::TcpServerClosed) => {
///                 should_exit = true;
///                 break;
///             }
///             Ok(e) => {
///                 println!("{}", e);
///                 should_exit = false;
///                 break;
///             }
///             Err(TryRecvError::Empty) => {
///                 sleep(Duration::from_secs(1));
///                 let _ = client.publish("topic", "Hello, world!".as_bytes().to_vec());
///             }
///             Err(e) => {
///                 println!("Receive signal error: {:?}", e);
///                 should_exit = false;
///                 break;
///             }
///         }
///     }
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
    stream: Option<Stream>,
    recv_thread: Option<JoinHandle<()>>,
    on_subscribes: Arc<RwLock<HashMap<String, OnRecvFn>>>,
    on_consumes: Arc<RwLock<HashMap<String, OnRecvFn>>>,
}

enum Stream {
    TcpStream(TcpStream),
    #[cfg(unix)]
    UnixStream(UnixStream),
}

impl Client {
    /// # Create a new client.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            stream: None,
            recv_thread: None,
            on_subscribes: Arc::new(RwLock::new(HashMap::new())),
            on_consumes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// # Subscribe a topic.
    ///
    /// ```rust
    /// client.subscribe("topic", |msg| {
    ///     println!("Received message: {:?}", msg);
    /// }).unwrap();
    /// ```
    pub fn subscribe<F>(&mut self, topic: &str, f: F) -> Result<(), AmqError>
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        self.on_subscribes
            .write()
            .unwrap()
            .insert(topic.to_string(), Arc::new(f));
        let message = Message::ReqSubscribeTopic(ReqMsgSubscriber {
            topic: topic.to_string(),
        });
        self.send(message)?;
        Ok(())
    }

    /// # Unsubscribe a topic.
    /// ```rust
    /// client.unsubscribe("topic").unwrap();
    /// ```
    pub fn unsubscribe(&mut self, topic: &str) -> Result<(), AmqError> {
        let message = Message::ReqUnsubscribeTopic(ReqMsgUnsubscriber {
            topic: topic.to_string(),
        });
        self.send(message)?;
        self.on_subscribes.write().unwrap().remove(topic);
        Ok(())
    }

    /// # Publish a message.
    ///
    /// ```rust
    /// client.publish("topic", "Hello, world!".as_bytes().to_vec()).unwrap();
    /// ```
    pub fn publish(&mut self, topic: &str, content: Vec<u8>) -> Result<(), AmqError> {
        let message = Message::ReqPublish(ReqMsgPublish {
            topic: topic.to_string(),
            message: content,
        });
        self.send(message)?;
        Ok(())
    }

    /// # Add a consume callback for a topic.
    ///
    /// ```rust
    /// client.consume("topic", |msg| {
    ///     println!("Received message: {:?}", msg);
    /// }).unwrap();
    /// ```
    pub fn consume<F>(&mut self, topic: &str, f: F) -> Result<(), AmqError>
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        self.on_consumes
            .write()
            .unwrap()
            .insert(topic.to_string(), Arc::new(f));
        let message = Message::ReqConsumerTopic(ReqMsgConsumerTopic {
            topic: topic.to_string(),
        });
        self.send(message)?;
        Ok(())
    }

    /// # Remove a consume callback for a topic.
    /// ```rust
    /// client.unconsume("topic").unwrap();
    /// ```
    pub fn unconsume(&mut self, topic: &str) -> Result<(), AmqError> {
        let message = Message::ReqUnconsumerTopic(ReqMsgUnconsumerTopic {
            topic: topic.to_string(),
        });
        self.send(message)?;
        self.on_consumes.write().unwrap().remove(topic);
        Ok(())
    }

    /// # Acknowledge a message.
    ///
    /// ```rust
    /// client.ack(message_id).unwrap();
    /// ```
    pub fn ack(&mut self, message_id: u64) -> Result<(), AmqError> {
        let message = Message::ReqConsumeAck(ReqMsgConsumeAck { id: message_id });
        self.send(message)?;
        Ok(())
    }

    /// # Acknowledge multiple messages.
    ///
    /// ```rust
    /// client.ack_multi(vec![message_id1, message_id2]).unwrap();
    /// ```
    pub fn ack_multi(&mut self, message_ids: Vec<u64>) -> Result<(), AmqError> {
        let message = Message::ReqConsumeAckMulti(ReqMsgConsumeAckMulti { ids: message_ids });
        self.send(message)?;
        Ok(())
    }

    /// # Connect to the server and start the receive task.
    pub fn connect(&mut self) -> Result<Receiver<AmqError>, Box<dyn Error>> {
        if self.config.path.is_empty() {
            let addr = self.config.get_address();
            let stream = TcpStream::connect(addr)?;
            self.tcp_conn(stream)
        } else {
            #[cfg(unix)]
            {
                let stream = UnixStream::connect(&self.config.path)?;
                self.unix_conn(stream)
            }
            #[cfg(not(unix))]
            {
                Err(Box::new(AmqError::UnsupportedPlatform))
            }
        }
    }

    pub fn tcp_conn(
        &mut self,
        mut stream: TcpStream,
    ) -> Result<Receiver<AmqError>, Box<dyn Error>> {
        stream.set_nodelay(true)?;
        stream.set_nonblocking(false)?;

        let msg = Message::ReqAuthorizer(ReqMsgAuthorizer {
            access_key: self.config.access_key.clone(),
            access_secret: self.config.access_secret.clone(),
        });
        let message = &serde_json::to_vec(&msg)?;
        let len_data = (message.len() as u32).to_be_bytes();
        stream.write_all(&len_data)?;
        stream.write_all(&message)?;

        // Read mesage header (4 bytes)
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        // Read message body (len bytes)
        let mut buf = vec![0u8; len];
        let _ = stream.read(&mut buf)?;
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

        let reader_stream = stream.try_clone()?;
        self.stream = Some(Stream::TcpStream(stream));

        let (tx, rx) = channel::<AmqError>();

        let on_subscribes = self.on_subscribes.clone();
        let on_consumes = self.on_consumes.clone();
        let recv_thread = spawn(move || {
            let mut reader = reader_stream;
            loop {
                // Read mesage header (4 bytes)
                let mut len_buf = [0u8; 4];
                let len = match reader.read_exact(&mut len_buf) {
                    Ok(_) => u32::from_be_bytes(len_buf) as usize,
                    Err(e) => {
                        tx.send(AmqError::TcpReceiveError(e.to_string())).unwrap();
                        break;
                    }
                };
                // Read message body (len bytes)
                let mut buf = vec![0u8; len];
                match reader.read(&mut buf) {
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
                                if let Some(cb) = on_subscribes.read().unwrap().get(topic) {
                                    cb(message.clone());
                                }
                            }
                            Message::RespConsume(RespMsgConsume { topic, message, .. }) => {
                                if let Some(cb) = on_consumes.read().unwrap().get(topic) {
                                    cb(message.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tx.send(AmqError::TcpReceiveError(e.to_string())).unwrap();
                        break;
                    }
                }
            }
        });

        self.recv_thread = Some(recv_thread);

        Ok(rx)
    }

    #[cfg(unix)]
    pub fn unix_conn(
        &mut self,
        mut stream: UnixStream,
    ) -> Result<Receiver<AmqError>, Box<dyn Error>> {
        stream.set_nonblocking(false)?;

        let msg = Message::ReqAuthorizer(ReqMsgAuthorizer {
            access_key: self.config.access_key.clone(),
            access_secret: self.config.access_secret.clone(),
        });
        let message = &serde_json::to_vec(&msg)?;
        let len_data = (message.len() as u32).to_be_bytes();
        stream.write_all(&len_data)?;
        stream.write_all(&message)?;

        // Read mesage header (4 bytes)
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        // Read message body (len bytes)
        let mut buf = vec![0u8; len];
        let _ = stream.read(&mut buf)?;
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

        let reader_stream = stream.try_clone()?;
        self.stream = Some(Stream::UnixStream(stream));

        let (tx, rx) = channel::<AmqError>();

        let on_subscribes = self.on_subscribes.clone();
        let on_consumes = self.on_consumes.clone();
        let recv_thread = spawn(move || {
            let mut reader = reader_stream;
            loop {
                // Read mesage header (4 bytes)
                let mut len_buf = [0u8; 4];
                let len = match reader.read_exact(&mut len_buf) {
                    Ok(_) => u32::from_be_bytes(len_buf) as usize,
                    Err(e) => {
                        tx.send(AmqError::TcpReceiveError(e.to_string())).unwrap();
                        break;
                    }
                };
                // Read message body (len bytes)
                let mut buf = vec![0u8; len];
                match reader.read(&mut buf) {
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
                                if let Some(cb) = on_subscribes.read().unwrap().get(topic) {
                                    cb(message.clone());
                                }
                            }
                            Message::RespConsume(RespMsgConsume { topic, message, .. }) => {
                                if let Some(cb) = on_consumes.read().unwrap().get(topic) {
                                    cb(message.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tx.send(AmqError::TcpReceiveError(e.to_string())).unwrap();
                        break;
                    }
                }
            }
        });

        self.recv_thread = Some(recv_thread);

        Ok(rx)
    }

    /// # Shutdown the client.
    pub fn shutdown(&mut self) {
        match &mut self.stream {
            Some(Stream::TcpStream(stream)) => {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            #[cfg(unix)]
            Some(Stream::UnixStream(stream)) => {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            None => {}
        }

        self.stream = None;
    }

    fn send(&mut self, msg: Message) -> Result<(), AmqError> {
        match &mut self.stream {
            Some(Stream::TcpStream(stream)) => {
                let data = serde_json::to_vec(&msg).unwrap();
                let len = (data.len() as u32).to_be_bytes();
                stream
                    .write_all(&len)
                    .map_err(|e| AmqError::TcpReceiveError(e.to_string()))?;
                stream
                    .write_all(&data)
                    .map_err(|e| AmqError::TcpReceiveError(e.to_string()))?;
                Ok(())
            }
            #[cfg(unix)]
            Some(Stream::UnixStream(stream)) => {
                let data = serde_json::to_vec(&msg).unwrap();
                let len = (data.len() as u32).to_be_bytes();
                stream
                    .write_all(&len)
                    .map_err(|e| AmqError::TcpReceiveError(e.to_string()))?;
                stream
                    .write_all(&data)
                    .map_err(|e| AmqError::TcpReceiveError(e.to_string()))?;
                Ok(())
            }
            None => Err(AmqError::TcpSendError("not connected".to_string())),
        }
    }
}
