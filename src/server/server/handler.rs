use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use amq::message::{Message, MsgStatus, RespMsgAuthorizer, RespMsgPing};

use crate::server::state::State;

pub async fn handler<R, W>(mut reader: R, mut writer: W, state: State)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let id = state.get_connection_id().await;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(10);

    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let _ = writer.write_u32(message.len() as u32).await;
            let _ = writer.write_all(&message).await;
        }
    });

    loop {
        // 读取消息头 (4字节)
        let len = match reader.read_u32().await {
            Ok(l) => l as usize,
            Err(_) => {
                state.cleanup_connection(id).await;
                return;
            }
        };
        // 读取消息体 (len)
        let mut buf = vec![0; len];
        match reader.read(&mut buf).await {
            Ok(0) => {
                state.cleanup_connection(id).await;
                return;
            }
            Ok(n) => {
                let received: Message = Message::deserialize(&buf[..n]).unwrap();
                match received {
                    Message::ReqPing(_) => {
                        let _ = tx
                            .send(Message::RespPing(RespMsgPing {}).serialize().unwrap())
                            .await;
                    }
                    Message::ReqAuthorizer(req) => {
                        if req.access_key == state.config.access_key
                            && req.access_secret == state.config.access_secret
                        {
                            let _ = tx
                                .send(
                                    Message::RespAuthorizer(RespMsgAuthorizer {
                                        id: 0,
                                        status: MsgStatus::Success,
                                        msg: "authorized".to_string(),
                                    })
                                    .serialize()
                                    .unwrap(),
                                )
                                .await;
                            break;
                        } else {
                            let _ = tx
                                .send(
                                    Message::RespAuthorizer(RespMsgAuthorizer {
                                        id: 0,
                                        status: MsgStatus::Failure,
                                        msg: "unauthorized".to_string(),
                                    })
                                    .serialize()
                                    .unwrap(),
                                )
                                .await;
                        }
                    }
                    _ => {
                        let _ = tx
                            .send(
                                Message::RespAuthorizer(RespMsgAuthorizer {
                                    id: 0,
                                    status: MsgStatus::Failure,
                                    msg: "unauthorized".to_string(),
                                })
                                .serialize()
                                .unwrap(),
                            )
                            .await;
                    }
                }
            }
            Err(_) => {
                state.cleanup_connection(id).await;
                return;
            }
        }
    }

    loop {
        // 读取消息头 (4字节)
        let len = match reader.read_u32().await {
            Ok(l) => l as usize,
            Err(_) => {
                state.cleanup_connection(id).await;
                break;
            }
        };
        // 读取消息体 (len)
        let mut buf = vec![0; len];
        match reader.read(&mut buf).await {
            Ok(0) => {
                state.cleanup_connection(id).await;
                break;
            }
            Ok(n) => {
                let received: Message = Message::deserialize(&buf[..n]).unwrap();
                match received {
                    Message::ReqPing(_) => {
                        let _ = tx
                            .send(Message::RespPing(RespMsgPing {}).serialize().unwrap())
                            .await;
                    }
                    // 订阅主题
                    Message::ReqSubscribeTopic(req) => {
                        let _ = tx
                            .send(state.add_subscriber(id, req.topic, tx.clone()).await)
                            .await;
                    }
                    // 取消订阅主题
                    Message::RespUnsubscribeTopic(req) => {
                        let _ = tx.send(state.remove_subscriber(id, req.topic).await).await;
                    }
                    // 发布消息
                    Message::ReqPublish(req) => {
                        let _ = tx.send(state.publish(req.topic, req.message).await).await;
                    }
                    // 添加消费者
                    Message::ReqConsumerTopic(req) => {
                        let _ = tx
                            .send(state.add_consumer(id, req.topic, tx.clone()).await)
                            .await;
                    }
                    // 取消消费者
                    Message::ReqUnconsumerTopic(req) => {
                        let _ = tx.send(state.remove_consumer(id, req.topic).await).await;
                    }
                    // 拉取消息
                    Message::ReqPullMessage(req) => {
                        let _ = tx
                            .send(state.pull_message(req.topic, req.total).await)
                            .await;
                    }
                    // 生产普通消息
                    Message::ReqProduceNormal(req) => {
                        let _ = tx
                            .send(state.produce_normal(req.topic, req.message).await)
                            .await;
                    }
                    // 生产有序消息
                    Message::ReqProduceOrdered(req) => {
                        let _ = tx
                            .send(state.produce_ordered(req.topic, req.message).await)
                            .await;
                    }
                    // 生产延迟消息
                    Message::ReqProduceDelay(req) => {
                        let _ = tx
                            .send(state.produce_delay(req.topic, req.message, req.delay).await)
                            .await;
                    }
                    // 确认消息被消费
                    Message::ReqConsumeAck(req) => {
                        let _ = tx.send(state.ack_message(req.id).await).await;
                    }
                    Message::ReqConsumeAckMulti(req) => {
                        let _ = tx.send(state.ack_message_multi(req.ids).await).await;
                    }
                    Message::ReqReconsumeLater(req) => {
                        let _ = tx.send(state.reconsume_message(req.id).await).await;
                    }
                    Message::ReqReconsumeDelay(req) => {
                        let _ = tx.send(state.reconsume_delay_message(req.id, req.delay).await).await;
                    }
                    _ => {
                        let _ = tx
                            .send(
                                Message::Error("Unknown message type".to_string())
                                    .serialize()
                                    .unwrap(),
                            )
                            .await;
                    }
                }
            }
            Err(_) => {
                state.cleanup_connection(id).await;
                break;
            }
        }
    }
}
