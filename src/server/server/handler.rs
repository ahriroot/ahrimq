use tokio::{
    io::{self, AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
};

use amq::message::{Message, RespMsgPing};

use crate::server::state::State;

pub async fn handler(socket: TcpStream, state: State) {
    let id = state.get_connection_id().await;

    let (mut reader, mut writer) = io::split(socket);
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
