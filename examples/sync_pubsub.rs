use std::{error::Error, sync::mpsc::TryRecvError, thread::sleep, time::Duration};

use amq::{error::AmqError, Config, SyncClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    loop {
        let config = Config::new().unwrap();

        let mut client = SyncClient::new(config);

        let rx = match client.connect() {
            Ok(rx) => rx,
            Err(e) => {
                println!("Connection error: {}", e);
                sleep(Duration::from_secs(1));
                continue;
            }
        };

        client.subscribe("topic", |msg| {
            println!("Received message: {:?}", msg);
        })?;

        let should_exit;
        loop {
            let result = rx.try_recv();
            match result {
                Ok(AmqError::TcpServerClosed) => {
                    should_exit = true;
                    break;
                }
                Ok(e) => {
                    println!("{}", e);
                    should_exit = false;
                    break;
                }
                Err(TryRecvError::Empty) => {
                    sleep(Duration::from_secs(1));
                    let _ = client.publish("topic", "Hello, world!".as_bytes().to_vec());
                }
                Err(e) => {
                    println!("Receive signal error: {:?}", e);
                    should_exit = false;
                    break;
                }
            }
        }

        if should_exit {
            break;
        }

        println!("Reconnecting...");
    }

    Ok(())
}
