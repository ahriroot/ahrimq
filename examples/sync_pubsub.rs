use std::{
    error::Error,
    sync::{mpsc::TryRecvError, Arc, Mutex},
    thread::sleep,
    time::Duration,
};

use amq::{error::AmqError, Config, SyncClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let state = Arc::new(Mutex::new(0));

    loop {
        let config = Config::new().unwrap();

        let mut client = SyncClient::new(config, state.clone());

        let rx = match client.connect() {
            Ok(rx) => rx,
            Err(e) => {
                println!("Connection error: {}", e);
                sleep(Duration::from_secs(1));
                continue;
            }
        };

        client.subscribe("topic", |state, msg| {
            let mut state = state.lock().unwrap();
            *state += 1;
            println!("Received message: {} {:?}", state, msg);
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
