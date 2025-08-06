use std::{error::Error, time::Duration};

use amq::{error::AmqError, AsyncClient, Config};
use tokio::{select, time::sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    loop {
        let config = Config::new().unwrap();

        let mut client = AsyncClient::new(config);

        let rx = match client.connect().await {
            Ok(rx) => rx,
            Err(e) => {
                println!("Connection error: {}", e);
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        client
            .subscribe("topic", |msg| async move {
                println!("Received message: {:?}", msg);
            })
            .await?;

        let should_exit = select! {
            // Receive signal when connection closed
            result = rx => {
                client.shutdown().await;

                match result {
                    // Server closed connection
                    Ok(AmqError::TcpServerClosed) => {
                        true
                    }
                    // Other error
                    Ok(e) => {
                        println!("{}", e);
                        false
                    }
                    Err(e) => {
                        println!("Receive signal error: {:?}", e);
                        false
                    }
                }
            }

            // Send message every 1s
            _ = async {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let _ = client.publish("topic", "Hello, world!".as_bytes().to_vec()).await;
                }
            } => {
                false // Exit loop on error, and reconnect
            }
        };

        if should_exit {
            break;
        }

        println!("Reconnecting...");
    }

    Ok(())
}
