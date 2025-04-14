mod server;

use tokio::{
    signal::unix::{signal, SignalKind},
    sync::oneshot,
};

use server::amq::start;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();

    // 阻塞等待任意信号
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        let mut sigterm = signal(SignalKind::terminate()).unwrap();

        tokio::select! {
            _ = sigint.recv() => {
                println!("Received SIGINT (Ctrl+C)");
            },
            _ = sigterm.recv() => {
                println!("Received SIGTERM (docker stop or systemctl stop)");
            },
        }

        let _ = shutdown_sender.send(());
    });

    start(shutdown_receiver).await?;
    Ok(())
}
