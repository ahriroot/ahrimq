mod server;

#[cfg(unix)]
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::oneshot,
};

#[cfg(windows)]
use tokio::{
    signal::windows::{ctrl_break, ctrl_c},
    sync::oneshot,
};

use server::amq::start;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();

    // 阻塞等待任意信号
    #[cfg(unix)]
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

    #[cfg(windows)]
    tokio::spawn(async move {
        let mut ctrl_c_signal = ctrl_c().expect("Failed to listen for Ctrl+C");
        let mut ctrl_break_signal = ctrl_break().expect("Failed to listen for Ctrl+Break");

        tokio::select! {
            _ = ctrl_c_signal.recv() => {
                println!("Received Ctrl+C (similar to SIGINT)");
            },
            _ = ctrl_break_signal.recv() => {
                println!("Received Ctrl+Break (similar to SIGTERM)");
            },
        }

        let _ = shutdown_sender.send(());
    });

    start(shutdown_receiver).await?;
    Ok(())
}
