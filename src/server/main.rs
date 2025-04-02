mod server;

use server::amq::start;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start().await?;
    Ok(())
}
