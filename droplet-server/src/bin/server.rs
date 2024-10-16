use anyhow::Result;
use clap::Parser;
use tokio_graceful_shutdown::Toplevel;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();

    Ok(())
}
