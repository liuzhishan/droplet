use anyhow::Result;
use clap::Parser;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    Ok(())
}
