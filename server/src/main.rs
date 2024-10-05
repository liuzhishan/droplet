use anyhow::Result;
use clap::Parser;
use tokio_graceful_shutdown::Toplevel;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config/server.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    env_logger::init();

    // Run the server

    Ok(())
}
