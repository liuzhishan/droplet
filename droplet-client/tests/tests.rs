use anyhow::Result;
use log::info;

use droplet_client::client::Client;
use droplet_core::droplet::ColumnInfo;
use droplet_core::{droplet::DataType, tool::setup_log};

#[tokio::test]
async fn test_heartbeat() -> Result<()> {
    setup_log();

    let mut client = Client::get_default_client().await?;
    client.heartbeat(0).await?;

    Ok(())
}
