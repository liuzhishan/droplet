use anyhow::Result;
use log::info;

use droplet_meta_server::tool::get_meta_server_default_client;

use droplet_core::{
    droplet::{HeartbeatRequest, HeartbeatResponse, NodeStatus},
    tool::setup_log,
};

#[tokio::test]
async fn test_meta_server_heartbeat() -> Result<()> {
    setup_log();

    let mut meta_client = get_meta_server_default_client().await?;

    let request = HeartbeatRequest {
        node_id: "test_node".to_string(),
        status: NodeStatus::Healthy.into(),
    };

    let response = meta_client.heartbeat(request).await?;

    info!("response: {:?}", response);

    Ok(())
}
