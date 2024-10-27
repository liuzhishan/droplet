use anyhow::{bail, Result};
use droplet_core::droplet::droplet_client::DropletClient;
use droplet_core::tool::MESSAGE_LIMIT;
use gethostname::gethostname;
use local_ip_address::local_ip;

use log::{error, info};

use droplet_core::droplet::meta_client::MetaClient;
use droplet_core::droplet::RegisterNodeRequest;
use droplet_core::droplet::RegisterNodeResponse;
use droplet_core::error_bail;
use droplet_meta_server::tool::get_meta_server_default_client;

pub const DROPPLET_SERVER_PORT: i32 = 50052;

pub async fn register_node_to_meta_server() -> Result<()> {
    let hostname = gethostname()
        .into_string()
        .map_err(|_| anyhow::anyhow!("Failed to get hostname"))?;

    let local_ip = local_ip().map_err(|_| anyhow::anyhow!("Failed to get local IP"))?;

    let req = RegisterNodeRequest {
        node_name: hostname.clone(),
        node_ip: local_ip.to_string(),
        node_port: DROPPLET_SERVER_PORT as u32,
    };

    let mut meta_client = get_meta_server_default_client().await?;
    match meta_client.register_node(req).await {
        Ok(res) => {
            let resp = res.into_inner();

            if resp.success {
                info!(
                    "Registered node to meta server successfully, node_id: {}, node_name: {}, node_ip: {}, node_port: {}",
                    resp.node_id, hostname, local_ip, DROPPLET_SERVER_PORT
                );
                Ok(())
            } else {
                error_bail!(
                    "Failed to register node to meta server: {:?}",
                    resp.error_message
                );
            }
        }
        Err(e) => {
            error_bail!("Failed to register node to meta server: {:?}", e);
        }
    }
}

pub async fn get_droplet_default_client() -> Result<DropletClient<tonic::transport::Channel>> {
    let my_local_ip = local_ip()?;

    match DropletClient::connect(format!("http://{}:{}", my_local_ip, DROPPLET_SERVER_PORT)).await {
        Ok(client) => Ok(client
            .max_decoding_message_size(MESSAGE_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT)),
        Err(err) => Err(err.into()),
    }
}

pub async fn get_droplet_client(
    droplet_server_endpoint: &String,
) -> Result<DropletClient<tonic::transport::Channel>> {
    match DropletClient::connect(format!("http://{}", droplet_server_endpoint.clone())).await {
        Ok(client) => Ok(client
            .max_decoding_message_size(MESSAGE_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT)),
        Err(err) => Err(err.into()),
    }
}
