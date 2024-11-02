use anyhow::Result;

use local_ip_address::local_ip;

use droplet_core::tool::MESSAGE_LIMIT;

use droplet_core::droplet::meta_client::MetaClient;

pub const META_SERVER_PORT: i32 = 50051;

/// For test.
pub async fn get_meta_server_default_client() -> Result<MetaClient<tonic::transport::Channel>> {
    let my_local_ip = local_ip()?;

    match MetaClient::connect(format!("http://{}:{}", my_local_ip, META_SERVER_PORT)).await {
        Ok(client) => Ok(client
            .max_decoding_message_size(MESSAGE_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT)),
        Err(err) => Err(err.into()),
    }
}

pub async fn get_meta_server_client(
    meta_server_endpoint: &String,
) -> Result<MetaClient<tonic::transport::Channel>> {
    match MetaClient::connect(format!("http://{}", meta_server_endpoint)).await {
        Ok(client) => Ok(client
            .max_decoding_message_size(MESSAGE_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT)),
        Err(err) => Err(err.into()),
    }
}
