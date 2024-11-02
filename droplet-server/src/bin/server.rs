use anyhow::Result;
use log::info;

use local_ip_address::local_ip;
use std::sync::Arc;
use tonic::transport::Server;

use droplet_core::db::db::DB;
use droplet_core::droplet::droplet_server::DropletServer;
use droplet_core::tool::init_log;
use droplet_core::tool::wait_for_signal;
use droplet_core::tool::MESSAGE_LIMIT;
use droplet_server::request_handler::DropletServerImpl;
use droplet_server::tool::register_node_to_meta_server;
use droplet_server::tool::DROPPLET_SERVER_PORT;

async fn serve() -> Result<()> {
    let my_local_ip = local_ip().unwrap();

    let addr = format!("{}:{}", my_local_ip, DROPPLET_SERVER_PORT)
        .parse()
        .unwrap();

    let db = Arc::new(DB::new()?);

    let droplet_server = DropletServerImpl::new(db);

    let signal = wait_for_signal();

    info!(
        "Starting gRPC Server..., ip: {}, port: {}",
        my_local_ip, DROPPLET_SERVER_PORT
    );

    register_node_to_meta_server().await?;

    Server::builder()
        .add_service(
            DropletServer::new(droplet_server)
                .max_decoding_message_size(MESSAGE_LIMIT)
                .max_encoding_message_size(MESSAGE_LIMIT),
        )
        .serve_with_shutdown(addr, signal)
        .await
        .unwrap();

    Ok(())
}

fn main() -> Result<()> {
    init_log();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve())?;

    Ok(())
}
