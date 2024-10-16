use anyhow::{anyhow, Result};
use droplet_core::tool::init_log;
use log::error;
use log::info;

use local_ip_address::local_ip;
use std::net::TcpListener;
use tonic::transport::Server;

use droplet_core::tool::wait_for_signal;
use droplet_core::droplet::meta_server::MetaServer;

use droplet_core::tool::MESSAGE_LIMIT;
use droplet_meta_server::tool::META_SERVER_PORT;

use droplet_meta_server::request_handler::MetaServerImpl;

async fn serve() {
    let my_local_ip = local_ip().unwrap();

    let addr = format!("{}:{}", my_local_ip, META_SERVER_PORT)
        .parse()
        .unwrap();
    let meta_server = MetaServerImpl::new();

    let signal = wait_for_signal();

    info!(
        "Starting gRPC Server..., ip: {}, port: {}",
        my_local_ip, META_SERVER_PORT
    );

    Server::builder()
        .add_service(
            MetaServer::new(meta_server)
                .max_decoding_message_size(MESSAGE_LIMIT)
                .max_encoding_message_size(MESSAGE_LIMIT),
        )
        .serve_with_shutdown(addr, signal)
        .await
        .unwrap();
}

fn main() -> Result<()> {
    init_log();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve());

    Ok(())
}
