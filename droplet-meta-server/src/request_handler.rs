use std::borrow::BorrowMut;
use std::time::Duration;

use anyhow::bail;
use log::{error, info};
use std::cell::RefCell;
use sync_unsafe_cell::SyncUnsafeCell;
use tracing::instrument::WithSubscriber;

use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use prost_types::Any;
use tokio_graceful_shutdown::{SubsystemBuilder, Toplevel};
use tonic::{transport::Server, Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};

use droplet_core::droplet::{meta_server::Meta, HeartbeatRequest, HeartbeatResponse};
use droplet_core::grpc_util::{send_bad_request_error, send_error_message};
pub struct MetaServerImpl {}

impl MetaServerImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl Meta for MetaServerImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!("heartbeat");

        let response = HeartbeatResponse::default();

        Ok(Response::new(response))
    }
}
